from __future__ import annotations

import asyncio
import json
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime

from aiolimiter import AsyncLimiter
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException as RequestsError

from ..client.auth import DbdAuth
from ..client.crypto import decrypt_envelope
from ..client.session import WARMUP_PAGE_URL, create_session
from ..core.exceptions import DbdAuthError, DbdForbiddenError, DbdTokenExpiredError
from ..core.settings import settings


class DeepHttpError(RuntimeError):
    pass


class DeepNotAvailable(DeepHttpError):
    def __init__(self, status: int = 404):
        super().__init__(f"HTTP {status}")
        self.status = int(status)


class DeepPermanentHttpError(DeepHttpError):
    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.status = int(status)


class DeepTransientHttpError(DeepHttpError):
    def __init__(self, message: str, *, status: int | None = None):
        super().__init__(message)
        self.status = status


class DeepDecodeError(DeepHttpError):
    def __init__(self, message: str, *, status: int = 200):
        super().__init__(message)
        self.status = int(status)


@dataclass(frozen=True)
class DeepResponse:
    data: object
    status: int
    requested_at: datetime
    received_at: datetime


def _retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 1.0


class FixedRateDbdClient:
    """DBD HTTP client with one fixed aggregate admission rate.

    429/503 responses can impose a temporary shared cooldown, but they never
    mutate the configured requests-per-second ceiling. This is intentionally
    independent of the legacy Redis/adaptive EndpointRateController.
    """

    def __init__(
        self,
        *,
        concurrency: int,
        rate_per_second: float,
        max_retries: int,
        timeout: float | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        if rate_per_second <= 0 or not math.isfinite(rate_per_second):
            raise ValueError("rate_per_second must be finite and positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        self.concurrency = int(concurrency)
        self.rate_per_second = float(rate_per_second)
        self.max_retries = int(max_retries)
        self.timeout = float(timeout or settings.request_timeout)

        self._limiter = AsyncLimiter(1, 1 / self.rate_per_second)
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._cooldown_lock = asyncio.Lock()
        self._cooldown_until = 0.0
        self._auth = DbdAuth()
        self._auth_lock = asyncio.Lock()
        self._auth_session = None
        self._session: AsyncSession | None = None
        self._token: str | None = None

        self.requests_made = 0
        self.retries_made = 0
        self.rate_limit_hits = 0

    async def __aenter__(self) -> "FixedRateDbdClient":
        self._auth_session = create_session()
        self._session = AsyncSession(
            impersonate="chrome",
            max_clients=self.concurrency,
        )
        try:
            await self._ensure_token()
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_args) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._auth_session is not None:
            self._auth_session.close()
            self._auth_session = None

    async def _ensure_token(self, *, force: bool = False) -> str:
        if self._auth_session is None:
            raise RuntimeError("HTTP client is not open")
        async with self._auth_lock:
            if force:
                self._auth.invalidate()
                self._token = None
            if self._token is None or time.time() >= self._auth._expires_at - 15:
                self._token = await asyncio.to_thread(
                    self._auth.get_token,
                    self._auth_session,
                )
                if self._session is not None:
                    self._session.cookies.update(self._auth_session.cookies)
            return self._token

    def _headers(self, token: str, referer: str) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Origin": settings.dbd_base_url,
            "Referer": referer,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

    async def _set_cooldown(self, seconds: float) -> None:
        seconds = max(0.0, float(seconds))
        async with self._cooldown_lock:
            self._cooldown_until = max(
                self._cooldown_until,
                time.monotonic() + seconds,
            )

    async def _admit(self) -> None:
        while True:
            async with self._cooldown_lock:
                delay = self._cooldown_until - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
                continue

            await self._limiter.acquire()

            # A different request can receive 429/503 while this coroutine is
            # queued inside the limiter. Re-check after admission so already
            # waiting workers cannot leak requests through the shared cooldown.
            async with self._cooldown_lock:
                delay = self._cooldown_until - time.monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(delay)
            # Re-acquire the limiter after cooldown. Otherwise all waiters that
            # slept to the same deadline could burst at once.

    @staticmethod
    def _backoff(attempt: int, server_seconds: float = 0.0) -> float:
        exponential = min(30.0, 2.0 ** max(0, attempt - 1))
        jitter = random.uniform(0.0, min(1.0, exponential / 4.0))
        return max(float(server_seconds), exponential + jitter)

    def _decode(self, response, path: str, token: str) -> object:
        data = json.loads(response.content, parse_float=Decimal)
        if response.headers.get("x-encrypted", "").lower() == "true":
            data = decrypt_envelope(data, path, token)
        if response.headers.get("x-wrapped", "").lower() == "true":
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError("Malformed wrapped response")
            data = data["data"]
        return data

    async def fetch(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, object] | None = None,
        referer: str | None = None,
    ) -> DeepResponse:
        if self._session is None:
            raise RuntimeError("Use async with FixedRateDbdClient")

        referer = referer or WARMUP_PAGE_URL
        last_error: BaseException | None = None

        for attempt in range(1, self.max_retries + 2):
            if attempt > 1:
                self.retries_made += 1

            await self._admit()
            token = await self._ensure_token()
            requested_at = datetime.now(timezone.utc).replace(tzinfo=None)

            try:
                async with self._semaphore:
                    self.requests_made += 1
                    response = await self._session.request(
                        method,
                        settings.dbd_base_url.rstrip("/") + path,
                        params=params,
                        headers=self._headers(token, referer),
                        timeout=self.timeout,
                    )
            except (RequestsError, TimeoutError) as exc:
                last_error = exc
                if attempt > self.max_retries:
                    raise DeepTransientHttpError(
                        f"{type(exc).__name__} after {attempt} attempt(s)"
                    ) from exc
                await asyncio.sleep(self._backoff(attempt))
                continue

            status = int(response.status_code)

            if status == 401:
                last_error = DbdAuthError("HTTP 401")
                if attempt > self.max_retries:
                    raise last_error
                await self._ensure_token(force=True)
                await asyncio.sleep(self._backoff(attempt))
                continue

            if status == 403:
                raise DbdForbiddenError("HTTP 403")

            if status == 404:
                raise DeepNotAvailable(status)

            if status in (429, 500, 502, 503, 504):
                if status == 429:
                    self.rate_limit_hits += 1
                server_seconds = _retry_after(response.headers.get("retry-after"))
                delay = self._backoff(attempt, server_seconds)
                if status in (429, 503):
                    await self._set_cooldown(delay)
                last_error = DeepTransientHttpError(f"HTTP {status}", status=status)
                if attempt > self.max_retries:
                    raise last_error
                if status not in (429, 503):
                    await asyncio.sleep(delay)
                continue

            if status >= 400:
                raise DeepPermanentHttpError(status)

            try:
                data = self._decode(response, path, token)
            except DbdTokenExpiredError as exc:
                if attempt > self.max_retries:
                    raise DbdAuthError(
                        "DBD token remained expired after response-decode retries"
                    ) from exc
                await self._ensure_token(force=True)
                await asyncio.sleep(self._backoff(attempt))
                continue
            except Exception as exc:
                raise DeepDecodeError(
                    f"HTTP {status} response decode failed: {type(exc).__name__}: {exc}",
                    status=status,
                ) from exc

            return DeepResponse(
                data=data,
                status=status,
                requested_at=requested_at,
                received_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )

        if last_error is not None:
            raise DeepTransientHttpError(str(last_error)) from last_error
        raise DeepTransientHttpError("request retry loop exhausted")
