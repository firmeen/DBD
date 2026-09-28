from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException as RequestsError

from ..core.exceptions import DbdAuthError, DbdForbiddenError, DbdTokenExpiredError
from ..core.settings import settings
from ..core.telemetry import EFFECTIVE_RATE, HTTP_RETRIES, LATENCY, REQUESTS, log
from .async_client import ApiResponse, PermanentHttpError, TransientHttpError, retry_after
from .auth import DbdAuth
from .crypto import decrypt_envelope
from .session import WARMUP_PAGE_URL, create_session


def _curl_code(exc) -> object:
    code = getattr(exc, "code", None)
    if code is not None:
        return code
    return getattr(exc, "curl_code", None)


class SimpleGlobalLimiter:
    """One process-wide paced limiter with temporary 429 slowdown."""

    def __init__(self, rate_per_second: float) -> None:
        rate = float(rate_per_second)
        if rate <= 0:
            raise ValueError("rate_per_second must be positive")
        self.target_rate = rate
        self.effective_rate = rate
        self._next_at = 0.0
        self._pause_until = 0.0
        self._penalty_until = 0.0
        self._lock = asyncio.Lock()
        EFFECTIVE_RATE.set(self.effective_rate)

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                if self._penalty_until and now >= self._penalty_until:
                    self.effective_rate = self.target_rate
                    self._penalty_until = 0.0
                    EFFECTIVE_RATE.set(self.effective_rate)

                delay = max(self._pause_until - now, self._next_at - now, 0.0)
                if delay <= 0:
                    now = time.monotonic()
                    self._next_at = max(self._next_at, now) + (1.0 / self.effective_rate)
                    return
                await asyncio.sleep(delay)

    async def penalize(self, *, retry_after_seconds: float, quiet_seconds: float = 30.0) -> None:
        async with self._lock:
            now = time.monotonic()
            self.effective_rate = max(0.5, self.target_rate * 0.5)
            self._pause_until = max(self._pause_until, now + max(0.0, retry_after_seconds))
            self._penalty_until = max(self._penalty_until, now + max(1.0, quiet_seconds))
            self._next_at = max(self._next_at, self._pause_until)
            EFFECTIVE_RATE.set(self.effective_rate)

    def snapshot(self) -> dict[str, float]:
        now = time.monotonic()
        if self._penalty_until and now >= self._penalty_until:
            effective = self.target_rate
        else:
            effective = self.effective_rate
        return {
            "target_rate": self.target_rate,
            "effective_rate": effective,
            "pause_remaining": max(0.0, self._pause_until - now),
        }


@dataclass
class _BackoffState:
    failures: int = 0
    pause_until: float = 0.0

    @property
    def state(self) -> str:
        return "paused" if self.pause_until > time.monotonic() else "open"


class SimpleAsyncDbdClient:
    """Single-process DBD client without Redis admission or hierarchical circuits."""

    def __init__(
        self,
        *,
        concurrency=None,
        rate_per_second=None,
        max_retries=None,
        endpoint_rates=None,
        redis_url=None,
        redis_key=None,
        redis_client=None,
    ) -> None:
        del endpoint_rates, redis_url, redis_key, redis_client
        self.concurrency = settings.dbd_concurrency if concurrency is None else int(concurrency)
        if self.concurrency < 1:
            raise ValueError("concurrency must be positive")
        configured_rate = (
            settings.dbd_rate_per_second if rate_per_second is None else float(rate_per_second)
        )
        self.limiter = SimpleGlobalLimiter(configured_rate)
        self.max_retries = settings.dbd_http_retries if max_retries is None else int(max_retries)
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._session: AsyncSession | None = None
        self._auth_session = None
        self._auth = DbdAuth()
        self._token: str | None = None
        self._auth_lock = asyncio.Lock()
        self._auth_refresh_task: asyncio.Task | None = None
        self._auth_backoff_until = 0.0
        self._auth_last_error: BaseException | None = None
        self._upstream = _BackoffState()
        self._upstream_lock = asyncio.Lock()
        self.requests_made = 0
        self.retries_made = 0
        self.rate_limit_hits = 0
        self.last_error: str | None = None
        self.last_error_at: float | None = None

    @property
    def effective_rate(self) -> float:
        return float(self.limiter.snapshot()["effective_rate"])

    @property
    def target_rate(self) -> float:
        return float(self.limiter.target_rate)

    @property
    def upstream_state(self) -> str:
        return self._upstream.state

    async def __aenter__(self):
        self._session = AsyncSession(impersonate="chrome", max_clients=self.concurrency)
        self._auth_session = create_session()
        return self

    async def __aexit__(self, *args):
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._auth_session is not None:
            self._auth_session.close()
            self._auth_session = None

    def _token_valid(self) -> bool:
        return bool(self._token) and time.time() < self._auth._expires_at - 15

    async def _refresh_token_once(self) -> str:
        try:
            token = await asyncio.to_thread(self._auth.get_token, self._auth_session)
            self._token = token
            self._auth_backoff_until = 0.0
            self._auth_last_error = None
            if self._session is not None and self._auth_session is not None:
                self._session.cookies.update(self._auth_session.cookies)
            return token
        except BaseException as exc:
            self._auth_last_error = exc
            self._auth_backoff_until = time.monotonic() + max(
                1.0, float(settings.dbd_upstream_defer_seconds)
            )
            raise

    async def _ensure_token(self, *, rejected=None, reset_warmup=False) -> str:
        if rejected is not None and rejected == self._token:
            async with self._auth_lock:
                if rejected == self._token:
                    self._auth.invalidate(reset_warmup=reset_warmup)
                    self._token = None
                    self._auth_refresh_task = None

        if self._token_valid():
            return str(self._token)

        async with self._auth_lock:
            if self._token_valid():
                return str(self._token)

            now = time.monotonic()
            if now < self._auth_backoff_until and self._auth_last_error is not None:
                raise self._auth_last_error

            if self._auth_refresh_task is None or self._auth_refresh_task.done():
                self._auth_refresh_task = asyncio.create_task(self._refresh_token_once())
            task = self._auth_refresh_task

        return await task

    async def _wait_upstream(self) -> None:
        while True:
            delay = self._upstream.pause_until - time.monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    async def _mark_upstream_failure(self) -> None:
        async with self._upstream_lock:
            self._upstream.failures += 1
            if self._upstream.failures >= 3:
                step = min(15.0, 5.0 * (2 ** min(self._upstream.failures - 3, 2)))
                self._upstream.pause_until = max(
                    self._upstream.pause_until,
                    time.monotonic() + step,
                )

    async def _mark_upstream_success(self) -> None:
        async with self._upstream_lock:
            self._upstream.failures = 0
            self._upstream.pause_until = 0.0

    def _remember_error(self, exc: BaseException) -> None:
        self.last_error = f"{type(exc).__name__}: {str(exc) or type(exc).__name__}"
        self.last_error_at = time.time()

    async def fetch(
        self,
        method,
        path,
        *,
        json_body=None,
        params=None,
        referer=WARMUP_PAGE_URL,
        timeout=None,
        rate_key: str | None = None,
    ):
        del rate_key
        if self._session is None:
            raise RuntimeError("Use async with SimpleAsyncDbdClient")

        attempts = self.max_retries + 1
        last_exc: BaseException | None = None

        for attempt_number in range(1, attempts + 1):
            if attempt_number > 1:
                self.retries_made += 1
                HTTP_RETRIES.inc()

            try:
                token = await self._ensure_token()
                await self._wait_upstream()
                await self.limiter.acquire()

                async with self._semaphore:
                    requested = datetime.now(timezone.utc).replace(tzinfo=None)
                    self.requests_made += 1
                    with LATENCY.time():
                        response = await self._session.request(
                            method,
                            settings.dbd_base_url.rstrip("/") + path,
                            json=json_body,
                            params=params,
                            headers=self._headers(token, referer),
                            timeout=timeout or settings.request_timeout,
                        )

                status = int(response.status_code)
                REQUESTS.labels(str(status)).inc()

                if status == 429:
                    self.rate_limit_hits += 1
                    server_wait = retry_after(response.headers.get("retry-after"))
                    await self.limiter.penalize(
                        retry_after_seconds=max(1.0, server_wait),
                        quiet_seconds=30.0,
                    )
                    raise TransientHttpError("HTTP 429", status_code=429)

                if status == 401:
                    await self._mark_upstream_success()
                    await self._ensure_token(rejected=token)
                    raise TransientHttpError("HTTP 401", status_code=401)

                if status == 403:
                    await self._mark_upstream_success()
                    await self._ensure_token(rejected=token, reset_warmup=True)
                    raise DbdForbiddenError("HTTP 403")

                if status in (500, 502, 503, 504):
                    await self._mark_upstream_failure()
                    raise TransientHttpError(f"HTTP {status}", status_code=status)

                await self._mark_upstream_success()

                if status >= 400:
                    raise PermanentHttpError(f"HTTP {status}", status_code=status)

                try:
                    data = self._decode(response, path, token)
                except DbdTokenExpiredError:
                    await self._ensure_token(rejected=token)
                    raise
                except Exception as exc:
                    body = response.content or b""
                    log.warning(
                        "http_decode_error",
                        stage="decode",
                        path=path,
                        status=status,
                        exception_type=type(exc).__name__,
                        body_bytes=len(body),
                        body_sha256=sha256(body).hexdigest(),
                    )
                    raise

                return ApiResponse(
                    data=data,
                    status=status,
                    requested_at=requested,
                    received_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )

            except PermanentHttpError:
                raise
            except (DbdAuthError, DbdForbiddenError, DbdTokenExpiredError) as exc:
                self._remember_error(exc)
                last_exc = exc
            except (RequestsError, TimeoutError) as exc:
                REQUESTS.labels("network_error").inc()
                await self._mark_upstream_failure()
                log.warning(
                    "http_transport_error",
                    stage="request",
                    path=path,
                    exception_type=type(exc).__name__,
                    curl_code=_curl_code(exc),
                )
                self._remember_error(exc)
                last_exc = exc
            except TransientHttpError as exc:
                self._remember_error(exc)
                last_exc = exc

            if attempt_number >= attempts:
                assert last_exc is not None
                raise last_exc

            await asyncio.sleep(min(5.0, (2 ** (attempt_number - 1)) + random.random()))

        raise RuntimeError("request retry loop exited unexpectedly")

    def _headers(self, token, referer):
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

    def _decode(self, response, path, token):
        data = json.loads(response.content, parse_float=Decimal)
        if response.headers.get("x-encrypted", "").lower() == "true":
            data = decrypt_envelope(data, path, token)
        if response.headers.get("x-wrapped", "").lower() == "true":
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError("Malformed wrapped response")
            data = data["data"]
        return data

    async def request(self, method, path, **kwargs):
        return (await self.fetch(method, path, **kwargs)).data

    async def get(self, path, params=None, **kwargs):
        return await self.request("GET", path, params=params, **kwargs)

    async def post(self, path, json_body, **kwargs):
        return await self.request("POST", path, json_body=json_body, **kwargs)
