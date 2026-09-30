from __future__ import annotations

import asyncio
import json
import math
import os
import random
import time
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException as RequestsError

from ..core.exceptions import DbdAuthError, DbdForbiddenError, DbdTokenExpiredError
from ..core.settings import settings
from ..core.telemetry import HTTP_RETRIES, LATENCY, REQUESTS, log
from .auth import DbdAuth
from .crypto import decrypt_envelope
from .http import ApiResponse, PermanentHttpError, TransientHttpError, retry_after
from .redis_admission import FixedRedisAdmission
from .session import WARMUP_PAGE_URL, create_session

_AUTH_RETRY_SECONDS = 5.0
_AUTH_REQUEST_TIMEOUT_SECONDS = min(10.0, float(settings.request_timeout))


def _curl_code(exc: BaseException) -> object:
    code = getattr(exc, "code", None)
    if code is not None:
        return code
    return getattr(exc, "curl_code", None)


class _FixedLocalPacer:
    """Worker-local fixed upper bound with no adaptive behavior."""

    def __init__(self, rate_per_second: float) -> None:
        rate = float(rate_per_second)
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("rate_per_second must be a finite positive value")
        self.rate_per_second = rate
        self._interval = 1.0 / rate
        self._next_at = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            scheduled = max(now, self._next_at)
            self._next_at = scheduled + self._interval
            delay = max(0.0, scheduled - now)
        if delay > 0:
            await asyncio.sleep(delay)


class LongRunDbdClient:
    """Minimal long-run curl_cffi client with worker-local failure isolation.

    Each worker owns its own HTTP session, cookies, anonymous token lifecycle,
    local fixed-rate cap, retries, and auth recovery. Redis is used only for the
    fixed fleet admission ceiling and explicit Retry-After endpoint cooldowns.
    """

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
        del endpoint_rates, redis_client

        self.concurrency = (
            settings.dbd_concurrency if concurrency is None else int(concurrency)
        )
        if self.concurrency < 1:
            raise ValueError("concurrency must be positive")

        local_rate = (
            settings.dbd_rate_per_second
            if rate_per_second is None
            else float(rate_per_second)
        )
        if not math.isfinite(local_rate) or local_rate <= 0:
            raise ValueError("rate_per_second must be a finite positive value")

        fleet_rate = float(
            os.environ.get(
                "DBD_LONGRUN_FLEET_RATE",
                settings.dbd_fleet_rate_ceiling or 60.0,
            )
        )
        if not math.isfinite(fleet_rate) or fleet_rate <= 0:
            raise ValueError("DBD_LONGRUN_FLEET_RATE must be a finite positive value")

        configured_key = os.environ.get("DBD_LONGRUN_REDIS_KEY")
        if configured_key:
            admission_key = configured_key
        else:
            base_key = redis_key or settings.dbd_redis_rate_key
            admission_key = f"{base_key}:longrun:v1"

        self.local_pacer = _FixedLocalPacer(local_rate)
        self.admission = FixedRedisAdmission(
            rate_per_second=fleet_rate,
            redis_url=redis_url or settings.dbd_redis_url,
            key=admission_key,
            max_connections=settings.dbd_redis_max_connections,
        )
        self.max_retries = (
            settings.dbd_http_retries if max_retries is None else int(max_retries)
        )
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")

        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._session: AsyncSession | None = None
        self._auth_session = None
        self._auth = DbdAuth()
        self._auth_lock = asyncio.Lock()
        self._auth_refresh_task: asyncio.Task | None = None
        self._auth_backoff_until = 0.0
        self._token: str | None = None
        self._last_error: str | None = None
        self._last_error_at: float | None = None

        self.requests_made = 0
        self.responses_received = 0
        self.retries_made = 0
        self.rate_limit_hits = 0
        self.auth_failures = 0
        self.last_request_at: float | None = None
        self.last_response_at: float | None = None
        self.last_auth_success_at: float | None = None
        self.last_auth_error_at: float | None = None

    @property
    def target_rate(self) -> float:
        return float(self.local_pacer.rate_per_second)

    @property
    def effective_rate(self) -> float:
        return float(self.local_pacer.rate_per_second)

    @property
    def upstream_state(self) -> str:
        return str(self.admission.state)

    @property
    def auth_state(self) -> str:
        if time.monotonic() < self._auth_backoff_until:
            return "cooldown"
        if self._auth_refresh_task is not None and not self._auth_refresh_task.done():
            return "refreshing"
        return "ready" if self._token_valid() else "needed"

    @property
    def last_error(self) -> str | None:
        return self.admission.last_error or self._last_error

    @property
    def last_error_at(self) -> float | None:
        return self.admission.last_error_at or self._last_error_at

    async def __aenter__(self) -> "LongRunDbdClient":
        # Startup Redis availability is checked once by the parent preflight.
        # Workers connect lazily at admission time so a later Redis outage parks
        # requests in-place instead of turning dependency loss into restart churn.
        self._session = AsyncSession(
            impersonate="chrome",
            http_version="v1",
            max_clients=self.concurrency,
        )
        self._auth_session = create_session(http_version="v1")
        return self

    async def __aexit__(self, *args) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._auth_session is not None:
            self._auth_session.close()
            self._auth_session = None
        await self.admission.close()

    def _remember_error(self, exc: BaseException) -> None:
        self._last_error = f"{type(exc).__name__}: {str(exc) or type(exc).__name__}"
        self._last_error_at = time.time()

    def _token_valid(self) -> bool:
        return bool(self._token) and time.time() < self._auth._expires_at - 15

    def _reset_auth_session(self) -> None:
        if self._auth_session is not None:
            try:
                self._auth_session.close()
            except Exception:
                pass
        self._auth_session = create_session(http_version="v1")
        self._auth.invalidate(reset_warmup=True)
        self._token = None
        self._auth_refresh_task = None

    async def _refresh_token_once(self) -> str:
        try:
            token = await asyncio.to_thread(
                self._auth.get_token,
                self._auth_session,
                max_attempts=1,
                timeout_seconds=_AUTH_REQUEST_TIMEOUT_SECONDS,
            )
        except DbdAuthError as exc:
            self.auth_failures += 1
            self.last_auth_error_at = time.time()
            self._remember_error(exc)
            message = str(exc).lower()
            bootstrap_failed = (
                getattr(exc, "status_code", None) == 403
                or "warmup_" in message
                or "curl=16" in message
                or "curl=55" in message
            )
            if bootstrap_failed:
                self._reset_auth_session()
            self._auth_backoff_until = (
                time.monotonic()
                + _AUTH_RETRY_SECONDS
                + random.uniform(0.5, 2.5)
            )
            raise

        self._token = token
        self._auth_backoff_until = 0.0
        self.last_auth_success_at = time.time()
        if self._session is not None and self._auth_session is not None:
            self._session.cookies.update(self._auth_session.cookies)
        return token

    async def _ensure_token(
        self,
        *,
        rejected: str | None = None,
        reset_warmup: bool = False,
    ) -> str:
        if rejected is not None and rejected == self._token:
            async with self._auth_lock:
                if rejected == self._token:
                    self._auth.invalidate(reset_warmup=reset_warmup)
                    self._token = None
                    self._auth_refresh_task = None
                    self._auth_backoff_until = 0.0

        while True:
            if self._token_valid():
                return str(self._token)

            task = None
            wait_seconds = 0.0
            async with self._auth_lock:
                if self._token_valid():
                    return str(self._token)

                wait_seconds = max(
                    0.0,
                    self._auth_backoff_until - time.monotonic(),
                )
                if wait_seconds <= 0:
                    if (
                        self._auth_refresh_task is None
                        or self._auth_refresh_task.done()
                    ):
                        self._auth_refresh_task = asyncio.create_task(
                            self._refresh_token_once()
                        )
                    task = self._auth_refresh_task

            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
                continue

            assert task is not None
            try:
                return await task
            except DbdAuthError:
                # Authentication recovery belongs to this worker, not to each
                # durable fetch task. Keep callers behind one worker-local gate.
                continue

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

    def _decode(self, response, path: str, token: str):
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
        method,
        path,
        *,
        json_body=None,
        params=None,
        referer=WARMUP_PAGE_URL,
        timeout=None,
        rate_key: str | None = None,
    ) -> ApiResponse:
        if self._session is None:
            raise RuntimeError("Use async with LongRunDbdClient")

        attempts = self.max_retries + 1
        last_exc: BaseException | None = None

        for attempt_number in range(1, attempts + 1):
            if attempt_number > 1:
                self.retries_made += 1
                HTTP_RETRIES.inc()

            try:
                # Auth first: a broken session must not consume fleet admission.
                token = await self._ensure_token()

                # Local and aggregate caps are fixed. Neither changes after errors.
                await self.local_pacer.acquire()
                await self.admission.acquire(rate_key)

                async with self._semaphore:
                    requested = datetime.now(timezone.utc).replace(tzinfo=None)
                    self.requests_made += 1
                    self.last_request_at = time.time()
                    with LATENCY.time():
                        response = await self._session.request(
                            method,
                            settings.dbd_base_url.rstrip("/") + path,
                            json=json_body,
                            params=params,
                            headers=self._headers(token, referer),
                            timeout=timeout or settings.request_timeout,
                        )

                self.responses_received += 1
                self.last_response_at = time.time()
                status = int(response.status_code)
                REQUESTS.labels(str(status)).inc()

                retry_after_header = response.headers.get("retry-after")
                explicit_pressure = status == 429 or (
                    status == 503
                    and retry_after_header is not None
                    and str(retry_after_header).strip() != ""
                )
                if explicit_pressure:
                    self.rate_limit_hits += 1
                    delay = max(1.0, retry_after(retry_after_header))
                    await self.admission.cooldown(rate_key, delay)
                    raise TransientHttpError(
                        f"HTTP {status}",
                        status_code=status,
                    )

                if status == 401:
                    await self._ensure_token(rejected=token)
                    raise TransientHttpError("HTTP 401", status_code=401)

                if status == 403:
                    # First recover the bearer token on the existing warmed
                    # browser session. Re-running document warmup immediately
                    # on every API 403 creates a synchronized WAF stampede.
                    await self._ensure_token(
                        rejected=token,
                        reset_warmup=False,
                    )
                    raise DbdForbiddenError("HTTP 403")

                if status in (500, 502, 503, 504):
                    raise TransientHttpError(
                        f"HTTP {status}",
                        status_code=status,
                    )

                if status >= 400:
                    raise PermanentHttpError(
                        f"HTTP {status}",
                        status_code=status,
                    )

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
                        endpoint=rate_key,
                        status=status,
                        exception_type=type(exc).__name__,
                        body_bytes=len(body),
                        body_sha256=sha256(body).hexdigest(),
                    )
                    raise

                self._last_error = None
                self._last_error_at = None
                return ApiResponse(
                    data=data,
                    status=status,
                    requested_at=requested,
                    received_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )

            except PermanentHttpError:
                raise
            except asyncio.CancelledError:
                raise
            except (DbdAuthError, DbdForbiddenError, DbdTokenExpiredError) as exc:
                self._remember_error(exc)
                last_exc = exc
            except (RequestsError, TimeoutError) as exc:
                REQUESTS.labels("network_error").inc()
                self._remember_error(exc)
                log.warning(
                    "http_transport_error",
                    stage="request",
                    path=path,
                    endpoint=rate_key,
                    exception_type=type(exc).__name__,
                    curl_code=_curl_code(exc),
                )
                last_exc = exc
            except TransientHttpError as exc:
                self._remember_error(exc)
                last_exc = exc

            if attempt_number >= attempts:
                assert last_exc is not None
                raise last_exc

            await asyncio.sleep(
                min(5.0, (2 ** (attempt_number - 1)) + random.random())
            )

        raise RuntimeError("request retry loop exited unexpectedly")

    async def request(self, method, path, **kwargs):
        return (await self.fetch(method, path, **kwargs)).data

    async def get(self, path, params=None, **kwargs):
        return await self.request("GET", path, params=params, **kwargs)

    async def post(self, path, json_body, **kwargs):
        return await self.request("POST", path, json_body=json_body, **kwargs)
