from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from hashlib import sha256

from curl_cffi.requests import AsyncSession
from redis.asyncio import Redis
from curl_cffi.requests.exceptions import RequestException as RequestsError
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from ..core.exceptions import DbdForbiddenError, DbdTokenExpiredError
from ..core.settings import settings
from ..core.telemetry import (
    EFFECTIVE_RATE,
    HTTP_RETRIES,
    LATENCY,
    REDIS_COMMANDS_PER_HTTP,
    REQUESTS,
    log,
)
from .auth import DbdAuth
from .crypto import decrypt_envelope
from .rate_control import EndpointRateController
from .rate_limit import TokenBucket, protective_cooldown
from .session import WARMUP_PAGE_URL, create_session


class TransientHttpError(RuntimeError):
    pass


class PermanentHttpError(ValueError):
    pass


@dataclass
class ApiResponse:
    data: object
    status: int
    requested_at: datetime
    received_at: datetime


def retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 1.0


def _curl_code(exc) -> object:
    code = getattr(exc, "code", None)
    if code is not None:
        return code
    return getattr(exc, "curl_code", None)


class AsyncDbdClient:
    def __init__(
        self,
        *,
        concurrency=None,
        rate_per_second=None,
        max_retries=None,
        endpoint_rates=None,
        redis_url=None,
        redis_key=None,
        redis_client: Redis | None = None,
    ):
        self.concurrency = settings.dbd_concurrency if concurrency is None else concurrency
        if self.concurrency < 1:
            raise ValueError("concurrency must be positive")
        global_rate = settings.dbd_rate_per_second if rate_per_second is None else rate_per_second
        self.rate = TokenBucket(global_rate)
        self._shared_admission = bool(redis_url)
        self.endpoint_rate = EndpointRateController(
            global_rate,
            endpoint_rates=settings.dbd_endpoint_rates if endpoint_rates is None else endpoint_rates,
            redis_url=redis_url,
            redis_key=redis_key or settings.dbd_redis_rate_key,
            shared_rate_ceiling=settings.dbd_fleet_rate_ceiling,
            redis_max_connections=settings.dbd_redis_max_connections,
            redis_client=redis_client,
            circuit_failure_threshold=settings.dbd_circuit_failure_threshold,
            circuit_window_seconds=settings.dbd_circuit_window_seconds,
            circuit_open_seconds=settings.dbd_circuit_open_seconds,
            global_pressure_window_seconds=settings.dbd_global_pressure_window_seconds,
            global_pressure_endpoint_threshold=settings.dbd_global_pressure_endpoint_threshold,
            circuit_probe_seconds=max(
                float(settings.request_timeout) + 10.0,
                float(settings.dbd_circuit_open_seconds),
            ),
        )
        EFFECTIVE_RATE.set(self.rate.rate)
        self.max_retries = settings.dbd_http_retries if max_retries is None else max_retries
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._auth = DbdAuth()
        self._auth_lock = asyncio.Lock()
        self._token = None
        self._session = None
        self._auth_session = None
        self._penalty_streaks: dict[str, int] = {}
        self._last_penalty_at: dict[str, float] = {}
        self.requests_made = self.retries_made = self.rate_limit_hits = 0

    def _next_penalty_streak(self, key: str) -> int:
        now = time.monotonic()
        last = self._last_penalty_at.get(key, 0.0)
        streak = self._penalty_streaks.get(key, 0) + 1 if now - last <= 30.0 else 1
        self._penalty_streaks[key] = streak
        self._last_penalty_at[key] = now
        return streak

    async def __aenter__(self):
        await self.endpoint_rate.open()
        self._session = AsyncSession(impersonate="chrome", max_clients=self.concurrency)
        self._auth_session = create_session()
        try:
            await self._ensure_token()
        except BaseException:
            await self.__aexit__()
            raise
        return self

    async def __aexit__(self, *args):
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._auth_session is not None:
            self._auth_session.close()
            self._auth_session = None
        await self.endpoint_rate.close()

    async def _ensure_token(self, *, rejected=None, force=False):
        async with self._auth_lock:
            if force or (rejected is not None and rejected == self._token):
                self._auth.invalidate()
                self._token = None
            if self._token is None or time.time() >= self._auth._expires_at - 15:
                self._token = await asyncio.to_thread(self._blocking_token)
                if self._session is not None and self._auth_session is not None:
                    self._session.cookies.update(self._auth_session.cookies)
            return self._token

    def _blocking_token(self):
        return self._auth.get_token(self._auth_session)

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
        if self._session is None:
            raise RuntimeError("Use async with AsyncDbdClient")
        retry = AsyncRetrying(
            stop=stop_after_attempt(self.max_retries + 1),
            wait=wait_random_exponential(multiplier=1, max=30),
            retry=retry_if_exception_type(
                (TransientHttpError, DbdTokenExpiredError, RequestsError, TimeoutError)
            ),
            reraise=True,
        )
        async for attempt in retry:
            with attempt:
                if attempt.retry_state.attempt_number > 1:
                    self.retries_made += 1
                    HTTP_RETRIES.inc()
                async with self._semaphore:
                    admission = None
                    if rate_key is not None or self._shared_admission:
                        admission = await self.endpoint_rate.acquire(rate_key)
                    else:
                        await self.rate.acquire()
                    token = await self._ensure_token()
                    requested = datetime.now(timezone.utc).replace(tzinfo=None)
                    self.requests_made += 1
                    with LATENCY.time():
                        try:
                            response = await self._session.request(
                                method,
                                settings.dbd_base_url.rstrip("/") + path,
                                json=json_body,
                                params=params,
                                headers=self._headers(token, referer),
                                timeout=timeout or settings.request_timeout,
                            )
                        except (RequestsError, TimeoutError) as exc:
                            REQUESTS.labels("network_error").inc()
                            log.warning(
                                "http_transport_error",
                                stage="request",
                                path=path,
                                endpoint=rate_key,
                                exception_type=type(exc).__name__,
                                curl_code=_curl_code(exc),
                            )
                            raise
                    status = response.status_code
                    REQUESTS.labels(str(status)).inc()
                    if status in (429, 503):
                        self.rate_limit_hits += 1
                        server_cooldown = retry_after(response.headers.get("retry-after"))
                        penalty_key = rate_key or "__global__"
                        streak = self._next_penalty_streak(penalty_key)
                        global_escalated = rate_key is None
                        if rate_key:
                            old_effective = self.endpoint_rate.effective_rate(rate_key)
                            expected_rate = max(0.5, old_effective * 0.5)
                            cooldown = protective_cooldown(
                                server_cooldown, expected_rate, streak
                            )
                            (
                                old_rate,
                                new_rate,
                                old_global_rate,
                                new_global_rate,
                                global_escalated,
                            ) = await self.endpoint_rate.penalize(rate_key, cooldown)
                        elif self._shared_admission:
                            old_effective = self.endpoint_rate.global_effective_rate()
                            expected_rate = max(0.5, old_effective * 0.5)
                            cooldown = protective_cooldown(
                                server_cooldown, expected_rate, streak
                            )
                            old_global_rate, new_global_rate = (
                                await self.endpoint_rate.penalize_global(cooldown)
                            )
                            old_rate, new_rate = old_global_rate, new_global_rate
                        else:
                            old_rate = self.rate.rate
                            new_rate = self.rate.slow_down()
                            cooldown = protective_cooldown(server_cooldown, new_rate, streak)
                            self.rate.cooldown(cooldown)
                            EFFECTIVE_RATE.set(new_rate)
                            old_global_rate = old_rate
                            new_global_rate = new_rate
                        log.warning(
                            "rate_penalty",
                            status=status,
                            path=path,
                            endpoint=rate_key,
                            old_rate=old_rate,
                            new_rate=new_rate,
                            old_global_rate=old_global_rate,
                            new_global_rate=new_global_rate,
                            global_escalated=global_escalated,
                            penalty_streak=streak,
                            server_retry_after_seconds=server_cooldown,
                            cooldown_seconds=cooldown,
                        )
                    if status == 401:
                        await self._ensure_token(rejected=token)
                        raise TransientHttpError("HTTP 401")
                    if status == 403:
                        raise DbdForbiddenError("HTTP 403")
                    if status in (429, 500, 502, 503, 504):
                        raise TransientHttpError(f"HTTP {status}")
                    if status >= 400:
                        raise PermanentHttpError(f"HTTP {status}")
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
                    if rate_key:
                        endpoint_recovered, global_recovered = await self.endpoint_rate.recover(
                            rate_key,
                            admission=admission,
                        )
                        recovered = endpoint_recovered or global_recovered
                        global_effective = self.endpoint_rate.global_effective_rate()
                    elif self._shared_admission:
                        recovered = await self.endpoint_rate.recover_global(
                            admission=admission,
                        )
                        global_effective = self.endpoint_rate.global_effective_rate()
                    else:
                        recovered = self.rate.recover()
                        global_effective = self.rate.rate
                        if recovered is not None:
                            EFFECTIVE_RATE.set(recovered)
                    if self.requests_made:
                        REDIS_COMMANDS_PER_HTTP.set(
                            self.endpoint_rate.redis_commands / self.requests_made
                        )
                    if recovered is not None:
                        log.info(
                            "rate_recovered",
                            path=path,
                            endpoint=rate_key,
                            effective_rate=recovered,
                            global_effective_rate=global_effective,
                        )
                    return ApiResponse(
                        data, status, requested, datetime.now(timezone.utc).replace(tzinfo=None)
                    )

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
