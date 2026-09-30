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
from curl_cffi.requests.exceptions import RequestException as RequestsError
from redis.asyncio import Redis
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from ..core.exceptions import DbdAuthError, DbdForbiddenError, DbdTokenExpiredError
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
from .rate_control import EndpointRateController, RateControlUnavailable
from .rate_limit import TokenBucket
from .session import WARMUP_PAGE_URL, create_session


class TransientHttpError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class PermanentHttpError(ValueError):
    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


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
        )
        EFFECTIVE_RATE.set(self.rate.rate)
        self.max_retries = settings.dbd_http_retries if max_retries is None else max_retries
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._auth = DbdAuth()
        self._auth_lock = asyncio.Lock()
        self._token = None
        self._session = None
        self._auth_session = None
        self.requests_made = self.retries_made = self.rate_limit_hits = 0

    async def __aenter__(self):
        await self.endpoint_rate.open()
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
        await self.endpoint_rate.close()

    async def _ensure_token(self, *, rejected=None, force=False, reset_warmup=False):
        async with self._auth_lock:
            if force or (rejected is not None and rejected == self._token):
                self._auth.invalidate(reset_warmup=reset_warmup)
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
                (
                    TransientHttpError,
                    DbdTokenExpiredError,
                    DbdForbiddenError,
                    DbdAuthError,
                    RateControlUnavailable,
                    RequestsError,
                    TimeoutError,
                )
            ),
            reraise=True,
        )
        async for attempt in retry:
            with attempt:
                if attempt.retry_state.attempt_number > 1:
                    self.retries_made += 1
                    HTTP_RETRIES.inc()
                async with self._semaphore:
                    if rate_key is not None or self._shared_admission:
                        await self.endpoint_rate.acquire(rate_key)
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
                    retry_after_header = response.headers.get("retry-after")
                    rate_pressure = status == 429 or (
                        status == 503
                        and retry_after_header is not None
                        and str(retry_after_header).strip() != ""
                    )
                    if rate_pressure:
                        self.rate_limit_hits += 1
                        cooldown = retry_after(retry_after_header)
                        if rate_key is not None or self._shared_admission:
                            await self.endpoint_rate.cooldown(rate_key, cooldown)
                        else:
                            self.rate.cooldown(cooldown)
                        log.warning(
                            "rate_cooldown",
                            status=status,
                            path=path,
                            endpoint=rate_key,
                            retry_after_seconds=cooldown,
                            worker_rate=self.endpoint_rate.global_effective_rate(),
                            fleet_rate_ceiling=self.endpoint_rate.shared_rate_ceiling,
                        )

                    if status == 401:
                        await self._ensure_token(rejected=token)
                        raise TransientHttpError("HTTP 401", status_code=401)
                    if status == 403:
                        await self._ensure_token(rejected=token, reset_warmup=True)
                        raise DbdForbiddenError("HTTP 403")
                    if status in (429, 500, 502, 503, 504):
                        raise TransientHttpError(f"HTTP {status}", status_code=status)
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
                            endpoint=rate_key,
                            status=status,
                            exception_type=type(exc).__name__,
                            body_bytes=len(body),
                            body_sha256=sha256(body).hexdigest(),
                        )
                        raise

                    if self.requests_made:
                        REDIS_COMMANDS_PER_HTTP.set(
                            self.endpoint_rate.redis_commands / self.requests_made
                        )
                    return ApiResponse(
                        data,
                        status,
                        requested,
                        datetime.now(timezone.utc).replace(tzinfo=None),
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
