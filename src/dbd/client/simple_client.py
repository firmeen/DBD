from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from decimal import Decimal

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException as RequestsError

from ..core.exceptions import DbdAuthError, DbdForbiddenError, DbdTokenExpiredError
from ..core.settings import settings
from ..core.telemetry import EFFECTIVE_RATE, HTTP_RETRIES, LATENCY, REQUESTS, log
from .async_client import ApiResponse, PermanentHttpError, TransientHttpError, retry_after
from .auth import DbdAuth
from .crypto import decrypt_envelope
from .rate_limit import TokenBucket
from .session import WARMUP_PAGE_URL, create_session


class SimpleDbdClient:
    """Single-process DBD client with one explicit global pacing target.

    This client intentionally excludes Redis coordination, hierarchical endpoint
    pacing and distributed circuit ownership. Rate pressure (429 or Retry-After)
    produces a bounded cooldown. Availability failures (auth/network/ordinary 5xx)
    pause new admissions briefly without lowering the configured target rate.
    """

    def __init__(
        self,
        *,
        concurrency: int | None = None,
        rate_per_second: float | None = None,
        max_retries: int | None = None,
        **_ignored,
    ) -> None:
        self.concurrency = int(
            settings.dbd_collector_concurrency if concurrency is None else concurrency
        )
        if not 1 <= self.concurrency <= 256:
            raise ValueError("concurrency must be 1..256")

        target = float(
            settings.dbd_collector_rate_per_second
            if rate_per_second is None
            else rate_per_second
        )
        self.rate = TokenBucket(target)
        self.target_rate = target
        self.max_retries = min(
            2,
            settings.dbd_http_retries if max_retries is None else int(max_retries),
        )
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")

        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._auth = DbdAuth(max_attempts=1)
        self._auth_lock = asyncio.Lock()
        self._token: str | None = None
        self._session = None
        self._auth_session = None

        self._availability_lock = asyncio.Lock()
        self._availability_failures = 0
        self._pause_until = 0.0

        self.requests_made = 0
        self.retries_made = 0
        self.rate_limit_hits = 0
        self.redis_commands = 0
        self.redis_admissions = 0
        self.auth_state = "cold"
        self.last_error: str | None = None
        EFFECTIVE_RATE.set(self.target_rate)

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

    def _blocking_token(self):
        return self._auth.get_token(self._auth_session)

    async def _ensure_token(
        self,
        *,
        rejected: str | None = None,
        reset_warmup: bool = False,
    ) -> str:
        async with self._auth_lock:
            if rejected is not None and rejected == self._token:
                self._auth.invalidate(reset_warmup=reset_warmup)
                self._token = None
            if self._token is None:
                delay = self._pause_until - time.monotonic()
                if delay > 0:
                    self.auth_state = "waiting"
                    await asyncio.sleep(delay)
            if self._token is None or time.time() >= self._auth._expires_at - 15:
                self.auth_state = "refreshing"
                try:
                    self._token = await asyncio.to_thread(self._blocking_token)
                except DbdAuthError as exc:
                    self.auth_state = "unavailable"
                    await self._pause_availability(exc)
                    raise
                except BaseException:
                    self.auth_state = "unavailable"
                    raise
                self.auth_state = "healthy"
                if self._session is not None and self._auth_session is not None:
                    self._session.cookies.update(self._auth_session.cookies)
            return self._token

    async def _wait_availability(self) -> None:
        while True:
            delay = self._pause_until - time.monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(min(delay, 1.0))

    async def _pause_availability(self, error: BaseException | str) -> float:
        async with self._availability_lock:
            self._availability_failures = min(8, self._availability_failures + 1)
            delay = min(15.0, float(2 ** min(3, self._availability_failures)))
            self._pause_until = max(self._pause_until, time.monotonic() + delay)
            self.last_error = str(error)[:240]
            return delay

    async def _availability_ok(self) -> None:
        async with self._availability_lock:
            self._availability_failures = 0
            self._pause_until = 0.0
            self.last_error = None

    @property
    def availability_state(self) -> str:
        return "paused" if time.monotonic() < self._pause_until else "healthy"

    def circuit_state(self, endpoint: str | None = None) -> str:
        return self.availability_state

    def global_effective_rate(self) -> float:
        return float(self.rate.rate)

    def _penalize_rate(self) -> float:
        value = max(1.0, min(self.rate.rate, self.target_rate * 0.5))
        self.rate._set_rate(value)
        EFFECTIVE_RATE.set(value)
        return value

    def _restore_target_rate(self) -> None:
        if self.rate.rate != self.target_rate:
            self.rate._set_rate(self.target_rate)
            EFFECTIVE_RATE.set(self.target_rate)

    @staticmethod
    def _headers(token: str, referer: str) -> dict[str, str]:
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
    ) -> ApiResponse:
        if self._session is None:
            raise RuntimeError("Use async with SimpleDbdClient")

        for attempt in range(self.max_retries + 1):
            if attempt:
                self.retries_made += 1
                HTTP_RETRIES.inc()

            async with self._semaphore:
                await self._wait_availability()
                await self.rate.acquire()

                try:
                    token = await self._ensure_token()
                except DbdAuthError as exc:
                    if self._pause_until <= time.monotonic():
                        await self._pause_availability(exc)
                    # DbdAuth already owns its bounded refresh retry budget. Do not
                    # multiply it by another HTTP retry loop.
                    raise

                requested = datetime.now(timezone.utc).replace(tzinfo=None)
                self.requests_made += 1
                try:
                    with LATENCY.time():
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
                    delay = await self._pause_availability(exc)
                    log.warning(
                        "collector_transport_pause",
                        path=path,
                        endpoint=rate_key,
                        delay_seconds=delay,
                        error_code=type(exc).__name__,
                    )
                    if attempt < self.max_retries:
                        continue
                    raise

                status = int(response.status_code)
                REQUESTS.labels(str(status)).inc()
                retry_after_header = response.headers.get("retry-after")
                rate_pressure = status == 429 or (
                    status == 503
                    and retry_after_header is not None
                    and str(retry_after_header).strip() != ""
                )

                if rate_pressure:
                    self.rate_limit_hits += 1
                    cooldown = max(1.0, retry_after(retry_after_header))
                    effective_rate = self._penalize_rate()
                    self.rate.cooldown(cooldown)
                    self.last_error = f"HTTP {status}"
                    log.warning(
                        "collector_rate_cooldown",
                        status=status,
                        path=path,
                        endpoint=rate_key,
                        cooldown_seconds=cooldown,
                        effective_rate=effective_rate,
                        target_rate=self.target_rate,
                    )
                    if attempt < self.max_retries:
                        continue
                    raise TransientHttpError(
                        f"HTTP {status}", status_code=status
                    )

                if status == 401:
                    await self._ensure_token(rejected=token)
                    if attempt < self.max_retries:
                        continue
                    raise TransientHttpError("HTTP 401", status_code=401)

                if status == 403:
                    self._auth.invalidate(reset_warmup=True)
                    self._token = None
                    await self._pause_availability("HTTP 403")
                    if attempt < self.max_retries:
                        continue
                    raise DbdForbiddenError("HTTP 403")

                if status in (500, 502, 504) or status == 503:
                    delay = await self._pause_availability(f"HTTP {status}")
                    log.warning(
                        "collector_upstream_pause",
                        status=status,
                        path=path,
                        endpoint=rate_key,
                        delay_seconds=delay,
                    )
                    if attempt < self.max_retries:
                        continue
                    raise TransientHttpError(
                        f"HTTP {status}", status_code=status
                    )

                if status >= 400:
                    # A business-level 4xx still proves that DBD is reachable.
                    # Preserve its endpoint semantics while clearing availability
                    # outage state.
                    await self._availability_ok()
                    self.auth_state = "healthy"
                    raise PermanentHttpError(
                        f"HTTP {status}", status_code=status
                    )

                try:
                    data = self._decode(response, path, token)
                except DbdTokenExpiredError:
                    self._auth.invalidate()
                    self._token = None
                    if attempt < self.max_retries:
                        continue
                    raise

                await self._availability_ok()
                self._restore_target_rate()
                self.auth_state = "healthy"
                return ApiResponse(
                    data=data,
                    status=status,
                    requested_at=requested,
                    received_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )

        raise RuntimeError("HTTP retry loop exhausted unexpectedly")

    def _decode(self, response, path: str, token: str):
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
