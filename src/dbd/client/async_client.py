from __future__ import annotations

import asyncio
import random

from curl_cffi.requests import AsyncSession

from ..core.exceptions import DbdForbiddenError, DbdTokenExpiredError
from ..core.settings import settings
from .auth import DbdAuth
from .crypto import decrypt_envelope
from .rate_limit import TokenBucket
from .session import WARMUP_PAGE_URL, create_session

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class AsyncDbdClient:
    """Concurrent DBD client for bulk work.

    The sync DbdClient stays the right tool for one company. This exists for
    the enumeration workload, where the cost is dominated by waiting on the
    network: measured at ~0.12 s/request with 10 in flight versus ~1.1 s/request
    sequentially.

    Three things keep it polite rather than merely fast:
      - one global TokenBucket caps requests/second across every worker,
      - a Semaphore caps how many are in flight at once,
      - a 429/503 permanently reduces the rate instead of just retrying into it.
    """

    def __init__(
        self,
        *,
        concurrency: int | None = None,
        rate_per_second: float | None = None,
        max_retries: int = 4,
    ) -> None:
        self.concurrency = concurrency or settings.dbd_concurrency
        self.rate = TokenBucket(rate_per_second or settings.dbd_rate_per_second)
        self.max_retries = max_retries

        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._session: AsyncSession | None = None
        self._auth = DbdAuth()
        self._auth_lock = asyncio.Lock()
        self._token: str | None = None

        self.requests_made = 0
        self.retries_made = 0
        self.rate_limit_hits = 0

    async def __aenter__(self) -> "AsyncDbdClient":
        self._session = AsyncSession(impersonate="chrome")
        await self._ensure_token()
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _ensure_token(self, *, force: bool = False) -> str:
        async with self._auth_lock:
            if force:
                self._auth.invalidate()
                self._token = None

            if self._token is None:
                # The handshake (warm-up cookies + /api/refresh) is proven on the
                # sync session; run it in a thread rather than reimplementing it.
                self._token = await asyncio.to_thread(self._blocking_token)

            return self._token

    def _blocking_token(self) -> str:
        session = create_session()
        return self._auth.get_token(session)

    def _headers(self, token: str, referer: str) -> dict:
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

    async def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        params: dict | None = None,
        referer: str = WARMUP_PAGE_URL,
        timeout: int | None = None,
    ) -> object:
        """One authenticated, rate-limited, decrypted request with retries."""
        assert self._session is not None, "use `async with AsyncDbdClient(...)`"

        url = settings.dbd_base_url + path
        timeout = timeout or settings.request_timeout
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            token = await self._ensure_token()

            await self.rate.acquire()
            async with self._semaphore:
                try:
                    response = await self._session.request(
                        method,
                        url,
                        json=json_body,
                        params=params,
                        headers=self._headers(token, referer),
                        timeout=timeout,
                    )
                    self.requests_made += 1
                except Exception as exc:  # network/timeout
                    last_error = exc
                    self.retries_made += 1
                    await self._backoff(attempt)
                    continue

            status = response.status_code

            if status == 401:
                await self._ensure_token(force=True)
                self.retries_made += 1
                continue

            if status == 403:
                raise DbdForbiddenError(f"403 Forbidden: {path}")

            if status in _RETRYABLE_STATUS:
                self.rate_limit_hits += 1
                self.retries_made += 1
                if status in (429, 503):
                    self.rate.slow_down()
                await self._backoff(attempt, response.headers.get("retry-after"))
                continue

            if status >= 400:
                raise RuntimeError(f"HTTP {status} for {path}: {response.text[:200]}")

            return self._decode(response, path, token)

        raise RuntimeError(f"{path} failed after {self.max_retries} retries: {last_error}")

    async def _backoff(self, attempt: int, retry_after: str | None = None) -> None:
        if retry_after:
            try:
                await asyncio.sleep(max(1.0, float(retry_after)))
                return
            except ValueError:
                pass
        # Exponential with jitter, so concurrent workers do not retry in lockstep.
        await asyncio.sleep(min(30.0, (2**attempt) * (0.5 + random.random())))

    def _decode(self, response, path: str, token: str) -> object:
        data = response.json()

        if response.headers.get("x-encrypted", "").lower() == "true" and isinstance(data, dict) and "ct" in data:
            try:
                data = decrypt_envelope(data, path, token)
            except DbdTokenExpiredError:
                raise

        if response.headers.get("x-wrapped", "").lower() == "true" and isinstance(data, dict) and "data" in data:
            data = data["data"]

        return data

    async def get(self, path: str, params: dict | None = None, **kw) -> object:
        return await self.request("GET", path, params=params, **kw)

    async def post(self, path: str, json_body: dict, **kw) -> object:
        return await self.request("POST", path, json_body=json_body, **kw)
