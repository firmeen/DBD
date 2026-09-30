from __future__ import annotations

import asyncio
import math
import time

from redis.exceptions import RedisError

from ..core.redis import close_async_redis, create_async_redis
from ..core.settings import settings

_ADMISSION_LUA = r"""
local now = redis.call('TIME')
local now_us = (tonumber(now[1]) * 1000000) + tonumber(now[2])

local cooldown_ms = redis.call('PTTL', KEYS[2])
if cooldown_ms > 0 then
  return cooldown_ms * 1000
end

local interval_us = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local next_us = tonumber(redis.call('GET', KEYS[1]) or '0')
local scheduled_us = now_us
if next_us > scheduled_us then
  scheduled_us = next_us
end

local wait_us = scheduled_us - now_us
redis.call(
  'SET',
  KEYS[1],
  tostring(scheduled_us + interval_us),
  'PX',
  ttl_ms
)
return wait_us
"""

_COOLDOWN_LUA = r"""
local requested_ms = tonumber(ARGV[1])
local current_ms = redis.call('PTTL', KEYS[1])
if current_ms < requested_ms then
  redis.call('SET', KEYS[1], '1', 'PX', requested_ms)
  return requested_ms
end
return current_ms
"""


class FixedRedisAdmission:
    """One fixed aggregate request pacer shared by all long-run workers.

    Redis owns only aggregate admission and explicit Retry-After cooldowns.
    It does not own adaptive rate, authentication state, transport health,
    worker lifecycle, or durable scheduler state.
    """

    def __init__(
        self,
        *,
        rate_per_second: float,
        redis_url: str | None = None,
        key: str,
        max_connections: int | None = None,
    ) -> None:
        rate = float(rate_per_second)
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("rate_per_second must be a finite positive value")
        if not key or not key.strip():
            raise ValueError("Redis admission key must not be empty")

        self.rate_per_second = rate
        self.redis_url = redis_url or settings.dbd_redis_url
        if not self.redis_url:
            raise ValueError("DBD_REDIS_URL is required for long-run execution")

        self.key = key.strip()
        self._interval_us = max(1, int(round(1_000_000.0 / rate)))
        self._ttl_ms = max(60_000, int(math.ceil((64.0 / rate) * 1000.0)))
        self._max_connections = max_connections
        self._client = None

        self.state = "closed"
        self.last_error: str | None = None
        self.last_error_at: float | None = None
        self.redis_commands = 0
        self.admissions = 0

    async def open(self) -> None:
        if self._client is not None:
            return
        self._client = create_async_redis(
            self.redis_url,
            max_connections=self._max_connections,
        )
        try:
            await self._client.ping()
            self.redis_commands += 1
        except BaseException:
            client = self._client
            self._client = None
            await close_async_redis(client)
            raise
        self.state = "ready"
        self.last_error = None
        self.last_error_at = None

    async def close(self) -> None:
        client = self._client
        self._client = None
        self.state = "closed"
        if client is not None:
            await close_async_redis(client)

    async def __aenter__(self) -> "FixedRedisAdmission":
        await self.open()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    def _cooldown_key(self, endpoint: str | None) -> str:
        lane = endpoint or "__global__"
        safe_lane = "".join(
            char if char.isalnum() or char in ("_", "-", ".") else "_"
            for char in str(lane)
        )
        return f"{self.key}:cooldown:{safe_lane}"

    def _remember_error(self, exc: BaseException) -> None:
        self.state = "redis_wait"
        self.last_error = f"{type(exc).__name__}: {str(exc) or type(exc).__name__}"
        self.last_error_at = time.time()

    async def acquire(self, endpoint: str | None = None) -> None:
        """Reserve exactly one future request slot.

        Redis failure is fail-closed: no new HTTP request starts until aggregate
        admission can be proven again. Existing archive/project work is unaffected.
        """

        backoff = 0.25
        while True:
            if self._client is None:
                try:
                    await self.open()
                except RedisError as exc:
                    self._remember_error(exc)
                    await asyncio.sleep(backoff)
                    backoff = min(5.0, backoff * 2.0)
                    continue

            try:
                wait_us = int(
                    await self._client.eval(
                        _ADMISSION_LUA,
                        2,
                        self.key,
                        self._cooldown_key(endpoint),
                        self._interval_us,
                        self._ttl_ms,
                    )
                )
                self.redis_commands += 1
                self.admissions += 1
                self.state = "ready"
                self.last_error = None
                self.last_error_at = None
                if wait_us > 0:
                    await asyncio.sleep(wait_us / 1_000_000.0)
                return
            except RedisError as exc:
                self.redis_commands += 1
                self._remember_error(exc)
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 2.0)

    async def cooldown(self, endpoint: str | None, seconds: float) -> None:
        """Share only explicit upstream Retry-After evidence.

        A cooldown never changes the configured fleet rate. It only prevents the
        affected endpoint lane from receiving a new reservation until the stated
        delay has elapsed.
        """

        delay = max(0.0, float(seconds))
        if delay <= 0:
            return
        requested_ms = max(1, int(math.ceil(delay * 1000.0)))
        backoff = 0.25
        while True:
            if self._client is None:
                try:
                    await self.open()
                except RedisError as exc:
                    self._remember_error(exc)
                    await asyncio.sleep(backoff)
                    backoff = min(5.0, backoff * 2.0)
                    continue
            try:
                await self._client.eval(
                    _COOLDOWN_LUA,
                    1,
                    self._cooldown_key(endpoint),
                    requested_ms,
                )
                self.redis_commands += 1
                self.state = "ready"
                return
            except RedisError as exc:
                self.redis_commands += 1
                self._remember_error(exc)
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 2.0)

    def snapshot(self) -> dict[str, object]:
        return {
            "state": self.state,
            "rate_per_second": self.rate_per_second,
            "redis_commands": int(self.redis_commands),
            "admissions": int(self.admissions),
            "last_error": self.last_error,
            "last_error_at": self.last_error_at,
        }
