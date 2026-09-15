from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping

from redis.asyncio import Redis

from ..core.telemetry import ENDPOINT_EFFECTIVE_RATE, EFFECTIVE_RATE
from .rate_limit import TokenBucket


class RedisPacer:
    """Redis-backed global request pacer shared by independent worker processes.

    The Lua script reserves one future admission slot atomically using Redis TIME,
    so workers on different machines share one smooth global cap without relying
    on synchronized local clocks or fixed windows with boundary bursts.
    """

    _SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local interval_ms = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local next_ms = tonumber(redis.call('GET', KEYS[1]) or now_ms)
if next_ms < now_ms then next_ms = now_ms end
local wait_ms = next_ms - now_ms
local reserved_next = next_ms + interval_ms
redis.call('PSETEX', KEYS[1], ttl_ms, tostring(reserved_next))
return wait_ms
"""

    def __init__(self, url: str, key: str, rate_per_second: float):
        if rate_per_second <= 0 or not math.isfinite(rate_per_second):
            raise ValueError("rate_per_second must be a finite positive number")
        self.url = url
        self.key = key
        self.rate = float(rate_per_second)
        self.target_rate = self.rate
        self._redis: Redis | None = None

    async def open(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.url, decode_responses=True)
            await self._redis.ping()

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def acquire(self) -> None:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        interval_ms = max(1.0, 1000.0 / self.rate)
        ttl_ms = max(10_000, int(interval_ms * 10_000))
        wait_ms = float(await self._redis.eval(self._SCRIPT, 1, self.key, interval_ms, ttl_ms))
        if wait_ms > 0:
            await asyncio.sleep(wait_ms / 1000.0)


class EndpointRateController:
    """Global cap plus independent adaptive endpoint limiters.

    A 429/503 penalizes only the endpoint that produced it. The global pacer is a
    hard admission ceiling; in distributed mode it is coordinated through Redis.
    """

    def __init__(
        self,
        global_rate: float,
        *,
        endpoint_rates: Mapping[str, float] | None = None,
        redis_url: str | None = None,
        redis_key: str = "dbd:global-rate",
    ):
        if global_rate <= 0 or not math.isfinite(global_rate):
            raise ValueError("global_rate must be a finite positive number")
        self.target_rate = float(global_rate)
        self._endpoint_targets = {
            str(key): float(value) for key, value in (endpoint_rates or {}).items()
        }
        if any(value <= 0 or not math.isfinite(value) for value in self._endpoint_targets.values()):
            raise ValueError("endpoint rates must be finite positive numbers")
        self._global = (
            RedisPacer(redis_url, redis_key, self.target_rate)
            if redis_url
            else TokenBucket(self.target_rate)
        )
        self._endpoints: dict[str, TokenBucket] = {}
        EFFECTIVE_RATE.set(self.target_rate)

    async def open(self) -> None:
        if isinstance(self._global, RedisPacer):
            await self._global.open()

    async def close(self) -> None:
        if isinstance(self._global, RedisPacer):
            await self._global.close()

    def _bucket(self, endpoint: str) -> TokenBucket:
        bucket = self._endpoints.get(endpoint)
        if bucket is None:
            target = self._endpoint_targets.get(endpoint, self.target_rate)
            bucket = self._endpoints[endpoint] = TokenBucket(target)
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(bucket.rate)
        return bucket

    async def acquire(self, endpoint: str) -> None:
        await self._global.acquire()
        await self._bucket(endpoint).acquire()

    def penalize(self, endpoint: str, cooldown_seconds: float) -> tuple[float, float]:
        bucket = self._bucket(endpoint)
        old_rate = bucket.rate
        new_rate = bucket.slow_down()
        bucket.cooldown(cooldown_seconds)
        ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(new_rate)
        return old_rate, new_rate

    def recover(self, endpoint: str) -> float | None:
        bucket = self._bucket(endpoint)
        recovered = bucket.recover()
        if recovered is not None:
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(recovered)
        return recovered

    def effective_rate(self, endpoint: str) -> float:
        return self._bucket(endpoint).rate
