from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping

from redis.asyncio import Redis

from ..core.telemetry import EFFECTIVE_RATE, ENDPOINT_EFFECTIVE_RATE
from .rate_limit import TokenBucket


def _positive_rate(value: float) -> float:
    value = float(value)
    if value <= 0 or not math.isfinite(value):
        raise ValueError("rate_per_second must be a finite positive number")
    return value


class RedisPacer:
    """Redis-backed smooth adaptive pacer shared by independent processes.

    The pacing schedule, effective rate, cooldown and last-penalty timestamp live in
    Redis. A penalty observed by one process therefore constrains every process that
    uses the same key instead of only slowing the worker that happened to receive
    the 429/503 response.
    """

    _ACQUIRE_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local target = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local rate = tonumber(redis.call('GET', KEYS[2]) or target)
if rate > target then rate = target end
local cooldown_ms = tonumber(redis.call('GET', KEYS[3]) or 0)
local base_ms = now_ms
if cooldown_ms > base_ms then base_ms = cooldown_ms end
local next_ms = tonumber(redis.call('GET', KEYS[1]) or base_ms)
if next_ms < base_ms then next_ms = base_ms end
local interval_ms = math.max(1, 1000 / rate)
local wait_ms = next_ms - now_ms
local reserved_next = next_ms + interval_ms
redis.call('PSETEX', KEYS[1], ttl_ms, tostring(reserved_next))
redis.call('PSETEX', KEYS[2], ttl_ms, tostring(rate))
if cooldown_ms > 0 then redis.call('PEXPIRE', KEYS[3], ttl_ms) end
if redis.call('EXISTS', KEYS[4]) == 1 then redis.call('PEXPIRE', KEYS[4], ttl_ms) end
return {tostring(wait_ms), tostring(rate)}
"""

    _PENALIZE_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local target = tonumber(ARGV[1])
local factor = tonumber(ARGV[2])
local floor_rate = tonumber(ARGV[3])
local cooldown_add_ms = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])
local old_rate = tonumber(redis.call('GET', KEYS[2]) or target)
if old_rate > target then old_rate = target end
local new_rate = math.min(old_rate, math.max(floor_rate, old_rate * factor))
local old_cooldown = tonumber(redis.call('GET', KEYS[3]) or 0)
local cooldown_until = math.max(old_cooldown, now_ms + cooldown_add_ms)
redis.call('PSETEX', KEYS[2], ttl_ms, tostring(new_rate))
redis.call('PSETEX', KEYS[3], ttl_ms, tostring(cooldown_until))
redis.call('PSETEX', KEYS[4], ttl_ms, tostring(now_ms))
return {tostring(old_rate), tostring(new_rate)}
"""

    _RECOVER_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local target = tonumber(ARGV[1])
local factor = tonumber(ARGV[2])
local step = tonumber(ARGV[3])
local quiet_ms = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])
local rate = tonumber(redis.call('GET', KEYS[2]) or target)
if rate > target then rate = target end
local cooldown_until = tonumber(redis.call('GET', KEYS[3]) or 0)
local last_penalty = tonumber(redis.call('GET', KEYS[4]) or 0)
if rate >= target or now_ms < cooldown_until or (now_ms - last_penalty) < quiet_ms then
  redis.call('PSETEX', KEYS[2], ttl_ms, tostring(rate))
  return {'', tostring(rate)}
end
local new_rate = math.min(target, math.max(rate + step, rate * factor))
if new_rate <= rate then
  return {'', tostring(rate)}
end
redis.call('PSETEX', KEYS[2], ttl_ms, tostring(new_rate))
redis.call('PSETEX', KEYS[4], ttl_ms, tostring(now_ms))
return {tostring(new_rate), tostring(new_rate)}
"""

    def __init__(self, url: str, key: str, rate_per_second: float):
        self.url = url
        self.key = key
        self.rate = _positive_rate(rate_per_second)
        self.target_rate = self.rate
        self._redis: Redis | None = None
        self._state_ttl_ms = 600_000

    @property
    def _keys(self) -> tuple[str, str, str, str]:
        return (
            self.key,
            f"{self.key}:effective-rate",
            f"{self.key}:cooldown-until-ms",
            f"{self.key}:last-penalty-ms",
        )

    async def open(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.url, decode_responses=True)
            await self._redis.ping()

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    def set_rate(self, value: float) -> float:
        # Runtime retargeting is used by the local probe. Distributed workers keep
        # one immutable configured ceiling for a run; Redis retains any active
        # shared penalty instead of letting a later process startup erase it.
        self.rate = self.target_rate = _positive_rate(value)
        return self.rate

    async def acquire(self) -> float:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        wait_raw, rate_raw = await self._redis.eval(
            self._ACQUIRE_SCRIPT,
            len(self._keys),
            *self._keys,
            self.target_rate,
            self._state_ttl_ms,
        )
        self.rate = float(rate_raw)
        wait_ms = float(wait_raw)
        if wait_ms > 0:
            await asyncio.sleep(wait_ms / 1000.0)
        return self.rate

    async def penalize(
        self,
        cooldown_seconds: float,
        *,
        factor: float = 0.5,
        floor: float = 0.5,
    ) -> tuple[float, float]:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        old_raw, new_raw = await self._redis.eval(
            self._PENALIZE_SCRIPT,
            len(self._keys),
            *self._keys,
            self.target_rate,
            factor,
            floor,
            max(0.0, float(cooldown_seconds)) * 1000.0,
            self._state_ttl_ms,
        )
        old_rate = float(old_raw)
        self.rate = float(new_raw)
        return old_rate, self.rate

    async def recover(
        self,
        *,
        factor: float = 1.25,
        step: float = 0.5,
        quiet_seconds: float = 30.0,
    ) -> float | None:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        recovered_raw, current_raw = await self._redis.eval(
            self._RECOVER_SCRIPT,
            len(self._keys),
            *self._keys,
            self.target_rate,
            factor,
            step,
            quiet_seconds * 1000.0,
            self._state_ttl_ms,
        )
        self.rate = float(current_raw)
        return float(recovered_raw) if recovered_raw else None

    async def clear_state(self) -> None:
        """Delete only this pacer's test/operational pacing state."""
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        await self._redis.delete(*self._keys)


class EndpointRateController:
    """Global safety cap plus independent adaptive endpoint limiters.

    Every request passes local endpoint/global adaptive buckets. Distributed workers
    additionally pass shared Redis endpoint/global pacers. Redis also carries the
    adaptive penalty and cooldown state, so one process receiving 429/503 slows the
    aggregate fleet rather than only itself.
    """

    def __init__(
        self,
        global_rate: float,
        *,
        endpoint_rates: Mapping[str, float] | None = None,
        redis_url: str | None = None,
        redis_key: str = "dbd:global-rate",
    ):
        self.target_rate = _positive_rate(global_rate)
        self._endpoint_targets = {
            str(key): _positive_rate(value) for key, value in (endpoint_rates or {}).items()
        }
        self._redis_url = redis_url
        self._redis_key = redis_key
        self._global_local = TokenBucket(self.target_rate)
        self._global_shared = (
            RedisPacer(redis_url, redis_key, self.target_rate) if redis_url else None
        )
        self._shared_endpoints: dict[str, RedisPacer] = {}
        if redis_url:
            self._shared_endpoints = {
                endpoint: RedisPacer(
                    redis_url,
                    f"{redis_key}:endpoint:{endpoint}",
                    min(self.target_rate, target),
                )
                for endpoint, target in self._endpoint_targets.items()
            }
        self._endpoints: dict[str, TokenBucket] = {}
        EFFECTIVE_RATE.set(self._global_local.rate)

    async def open(self) -> None:
        if self._global_shared is not None:
            await self._global_shared.open()
        for pacer in self._shared_endpoints.values():
            await pacer.open()

    async def close(self) -> None:
        for pacer in self._shared_endpoints.values():
            await pacer.close()
        if self._global_shared is not None:
            await self._global_shared.close()

    def _target_for(self, endpoint: str) -> float:
        return min(self.target_rate, self._endpoint_targets.get(endpoint, self.target_rate))

    def _bucket(self, endpoint: str) -> TokenBucket:
        bucket = self._endpoints.get(endpoint)
        if bucket is None:
            bucket = self._endpoints[endpoint] = TokenBucket(self._target_for(endpoint))
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(bucket.rate)
        return bucket

    def _endpoint_effective(self, endpoint: str) -> float:
        local = self._bucket(endpoint).rate
        shared = self._shared_endpoints.get(endpoint)
        return min(local, shared.rate) if shared is not None else local

    def _global_effective(self) -> float:
        shared = self._global_shared
        return min(self._global_local.rate, shared.rate) if shared is not None else self._global_local.rate

    def set_target_rate(self, value: float) -> float:
        """Retarget a local probe/controller and reset local adaptive state."""
        self.target_rate = _positive_rate(value)
        self._global_local.target_rate = self.target_rate
        self._global_local._set_rate(self.target_rate)
        if self._global_shared is not None:
            self._global_shared.set_rate(self.target_rate)
        for endpoint, pacer in self._shared_endpoints.items():
            pacer.set_rate(self._target_for(endpoint))
        for endpoint, bucket in self._endpoints.items():
            target = self._target_for(endpoint)
            bucket.target_rate = target
            bucket._set_rate(target)
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(self._endpoint_effective(endpoint))
        EFFECTIVE_RATE.set(self._global_effective())
        return self.target_rate

    async def acquire(self, endpoint: str) -> None:
        # Endpoint admission happens before global admission so a deliberately slow
        # endpoint does not reserve a scarce global slot while waiting locally.
        await self._bucket(endpoint).acquire()
        shared_endpoint = self._shared_endpoints.get(endpoint)
        if shared_endpoint is not None:
            await shared_endpoint.acquire()
        ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(self._endpoint_effective(endpoint))

        await self._global_local.acquire()
        if self._global_shared is not None:
            await self._global_shared.acquire()
        EFFECTIVE_RATE.set(self._global_effective())

    async def penalize(
        self, endpoint: str, cooldown_seconds: float
    ) -> tuple[float, float, float, float]:
        endpoint_bucket = self._bucket(endpoint)
        old_endpoint = self._endpoint_effective(endpoint)
        endpoint_bucket.slow_down()
        endpoint_bucket.cooldown(cooldown_seconds)
        shared_endpoint = self._shared_endpoints.get(endpoint)
        if shared_endpoint is not None:
            await shared_endpoint.penalize(cooldown_seconds)
            if shared_endpoint.rate < endpoint_bucket.rate:
                endpoint_bucket._set_rate(shared_endpoint.rate)
        new_endpoint = self._endpoint_effective(endpoint)
        ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(new_endpoint)

        old_global = self._global_effective()
        self._global_local.slow_down()
        self._global_local.cooldown(cooldown_seconds)
        if self._global_shared is not None:
            await self._global_shared.penalize(cooldown_seconds)
            if self._global_shared.rate < self._global_local.rate:
                self._global_local._set_rate(self._global_shared.rate)
        new_global = self._global_effective()
        EFFECTIVE_RATE.set(new_global)
        return old_endpoint, new_endpoint, old_global, new_global

    async def recover(self, endpoint: str) -> tuple[float | None, float | None]:
        endpoint_bucket = self._bucket(endpoint)
        local_endpoint_recovered = endpoint_bucket.recover()
        shared_endpoint = self._shared_endpoints.get(endpoint)
        shared_endpoint_recovered = None
        if shared_endpoint is not None:
            shared_endpoint_recovered = await shared_endpoint.recover()
        endpoint_changed = local_endpoint_recovered is not None or shared_endpoint_recovered is not None
        endpoint_effective = self._endpoint_effective(endpoint)
        ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(endpoint_effective)

        local_global_recovered = self._global_local.recover()
        shared_global_recovered = None
        if self._global_shared is not None:
            shared_global_recovered = await self._global_shared.recover()
        global_changed = local_global_recovered is not None or shared_global_recovered is not None
        global_effective = self._global_effective()
        EFFECTIVE_RATE.set(global_effective)

        return (
            endpoint_effective if endpoint_changed else None,
            global_effective if global_changed else None,
        )

    def effective_rate(self, endpoint: str) -> float:
        return self._endpoint_effective(endpoint)

    def global_effective_rate(self) -> float:
        return self._global_effective()
