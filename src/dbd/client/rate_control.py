from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Mapping

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ..core.redis import close_async_redis, create_async_redis, redis_pool_snapshot
from ..core.telemetry import (
    EFFECTIVE_RATE,
    ENDPOINT_EFFECTIVE_RATE,
    REDIS_COMMANDS,
    REDIS_POOL_CONNECTIONS,
    REDIS_RTT,
)
from .rate_limit import TokenBucket


def _positive_rate(value: float) -> float:
    value = float(value)
    if value <= 0 or not math.isfinite(value):
        raise ValueError("rate_per_second must be a finite positive number")
    return value


class RateControlUnavailable(RuntimeError):
    """Redis admission is unavailable before an upstream request can start."""


class EndpointRateController:
    """Worker-local pacing plus one Redis-enforced hard fleet ceiling.

    Redis owns request admission and explicit cooldown timing only. Authentication,
    transport and ordinary upstream failures stay local to the worker process.
    """

    _ADMISSION_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local has_endpoint = ARGV[1] == '1'
local global_ceiling = tonumber(ARGV[2])
local endpoint_ceiling = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local runtime_target = tonumber(redis.call('GET', KEYS[3]) or global_ceiling)
if runtime_target == nil or runtime_target <= 0 then runtime_target = global_ceiling end
local global_rate = math.min(global_ceiling, runtime_target)

local global_cooldown = tonumber(redis.call('GET', KEYS[2]) or 0)
if global_cooldown > now_ms then
  return {tostring(global_cooldown - now_ms), 'global_cooldown', tostring(global_rate)}
end

if has_endpoint then
  local endpoint_cooldown = tonumber(redis.call('GET', KEYS[5]) or 0)
  if endpoint_cooldown > now_ms then
    return {tostring(endpoint_cooldown - now_ms), 'endpoint_cooldown', tostring(global_rate)}
  end

  if endpoint_ceiling > 0 then
    local endpoint_next = tonumber(redis.call('GET', KEYS[4]) or now_ms)
    if endpoint_next > now_ms then
      return {tostring(endpoint_next - now_ms), 'endpoint_rate', tostring(global_rate)}
    end
  end
end

local global_next = tonumber(redis.call('GET', KEYS[1]) or now_ms)
if global_next < now_ms then global_next = now_ms end
local wait_ms = global_next - now_ms
local global_interval = math.max(1, 1000 / global_rate)
redis.call('PSETEX', KEYS[1], ttl_ms, tostring(global_next + global_interval))

if has_endpoint and endpoint_ceiling > 0 then
  local endpoint_interval = math.max(1, 1000 / endpoint_ceiling)
  redis.call('PSETEX', KEYS[4], ttl_ms, tostring(global_next + endpoint_interval))
end

if redis.call('EXISTS', KEYS[2]) == 1 then redis.call('PEXPIRE', KEYS[2], ttl_ms) end
if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('PEXPIRE', KEYS[3], ttl_ms) end
if redis.call('EXISTS', KEYS[5]) == 1 then redis.call('PEXPIRE', KEYS[5], ttl_ms) end

return {tostring(wait_ms), 'admitted', tostring(global_rate)}
"""

    _COOLDOWN_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local requested_until = now_ms + tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local current_until = tonumber(redis.call('GET', KEYS[1]) or 0)
local cooldown_until = math.max(current_until, requested_until)
redis.call('PSETEX', KEYS[1], ttl_ms, tostring(cooldown_until))
return tostring(math.max(0, cooldown_until - now_ms))
"""

    def __init__(
        self,
        global_rate: float,
        *,
        endpoint_rates: Mapping[str, float] | None = None,
        redis_url: str | None = None,
        redis_key: str = "dbd:global-rate",
        shared_rate_ceiling: float | None = None,
        redis_max_connections: int | None = None,
        redis_client: Redis | None = None,
        **_legacy_options,
    ):
        self.target_rate = _positive_rate(global_rate)
        self.shared_rate_ceiling = _positive_rate(shared_rate_ceiling or global_rate)
        self._endpoint_targets = {
            str(key): _positive_rate(value)
            for key, value in (endpoint_rates or {}).items()
        }
        self._redis_key = redis_key
        self._redis = redis_client
        self._owns_redis = redis_client is None and redis_url is not None
        if self._redis is None and redis_url:
            self._redis = create_async_redis(
                redis_url,
                max_connections=redis_max_connections,
            )
        self._global_local = TokenBucket(self.target_rate)
        self._endpoints: dict[str, TokenBucket] = {}
        self._admission_script = None
        self._cooldown_script = None
        self._state_ttl_ms = 600_000
        self.redis_commands = 0
        self.redis_admissions = 0
        EFFECTIVE_RATE.set(self.target_rate)

    async def open(self) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.ping()
            self._admission_script = self._redis.register_script(self._ADMISSION_SCRIPT)
            self._cooldown_script = self._redis.register_script(self._COOLDOWN_SCRIPT)
            await self._redis.script_load(self._ADMISSION_SCRIPT)
            await self._redis.script_load(self._COOLDOWN_SCRIPT)
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise RateControlUnavailable(
                f"Redis hard admission unavailable: {type(exc).__name__}"
            ) from exc
        self._publish_pool_metrics()

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            await close_async_redis(self._redis)
            self._redis = None
        self._admission_script = None
        self._cooldown_script = None

    def _target_for(self, endpoint: str) -> float:
        return min(
            self.target_rate,
            self._endpoint_targets.get(endpoint, self.target_rate),
        )

    def _shared_target_for(self, endpoint: str) -> float:
        configured = self._endpoint_targets.get(endpoint)
        if configured is None:
            return 0.0
        return min(self.shared_rate_ceiling, configured)

    def _bucket(self, endpoint: str) -> TokenBucket:
        bucket = self._endpoints.get(endpoint)
        if bucket is None:
            bucket = self._endpoints[endpoint] = TokenBucket(self._target_for(endpoint))
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(bucket.rate)
        return bucket

    def _shared_keys(self, endpoint: str | None) -> list[str]:
        endpoint_key = endpoint or "__global__"
        return [
            f"{self._redis_key}:hard-next-ms",
            f"{self._redis_key}:hard-cooldown-until-ms",
            f"{self._redis_key}:target-rate",
            f"{self._redis_key}:endpoint:{endpoint_key}:hard-next-ms",
            f"{self._redis_key}:endpoint:{endpoint_key}:cooldown-until-ms",
        ]

    def _publish_pool_metrics(self) -> None:
        if self._redis is None:
            return
        pool = redis_pool_snapshot(self._redis)
        REDIS_POOL_CONNECTIONS.labels("created").set(int(pool.get("created", 0)))
        REDIS_POOL_CONNECTIONS.labels("in_use").set(int(pool.get("in_use", 0)))

    def _observe_redis_command(self, operation: str, started: float) -> None:
        self.redis_commands += 1
        REDIS_COMMANDS.labels(operation).inc()
        REDIS_RTT.labels(operation).observe(time.perf_counter() - started)
        self._publish_pool_metrics()

    async def _shared_admit(self, endpoint: str | None) -> None:
        if self._redis is None:
            raise RuntimeError("Shared Redis admission is not configured")
        if self._admission_script is None:
            self._admission_script = self._redis.register_script(self._ADMISSION_SCRIPT)

        while True:
            started = time.perf_counter()
            try:
                result = await self._admission_script(
                    keys=self._shared_keys(endpoint),
                    args=[
                        "1" if endpoint is not None else "0",
                        self.shared_rate_ceiling,
                        (
                            self._shared_target_for(endpoint)
                            if endpoint is not None
                            else 0.0
                        ),
                        self._state_ttl_ms,
                    ],
                )
            except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
                raise RateControlUnavailable(
                    f"Redis hard admission unavailable: {type(exc).__name__}"
                ) from exc
            finally:
                self._observe_redis_command("admission", started)

            wait_seconds = max(0.0, float(result[0]) / 1000.0)
            if str(result[1]) == "admitted":
                self.redis_admissions += 1
                if wait_seconds:
                    await asyncio.sleep(wait_seconds)
                return
            await asyncio.sleep(max(0.01, wait_seconds))

    async def _shared_cooldown(self, endpoint: str | None, seconds: float) -> float:
        if self._redis is None:
            return max(0.0, float(seconds))
        if self._cooldown_script is None:
            self._cooldown_script = self._redis.register_script(self._COOLDOWN_SCRIPT)

        cooldown_key = self._shared_keys(endpoint)[4 if endpoint is not None else 1]
        started = time.perf_counter()
        try:
            wait_raw = await self._cooldown_script(
                keys=[cooldown_key],
                args=[
                    max(0.0, float(seconds)) * 1000.0,
                    self._state_ttl_ms,
                ],
            )
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise RateControlUnavailable(
                f"Redis cooldown update unavailable: {type(exc).__name__}"
            ) from exc
        finally:
            self._observe_redis_command("cooldown", started)
        return max(0.0, float(wait_raw) / 1000.0)

    def set_target_rate(self, value: float) -> float:
        self.target_rate = _positive_rate(value)
        self._global_local.target_rate = self.target_rate
        self._global_local._set_rate(self.target_rate)
        for endpoint, bucket in self._endpoints.items():
            target = self._target_for(endpoint)
            bucket.target_rate = target
            bucket._set_rate(target)
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(bucket.rate)
        EFFECTIVE_RATE.set(self.target_rate)
        return self.target_rate

    async def acquire(self, endpoint: str | None) -> None:
        if endpoint is not None:
            bucket = self._bucket(endpoint)
            await bucket.acquire()
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(bucket.rate)
        await self._global_local.acquire()
        EFFECTIVE_RATE.set(self.target_rate)
        if self._redis is not None:
            await self._shared_admit(endpoint)

    async def cooldown(self, endpoint: str | None, seconds: float) -> float:
        seconds = max(0.0, float(seconds))
        if endpoint is None:
            self._global_local.cooldown(seconds)
        else:
            self._bucket(endpoint).cooldown(seconds)
        if self._redis is not None:
            return await self._shared_cooldown(endpoint, seconds)
        return seconds

    def global_effective_rate(self) -> float:
        return self.target_rate
