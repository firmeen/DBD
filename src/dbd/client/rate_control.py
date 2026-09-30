from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ..core.redis import close_async_redis, create_async_redis, redis_pool_snapshot
from ..core.settings import settings
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
    """Redis admission is temporarily unavailable.

    Distributed workers must fail closed before HTTP and let durable work return
    to MySQL attempt-neutrally instead of converting control-plane loss into a
    business-resource failure.
    """


class RedisPacer:
    """Small fixed-rate Redis pacer.

    This compatibility primitive intentionally has no adaptive penalty or circuit
    state. Its configured rate is a hard ceiling; cooldown only delays admission.
    """

    _ACQUIRE_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local ceiling = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local runtime_target = tonumber(redis.call('GET', KEYS[3]) or ceiling)
if runtime_target == nil or runtime_target <= 0 then runtime_target = ceiling end
local rate = math.min(ceiling, runtime_target)

local cooldown_until = tonumber(redis.call('GET', KEYS[2]) or 0)
if cooldown_until > now_ms then
  return {tostring(cooldown_until - now_ms), tostring(rate), 'cooldown'}
end

local next_ms = tonumber(redis.call('GET', KEYS[1]) or now_ms)
if next_ms < now_ms then next_ms = now_ms end
local interval_ms = math.max(1, 1000 / rate)
local wait_ms = next_ms - now_ms
redis.call('PSETEX', KEYS[1], ttl_ms, tostring(next_ms + interval_ms))
if redis.call('EXISTS', KEYS[2]) == 1 then redis.call('PEXPIRE', KEYS[2], ttl_ms) end
if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('PEXPIRE', KEYS[3], ttl_ms) end
return {tostring(wait_ms), tostring(rate), 'admitted'}
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
        url: str,
        key: str,
        rate_per_second: float,
        *,
        client: Redis | None = None,
        max_connections: int | None = None,
    ):
        self.url = url
        self.key = key
        self.rate = _positive_rate(rate_per_second)
        self.target_rate = self.rate
        self._redis: Redis | None = client
        self._owns_redis = client is None
        self._max_connections = max_connections
        self._state_ttl_ms = 600_000
        self._acquire_registered = None
        self._cooldown_registered = None

    @property
    def _keys(self) -> tuple[str, str, str]:
        return (
            f"{self.key}:hard-next-ms",
            f"{self.key}:hard-cooldown-until-ms",
            f"{self.key}:target-rate",
        )

    async def open(self) -> None:
        if self._redis is None:
            self._redis = create_async_redis(
                self.url,
                max_connections=self._max_connections,
            )
        await self._redis.ping()
        await self.prepare_scripts()

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            await close_async_redis(self._redis)
            self._redis = None
        self._acquire_registered = None
        self._cooldown_registered = None

    async def prepare_scripts(self) -> None:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        self._acquire_registered = self._redis.register_script(self._ACQUIRE_SCRIPT)
        self._cooldown_registered = self._redis.register_script(self._COOLDOWN_SCRIPT)
        await self._redis.script_load(self._ACQUIRE_SCRIPT)
        await self._redis.script_load(self._COOLDOWN_SCRIPT)

    def set_rate(self, value: float) -> float:
        self.rate = self.target_rate = _positive_rate(value)
        return self.rate

    async def acquire(self) -> float:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        if self._acquire_registered is None:
            self._acquire_registered = self._redis.register_script(self._ACQUIRE_SCRIPT)
        while True:
            try:
                wait_raw, rate_raw, mode = await self._acquire_registered(
                    keys=list(self._keys),
                    args=[self.target_rate, self._state_ttl_ms],
                )
            except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
                raise RateControlUnavailable(
                    f"Redis hard admission unavailable: {type(exc).__name__}"
                ) from exc
            self.rate = float(rate_raw)
            wait_seconds = max(0.0, float(wait_raw) / 1000.0)
            if str(mode) == "admitted":
                if wait_seconds:
                    await asyncio.sleep(wait_seconds)
                return self.rate
            await asyncio.sleep(max(0.01, wait_seconds))

    async def cooldown(self, seconds: float) -> float:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        if self._cooldown_registered is None:
            self._cooldown_registered = self._redis.register_script(self._COOLDOWN_SCRIPT)
        try:
            wait_raw = await self._cooldown_registered(
                keys=[self._keys[1]],
                args=[max(0.0, float(seconds)) * 1000.0, self._state_ttl_ms],
            )
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            raise RateControlUnavailable(
                f"Redis cooldown update unavailable: {type(exc).__name__}"
            ) from exc
        return max(0.0, float(wait_raw) / 1000.0)

    async def penalize(
        self,
        cooldown_seconds: float,
        *,
        factor: float = 0.5,
        floor: float = 0.5,
    ) -> tuple[float, float]:
        _ = factor, floor
        old_rate = self.rate
        await self.cooldown(cooldown_seconds)
        return old_rate, old_rate

    async def recover(
        self,
        *,
        factor: float = 1.25,
        step: float = 0.5,
        quiet_seconds: float = 30.0,
    ) -> float | None:
        _ = factor, step, quiet_seconds
        return None

    async def clear_state(self) -> None:
        if self._redis is None:
            raise RuntimeError("RedisPacer is not open")
        await self._redis.delete(*self._keys)


@dataclass(frozen=True)
class AdmissionState:
    global_state: str = "closed"
    endpoint_state: str | None = None
    probe_token: str | None = None
    shared: bool = False


class EndpointRateController:
    """Worker-local limiting plus one Redis-enforced hard fleet ceiling.

    Authentication, transport, 5xx and generic availability failures are worker
    local. Redis owns only request admission and explicit cooldown timing. An
    endpoint Retry-After never advances the global reservation cursor, so unrelated
    endpoints continue using available fleet capacity.
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
  return {
    tostring(global_cooldown - now_ms),
    'global_cooldown',
    tostring(global_rate),
    tostring(endpoint_ceiling)
  }
end

if has_endpoint then
  local endpoint_cooldown = tonumber(redis.call('GET', KEYS[5]) or 0)
  if endpoint_cooldown > now_ms then
    return {
      tostring(endpoint_cooldown - now_ms),
      'endpoint_cooldown',
      tostring(global_rate),
      tostring(endpoint_ceiling)
    }
  end

  if endpoint_ceiling > 0 then
    local endpoint_next = tonumber(redis.call('GET', KEYS[4]) or now_ms)
    if endpoint_next > now_ms then
      return {
        tostring(endpoint_next - now_ms),
        'endpoint_rate',
        tostring(global_rate),
        tostring(endpoint_ceiling)
      }
    end
  end
end

local global_next = tonumber(redis.call('GET', KEYS[1]) or now_ms)
if global_next < now_ms then global_next = now_ms end
local wait_ms = global_next - now_ms
local global_interval = math.max(1, 1000 / global_rate)
redis.call('PSETEX', KEYS[1], ttl_ms, tostring(global_next + global_interval))

if endpoint_ceiling > 0 and has_endpoint then
  local endpoint_interval = math.max(1, 1000 / endpoint_ceiling)
  redis.call('PSETEX', KEYS[4], ttl_ms, tostring(global_next + endpoint_interval))
end

if redis.call('EXISTS', KEYS[2]) == 1 then redis.call('PEXPIRE', KEYS[2], ttl_ms) end
if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('PEXPIRE', KEYS[3], ttl_ms) end
if redis.call('EXISTS', KEYS[5]) == 1 then redis.call('PEXPIRE', KEYS[5], ttl_ms) end

return {
  tostring(wait_ms),
  'admitted',
  tostring(global_rate),
  tostring(endpoint_ceiling)
}
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
        circuit_failure_threshold: int = 5,
        circuit_window_seconds: float = 30.0,
        circuit_open_seconds: float = 30.0,
        global_pressure_window_seconds: float = 10.0,
        global_pressure_endpoint_threshold: int = 2,
        circuit_probe_seconds: float = 40.0,
    ):
        _ = (
            circuit_failure_threshold,
            circuit_window_seconds,
            circuit_open_seconds,
            global_pressure_window_seconds,
            global_pressure_endpoint_threshold,
            circuit_probe_seconds,
        )
        self.target_rate = _positive_rate(global_rate)
        self.shared_rate_ceiling = _positive_rate(shared_rate_ceiling or global_rate)
        self._endpoint_targets = {
            str(key): _positive_rate(value) for key, value in (endpoint_rates or {}).items()
        }
        self._redis_url = redis_url
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
        self._last_shared_rate = self.shared_rate_ceiling
        self.redis_commands = 0
        self.redis_admissions = 0
        self._global_shared = None
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
        pool = redis_pool_snapshot(self._redis)
        REDIS_POOL_CONNECTIONS.labels("created").set(int(pool.get("created", 0)))
        REDIS_POOL_CONNECTIONS.labels("in_use").set(int(pool.get("in_use", 0)))

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            await close_async_redis(self._redis)
            self._redis = None
        self._admission_script = None
        self._cooldown_script = None

    def _target_for(self, endpoint: str) -> float:
        return min(self.target_rate, self._endpoint_targets.get(endpoint, self.target_rate))

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

    def _shared_endpoint(self, endpoint: str):
        _ = endpoint
        return None

    def _shared_keys(self, endpoint: str | None) -> list[str]:
        endpoint_key = endpoint or "__global__"
        return [
            f"{self._redis_key}:hard-next-ms",
            f"{self._redis_key}:hard-cooldown-until-ms",
            f"{self._redis_key}:target-rate",
            f"{self._redis_key}:endpoint:{endpoint_key}:hard-next-ms",
            f"{self._redis_key}:endpoint:{endpoint_key}:cooldown-until-ms",
        ]

    def _observe_redis_command(self, operation: str, started: float) -> None:
        elapsed = time.perf_counter() - started
        self.redis_commands += 1
        REDIS_COMMANDS.labels(operation).inc()
        REDIS_RTT.labels(operation).observe(elapsed)
        if self._redis is not None:
            pool = redis_pool_snapshot(self._redis)
            REDIS_POOL_CONNECTIONS.labels("created").set(int(pool.get("created", 0)))
            REDIS_POOL_CONNECTIONS.labels("in_use").set(int(pool.get("in_use", 0)))

    async def _shared_admit(self, endpoint: str | None) -> AdmissionState:
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
                        self._shared_target_for(endpoint) if endpoint is not None else 0.0,
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
            mode = str(result[1])
            self._last_shared_rate = float(result[2])
            if mode == "admitted":
                self.redis_admissions += 1
                if wait_seconds:
                    await asyncio.sleep(wait_seconds)
                return AdmissionState(shared=True)
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
                args=[max(0.0, float(seconds)) * 1000.0, self._state_ttl_ms],
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

    async def acquire(self, endpoint: str | None) -> AdmissionState:
        if endpoint is not None:
            await self._bucket(endpoint).acquire()
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(self._bucket(endpoint).rate)
        await self._global_local.acquire()
        EFFECTIVE_RATE.set(self.target_rate)
        if self._redis is None:
            return AdmissionState(shared=False)
        return await self._shared_admit(endpoint)

    async def cooldown(self, endpoint: str | None, seconds: float) -> float:
        seconds = max(0.0, float(seconds))
        if endpoint is None:
            self._global_local.cooldown(seconds)
        else:
            self._bucket(endpoint).cooldown(seconds)
        if self._redis is not None:
            seconds = await self._shared_cooldown(endpoint, seconds)
        return seconds

    async def availability_failure(self, endpoint: str | None = None) -> str:
        _ = endpoint
        return "closed"

    def circuit_state(self, endpoint: str | None = None) -> str:
        _ = endpoint
        return "closed"

    async def penalize(
        self,
        endpoint: str,
        cooldown_seconds: float,
    ) -> tuple[float, float, float, float, bool]:
        old_endpoint = self.effective_rate(endpoint)
        old_global = self.global_effective_rate()
        await self.cooldown(endpoint, cooldown_seconds)
        return old_endpoint, old_endpoint, old_global, old_global, False

    async def penalize_global(self, cooldown_seconds: float) -> tuple[float, float]:
        old_global = self.global_effective_rate()
        await self.cooldown(None, cooldown_seconds)
        return old_global, old_global

    async def recover(
        self,
        endpoint: str,
        *,
        admission: AdmissionState | None = None,
    ) -> tuple[float | None, float | None]:
        _ = endpoint, admission
        return None, None

    async def recover_global(
        self,
        *,
        admission: AdmissionState | None = None,
    ) -> float | None:
        _ = admission
        return None

    def effective_rate(self, endpoint: str) -> float:
        return self._bucket(endpoint).rate

    def global_effective_rate(self) -> float:
        return self.target_rate
