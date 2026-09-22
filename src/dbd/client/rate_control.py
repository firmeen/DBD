from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import uuid4

from redis.asyncio import Redis

from ..core.redis import create_async_redis, redis_pool_snapshot
from ..core.settings import settings
from ..core.telemetry import (
    EFFECTIVE_RATE,
    ENDPOINT_EFFECTIVE_RATE,
    REDIS_COMMANDS,
    REDIS_POOL_CONNECTIONS,
    REDIS_RTT,
)
from .circuit import AdaptiveCircuitBreaker
from .rate_limit import TokenBucket


def _positive_rate(value: float) -> float:
    value = float(value)
    if value <= 0 or not math.isfinite(value):
        raise ValueError("rate_per_second must be a finite positive number")
    return value


class RedisPacer:
    """Redis-backed smooth adaptive pacer shared by independent processes.

    `target_rate` is a hard configured ceiling. A supervisor may additionally
    publish `<key>:target-rate`; the Lua scripts take the lower value so scale-down
    applies immediately while scale-up preserves any active penalty and recovers
    gradually instead of resetting shared safety state.
    """

    _ACQUIRE_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local ceiling = tonumber(ARGV[1])
local runtime_target = tonumber(redis.call('GET', KEYS[5]) or ceiling)
if runtime_target == nil or runtime_target <= 0 then runtime_target = ceiling end
local target = math.min(ceiling, runtime_target)
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
if redis.call('EXISTS', KEYS[5]) == 1 then redis.call('PEXPIRE', KEYS[5], ttl_ms) end
return {tostring(wait_ms), tostring(rate)}
"""

    _PENALIZE_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local ceiling = tonumber(ARGV[1])
local runtime_target = tonumber(redis.call('GET', KEYS[5]) or ceiling)
if runtime_target == nil or runtime_target <= 0 then runtime_target = ceiling end
local target = math.min(ceiling, runtime_target)
local factor = tonumber(ARGV[2])
local floor_rate = tonumber(ARGV[3])
local cooldown_add_ms = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])
local old_rate = tonumber(redis.call('GET', KEYS[2]) or target)
if old_rate > target then old_rate = target end
local new_rate = math.min(target, math.min(old_rate, math.max(floor_rate, old_rate * factor)))
local old_cooldown = tonumber(redis.call('GET', KEYS[3]) or 0)
local cooldown_until = math.max(old_cooldown, now_ms + cooldown_add_ms)
redis.call('PSETEX', KEYS[2], ttl_ms, tostring(new_rate))
redis.call('PSETEX', KEYS[3], ttl_ms, tostring(cooldown_until))
redis.call('PSETEX', KEYS[4], ttl_ms, tostring(now_ms))
if redis.call('EXISTS', KEYS[5]) == 1 then redis.call('PEXPIRE', KEYS[5], ttl_ms) end
return {tostring(old_rate), tostring(new_rate)}
"""

    _RECOVER_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local ceiling = tonumber(ARGV[1])
local runtime_target = tonumber(redis.call('GET', KEYS[5]) or ceiling)
if runtime_target == nil or runtime_target <= 0 then runtime_target = ceiling end
local target = math.min(ceiling, runtime_target)
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
  if redis.call('EXISTS', KEYS[5]) == 1 then redis.call('PEXPIRE', KEYS[5], ttl_ms) end
  return {'', tostring(rate)}
end
local new_rate = math.min(target, math.max(rate + step, rate * factor))
if new_rate <= rate then
  return {'', tostring(rate)}
end
redis.call('PSETEX', KEYS[2], ttl_ms, tostring(new_rate))
redis.call('PSETEX', KEYS[4], ttl_ms, tostring(now_ms))
if redis.call('EXISTS', KEYS[5]) == 1 then redis.call('PEXPIRE', KEYS[5], ttl_ms) end
return {tostring(new_rate), tostring(new_rate)}
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

    @property
    def _keys(self) -> tuple[str, str, str, str, str]:
        return (
            self.key,
            f"{self.key}:effective-rate",
            f"{self.key}:cooldown-until-ms",
            f"{self.key}:last-penalty-ms",
            f"{self.key}:target-rate",
        )

    async def open(self) -> None:
        if self._redis is None:
            kwargs = {"decode_responses": True}
            if self._max_connections is not None:
                kwargs["max_connections"] = max(1, int(self._max_connections))
            self._redis = Redis.from_url(self.url, **kwargs)
            await self._redis.ping()

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            await self._redis.aclose()
            self._redis = None

    def set_rate(self, value: float) -> float:
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


@dataclass(frozen=True)
class AdmissionState:
    global_state: str = "closed"
    endpoint_state: str | None = None
    shared: bool = False


class EndpointRateController:
    """Hierarchical per-worker + shared fleet/endpoint adaptive admission.

    Each worker has a hard local global cap (`global_rate`). Distributed workers
    additionally share a fleet pacer whose hard ceiling can be larger. Endpoint
    pacers are created lazily for every endpoint so a 429 observed by one process
    constrains that endpoint across the fleet without automatically halving the
    global fleet rate.
    """

    _ADMISSION_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local has_endpoint = ARGV[1] == '1'
local global_ceiling = tonumber(ARGV[2])
local endpoint_ceiling = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])
local probe_token = ARGV[5]
local probe_ttl_ms = tonumber(ARGV[6])
local circuit_ttl_ms = tonumber(ARGV[7])

local function circuit_status(state_key, opened_key, probe_key)
  local state = redis.call('GET', state_key) or 'closed'
  local opened_until = tonumber(redis.call('GET', opened_key) or 0)
  if state == 'open' then
    if opened_until > now_ms then
      return opened_until - now_ms, 'open', false
    end
    if redis.call('EXISTS', probe_key) == 1 then
      return 250, 'open', false
    end
    return 0, 'half_open', true
  end
  if state == 'half_open' then
    if redis.call('EXISTS', probe_key) == 1 then
      return 250, 'half_open', false
    end
    return 0, 'half_open', true
  end
  return 0, 'closed', false
end

local gwait, gstate, gprobe = circuit_status(KEYS[11], KEYS[12], KEYS[15])
local ewait = 0
local estate = 'closed'
local eprobe = false
if has_endpoint then
  ewait, estate, eprobe = circuit_status(KEYS[16], KEYS[17], KEYS[20])
end
if gwait > 0 or ewait > 0 then
  return {
    tostring(math.max(gwait, ewait)),
    '',
    '',
    'circuit',
    gstate,
    estate
  }
end

local global_claimed = false
if gprobe then
  local claimed = redis.call(
    'SET', KEYS[15], probe_token, 'PX', probe_ttl_ms, 'NX'
  )
  if not claimed then
    return {'250', '', '', 'circuit', gstate, estate}
  end
  global_claimed = true
end

local endpoint_claimed = false
if has_endpoint and eprobe then
  local claimed = redis.call(
    'SET', KEYS[20], probe_token, 'PX', probe_ttl_ms, 'NX'
  )
  if not claimed then
    if global_claimed and redis.call('GET', KEYS[15]) == probe_token then
      redis.call('DEL', KEYS[15])
    end
    return {'250', '', '', 'circuit', gstate, estate}
  end
  endpoint_claimed = true
end

if global_claimed then
  redis.call('PSETEX', KEYS[11], circuit_ttl_ms, 'half_open')
  gstate = 'half_open'
end
if endpoint_claimed then
  redis.call('PSETEX', KEYS[16], circuit_ttl_ms, 'half_open')
  estate = 'half_open'
end

local function pacing_state(
  next_key,
  rate_key,
  cooldown_key,
  target_key,
  ceiling
)
  local runtime_target = tonumber(redis.call('GET', target_key) or ceiling)
  if runtime_target == nil or runtime_target <= 0 then
    runtime_target = ceiling
  end
  local target = math.min(ceiling, runtime_target)
  local rate = tonumber(redis.call('GET', rate_key) or target)
  if rate == nil or rate <= 0 then rate = target end
  if rate > target then rate = target end
  local cooldown_until = tonumber(redis.call('GET', cooldown_key) or 0)
  local base_ms = math.max(now_ms, cooldown_until)
  local next_ms = tonumber(redis.call('GET', next_key) or base_ms)
  if next_ms < base_ms then next_ms = base_ms end
  return rate, next_ms
end

local global_rate, global_next = pacing_state(
  KEYS[1], KEYS[2], KEYS[3], KEYS[5], global_ceiling
)
local endpoint_rate = 0
local endpoint_next = now_ms
if has_endpoint then
  endpoint_rate, endpoint_next = pacing_state(
    KEYS[6], KEYS[7], KEYS[8], KEYS[10], endpoint_ceiling
  )
end

local admit_ms = math.max(now_ms, global_next, endpoint_next)
local global_interval = math.max(1, 1000 / global_rate)
redis.call(
  'PSETEX',
  KEYS[1],
  ttl_ms,
  tostring(admit_ms + global_interval)
)
redis.call('PSETEX', KEYS[2], ttl_ms, tostring(global_rate))
if redis.call('EXISTS', KEYS[3]) == 1 then
  redis.call('PEXPIRE', KEYS[3], ttl_ms)
end
if redis.call('EXISTS', KEYS[4]) == 1 then
  redis.call('PEXPIRE', KEYS[4], ttl_ms)
end
if redis.call('EXISTS', KEYS[5]) == 1 then
  redis.call('PEXPIRE', KEYS[5], ttl_ms)
end

if has_endpoint then
  local endpoint_interval = math.max(1, 1000 / endpoint_rate)
  redis.call(
    'PSETEX',
    KEYS[6],
    ttl_ms,
    tostring(admit_ms + endpoint_interval)
  )
  redis.call('PSETEX', KEYS[7], ttl_ms, tostring(endpoint_rate))
  if redis.call('EXISTS', KEYS[8]) == 1 then
    redis.call('PEXPIRE', KEYS[8], ttl_ms)
  end
  if redis.call('EXISTS', KEYS[9]) == 1 then
    redis.call('PEXPIRE', KEYS[9], ttl_ms)
  end
  if redis.call('EXISTS', KEYS[10]) == 1 then
    redis.call('PEXPIRE', KEYS[10], ttl_ms)
  end
end

return {
  tostring(math.max(0, admit_ms - now_ms)),
  tostring(global_rate),
  has_endpoint and tostring(endpoint_rate) or '',
  'paced',
  gstate,
  estate
}
"""

    _SUCCESS_RECOVERY_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local has_endpoint = ARGV[1] == '1'
local global_ceiling = tonumber(ARGV[2])
local endpoint_ceiling = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])
local quiet_ms = tonumber(ARGV[5])
local factor = tonumber(ARGV[6])
local step = tonumber(ARGV[7])
local global_admission_state = ARGV[8]
local endpoint_admission_state = ARGV[9]

local function close_half_open(state_key, opened_key, failures_key, window_key, probe_key)
  redis.call('PSETEX', state_key, ttl_ms, 'closed')
  redis.call('DEL', opened_key, failures_key, window_key, probe_key)
end

if global_admission_state == 'half_open' then
  close_half_open(KEYS[11], KEYS[12], KEYS[13], KEYS[14], KEYS[15])
end
if has_endpoint and endpoint_admission_state == 'half_open' then
  close_half_open(KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20])
end

local function recover_rate(rate_key, cooldown_key, penalty_key, target_key, ceiling)
  local runtime_target = tonumber(redis.call('GET', target_key) or ceiling)
  if runtime_target == nil or runtime_target <= 0 then
    runtime_target = ceiling
  end
  local target = math.min(ceiling, runtime_target)
  local rate = tonumber(redis.call('GET', rate_key) or target)
  if rate == nil or rate <= 0 then rate = target end
  if rate > target then rate = target end
  local cooldown_until = tonumber(redis.call('GET', cooldown_key) or 0)
  local last_penalty = tonumber(redis.call('GET', penalty_key) or 0)
  if rate >= target
    or now_ms < cooldown_until
    or (now_ms - last_penalty) < quiet_ms then
    redis.call('PSETEX', rate_key, ttl_ms, tostring(rate))
    return rate
  end
  local recovered = math.min(target, math.max(rate + step, rate * factor))
  if recovered > rate then
    rate = recovered
    redis.call('PSETEX', rate_key, ttl_ms, tostring(rate))
    redis.call('PSETEX', penalty_key, ttl_ms, tostring(now_ms))
  end
  return rate
end

local global_rate = recover_rate(
  KEYS[2], KEYS[3], KEYS[4], KEYS[5], global_ceiling
)
local endpoint_rate = 0
if has_endpoint then
  endpoint_rate = recover_rate(
    KEYS[7], KEYS[8], KEYS[9], KEYS[10], endpoint_ceiling
  )
end
return {
  tostring(global_rate),
  has_endpoint and tostring(endpoint_rate) or ''
}
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
        self._global_shared = (
            RedisPacer(
                redis_url,
                redis_key,
                self.shared_rate_ceiling,
                client=self._redis,
            )
            if redis_url
            else None
        )
        self._shared_endpoints: dict[str, RedisPacer] = {}
        self._endpoints: dict[str, TokenBucket] = {}
        self._admission_script = None
        self._success_script = None
        self._next_shared_recovery_at = 0.0
        self._global_recovery_needed = False
        self._endpoint_recovery_needed: set[str] = set()
        self.redis_commands = 0
        self.redis_admissions = 0
        self._breaker = AdaptiveCircuitBreaker(
            redis=self._redis,
            namespace=f"{redis_key}:circuit",
            failure_threshold=circuit_failure_threshold,
            window_seconds=circuit_window_seconds,
            open_seconds=circuit_open_seconds,
            pressure_window_seconds=global_pressure_window_seconds,
            pressure_endpoint_threshold=global_pressure_endpoint_threshold,
            probe_seconds=circuit_probe_seconds,
        )
        EFFECTIVE_RATE.set(self._global_effective())

    async def open(self) -> None:
        if self._redis is not None:
            await self._redis.ping()
            self._admission_script = self._redis.register_script(
                self._ADMISSION_SCRIPT
            )
            self._success_script = self._redis.register_script(
                self._SUCCESS_RECOVERY_SCRIPT
            )
            # Preload once so the hot path is EVALSHA from the first request.
            await self._redis.script_load(self._ADMISSION_SCRIPT)
            await self._redis.script_load(self._SUCCESS_RECOVERY_SCRIPT)
            await self._breaker.prepare_scripts()
        elif self._global_shared is not None:
            await self._global_shared.open()

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            await self._redis.aclose()
            self._redis = None
        elif self._global_shared is not None:
            await self._global_shared.close()
        self._admission_script = None
        self._success_script = None

    def _target_for(self, endpoint: str) -> float:
        return min(self.target_rate, self._endpoint_targets.get(endpoint, self.target_rate))

    def _shared_target_for(self, endpoint: str) -> float:
        return min(
            self.shared_rate_ceiling,
            self._endpoint_targets.get(endpoint, self.shared_rate_ceiling),
        )

    def _bucket(self, endpoint: str) -> TokenBucket:
        bucket = self._endpoints.get(endpoint)
        if bucket is None:
            bucket = self._endpoints[endpoint] = TokenBucket(self._target_for(endpoint))
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(bucket.rate)
        return bucket

    def _shared_endpoint(self, endpoint: str) -> RedisPacer | None:
        if self._redis is None or self._redis_url is None:
            return None
        pacer = self._shared_endpoints.get(endpoint)
        if pacer is None:
            pacer = RedisPacer(
                self._redis_url,
                f"{self._redis_key}:endpoint:{endpoint}",
                self._shared_target_for(endpoint),
                client=self._redis,
            )
            self._shared_endpoints[endpoint] = pacer
        return pacer

    def _endpoint_effective(self, endpoint: str) -> float:
        shared = self._shared_endpoints.get(endpoint)
        return shared.rate if shared is not None else self._bucket(endpoint).rate

    def _global_effective(self) -> float:
        shared = self._global_shared
        return shared.rate if shared is not None else self._global_local.rate

    def _shared_keys(self, endpoint: str | None) -> list[str]:
        if self._global_shared is None:
            raise RuntimeError("Shared Redis admission is not configured")
        global_pacer = list(self._global_shared._keys)
        global_circuit = list(self._breaker._keys("__global__"))
        if endpoint is None:
            endpoint_pacer = global_pacer
            endpoint_circuit = global_circuit
        else:
            shared_endpoint = self._shared_endpoint(endpoint)
            if shared_endpoint is None:
                raise RuntimeError("Shared endpoint admission is not configured")
            endpoint_pacer = list(shared_endpoint._keys)
            endpoint_circuit = list(self._breaker._keys(endpoint))
        return [
            *global_pacer,
            *endpoint_pacer,
            *global_circuit,
            *endpoint_circuit,
        ]

    def _observe_redis_command(self, operation: str, started: float) -> None:
        REDIS_RTT.labels(operation).observe(time.perf_counter() - started)
        REDIS_COMMANDS.labels(operation).inc()
        self.redis_commands += 1
        if self._redis is not None:
            snapshot = redis_pool_snapshot(self._redis)
            for state in ("in_use", "available", "created", "max"):
                REDIS_POOL_CONNECTIONS.labels(state).set(snapshot[state])

    async def _shared_admit(self, endpoint: str | None) -> AdmissionState:
        if self._redis is None:
            raise RuntimeError("Shared Redis admission is not configured")
        if self._admission_script is None:
            self._admission_script = self._redis.register_script(
                self._ADMISSION_SCRIPT
            )

        shared_endpoint = self._shared_endpoint(endpoint) if endpoint else None
        endpoint_ceiling = (
            self._shared_target_for(endpoint)
            if endpoint is not None
            else self.shared_rate_ceiling
        )
        keys = self._shared_keys(endpoint)

        while True:
            started = time.perf_counter()
            try:
                result = await self._admission_script(
                    keys=keys,
                    args=[
                        "1" if endpoint is not None else "0",
                        self.shared_rate_ceiling,
                        endpoint_ceiling,
                        self._global_shared._state_ttl_ms,
                        uuid4().hex,
                        int(self._breaker.probe_seconds * 1000),
                        self._breaker._state_ttl_ms,
                    ],
                )
            finally:
                self._observe_redis_command("admission", started)

            wait_ms = max(0.0, float(result[0]))
            mode = str(result[3])
            global_state = str(result[4])
            endpoint_state = str(result[5]) if endpoint is not None else None
            if mode == "circuit":
                await asyncio.sleep(min(max(wait_ms / 1000.0, 0.001), 1.0))
                continue

            global_rate = float(result[1])
            self._global_shared.rate = global_rate
            if global_rate < self.shared_rate_ceiling:
                self._global_recovery_needed = True
            if shared_endpoint is not None and result[2] != "":
                endpoint_rate = float(result[2])
                shared_endpoint.rate = endpoint_rate
                if endpoint_rate < self._shared_target_for(endpoint):
                    self._endpoint_recovery_needed.add(endpoint)
                ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(endpoint_rate)
            EFFECTIVE_RATE.set(global_rate)
            self.redis_admissions += 1
            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000.0)
            return AdmissionState(
                global_state=global_state,
                endpoint_state=endpoint_state,
                shared=True,
            )

    async def _shared_success_recovery(
        self,
        endpoint: str | None,
        admission: AdmissionState,
    ) -> tuple[float | None, float | None]:
        if self._redis is None:
            return None, None
        half_open = admission.global_state == "half_open" or (
            admission.endpoint_state == "half_open"
        )
        endpoint_needs_recovery = (
            endpoint is not None and endpoint in self._endpoint_recovery_needed
        )
        now = time.monotonic()
        if (
            not half_open
            and not self._global_recovery_needed
            and not endpoint_needs_recovery
        ):
            return None, None
        if not half_open and now < self._next_shared_recovery_at:
            return None, None

        if self._success_script is None:
            self._success_script = self._redis.register_script(
                self._SUCCESS_RECOVERY_SCRIPT
            )
        endpoint_ceiling = (
            self._shared_target_for(endpoint)
            if endpoint is not None
            else self.shared_rate_ceiling
        )
        started = time.perf_counter()
        try:
            result = await self._success_script(
                keys=self._shared_keys(endpoint),
                args=[
                    "1" if endpoint is not None else "0",
                    self.shared_rate_ceiling,
                    endpoint_ceiling,
                    self._global_shared._state_ttl_ms,
                    30_000,
                    1.25,
                    0.5,
                    admission.global_state,
                    admission.endpoint_state or "closed",
                ],
            )
        finally:
            self._observe_redis_command("success_recovery", started)

        self._next_shared_recovery_at = (
            now + float(settings.dbd_redis_recovery_interval_seconds)
        )
        previous_global = self._global_shared.rate
        global_rate = float(result[0])
        self._global_shared.rate = global_rate
        self._global_recovery_needed = global_rate < self.shared_rate_ceiling
        global_changed = global_rate if global_rate != previous_global else None

        endpoint_changed = None
        if endpoint is not None and result[1] != "":
            shared_endpoint = self._shared_endpoint(endpoint)
            previous_endpoint = shared_endpoint.rate
            endpoint_rate = float(result[1])
            shared_endpoint.rate = endpoint_rate
            if endpoint_rate >= self._shared_target_for(endpoint):
                self._endpoint_recovery_needed.discard(endpoint)
            else:
                self._endpoint_recovery_needed.add(endpoint)
            if endpoint_rate != previous_endpoint:
                endpoint_changed = endpoint_rate
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(endpoint_rate)
        EFFECTIVE_RATE.set(global_rate)
        return endpoint_changed, global_changed

    def set_target_rate(self, value: float) -> float:
        """Retarget the local per-worker cap without erasing shared fleet penalties."""
        self.target_rate = _positive_rate(value)
        self._global_local.target_rate = self.target_rate
        self._global_local._set_rate(self.target_rate)
        for endpoint, bucket in self._endpoints.items():
            target = self._target_for(endpoint)
            bucket.target_rate = target
            bucket._set_rate(target)
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(self._endpoint_effective(endpoint))
        EFFECTIVE_RATE.set(self._global_effective())
        return self.target_rate

    async def acquire(self, endpoint: str | None) -> AdmissionState:
        if self._redis is None:
            global_state = await self._breaker.before("__global__")
            endpoint_state = None
            if endpoint is not None:
                endpoint_state = await self._breaker.before(endpoint)
                await self._bucket(endpoint).acquire()
                ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(
                    self._endpoint_effective(endpoint)
                )
            await self._global_local.acquire()
            EFFECTIVE_RATE.set(self._global_effective())
            return AdmissionState(
                global_state=global_state,
                endpoint_state=endpoint_state,
                shared=False,
            )

        # Local buckets protect one process. The Redis script then atomically
        # evaluates both circuits and reserves endpoint/global fleet pacing for
        # the same admission timestamp, avoiding four healthy-path Redis RTTs.
        if endpoint is not None:
            await self._bucket(endpoint).acquire()
        await self._global_local.acquire()
        return await self._shared_admit(endpoint)

    async def penalize(
        self,
        endpoint: str,
        cooldown_seconds: float,
    ) -> tuple[float, float, float, float, bool]:
        await self._breaker.failure(endpoint)
        self._endpoint_recovery_needed.add(endpoint)
        endpoint_bucket = self._bucket(endpoint)
        shared_endpoint = self._shared_endpoint(endpoint)
        old_endpoint = self._endpoint_effective(endpoint)
        endpoint_bucket.slow_down()
        endpoint_bucket.cooldown(cooldown_seconds)
        if shared_endpoint is not None:
            await shared_endpoint.penalize(cooldown_seconds)
            if shared_endpoint.rate < endpoint_bucket.rate:
                endpoint_bucket._set_rate(shared_endpoint.rate)
        new_endpoint = self._endpoint_effective(endpoint)
        ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(new_endpoint)

        old_global = self._global_effective()
        global_escalated = await self._breaker.record_endpoint_pressure(endpoint)
        if global_escalated:
            self._global_recovery_needed = True
            await self._breaker.failure("__global__")
            self._global_local.slow_down()
            self._global_local.cooldown(cooldown_seconds)
            if self._global_shared is not None:
                await self._global_shared.penalize(cooldown_seconds)
                if self._global_shared.rate < self._global_local.rate:
                    self._global_local._set_rate(self._global_shared.rate)
        new_global = self._global_effective()
        EFFECTIVE_RATE.set(new_global)
        return old_endpoint, new_endpoint, old_global, new_global, global_escalated

    async def penalize_global(self, cooldown_seconds: float) -> tuple[float, float]:
        self._global_recovery_needed = True
        await self._breaker.failure("__global__")
        old_global = self._global_effective()
        self._global_local.slow_down()
        self._global_local.cooldown(cooldown_seconds)
        if self._global_shared is not None:
            await self._global_shared.penalize(cooldown_seconds)
            if self._global_shared.rate < self._global_local.rate:
                self._global_local._set_rate(self._global_shared.rate)
        new_global = self._global_effective()
        EFFECTIVE_RATE.set(new_global)
        return old_global, new_global

    async def recover(
        self,
        endpoint: str,
        admission: AdmissionState | None = None,
    ) -> tuple[float | None, float | None]:
        endpoint_bucket = self._bucket(endpoint)
        local_endpoint_recovered = endpoint_bucket.recover()
        local_global_recovered = self._global_local.recover()

        if self._redis is None:
            await self._breaker.success(endpoint)
            await self._breaker.success("__global__")
            endpoint_effective = self._endpoint_effective(endpoint)
            global_effective = self._global_effective()
            ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(endpoint_effective)
            EFFECTIVE_RATE.set(global_effective)
            return (
                endpoint_effective if local_endpoint_recovered is not None else None,
                global_effective if local_global_recovered is not None else None,
            )

        shared_endpoint_changed, shared_global_changed = (
            await self._shared_success_recovery(
                endpoint,
                admission or AdmissionState(shared=True),
            )
        )
        endpoint_effective = self._endpoint_effective(endpoint)
        global_effective = self._global_effective()
        ENDPOINT_EFFECTIVE_RATE.labels(endpoint).set(endpoint_effective)
        EFFECTIVE_RATE.set(global_effective)
        return (
            endpoint_effective
            if local_endpoint_recovered is not None
            or shared_endpoint_changed is not None
            else None,
            global_effective
            if local_global_recovered is not None
            or shared_global_changed is not None
            else None,
        )

    async def recover_global(
        self,
        admission: AdmissionState | None = None,
    ) -> float | None:
        local_recovered = self._global_local.recover()
        if self._redis is None:
            await self._breaker.success("__global__")
            effective = self._global_effective()
            EFFECTIVE_RATE.set(effective)
            return effective if local_recovered is not None else None

        _, shared_recovered = await self._shared_success_recovery(
            None,
            admission or AdmissionState(shared=True),
        )
        effective = self._global_effective()
        EFFECTIVE_RATE.set(effective)
        return effective if local_recovered is not None or shared_recovered is not None else None

    def effective_rate(self, endpoint: str) -> float:
        return self._endpoint_effective(endpoint)

    def global_effective_rate(self) -> float:
        return self._global_effective()
