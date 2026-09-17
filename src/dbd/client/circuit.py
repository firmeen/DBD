from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from uuid import uuid4

from redis.asyncio import Redis


@dataclass
class _LocalCircuit:
    state: str = "closed"
    failures: int = 0
    window_started_at: float = 0.0
    opened_until: float = 0.0
    probe_until: float = 0.0


class AdaptiveCircuitBreaker:
    """Endpoint/global circuit protection with optional Redis-shared state.

    CLOSED collects failures inside a rolling window. Reaching the threshold opens
    the circuit for a bounded cooldown. After the cooldown exactly one caller gets
    a HALF_OPEN probe; success closes the circuit and failure opens it again.

    The pressure tracker is separate from endpoint circuit state. It escalates to a
    global penalty only after multiple distinct endpoints show pressure inside the
    configured window, avoiding fleet-wide collapse from one endpoint-specific 429.
    """

    _BEFORE_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local state = redis.call('GET', KEYS[1]) or 'closed'
local opened_until = tonumber(redis.call('GET', KEYS[2]) or 0)
local probe_ttl = tonumber(ARGV[2])
local state_ttl = tonumber(ARGV[3])

if state == 'open' then
  if opened_until > now_ms then
    return {tostring(opened_until - now_ms), 'open'}
  end
  local claimed = redis.call('SET', KEYS[5], ARGV[1], 'PX', probe_ttl, 'NX')
  if claimed then
    redis.call('PSETEX', KEYS[1], state_ttl, 'half_open')
    return {'0', 'half_open'}
  end
  return {'250', 'open'}
end

if state == 'half_open' then
  local claimed = redis.call('SET', KEYS[5], ARGV[1], 'PX', probe_ttl, 'NX')
  if claimed then
    return {'0', 'half_open'}
  end
  return {'250', 'half_open'}
end

return {'0', 'closed'}
"""

    _FAILURE_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local threshold = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local open_ms = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])
local state = redis.call('GET', KEYS[1]) or 'closed'

if state == 'half_open' or state == 'open' then
  redis.call('PSETEX', KEYS[1], ttl_ms, 'open')
  redis.call('PSETEX', KEYS[2], ttl_ms, tostring(now_ms + open_ms))
  redis.call('PSETEX', KEYS[3], ttl_ms, tostring(threshold))
  redis.call('PSETEX', KEYS[4], ttl_ms, tostring(now_ms))
  redis.call('DEL', KEYS[5])
  return 'open'
end

local window_start = tonumber(redis.call('GET', KEYS[4]) or 0)
local failures = tonumber(redis.call('GET', KEYS[3]) or 0)
if window_start == 0 or (now_ms - window_start) > window_ms then
  window_start = now_ms
  failures = 0
end
failures = failures + 1
redis.call('PSETEX', KEYS[3], ttl_ms, tostring(failures))
redis.call('PSETEX', KEYS[4], ttl_ms, tostring(window_start))

if failures >= threshold then
  redis.call('PSETEX', KEYS[1], ttl_ms, 'open')
  redis.call('PSETEX', KEYS[2], ttl_ms, tostring(now_ms + open_ms))
  redis.call('DEL', KEYS[5])
  return 'open'
end
redis.call('PSETEX', KEYS[1], ttl_ms, 'closed')
return 'closed'
"""

    _SUCCESS_SCRIPT = """
local now = redis.call('TIME')
local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)
local window_ms = tonumber(ARGV[1])
local ttl_ms = tonumber(ARGV[2])
local state = redis.call('GET', KEYS[1]) or 'closed'

if state == 'half_open' then
  redis.call('PSETEX', KEYS[1], ttl_ms, 'closed')
  redis.call('DEL', KEYS[2], KEYS[3], KEYS[4], KEYS[5])
  return 'closed'
end

if state == 'closed' then
  local window_start = tonumber(redis.call('GET', KEYS[4]) or 0)
  if window_start > 0 and (now_ms - window_start) > window_ms then
    redis.call('DEL', KEYS[3], KEYS[4])
  end
end
return state
"""

    def __init__(
        self,
        *,
        redis: Redis | None,
        namespace: str,
        failure_threshold: int = 5,
        window_seconds: float = 30.0,
        open_seconds: float = 30.0,
        pressure_window_seconds: float = 10.0,
        pressure_endpoint_threshold: int = 2,
        probe_seconds: float = 40.0,
    ) -> None:
        self.redis = redis
        self.namespace = namespace.rstrip(":")
        self.failure_threshold = max(2, int(failure_threshold))
        self.window_seconds = max(0.1, float(window_seconds))
        self.open_seconds = max(0.1, float(open_seconds))
        self.pressure_window_seconds = max(0.1, float(pressure_window_seconds))
        self.pressure_endpoint_threshold = max(2, int(pressure_endpoint_threshold))
        self.probe_seconds = max(1.0, float(probe_seconds))
        self._local: dict[str, _LocalCircuit] = {}
        self._local_pressure: dict[str, float] = {}

    def _keys(self, key: str) -> tuple[str, str, str, str, str]:
        prefix = f"{self.namespace}:{key}"
        return (
            f"{prefix}:state",
            f"{prefix}:opened-until-ms",
            f"{prefix}:failures",
            f"{prefix}:window-start-ms",
            f"{prefix}:probe",
        )

    @property
    def _state_ttl_ms(self) -> int:
        return int(
            max(
                self.window_seconds * 4,
                self.open_seconds * 4,
                self.probe_seconds * 2,
                120.0,
            )
            * 1000
        )

    async def before(self, key: str) -> str:
        """Block admission while OPEN and return the state that admitted the call."""
        while True:
            wait_seconds, state = await self._before_once(key)
            if wait_seconds <= 0:
                return state
            await asyncio.sleep(min(wait_seconds, 1.0))

    async def _before_once(self, key: str) -> tuple[float, str]:
        if self.redis is None:
            state = self._local.setdefault(key, _LocalCircuit())
            now = time.monotonic()
            if state.state == "open":
                if now < state.opened_until:
                    return state.opened_until - now, "open"
                if now >= state.probe_until:
                    state.state = "half_open"
                    state.probe_until = now + self.probe_seconds
                    return 0.0, "half_open"
                return 0.25, "open"
            if state.state == "half_open":
                if now >= state.probe_until:
                    state.probe_until = now + self.probe_seconds
                    return 0.0, "half_open"
                return 0.25, "half_open"
            return 0.0, "closed"

        wait_raw, state = await self.redis.eval(
            self._BEFORE_SCRIPT,
            5,
            *self._keys(key),
            uuid4().hex,
            int(self.probe_seconds * 1000),
            self._state_ttl_ms,
        )
        return max(0.0, float(wait_raw) / 1000.0), str(state)

    async def failure(self, key: str) -> str:
        if self.redis is None:
            state = self._local.setdefault(key, _LocalCircuit())
            now = time.monotonic()
            if state.state in {"open", "half_open"}:
                state.state = "open"
                state.opened_until = now + self.open_seconds
                state.failures = self.failure_threshold
                state.window_started_at = now
                state.probe_until = 0.0
                return state.state
            if state.window_started_at == 0.0 or now - state.window_started_at > self.window_seconds:
                state.window_started_at = now
                state.failures = 0
            state.failures += 1
            if state.failures >= self.failure_threshold:
                state.state = "open"
                state.opened_until = now + self.open_seconds
                state.probe_until = 0.0
            return state.state

        result = await self.redis.eval(
            self._FAILURE_SCRIPT,
            5,
            *self._keys(key),
            self.failure_threshold,
            int(self.window_seconds * 1000),
            int(self.open_seconds * 1000),
            self._state_ttl_ms,
        )
        return str(result)

    async def success(self, key: str) -> str:
        if self.redis is None:
            state = self._local.setdefault(key, _LocalCircuit())
            now = time.monotonic()
            if state.state == "half_open":
                state.state = "closed"
                state.failures = 0
                state.window_started_at = 0.0
                state.opened_until = 0.0
                state.probe_until = 0.0
            elif state.state == "closed" and state.window_started_at:
                if now - state.window_started_at > self.window_seconds:
                    state.failures = 0
                    state.window_started_at = 0.0
            return state.state

        result = await self.redis.eval(
            self._SUCCESS_SCRIPT,
            5,
            *self._keys(key),
            int(self.window_seconds * 1000),
            self._state_ttl_ms,
        )
        return str(result)

    async def record_endpoint_pressure(self, endpoint: str) -> bool:
        """Return True when distinct endpoint pressure justifies a global penalty."""
        if self.redis is None:
            now = time.monotonic()
            cutoff = now - self.pressure_window_seconds
            self._local_pressure = {
                key: observed
                for key, observed in self._local_pressure.items()
                if observed >= cutoff
            }
            self._local_pressure[str(endpoint)] = now
            return len(self._local_pressure) >= self.pressure_endpoint_threshold

        key = f"{self.namespace}:pressure:endpoints"
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - int(self.pressure_window_seconds * 1000)
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.zadd(key, {str(endpoint): now_ms})
            pipe.zremrangebyscore(key, 0, cutoff_ms)
            pipe.zcard(key)
            pipe.pexpire(key, max(1000, int(self.pressure_window_seconds * 3000)))
            results = await pipe.execute()
        return int(results[2] or 0) >= self.pressure_endpoint_threshold
