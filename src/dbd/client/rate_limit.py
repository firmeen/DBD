from __future__ import annotations

import asyncio
import math
import time

from aiolimiter import AsyncLimiter


def protective_cooldown(server_seconds: float, effective_rate: float, streak: int) -> float:
    """Extend tiny Retry-After values when repeated penalties imply a rolling window."""
    server = max(0.0, float(server_seconds))
    rate = max(0.5, float(effective_rate))
    streak = max(1, int(streak))
    rolling = min(60.0, max(2.0, (2.0 * streak) / rate))
    return max(server, rolling)


class TokenBucket:
    """One manager-wide paced limiter with shared cooldown and conservative recovery."""

    def __init__(self, rate_per_second: float, burst=None):
        if rate_per_second <= 0 or not math.isfinite(rate_per_second):
            raise ValueError("rate_per_second must be positive")
        self.target_rate = float(rate_per_second)
        self.rate = self.target_rate
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0
        self._last_penalty = 0.0

    async def acquire(self):
        async with self._lock:
            while True:
                delay = self._cooldown_until - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                await self._limiter.acquire()
                if time.monotonic() >= self._cooldown_until:
                    return

    def cooldown(self, seconds):
        self._cooldown_until = max(self._cooldown_until, time.monotonic() + seconds)

    def _set_rate(self, value: float) -> float:
        self.rate = float(value)
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        return self.rate

    def slow_down(self, factor=0.5, floor=0.5):
        self._last_penalty = time.monotonic()
        return self._set_rate(min(self.rate, max(floor, self.rate * factor)))

    def recover(self, factor=1.25, step=0.5, quiet_seconds=30.0):
        if self.rate >= self.target_rate:
            return None
        now = time.monotonic()
        if now < self._cooldown_until or now - self._last_penalty < quiet_seconds:
            return None
        new_rate = min(self.target_rate, max(self.rate + step, self.rate * factor))
        if new_rate <= self.rate:
            return None
        self._last_penalty = now
        return self._set_rate(new_rate)
