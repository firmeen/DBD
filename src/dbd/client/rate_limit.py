from __future__ import annotations

import asyncio
import math
import time

from aiolimiter import AsyncLimiter


class TokenBucket:
    """Fixed-rate local limiter with an explicit cooldown boundary."""

    def __init__(self, rate_per_second: float, burst=None):
        _ = burst
        if rate_per_second <= 0 or not math.isfinite(rate_per_second):
            raise ValueError("rate_per_second must be positive")
        self.target_rate = float(rate_per_second)
        self.rate = self.target_rate
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0

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
        self._cooldown_until = max(
            self._cooldown_until,
            time.monotonic() + max(0.0, float(seconds)),
        )

    def _set_rate(self, value: float) -> float:
        value = float(value)
        if value <= 0 or not math.isfinite(value):
            raise ValueError("rate_per_second must be positive")
        self.rate = value
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        return self.rate
