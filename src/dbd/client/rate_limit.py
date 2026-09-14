from __future__ import annotations

import asyncio
import time

from aiolimiter import AsyncLimiter


class TokenBucket:
    """One manager-wide paced limiter, with shared adaptive cooldown."""

    def __init__(self, rate_per_second: float, burst=None):
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        self.rate = float(rate_per_second)
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0

    async def acquire(self):
        # Serializing admission lets a changed limiter/cooldown affect queued
        # requests too, instead of leaving waiters on an obsolete limiter.
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

    def slow_down(self, factor=0.5, floor=0.5):
        self.rate = min(self.rate, max(floor, self.rate * factor))
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        return self.rate
