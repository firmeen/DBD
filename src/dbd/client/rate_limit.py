from __future__ import annotations

import asyncio
import time

from aiolimiter import AsyncLimiter


class TokenBucket:
    """One manager-wide paced limiter with shared cooldown and conservative recovery."""

    def __init__(self, rate_per_second: float, burst=None):
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        self.target_rate = float(rate_per_second)
        self.rate = self.target_rate
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0
        self._last_penalty = 0.0
        self._penalty_streak = 0

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

    def _set_rate(self, value: float) -> float:
        self.rate = float(value)
        self._limiter = AsyncLimiter(1, 1 / self.rate)
        return self.rate

    def slow_down(self, factor=0.5, floor=0.5):
        self._last_penalty = time.monotonic()
        return self._set_rate(min(self.rate, max(floor, self.rate * factor)))

    def penalize(self, retry_after_seconds: float) -> tuple[float, float]:
        """Slow down and hold admission long enough to drain a rolling server window.

        DBD Search can return a short Retry-After even while the preceding request
        window still carries quota debt. Re-entering immediately can therefore
        produce a 429 cascade that repeatedly halves the effective rate. Consecutive
        penalties inside one recovery window receive progressively longer protective
        cooldowns, while the server-provided Retry-After remains a hard minimum.
        """
        now = time.monotonic()
        if self._last_penalty <= 0 or now - self._last_penalty >= 30.0:
            self._penalty_streak = 0
        self._penalty_streak = min(10, self._penalty_streak + 1)
        new_rate = self.slow_down()
        debt_seconds = (2 ** min(self._penalty_streak, 4)) / max(new_rate, 0.5)
        protective = min(30.0, max(2.0, debt_seconds))
        cooldown_seconds = max(0.0, float(retry_after_seconds), protective)
        self.cooldown(cooldown_seconds)
        return new_rate, cooldown_seconds

    def recover(self, factor=1.25, step=0.5, quiet_seconds=30.0):
        """Increase effective rate slowly after a quiet period with no new penalty.

        Recovery never exceeds the user/configured target and advances at most
        once per quiet window. This avoids a permanent 0.5 req/s floor after one
        transient 429/503 without creating an immediate retry/admission storm.
        """
        if self.rate >= self.target_rate:
            return None
        now = time.monotonic()
        if now < self._cooldown_until or now - self._last_penalty < quiet_seconds:
            return None
        new_rate = min(self.target_rate, max(self.rate + step, self.rate * factor))
        if new_rate <= self.rate:
            return None
        self._last_penalty = now
        self._penalty_streak = 0
        return self._set_rate(new_rate)
