from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async token bucket, so the whole process holds one global request rate.

    Replaces a fixed per-request sleep: a sleep charges the same delay whether
    the server answered in 30 ms or 3 s, and it cannot express "N requests per
    second across 20 workers". This can.
    """

    def __init__(self, rate_per_second: float, burst: int | None = None) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")

        self.rate = float(rate_per_second)
        self.capacity = float(burst if burst is not None else max(1.0, rate_per_second))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
                self._updated = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                deficit = 1.0 - self._tokens

            # Sleep outside the lock so other workers can still refill/observe.
            await asyncio.sleep(deficit / self.rate)

    def slow_down(self, factor: float = 0.5, floor: float = 0.5) -> float:
        """Shed rate after a 429/503. Callers decide when to recover."""
        self.rate = max(floor, self.rate * factor)
        self.capacity = max(1.0, self.rate)
        return self.rate
