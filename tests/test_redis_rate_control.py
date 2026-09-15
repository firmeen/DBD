import asyncio
import os
import time
from uuid import uuid4

import pytest

from dbd.client.rate_control import RedisPacer

REDIS_URL = os.getenv("DBD_REDIS_TEST_URL")
pytestmark = pytest.mark.skipif(not REDIS_URL, reason="DBD_REDIS_TEST_URL is not configured")


async def test_two_redis_pacers_share_one_global_schedule():
    key = "dbd:test-rate:" + uuid4().hex
    first = RedisPacer(REDIS_URL, key, 10)
    second = RedisPacer(REDIS_URL, key, 10)
    await first.open()
    await second.open()
    try:
        started = time.perf_counter()
        await asyncio.gather(first.acquire(), second.acquire())
        elapsed = time.perf_counter() - started
        # One slot is immediate and the other is reserved ~100 ms later. Keep
        # generous tolerance for timer resolution while proving the clients do
        # not each get an independent local 10/s bucket.
        assert elapsed >= 0.06
        assert await first._redis.exists(key) == 1
    finally:
        if first._redis is not None:
            await first._redis.delete(key)
        await first.close()
        await second.close()
