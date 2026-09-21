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
        await first.clear_state()
        started = time.perf_counter()
        await asyncio.gather(first.acquire(), second.acquire())
        elapsed = time.perf_counter() - started
        # One slot is immediate and the other is reserved ~100 ms later. Keep
        # generous tolerance for timer resolution while proving the clients do
        # not each get an independent local 10/s bucket.
        assert elapsed >= 0.06
        assert await first._redis.exists(key) == 1
    finally:
        await first.clear_state()
        await first.close()
        await second.close()


async def test_penalty_and_cooldown_are_shared_across_processes():
    key = "dbd:test-adaptive-rate:" + uuid4().hex
    first = RedisPacer(REDIS_URL, key, 10)
    second = RedisPacer(REDIS_URL, key, 10)
    await first.open()
    await second.open()
    try:
        await first.clear_state()
        old_rate, new_rate = await first.penalize(0.08)
        assert (old_rate, new_rate) == (10, 5)

        started = time.perf_counter()
        observed = await second.acquire()
        elapsed = time.perf_counter() - started
        assert observed == 5
        assert second.rate == 5
        assert elapsed >= 0.05

        recovered = await second.recover(quiet_seconds=0)
        assert recovered == pytest.approx(6.25)
        observed_by_first = await first.acquire()
        assert observed_by_first == pytest.approx(6.25)
    finally:
        await first.clear_state()
        await first.close()
        await second.close()



async def test_redis_pacer_applies_connection_pool_cap():
    key = "dbd:test-capped-rate:" + uuid4().hex
    pacer = RedisPacer(REDIS_URL, key, 10, max_connections=2)
    await pacer.open()
    try:
        assert pacer._redis.connection_pool.max_connections == 2
    finally:
        await pacer.clear_state()
        await pacer.close()
