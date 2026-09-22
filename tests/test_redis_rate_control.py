import asyncio
import os
import time
from uuid import uuid4

import pytest

from dbd.client.rate_control import AdmissionState, EndpointRateController, RedisPacer

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



async def test_healthy_shared_admission_is_one_redis_round_trip():
    key = "dbd:test-fast-admission:" + uuid4().hex
    controller = EndpointRateController(
        1000,
        redis_url=REDIS_URL,
        redis_key=key,
        shared_rate_ceiling=1000,
    )
    await controller.open()
    try:
        admission = await controller.acquire("profile")
        assert admission.shared is True
        assert controller.redis_admissions == 1
        assert controller.redis_commands == 1

        # An ordinary healthy success does not write Redis recovery state.
        await controller.recover("profile", admission=admission)
        assert controller.redis_commands == 1
    finally:
        if controller._global_shared is not None:
            await controller._global_shared.clear_state()
        endpoint = controller._shared_endpoint("profile")
        if endpoint is not None:
            await endpoint.clear_state()
        await controller.close()


async def test_endpoint_penalty_is_immediately_visible_to_another_controller():
    key = "dbd:test-fast-penalty:" + uuid4().hex
    first = EndpointRateController(
        100,
        redis_url=REDIS_URL,
        redis_key=key,
        shared_rate_ceiling=100,
    )
    second = EndpointRateController(
        100,
        redis_url=REDIS_URL,
        redis_key=key,
        shared_rate_ceiling=100,
    )
    await first.open()
    await second.open()
    try:
        await first.penalize("profile", 0)
        observed = await second.acquire("profile")
        assert observed.shared is True
        assert second.effective_rate("profile") == pytest.approx(50)
    finally:
        for controller in (first, second):
            if controller._global_shared is not None:
                await controller._global_shared.clear_state()
            endpoint = controller._shared_endpoint("profile")
            if endpoint is not None:
                await endpoint.clear_state()
        await first.close()
        await second.close()



async def test_half_open_success_is_fenced_to_probe_owner():
    key = "dbd:test-half-open-owner:" + uuid4().hex
    controller = EndpointRateController(
        100,
        redis_url=REDIS_URL,
        redis_key=key,
        shared_rate_ceiling=100,
    )
    await controller.open()
    try:
        state_key, opened_key, failures_key, window_key, probe_key = (
            controller._breaker._keys("__global__")
        )
        await controller._redis.set(state_key, "half_open", px=60_000)
        await controller._redis.set(probe_key, "real-owner", px=60_000)

        await controller._shared_success_recovery(
            None,
            AdmissionState(
                global_state="half_open",
                probe_token="wrong-owner",
                shared=True,
            ),
        )
        assert await controller._redis.get(state_key) == "half_open"
        assert await controller._redis.get(probe_key) == "real-owner"

        await controller._shared_success_recovery(
            None,
            AdmissionState(
                global_state="half_open",
                probe_token="real-owner",
                shared=True,
            ),
        )
        assert await controller._redis.get(state_key) == "closed"
        assert await controller._redis.get(probe_key) is None
        assert await controller._redis.get(opened_key) is None
        assert await controller._redis.get(failures_key) is None
        assert await controller._redis.get(window_key) is None
    finally:
        await controller._redis.delete(
            *controller._breaker._keys("__global__"),
        )
        await controller.close()
