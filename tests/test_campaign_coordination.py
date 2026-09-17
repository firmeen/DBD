import os
from uuid import uuid4

import pytest

from dbd.runtime.coordination import CampaignCoordinator, WorkerIdentity

REDIS_URL = os.getenv("DBD_REDIS_TEST_URL")
pytestmark = pytest.mark.skipif(not REDIS_URL, reason="DBD_REDIS_TEST_URL is not configured")


async def test_campaign_stop_and_worker_heartbeat_are_shared():
    run_id = int(uuid4().hex[:8], 16)
    namespace = "dbd:test-campaign:" + uuid4().hex
    first = CampaignCoordinator(REDIS_URL, run_id, namespace=namespace)
    second = CampaignCoordinator(REDIS_URL, run_id, namespace=namespace)
    worker = WorkerIdentity("worker-a", run_id, 12345)
    await first.open()
    await second.open()
    try:
        await first.clear_stop()
        await first.register_worker(worker, ttl_seconds=5)
        workers = await second.workers()
        assert [item["worker_id"] for item in workers] == ["worker-a"]
        assert workers[0]["status"] == "active"

        await second.request_stop("test")
        assert await first.stop_requested() is True

        await first.heartbeat_worker(worker, status="draining", ttl_seconds=5)
        workers = await second.workers()
        assert workers[0]["status"] == "draining"

        # Simulate the long-lived workers set expiring while the process remains
        # active. A heartbeat must rebuild membership, otherwise status reports
        # would show zero workers after a 24h campaign.
        await first._client().delete(first.workers_key)
        assert await second.workers() == []
        await first.heartbeat_worker(worker, status="active", ttl_seconds=5)
        workers = await second.workers()
        assert [item["worker_id"] for item in workers] == ["worker-a"]

        assert await first.unregister_worker(worker) is True
        assert await second.workers() == []
    finally:
        await first.clear_stop()
        await first.close()
        await second.close()


async def test_per_worker_drain_is_targeted_and_cleared_on_unregister():
    run_id = int(uuid4().hex[:8], 16)
    namespace = "dbd:test-worker-drain:" + uuid4().hex
    coordinator = CampaignCoordinator(REDIS_URL, run_id, namespace=namespace)
    first = WorkerIdentity("worker-a", run_id, 111)
    second = WorkerIdentity("worker-b", run_id, 222)
    await coordinator.open()
    try:
        await coordinator.register_worker(first, ttl_seconds=5)
        await coordinator.register_worker(second, ttl_seconds=5)
        await coordinator.request_worker_drain(first.worker_id, "scale_down", ttl_seconds=5)

        assert await coordinator.worker_drain_requested(first) is True
        assert await coordinator.worker_drain_requested(second) is False

        assert await coordinator.unregister_worker(first) is True
        assert await coordinator.worker_drain_requested(first) is False
        assert [item["worker_id"] for item in await coordinator.workers()] == ["worker-b"]
    finally:
        await coordinator.unregister_worker(first)
        await coordinator.unregister_worker(second)
        await coordinator.close()


async def test_unregister_failure_is_best_effort_and_close_does_not_mask_errors():
    class BrokenRedis:
        async def delete(self, *args):
            raise ConnectionError("redis unavailable")

        async def srem(self, *args):
            raise ConnectionError("redis unavailable")

        async def aclose(self):
            raise ConnectionError("redis unavailable")

    coordinator = CampaignCoordinator("redis://unused", 1)
    coordinator._redis = BrokenRedis()
    worker = WorkerIdentity("worker-a", 1, 12345)

    assert await coordinator.unregister_worker(worker) is False
    await coordinator.close()
    assert coordinator._redis is None
