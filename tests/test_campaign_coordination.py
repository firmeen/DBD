import os
from uuid import uuid4

import pytest

from dbd.client.rate_control import EndpointRateController
from dbd.core.redis import create_async_redis
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
        assert await first.register_worker(worker, ttl_seconds=5) == "active"
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
        assert workers[0]["status"] == "draining"

        assert await first.unregister_worker(worker) is True
        assert await second.workers() == []
    finally:
        await first.clear_stop()
        await first.close()
        await second.close()


async def test_worker_self_fences_when_required_supervisor_lease_disappears():
    run_id = int(uuid4().hex[:8], 16)
    namespace = "dbd:test-supervisor-fence:" + uuid4().hex
    coordinator = CampaignCoordinator(
        REDIS_URL,
        run_id,
        namespace=namespace,
        require_supervisor_lease=True,
    )
    await coordinator.open()
    try:
        await coordinator._client().set(coordinator.supervisor_lease_key, "owner", ex=5)
        worker = WorkerIdentity("worker-a", run_id, 111)
        assert await coordinator.register_worker(worker, ttl_seconds=5) == "active"

        await coordinator._client().delete(coordinator.supervisor_lease_key)
        assert await coordinator.heartbeat_worker(worker, ttl_seconds=5) == "draining"
        workers = await coordinator.workers()
        assert workers[0]["status"] == "draining"
    finally:
        await coordinator.close()


async def test_preexisting_targeted_drain_survives_register_and_active_heartbeat():
    run_id = int(uuid4().hex[:8], 16)
    namespace = "dbd:test-pre-register-drain:" + uuid4().hex
    coordinator = CampaignCoordinator(REDIS_URL, run_id, namespace=namespace)
    worker = WorkerIdentity("worker-a", run_id, 111)
    await coordinator.open()
    try:
        await coordinator.request_worker_drain(worker.worker_id, "scale_down", ttl_seconds=5)
        assert await coordinator.register_worker(worker, ttl_seconds=5) == "draining"

        workers = await coordinator.workers()
        assert len(workers) == 1
        assert workers[0]["status"] == "draining"
        started_at = workers[0]["started_at"]

        heartbeat_status = await coordinator.heartbeat_worker(
            worker,
            status="active",
            ttl_seconds=5,
        )
        assert heartbeat_status == "draining"
        workers = await coordinator.workers()
        assert workers[0]["status"] == "draining"
        assert workers[0]["started_at"] == started_at
        assert await coordinator.worker_drain_requested(worker) is True
    finally:
        await coordinator.unregister_worker(worker)
        await coordinator.close()


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



async def test_campaign_coordinator_applies_connection_pool_cap():
    run_id = int(uuid4().hex[:8], 16)
    coordinator = CampaignCoordinator(
        REDIS_URL,
        run_id,
        namespace="dbd:test-capped-campaign:" + uuid4().hex,
        max_connections=2,
    )
    await coordinator.open()
    try:
        assert coordinator._client().connection_pool.max_connections == 2
    finally:
        await coordinator.close()



async def test_coordinator_and_rate_controller_share_one_external_redis_client():
    run_id = int(uuid4().hex[:8], 16)
    namespace = "dbd:test-shared-client:" + uuid4().hex
    rate_key = namespace + ":rate"
    client = create_async_redis(REDIS_URL, max_connections=3)
    coordinator = CampaignCoordinator(
        REDIS_URL,
        run_id,
        namespace=namespace,
        client=client,
    )
    controller = EndpointRateController(
        1000,
        redis_url=REDIS_URL,
        redis_key=rate_key,
        redis_client=client,
    )
    try:
        await coordinator.open()
        await controller.open()
        assert coordinator._client() is client
        assert controller._redis is client
        assert coordinator._client().connection_pool is controller._redis.connection_pool

        await coordinator.close()
        await controller.close()

        # Neither component owns the worker-wide shared client.
        assert await client.ping() is True
    finally:
        await client.aclose()
