from __future__ import annotations

import asyncio
import time
from uuid import uuid4

from ..client.async_client import AsyncDbdClient
from ..core.settings import settings
from ..core.telemetry import IN_FLIGHT, PIPELINE_QUEUE_DEPTH, PIPELINE_STAGE_TOTAL, TASKS, log
from ..storage.mysql.repositories.fabric_projection_repository import project_fabric
from ..storage.mysql.repositories.projection_repository import LeaseLost
from .resources import FINANCIAL, request_path
from .worker import _fail_task, _queue_get


async def process_fabric(
    repo,
    run_id,
    *,
    concurrency=None,
    rate=None,
    limit=None,
    client_factory=AsyncDbdClient,
    health=None,
    endpoint_rates=None,
    distributed=False,
):
    """Run one-row-per-company lazy collection state through fetch/archive/project."""
    fetch_workers = concurrency if concurrency is not None else settings.dbd_concurrency
    if not 1 <= fetch_workers <= 256 or (limit is not None and limit < 1):
        raise ValueError("Concurrency must be 1..256 and limit must be positive")
    if distributed and not settings.dbd_redis_url:
        raise ValueError("Distributed workers require DBD_REDIS_URL")

    _ = repo.archive_backend
    projection_workers = min(settings.dbd_projection_workers, settings.db_pool_size)
    archive_workers = settings.dbd_archive_workers
    owner = uuid4().hex
    stop = asyncio.Event()
    fetch_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.dbd_pipeline_queue_size)
    archive_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.dbd_pipeline_queue_size)
    project_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.dbd_pipeline_queue_size)
    writes = asyncio.Semaphore(projection_workers)
    claimed_attempts = 0
    maintenance_at = 0.0

    def queue_metrics():
        PIPELINE_QUEUE_DEPTH.labels("fabric_fetch").set(fetch_queue.qsize())
        PIPELINE_QUEUE_DEPTH.labels("fabric_archive").set(archive_queue.qsize())
        PIPELINE_QUEUE_DEPTH.labels("fabric_project").set(project_queue.qsize())

    async def heartbeat():
        while not stop.is_set():
            if health:
                await asyncio.to_thread(health)
            await repo.heartbeat(owner)
            try:
                await asyncio.wait_for(stop.wait(), min(10, settings.dbd_lease_seconds / 3))
            except TimeoutError:
                pass

    async def scheduler():
        nonlocal claimed_attempts, maintenance_at
        while not stop.is_set():
            if time.monotonic() >= maintenance_at:
                await repo.maintain(run_id)
                maintenance_at = time.monotonic() + 5

            remaining = None if limit is None else max(0, limit - claimed_attempts)
            if remaining == 0:
                if await repo.owner_running(owner) == 0:
                    stop.set()
                    return
                await asyncio.sleep(0.05)
                continue

            batch_limit = settings.dbd_claim_batch_size
            if remaining is not None:
                batch_limit = min(batch_limit, remaining)

            project_room = project_queue.maxsize - project_queue.qsize()
            project_batch = []
            if project_room > 0:
                project_batch = await repo.claim_project_batch(
                    run_id, owner, min(batch_limit, project_room)
                )
                for task in project_batch:
                    IN_FLIGHT.inc()
                    await project_queue.put({"task": task, "project_attempt_started": True})
                claimed_attempts += len(project_batch)

            if limit is not None and claimed_attempts >= limit:
                queue_metrics()
                continue

            remaining = None if limit is None else max(0, limit - claimed_attempts)
            fetch_room = fetch_queue.maxsize - fetch_queue.qsize()
            fetch_batch = []
            if fetch_room > 0:
                take = min(settings.dbd_claim_batch_size, fetch_room)
                if remaining is not None:
                    take = min(take, remaining)
                if take > 0:
                    fetch_batch = await repo.claim_batch(run_id, owner, stage="fetch", limit=take)
                    for task in fetch_batch:
                        IN_FLIGHT.inc()
                        await fetch_queue.put(task)
                    claimed_attempts += len(fetch_batch)

            queue_metrics()
            if project_batch or fetch_batch:
                await asyncio.sleep(0)
                continue

            progress = await repo.progress(run_id)
            if not any(progress.get(status, 0) for status in ("pending", "running", "retry_wait")):
                stop.set()
                return
            await asyncio.sleep(0.1)

    async def fetcher(client):
        while True:
            task = await _queue_get(fetch_queue, stop)
            if task is None:
                return
            try:
                endpoint = task["endpoint"]
                year = await repo.fiscal_year(task) if endpoint in FINANCIAL else None
                if endpoint in FINANCIAL and year is None:
                    if await repo.finish(task, "not_available", "no_submitted_fiscal_year"):
                        TASKS.labels(endpoint, "not_available").inc()
                        PIPELINE_STAGE_TOTAL.labels("fabric_fetch", "not_available").inc()
                    IN_FLIGHT.dec()
                    continue
                path = request_path(endpoint, task["jp_type_code"], task["jp_no"])
                params = {"fiscalYear": year} if year else None
                response = await client.fetch("GET", path, params=params, rate_key=endpoint)
                await archive_queue.put(
                    {"task": task, "response": response, "path": path, "params": params}
                )
                PIPELINE_STAGE_TOTAL.labels("fabric_fetch", "success").inc()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await _fail_task(repo, task, exc)
                IN_FLIGHT.dec()
            finally:
                fetch_queue.task_done()
                queue_metrics()

    async def archiver():
        while True:
            first = await _queue_get(archive_queue, stop)
            if first is None:
                return
            batch = [first]
            while len(batch) < settings.dbd_archive_batch_size:
                try:
                    batch.append(archive_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                archived = await repo.archive_batch(batch)
                kept = {item["task"]["id"] for item in archived}
                for item in archived:
                    await project_queue.put(item)
                    PIPELINE_STAGE_TOTAL.labels("fabric_archive", "success").inc()
                for item in batch:
                    if item["task"]["id"] not in kept:
                        IN_FLIGHT.dec()
                        log.warning(
                            "fabric_lease_lost_before_archive",
                            state_id=item["task"]["id"],
                            endpoint=item["task"]["endpoint"],
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                for item in batch:
                    await _fail_task(repo, item["task"], exc)
                    IN_FLIGHT.dec()
            finally:
                for _ in batch:
                    archive_queue.task_done()
                queue_metrics()

    async def projector():
        while True:
            item = await _queue_get(project_queue, stop)
            if item is None:
                return
            task = item["task"]
            archive_id = item.get("archive_id") or task.get("raw_response_id")
            try:
                if not archive_id:
                    raise ValueError("Fabric project state has no raw_response_id")
                if not item.get("project_attempt_started"):
                    attempts = await repo.begin_project_attempt(task)
                    if attempts is None:
                        raise LeaseLost("Fabric lease lost before projection")
                    task = dict(task)
                    task["project_attempts"] = attempts
                    item["task"] = task
                    item["project_attempt_started"] = True

                if "response" in item:
                    payload = item["response"].data
                    requested_at = item["response"].requested_at
                    path = item["path"]
                    params = item["params"] or {}
                else:
                    archive = await repo.load_archive(archive_id)
                    payload = archive["data"]
                    requested_at = archive["requested_at"]
                    path = archive["request_path"]
                    params = archive["request_context"].get("params") or {}
                scope = params.get("fiscalYear", "")
                async with writes:
                    outcome = await asyncio.to_thread(
                        project_fabric, task, archive_id, payload, requested_at, path, scope
                    )
                TASKS.labels(task["endpoint"], outcome).inc()
                PIPELINE_STAGE_TOTAL.labels("fabric_project", outcome).inc()
            except asyncio.CancelledError:
                raise
            except LeaseLost:
                PIPELINE_STAGE_TOTAL.labels("fabric_project", "lease_lost").inc()
                log.warning("fabric_lease_lost", state_id=task["id"], endpoint=task["endpoint"])
            except Exception as exc:
                await _fail_task(repo, task, exc, archive_id)
            finally:
                IN_FLIGHT.dec()
                project_queue.task_done()
                queue_metrics()

    kwargs = {
        "concurrency": fetch_workers,
        "rate_per_second": rate,
        "endpoint_rates": endpoint_rates,
        "redis_url": settings.dbd_redis_url if distributed else None,
        "redis_key": settings.dbd_redis_rate_key,
    }
    try:
        async with client_factory(**kwargs) as client:
            async with asyncio.TaskGroup() as group:
                group.create_task(heartbeat())
                group.create_task(scheduler())
                for _ in range(fetch_workers):
                    group.create_task(fetcher(client))
                for _ in range(archive_workers):
                    group.create_task(archiver())
                for _ in range(projection_workers):
                    group.create_task(projector())
    finally:
        stop.set()
        await repo.release(owner)
        queue_metrics()

    await repo.close_if_complete(run_id)
    return await repo.progress(run_id)
