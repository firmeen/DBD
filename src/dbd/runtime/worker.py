from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from uuid import uuid4

from ..client.async_client import AsyncDbdClient
from ..contracts.identity import parse_code
from ..core.exceptions import (
    ContractError,
    DbdAuthError,
    DbdForbiddenError,
    IdentityConflict,
)
from ..core.redis import create_async_redis, redis_pool_snapshot
from ..core.settings import settings
from ..core.telemetry import (
    IN_FLIGHT,
    PIPELINE_QUEUE_DEPTH,
    PIPELINE_STAGE_TOTAL,
    TASKS,
    log,
)
from ..storage.mysql.repositories.projection_repository import LeaseLost, project
from .coordination import CampaignCoordinator, WorkerIdentity
from .resources import DEFAULT_PARTS, FINANCIAL, endpoints, request_path

STATUS_OPERATING = ("1",)
_REDIS_HEARTBEAT_SECONDS = 2.0
_MAINTENANCE_INTERVAL_SECONDS = 5.0
_MAINTENANCE_LEASE_SECONDS = 8
_DRAIN_DB_POLL_SECONDS = 0.5
_IDLE_PROGRESS_POLL_SECONDS = 1.0


def code_batches(path: str, size: int):
    batch = []
    with open(path, encoding="utf-8-sig") as handle:
        for line_no, line in enumerate(handle, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                batch.append(parse_code(line))
            except ContractError as exc:
                raise ContractError(f"Invalid company code on input line {line_no}: {exc}") from None
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch


async def prepare(
    repo,
    *,
    file=None,
    master=False,
    parts=None,
    fiscal_year=None,
    statuses=None,
    scheduler_mode="legacy",
):
    resources = endpoints(list(parts or DEFAULT_PARTS))
    run_id = await repo.create_run(
        str(Path(file).resolve()) if file else "company_master",
        list(parts or DEFAULT_PARTS),
        fiscal_year,
        scheduler_mode=scheduler_mode,
    )
    if file:
        for batch in code_batches(file, settings.dbd_write_batch_size):
            await repo.import_batch(run_id, batch, resources)
    elif master:
        after = 0
        while (
            after := await repo.enqueue_master(
                run_id, resources, after, settings.dbd_write_batch_size, statuses
            )
        ) is not None:
            pass
    else:
        raise ValueError("Choose --file or --master")
    await repo.ready(run_id)
    return run_id


async def _finish_if_owned(repo, task, status, error=None) -> bool:
    finished = await repo.finish(task, status, error)
    if not finished:
        log.warning("lease_lost_before_finish", task_id=task["id"], endpoint=task["endpoint"])
    return finished


def _attempt_count(task) -> int:
    if task.get("stage") == "project":
        return int(task.get("project_attempts", 0))
    return int(task["attempts"])


async def _fail_task(repo, task, exc, archive_id=None) -> str | None:
    if isinstance(exc, IdentityConflict):
        code = type(exc).__name__
        finished = await repo.finish_identity_conflict(task, archive_id, code)
        if finished:
            TASKS.labels(task["endpoint"], "conflict").inc()
            PIPELINE_STAGE_TOTAL.labels("project", "conflict").inc()
            log.warning(
                "identity_conflict",
                task_id=task["id"],
                company_id=task["company_id"],
                endpoint=task["endpoint"],
                error_code=code,
            )
            return "failed"
        return None

    terminal = isinstance(exc, (ContractError, DbdForbiddenError, DbdAuthError, ValueError, TypeError))
    exhausted = _attempt_count(task) >= settings.dbd_max_task_attempts
    status = "failed" if terminal or exhausted else "retry_wait"
    code = type(exc).__name__
    if archive_id:
        await repo.mark_archive(
            archive_id, "invalid" if isinstance(exc, ContractError) else "failed", code
        )
    if await _finish_if_owned(repo, task, status, code):
        TASKS.labels(task["endpoint"], status).inc()
        PIPELINE_STAGE_TOTAL.labels(task.get("stage", "fetch"), status).inc()
        log.warning(
            "task_failed",
            task_id=task["id"],
            endpoint=task["endpoint"],
            stage=task.get("stage"),
            error_code=code,
            status=status,
        )
        return status
    return None


async def _queue_get(queue: asyncio.Queue, stop: asyncio.Event):
    while True:
        if stop.is_set() and queue.empty():
            return None
        try:
            return await asyncio.wait_for(queue.get(), 0.25)
        except TimeoutError:
            continue


async def process(
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
    """Run staged claim -> fetch -> archive -> project with graceful fleet draining."""
    fetch_workers = concurrency if concurrency is not None else settings.dbd_concurrency
    if not 1 <= fetch_workers <= 256 or (limit is not None and limit < 1):
        raise ValueError("Concurrency must be 1..256 and limit must be positive")
    if distributed and not settings.dbd_redis_url:
        raise ValueError("Distributed workers require DBD_REDIS_URL for one shared global rate cap")

    _ = repo.archive_backend

    projection_workers = min(settings.dbd_projection_workers, settings.db_pool_size)
    archive_workers = settings.dbd_archive_workers
    owner = os.environ.get("DBD_WORKER_ID") or uuid4().hex
    stop = asyncio.Event()
    drain_requested = asyncio.Event()
    fetch_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.dbd_pipeline_queue_size)
    archive_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.dbd_pipeline_queue_size)
    project_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.dbd_pipeline_queue_size)
    writes = asyncio.Semaphore(projection_workers)
    claimed_attempts = 0
    maintenance_at = 0.0
    drain_db_check_at = 0.0
    idle_progress_at = 0.0
    coordinator = None
    identity = None
    client_ref = None
    redis_client = None
    runtime_stats = {
        "completed": 0,
        "done": 0,
        "failed": 0,
        "retry_wait": 0,
        "not_available": 0,
        "fetch_active": 0,
        "archive_active": 0,
        "project_active": 0,
    }

    if distributed:
        redis_client = create_async_redis(
            settings.dbd_redis_url,
            max_connections=settings.dbd_redis_max_connections,
        )
        await redis_client.ping()
        coordinator = CampaignCoordinator(
            settings.dbd_redis_url,
            run_id,
            max_connections=settings.dbd_redis_max_connections,
            client=redis_client,
        )
        await coordinator.open()
        identity = WorkerIdentity(owner, int(run_id), os.getpid())
        registration_status = await coordinator.register_worker(identity)
        if registration_status == "draining":
            drain_requested.set()

    def queue_metrics():
        PIPELINE_QUEUE_DEPTH.labels("fetch").set(fetch_queue.qsize())
        PIPELINE_QUEUE_DEPTH.labels("archive").set(archive_queue.qsize())
        PIPELINE_QUEUE_DEPTH.labels("project").set(project_queue.qsize())

    def heartbeat_metrics() -> dict[str, object]:
        client = client_ref
        effective_rate = None
        if client is not None:
            controller = getattr(client, "endpoint_rate", None)
            if controller is not None:
                try:
                    effective_rate = float(controller.global_effective_rate())
                except Exception:
                    effective_rate = None
        active = {
            "fetch": int(runtime_stats["fetch_active"]),
            "archive": int(runtime_stats["archive_active"]),
            "project": int(runtime_stats["project_active"]),
        }
        current_stage = "idle"
        for stage in ("project", "archive", "fetch"):
            if active[stage] > 0:
                current_stage = stage
                break
        redis_commands = 0
        redis_admissions = 0
        if client is not None:
            controller = getattr(client, "endpoint_rate", None)
            if controller is not None:
                redis_commands = int(getattr(controller, "redis_commands", 0) or 0)
                redis_admissions = int(getattr(controller, "redis_admissions", 0) or 0)
        pool = redis_pool_snapshot(redis_client) if redis_client is not None else {}
        return {
            "current_stage": current_stage,
            "claimed": int(claimed_attempts),
            "completed": int(runtime_stats["completed"]),
            "done": int(runtime_stats["done"]),
            "failed": int(runtime_stats["failed"]),
            "retry_wait": int(runtime_stats["retry_wait"]),
            "not_available": int(runtime_stats["not_available"]),
            "fetch_active": active["fetch"],
            "archive_active": active["archive"],
            "project_active": active["project"],
            "fetch_queue": fetch_queue.qsize(),
            "archive_queue": archive_queue.qsize(),
            "project_queue": project_queue.qsize(),
            "inflight": sum(active.values())
            + fetch_queue.qsize()
            + archive_queue.qsize()
            + project_queue.qsize(),
            "requests": int(getattr(client, "requests_made", 0) or 0),
            "http_retries": int(getattr(client, "retries_made", 0) or 0),
            "rate_limit_hits": int(getattr(client, "rate_limit_hits", 0) or 0),
            "effective_rate": effective_rate,
            "redis_commands": redis_commands,
            "redis_admissions": redis_admissions,
            "redis_pool_in_use": int(pool.get("in_use", 0)),
            "redis_pool_created": int(pool.get("created", 0)),
            "redis_pool_max": int(pool.get("max", 0)),
            "drain_requested": drain_requested.is_set(),
        }

    async def publish_heartbeat(*, status: str | None = None) -> str | None:
        if coordinator is None or identity is None:
            return None
        resolved = await coordinator.heartbeat_worker(
            identity,
            status=status or ("draining" if drain_requested.is_set() else "active"),
            metrics=heartbeat_metrics(),
        )
        # The heartbeat Lua transition evaluates stop + targeted drain atomically.
        # This replaces the separate 0.5s control polling Redis round trip.
        if resolved == "draining":
            drain_requested.set()
        return resolved

    async def mysql_heartbeat():
        while not stop.is_set():
            if health:
                await asyncio.to_thread(health)
            await repo.heartbeat(owner)
            try:
                await asyncio.wait_for(stop.wait(), min(10, settings.dbd_lease_seconds / 3))
            except TimeoutError:
                pass

    async def redis_heartbeat():
        if coordinator is None or identity is None:
            return
        while not stop.is_set():
            await publish_heartbeat()
            try:
                await asyncio.wait_for(stop.wait(), _REDIS_HEARTBEAT_SECONDS)
            except TimeoutError:
                pass

    async def scheduler():
        nonlocal claimed_attempts, maintenance_at, drain_db_check_at, idle_progress_at
        while not stop.is_set():
            now = time.monotonic()
            if now >= maintenance_at:
                should_maintain = True
                if coordinator is not None and identity is not None:
                    should_maintain = await coordinator.acquire_maintenance_slot(
                        identity, ttl_seconds=_MAINTENANCE_LEASE_SECONDS
                    )
                if should_maintain:
                    await repo.maintain(run_id)
                maintenance_at = now + _MAINTENANCE_INTERVAL_SECONDS

            if drain_requested.is_set():
                # Do not hammer MySQL while in-memory work is visibly still moving.
                if not (fetch_queue.empty() and archive_queue.empty() and project_queue.empty()):
                    await asyncio.sleep(0.1)
                    continue
                now = time.monotonic()
                if now < drain_db_check_at:
                    await asyncio.sleep(min(0.1, drain_db_check_at - now))
                    continue
                drain_db_check_at = now + _DRAIN_DB_POLL_SECONDS
                if await repo.owner_running(owner) == 0:
                    stop.set()
                    return
                await asyncio.sleep(0.1)
                continue

            remaining = None if limit is None else max(0, limit - claimed_attempts)
            if remaining == 0:
                drain_requested.set()
                await publish_heartbeat(status="draining")
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
                idle_progress_at = time.monotonic() + _IDLE_PROGRESS_POLL_SECONDS
                await asyncio.sleep(0)
                continue

            now = time.monotonic()
            if now >= idle_progress_at:
                idle_progress_at = now + _IDLE_PROGRESS_POLL_SECONDS
                if not await repo.has_active_work(run_id):
                    drain_requested.set()
                    await publish_heartbeat(status="draining")
                    continue
            await asyncio.sleep(0.1)

    async def fetcher(client):
        while True:
            task = await _queue_get(fetch_queue, stop)
            if task is None:
                return
            runtime_stats["fetch_active"] += 1
            try:
                endpoint = task["endpoint"]
                year = await repo.fiscal_year(task) if endpoint in FINANCIAL else None
                if endpoint in FINANCIAL and year is None:
                    if await _finish_if_owned(repo, task, "not_available", "no_submitted_fiscal_year"):
                        TASKS.labels(endpoint, "not_available").inc()
                        PIPELINE_STAGE_TOTAL.labels("fetch", "not_available").inc()
                        runtime_stats["completed"] += 1
                        runtime_stats["not_available"] += 1
                    IN_FLIGHT.dec()
                    continue
                path = request_path(endpoint, task["jp_type_code"], task["jp_no"])
                params = {"fiscalYear": year} if year else None
                response = await client.fetch("GET", path, params=params, rate_key=endpoint)
                await archive_queue.put(
                    {"task": task, "response": response, "path": path, "params": params}
                )
                PIPELINE_STAGE_TOTAL.labels("fetch", "success").inc()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                status = await _fail_task(repo, task, exc)
                if status == "failed":
                    runtime_stats["failed"] += 1
                elif status == "retry_wait":
                    runtime_stats["retry_wait"] += 1
                IN_FLIGHT.dec()
            finally:
                runtime_stats["fetch_active"] -= 1
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
            runtime_stats["archive_active"] += len(batch)
            try:
                archived = await repo.archive_batch(batch)
                archived_ids = {item["task"]["id"] for item in archived}
                for item in archived:
                    await project_queue.put(item)
                    PIPELINE_STAGE_TOTAL.labels("archive", "success").inc()
                for item in batch:
                    if item["task"]["id"] not in archived_ids:
                        PIPELINE_STAGE_TOTAL.labels("archive", "lease_lost").inc()
                        IN_FLIGHT.dec()
                        log.warning(
                            "lease_lost_before_archive",
                            task_id=item["task"]["id"],
                            endpoint=item["task"]["endpoint"],
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                for item in batch:
                    status = await _fail_task(repo, item["task"], exc)
                    if status == "failed":
                        runtime_stats["failed"] += 1
                    elif status == "retry_wait":
                        runtime_stats["retry_wait"] += 1
                    IN_FLIGHT.dec()
            finally:
                runtime_stats["archive_active"] -= len(batch)
                for _ in batch:
                    archive_queue.task_done()
                queue_metrics()

    async def projector():
        while True:
            item = await _queue_get(project_queue, stop)
            if item is None:
                return
            runtime_stats["project_active"] += 1
            task = item["task"]
            archive_id = item.get("archive_id") or task.get("raw_response_id")
            try:
                if not archive_id:
                    raise ValueError("Project-stage task has no raw_response_id")
                if not item.get("project_attempt_started"):
                    project_attempts = await repo.begin_project_attempt(task)
                    if project_attempts is None:
                        raise LeaseLost("Lease lost before projection attempt")
                    task = dict(task)
                    task["project_attempts"] = project_attempts
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
                        project, task, archive_id, payload, requested_at, path, scope
                    )
                TASKS.labels(task["endpoint"], outcome).inc()
                PIPELINE_STAGE_TOTAL.labels("project", outcome).inc()
                runtime_stats["completed"] += 1
                if outcome == "not_available":
                    runtime_stats["not_available"] += 1
                else:
                    runtime_stats["done"] += 1
            except asyncio.CancelledError:
                raise
            except LeaseLost:
                PIPELINE_STAGE_TOTAL.labels("project", "lease_lost").inc()
                log.warning("lease_lost", task_id=task["id"], endpoint=task["endpoint"])
            except Exception as exc:
                status = await _fail_task(repo, task, exc, archive_id)
                if status == "failed":
                    runtime_stats["failed"] += 1
                elif status == "retry_wait":
                    runtime_stats["retry_wait"] += 1
            finally:
                runtime_stats["project_active"] -= 1
                IN_FLIGHT.dec()
                project_queue.task_done()
                queue_metrics()

    client_kwargs = {
        "concurrency": fetch_workers,
        "rate_per_second": rate,
        "endpoint_rates": endpoint_rates,
        "redis_url": settings.dbd_redis_url if distributed else None,
        "redis_key": settings.dbd_redis_rate_key,
    }
    if distributed:
        client_kwargs["redis_client"] = redis_client

    try:
        async with client_factory(**client_kwargs) as client:
            client_ref = client
            async with asyncio.TaskGroup() as group:
                if coordinator is not None:
                    group.create_task(redis_heartbeat())
                group.create_task(mysql_heartbeat())
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
        if coordinator is not None and identity is not None:
            await coordinator.unregister_worker(identity)
            await coordinator.close()
        if redis_client is not None:
            await redis_client.aclose()

    await repo.close_if_complete(run_id)
    return await repo.progress(run_id)
