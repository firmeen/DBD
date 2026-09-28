from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from uuid import uuid4

from curl_cffi.requests.exceptions import RequestException as RequestsError

from ..client.http import TransientHttpError
from ..contracts.identity import parse_code
from ..core.exceptions import (
    ContractError,
    DbdAuthError,
    DbdForbiddenError,
    IdentityConflict,
)
from ..core.prepare_dashboard import PreparationSnapshot
from ..core.settings import settings
from ..core.telemetry import (
    IN_FLIGHT,
    PIPELINE_QUEUE_DEPTH,
    PIPELINE_STAGE_TOTAL,
    TASKS,
    log,
)
from ..storage.mysql.repositories.projection_repository import project
from ..storage.mysql.repositories.projection_scheduler import LeaseLost
from .collection_fabric import mask_for_endpoints
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
    force_refresh=False,
    progress=None,
):
    resources = endpoints(list(parts or DEFAULT_PARTS))
    force_refresh_mask = mask_for_endpoints(resources) if force_refresh else 0
    started_at = time.monotonic()
    run_id = await repo.create_run(
        str(Path(file).resolve()) if file else "company_master",
        list(parts or DEFAULT_PARTS),
        fiscal_year,
        scheduler_mode=scheduler_mode,
        force_refresh_mask=force_refresh_mask,
    )
    scheduler_repo = repo.for_run({"scheduler_mode": scheduler_mode})
    batches = 0
    processed = 0
    total = 0
    batch_size = (
        settings.dbd_materialize_batch_size
        if master
        else settings.dbd_write_batch_size
    )

    def emit(phase: str) -> None:
        if progress is None:
            return
        metrics = (
            scheduler_repo.preparation_metrics()
            if hasattr(scheduler_repo, "preparation_metrics")
            else {}
        )
        progress(
            PreparationSnapshot(
                run_id=int(run_id),
                scheduler_mode=scheduler_mode,
                phase=phase,
                processed=int(metrics.get("companies", processed)),
                total=int(total),
                batches=int(batches),
                batch_size=int(batch_size),
                changed=int(metrics.get("changed", 0)),
                stale=int(metrics.get("stale", 0)),
                forced=int(metrics.get("forced", 0)),
                skipped=int(metrics.get("skipped", 0)),
                work_bearing=int(metrics.get("work_bearing", 0)),
                elapsed_seconds=max(0.0, time.monotonic() - started_at),
            )
        )

    if file:
        emit("importing")
        for batch in code_batches(file, batch_size):
            await scheduler_repo.import_batch(run_id, batch, resources)
            batches += 1
            processed += len(batch)
            emit("importing")
    elif master:
        emit("counting")
        population = await repo.master_population()
        total = int(
            population["eligible_all_statuses"]
            if not statuses
            else population["eligible_operating"]
        )
        emit("materializing")
        after = 0
        while True:
            next_after = await scheduler_repo.enqueue_master(
                run_id,
                resources,
                after,
                batch_size,
                statuses,
            )
            if next_after is None:
                break
            after = next_after
            batches += 1
            if not hasattr(scheduler_repo, "preparation_metrics"):
                processed = min(total, processed + batch_size)
            emit("materializing")
    else:
        raise ValueError("Choose --file or --master")

    await scheduler_repo.ready(run_id)
    if master and not hasattr(scheduler_repo, "preparation_metrics"):
        processed = total
    emit("ready")
    return run_id


async def _finish_if_owned(repo, task, status, error=None):
    finished = await repo.finish(task, status, error)
    if not finished:
        log.warning("lease_lost_before_finish", task_id=task["id"], endpoint=task["endpoint"])
    return finished


def _attempt_count(task) -> int:
    if task.get("stage") == "project":
        return int(task.get("project_attempts", 0))
    return int(task["attempts"])


class FailureOutcome(str):
    def __new__(cls, value: str, finish_result=None):
        item = str.__new__(cls, value)
        fallback_completed = 0 if value == "retry_wait" else 1
        item.completed_delta = int(
            getattr(finish_result, "completed_delta", fallback_completed)
        )
        item.done_delta = int(getattr(finish_result, "done_delta", 0))
        item.not_available_delta = int(
            getattr(finish_result, "not_available_delta", 0)
        )
        item.failed_delta = int(
            getattr(
                finish_result,
                "failed_delta",
                1 if value == "failed" else 0,
            )
        )
        return item


def _apply_failure_stats(runtime_stats, outcome) -> None:
    if outcome is None:
        return
    runtime_stats["completed"] += int(getattr(outcome, "completed_delta", 0))
    runtime_stats["done"] += int(getattr(outcome, "done_delta", 0))
    runtime_stats["not_available"] += int(
        getattr(outcome, "not_available_delta", 0)
    )
    runtime_stats["failed"] += int(getattr(outcome, "failed_delta", 0))
    if outcome == "retry_wait":
        runtime_stats["retry_wait"] += 1


def _http_status(exc: BaseException) -> int | None:
    value = getattr(exc, "status_code", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _upstream_transient(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            DbdForbiddenError,
            DbdAuthError,
            TransientHttpError,
            RequestsError,
            TimeoutError,
        ),
    )


async def _record_failure_event(repo, task, exc, *, outcome: str, archive_id=None) -> None:
    try:
        await repo.record_failure_event(
            task,
            outcome=outcome,
            error_code=type(exc).__name__,
            error_message=str(exc) or type(exc).__name__,
            http_status=_http_status(exc),
            attempt=_attempt_count(task),
            archive_id=archive_id,
        )
    except Exception as event_exc:
        log.warning(
            "failure_event_write_failed",
            task_id=task.get("id"),
            endpoint=task.get("endpoint"),
            error_code=type(event_exc).__name__,
        )


async def _fail_task(repo, task, exc, archive_id=None) -> FailureOutcome | None:
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
            await _record_failure_event(
                repo,
                task,
                exc,
                outcome="conflict",
                archive_id=archive_id,
            )
            return FailureOutcome("failed", finished)
        return None

    if task.get("stage", "fetch") == "fetch" and _upstream_transient(exc):
        code = type(exc).__name__
        finished = await repo.defer(
            task,
            code,
            delay_seconds=settings.dbd_upstream_defer_seconds,
        )
        if finished:
            TASKS.labels(task["endpoint"], "retry_wait").inc()
            PIPELINE_STAGE_TOTAL.labels("fetch", "retry_wait").inc()
            log.warning(
                "upstream_deferred",
                task_id=task["id"],
                endpoint=task["endpoint"],
                error_code=code,
                defer_seconds=settings.dbd_upstream_defer_seconds,
            )
            await _record_failure_event(
                repo,
                task,
                exc,
                outcome="deferred",
                archive_id=archive_id,
            )
            return FailureOutcome("retry_wait", finished)
        return None

    terminal = isinstance(exc, (ContractError, ValueError, TypeError))
    exhausted = _attempt_count(task) >= settings.dbd_max_task_attempts
    status = "failed" if terminal or exhausted else "retry_wait"
    code = type(exc).__name__
    if archive_id:
        await repo.mark_archive(
            archive_id, "invalid" if isinstance(exc, ContractError) else "failed", code
        )
    finished = await _finish_if_owned(repo, task, status, code)
    if finished:
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
        await _record_failure_event(
            repo,
            task,
            exc,
            outcome=status,
            archive_id=archive_id,
        )
        return FailureOutcome(status, finished)
    return None


async def _queue_get(
    queue: asyncio.Queue,
    stop: asyncio.Event,
    halt: asyncio.Event | None = None,
):
    while True:
        if halt is not None and halt.is_set():
            return None
        if stop.is_set() and queue.empty():
            return None
        try:
            item = await asyncio.wait_for(queue.get(), 0.25)
        except TimeoutError:
            continue
        if halt is not None and halt.is_set():
            # A drain can race with queue.get(). Put the leased item back so
            # the scheduler can release it together with the rest of the
            # queued-unstarted fetch work.
            queue.put_nowait(item)
            queue.task_done()
            return None
        return item


async def _release_queued_fetch_work(repo, owner: str, queue: asyncio.Queue) -> int:
    """Return queued-but-unstarted fetch leases to durable state attempt-neutrally."""
    released = 0
    while True:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            queue.task_done()
            IN_FLIGHT.dec()
            released += 1
    if released:
        await repo.release(owner)
    return released


async def process(
    repo,
    run_id,
    *,
    concurrency=None,
    rate=None,
    limit=None,
    client_factory=None,
    health=None,
    endpoint_rates=None,
    distributed=False,
    runtime_callback=None,
    stop_signal=None,
    periodic_maintenance=True,
):
    """Run staged claim -> fetch -> archive -> project with graceful draining.

    The default collector uses this pipeline in one Python process without Redis.
    Distributed coordination remains available only for legacy compatibility.
    """
    fetch_workers = concurrency if concurrency is not None else settings.dbd_concurrency
    if not 1 <= fetch_workers <= 256 or (limit is not None and limit < 1):
        raise ValueError("Concurrency must be 1..256 and limit must be positive")
    if distributed and not settings.dbd_redis_url:
        raise ValueError("Distributed workers require DBD_REDIS_URL for one shared global rate cap")

    _ = repo.archive_backend
    run = await repo.get_run(run_id)
    repo = repo.for_run(run)

    projection_workers = min(settings.dbd_projection_workers, settings.db_pool_size)
    archive_workers = settings.dbd_archive_workers
    project_claim_window = max(projection_workers, projection_workers * 2)
    fetch_claim_window = max(
        fetch_workers,
        fetch_workers * int(settings.dbd_fetch_claim_window_factor),
    )
    owner = os.environ.get("DBD_WORKER_ID") or uuid4().hex
    stop = asyncio.Event()
    drain_requested = asyncio.Event()
    release_queued_fetch = asyncio.Event()
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
    close_async_redis = None
    redis_pool_snapshot = None

    if client_factory is None:
        from ..client.async_client import AsyncDbdClient

        client_factory = AsyncDbdClient
    runtime_stats = {
        "completed": 0,
        "done": 0,
        "failed": 0,
        "retry_wait": 0,
        "retry_wait_claimed": 0,
        "not_available": 0,
        "fetch_active": 0,
        "archive_active": 0,
        "project_active": 0,
        "last_error": None,
        "last_error_at": None,
    }

    def remember_runtime_error(exc: BaseException) -> None:
        runtime_stats["last_error"] = (
            f"{type(exc).__name__}: {str(exc) or type(exc).__name__}"
        )
        runtime_stats["last_error_at"] = time.time()

    if distributed:
        from ..core.redis import (
            close_async_redis as _close_async_redis,
            create_async_redis,
            redis_pool_snapshot as _redis_pool_snapshot,
        )
        from .coordination import CampaignCoordinator, WorkerIdentity
        from .locking import assert_manager_lock_free, runtime_handoff_lock

        close_async_redis = _close_async_redis
        redis_pool_snapshot = _redis_pool_snapshot
        redis_client = create_async_redis(
            settings.dbd_redis_url,
            max_connections=settings.dbd_redis_max_connections,
        )
        try:
            # Registration is serialized with collector handoff. Once the
            # collector owns the long-lived manager lock, a legacy worker may
            # not join the run later and create a second HTTP engine.
            with runtime_handoff_lock():
                assert_manager_lock_free()
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
                release_queued_fetch.set()
        except BaseException:
            await close_async_redis(redis_client)
            raise

    def queue_metrics():
        PIPELINE_QUEUE_DEPTH.labels("fetch").set(fetch_queue.qsize())
        PIPELINE_QUEUE_DEPTH.labels("archive").set(archive_queue.qsize())
        PIPELINE_QUEUE_DEPTH.labels("project").set(project_queue.qsize())

    def heartbeat_metrics() -> dict[str, object]:
        client = client_ref
        effective_rate = None
        circuit_global = "unknown"
        if client is not None:
            controller = getattr(client, "endpoint_rate", None)
            if controller is not None:
                try:
                    effective_rate = float(controller.global_effective_rate())
                except Exception:
                    effective_rate = None
                try:
                    circuit_global = str(controller.circuit_state())
                except Exception:
                    circuit_global = "unknown"
            else:
                try:
                    effective_rate = float(getattr(client, "effective_rate"))
                except (AttributeError, TypeError, ValueError):
                    effective_rate = None
                circuit_global = str(getattr(client, "upstream_state", "open"))
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
        pool = (
            redis_pool_snapshot(redis_client)
            if redis_client is not None and redis_pool_snapshot is not None
            else {}
        )
        return {
            "current_stage": current_stage,
            "claimed": int(claimed_attempts),
            "completed": int(runtime_stats["completed"]),
            "done": int(runtime_stats["done"]),
            "failed": int(runtime_stats["failed"]),
            "retry_wait": int(runtime_stats["retry_wait"]),
            "retry_wait_claimed": int(runtime_stats["retry_wait_claimed"]),
            "not_available": int(runtime_stats["not_available"]),
            "fetch_active": active["fetch"],
            "archive_active": active["archive"],
            "project_active": active["project"],
            "fetch_queue": fetch_queue.qsize(),
            "fetch_claim_window": fetch_claim_window,
            "archive_queue": archive_queue.qsize(),
            "project_queue": project_queue.qsize(),
            "project_claim_window": project_claim_window,
            "inflight": sum(active.values())
            + fetch_queue.qsize()
            + archive_queue.qsize()
            + project_queue.qsize(),
            "requests": int(getattr(client, "requests_made", 0) or 0),
            "http_retries": int(getattr(client, "retries_made", 0) or 0),
            "rate_limit_hits": int(getattr(client, "rate_limit_hits", 0) or 0),
            "effective_rate": effective_rate,
            "target_rate": float(getattr(client, "target_rate", rate or settings.dbd_rate_per_second) or 0),
            "circuit_global": circuit_global,
            "upstream_state": str(getattr(client, "upstream_state", circuit_global)),
            "auth_state": str(getattr(client, "auth_state", "unknown")),
            "last_error": runtime_stats["last_error"]
            or getattr(client, "last_error", None),
            "last_error_at": runtime_stats["last_error_at"]
            or getattr(client, "last_error_at", None),
            "redis_commands": redis_commands,
            "redis_admissions": redis_admissions,
            "redis_pool_in_use": int(pool.get("in_use", 0)),
            "redis_pool_created": int(pool.get("created", 0)),
            "redis_pool_max": int(pool.get("max", 0)),
            "drain_requested": drain_requested.is_set(),
        }

    async def publish_heartbeat(
        *,
        status: str | None = None,
        release_on_drain: bool = True,
    ) -> str | None:
        if coordinator is None or identity is None:
            return None
        try:
            resolved = await coordinator.heartbeat_worker(
                identity,
                status=status or ("draining" if drain_requested.is_set() else "active"),
                metrics=heartbeat_metrics(),
            )
        except Exception as exc:
            # Redis is an ephemeral control plane. A transient heartbeat failure
            # must not cancel MySQL-owned in-flight work.
            log.warning(
                "redis_heartbeat_failed",
                worker_id=owner,
                error_code=type(exc).__name__,
            )
            return None
        # The heartbeat Lua transition evaluates stop + targeted drain atomically.
        # This replaces the separate 0.5s control polling Redis round trip.
        if resolved == "draining":
            drain_requested.set()
            if release_on_drain:
                release_queued_fetch.set()
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

    async def runtime_reporter():
        if runtime_callback is None:
            return
        while not stop.is_set():
            try:
                runtime_callback(heartbeat_metrics())
            except Exception as exc:
                log.warning(
                    "runtime_callback_failed",
                    error_code=type(exc).__name__,
                )
            try:
                await asyncio.wait_for(stop.wait(), 0.5)
            except TimeoutError:
                pass

    async def scheduler():
        nonlocal claimed_attempts, maintenance_at, drain_db_check_at, idle_progress_at
        while not stop.is_set():
            if stop_signal is not None and stop_signal.is_set() and not drain_requested.is_set():
                drain_requested.set()
                release_queued_fetch.set()
            now = time.monotonic()
            if periodic_maintenance and now >= maintenance_at:
                should_maintain = True
                if coordinator is not None and identity is not None:
                    try:
                        should_maintain = await coordinator.acquire_maintenance_slot(
                            identity,
                            ttl_seconds=_MAINTENANCE_LEASE_SECONDS,
                        )
                    except Exception as exc:
                        should_maintain = False
                        log.warning(
                            "redis_maintenance_slot_failed",
                            worker_id=owner,
                            error_code=type(exc).__name__,
                        )
                if should_maintain:
                    await repo.maintain(run_id)
                maintenance_at = now + _MAINTENANCE_INTERVAL_SECONDS

            if drain_requested.is_set():
                if release_queued_fetch.is_set():
                    # External/Guardian drain: fetchers stop dequeuing immediately.
                    # Finish only work already inside active HTTP/archive/project,
                    # then return queued-unstarted fetch leases to MySQL.
                    downstream_busy = (
                        int(runtime_stats["fetch_active"]) > 0
                        or not archive_queue.empty()
                        or int(runtime_stats["archive_active"]) > 0
                        or not project_queue.empty()
                        or int(runtime_stats["project_active"]) > 0
                    )
                    if downstream_busy:
                        await asyncio.sleep(0.1)
                        continue
                    released = await _release_queued_fetch_work(repo, owner, fetch_queue)
                    if released:
                        log.info(
                            "drain_released_queued_fetch",
                            worker_id=owner,
                            run_id=run_id,
                            released=released,
                        )
                        queue_metrics()
                else:
                    # Internal bounded invocation (--limit) still finishes every
                    # task it deliberately claimed before stopping.
                    if not (
                        fetch_queue.empty()
                        and archive_queue.empty()
                        and project_queue.empty()
                        and int(runtime_stats["fetch_active"]) == 0
                        and int(runtime_stats["archive_active"]) == 0
                        and int(runtime_stats["project_active"]) == 0
                    ):
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
                if release_queued_fetch.is_set():
                    # Any residual owned lease is no longer represented by active
                    # in-memory work. Release it attempt-neutrally before rechecking.
                    await repo.release(owner)
                await asyncio.sleep(0.1)
                continue

            remaining = None if limit is None else max(0, limit - claimed_attempts)
            if remaining == 0:
                drain_requested.set()
                await publish_heartbeat(status="draining", release_on_drain=False)
                continue

            batch_limit = settings.dbd_claim_batch_size
            if remaining is not None:
                batch_limit = min(batch_limit, remaining)

            project_inflight = int(runtime_stats["project_active"]) + project_queue.qsize()
            project_room = max(0, project_claim_window - project_inflight)
            project_batch = []
            if project_room > 0:
                project_batch = await repo.claim_project_batch(
                    run_id, owner, min(batch_limit, project_room)
                )
                for task in project_batch:
                    if task.get("status") == "retry_wait":
                        runtime_stats["retry_wait_claimed"] += 1
                    IN_FLIGHT.inc()
                    await project_queue.put({"task": task, "project_attempt_started": True})
                claimed_attempts += len(project_batch)

            if limit is not None and claimed_attempts >= limit:
                queue_metrics()
                continue

            remaining = None if limit is None else max(0, limit - claimed_attempts)
            fetch_inflight = int(runtime_stats["fetch_active"]) + fetch_queue.qsize()
            fetch_room = max(0, fetch_claim_window - fetch_inflight)
            fetch_batch = []
            if fetch_room > 0:
                take = min(settings.dbd_claim_batch_size, fetch_room)
                if remaining is not None:
                    take = min(take, remaining)
                if take > 0:
                    fetch_batch = await repo.claim_batch(run_id, owner, stage="fetch", limit=take)
                    for task in fetch_batch:
                        if task.get("status") == "retry_wait":
                            runtime_stats["retry_wait_claimed"] += 1
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
                    await publish_heartbeat(status="draining", release_on_drain=False)
                    continue
            await asyncio.sleep(0.1)

    async def fetcher(client):
        while True:
            task = await _queue_get(fetch_queue, stop, release_queued_fetch)
            if task is None:
                return
            runtime_stats["fetch_active"] += 1
            try:
                endpoint = task["endpoint"]
                year = await repo.fiscal_year(task) if endpoint in FINANCIAL else None
                if endpoint in FINANCIAL and year is None:
                    finished = await _finish_if_owned(
                        repo,
                        task,
                        "not_available",
                        "no_submitted_fiscal_year",
                    )
                    if finished:
                        TASKS.labels(endpoint, "not_available").inc()
                        PIPELINE_STAGE_TOTAL.labels("fetch", "not_available").inc()
                        runtime_stats["completed"] += int(
                            getattr(finished, "completed_delta", 1)
                        )
                        runtime_stats["done"] += int(
                            getattr(finished, "done_delta", 0)
                        )
                        runtime_stats["not_available"] += int(
                            getattr(finished, "not_available_delta", 1)
                        )
                        runtime_stats["failed"] += int(
                            getattr(finished, "failed_delta", 0)
                        )
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
                remember_runtime_error(exc)
                status = await _fail_task(repo, task, exc)
                _apply_failure_stats(runtime_stats, status)
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
                remember_runtime_error(exc)
                for item in batch:
                    status = await _fail_task(repo, item["task"], exc)
                    _apply_failure_stats(runtime_stats, status)
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
                        project,
                        task,
                        archive_id,
                        payload,
                        requested_at,
                        path,
                        scope,
                        repo.scheduler_mode,
                    )
                TASKS.labels(task["endpoint"], outcome).inc()
                PIPELINE_STAGE_TOTAL.labels("project", outcome).inc()
                runtime_stats["completed"] += int(
                    getattr(outcome, "completed_delta", 1)
                )
                runtime_stats["done"] += int(getattr(outcome, "done_delta", 0))
                runtime_stats["not_available"] += int(
                    getattr(outcome, "not_available_delta", 0)
                )
                runtime_stats["failed"] += int(getattr(outcome, "failed_delta", 0))
            except asyncio.CancelledError:
                raise
            except LeaseLost as exc:
                remember_runtime_error(exc)
                PIPELINE_STAGE_TOTAL.labels("project", "lease_lost").inc()
                log.warning("lease_lost", task_id=task["id"], endpoint=task["endpoint"])
            except Exception as exc:
                remember_runtime_error(exc)
                status = await _fail_task(repo, task, exc, archive_id)
                _apply_failure_stats(runtime_stats, status)
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
                if runtime_callback is not None:
                    group.create_task(runtime_reporter())
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
        if redis_client is not None and close_async_redis is not None:
            await close_async_redis(redis_client)

    await repo.close_if_complete(run_id)
    return await repo.progress(run_id)
