from __future__ import annotations

import asyncio
import time
from pathlib import Path
from uuid import uuid4

from ..client.async_client import AsyncDbdClient
from ..contracts.identity import parse_code
from ..core.exceptions import ContractError, DbdAuthError, DbdForbiddenError
from ..core.settings import settings
from ..core.telemetry import IN_FLIGHT, TASKS, log
from ..storage.mysql.repositories.projection_repository import LeaseLost, project
from .resources import FINANCIAL, endpoints, request_path


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


async def prepare(repo, *, file=None, master=False, parts=None, fiscal_year=None):
    resources = endpoints(parts or ["profile", "details", "financials"])
    run_id = await repo.create_run(
        str(Path(file).resolve()) if file else "company_master",
        parts or ["profile", "details", "financials"],
        fiscal_year,
    )
    if file:
        for batch in code_batches(file, settings.dbd_write_batch_size):
            await repo.import_batch(run_id, batch, resources)
    elif master:
        after = 0
        while (
            after := await repo.enqueue_master(run_id, resources, after, settings.dbd_write_batch_size)
        ) is not None:
            pass
    else:
        raise ValueError("Choose --file or --master")
    await repo.ready(run_id)
    return run_id


async def handle_task(repo, client, task, writes):
    archive_id = None
    try:
        endpoint = task["endpoint"]
        year = await repo.fiscal_year(task) if endpoint in FINANCIAL else None
        if endpoint in FINANCIAL and year is None:
            await repo.finish(task, "not_available", "no_submitted_fiscal_year")
            TASKS.labels(endpoint, "not_available").inc()
            return
        if endpoint == "industry_benchmark" and await repo.benchmark_covered(task, year):
            # Another company in the same TSIC/year/size already fetched this
            # scope in this run, and the row it wrote is the row this request
            # would rewrite unchanged. Skipping removes the request, not data.
            await repo.finish(task, "not_available", "benchmark_already_current")
            TASKS.labels(endpoint, "not_available").inc()
            return
        path = request_path(endpoint, task["jp_type_code"], task["jp_no"])
        params = {"fiscalYear": year} if year else None
        response = await client.fetch("GET", path, params=params)
        archive_id = await repo.archive(task, response, path, params)
        async with writes:
            # Each thread gets a pooled connection and a ContextVar-isolated
            # transaction; the semaphore bounds both DB pressure and payload RAM.
            outcome = await asyncio.to_thread(
                project, task, archive_id, response.data, response.requested_at, path, year or ""
            )
        TASKS.labels(endpoint, outcome).inc()
    except asyncio.CancelledError:
        raise
    except LeaseLost:
        log.warning("lease_lost", task_id=task["id"])
    except Exception as exc:
        terminal = isinstance(exc, (ContractError, DbdForbiddenError, DbdAuthError, ValueError, TypeError))
        status = "failed" if terminal or task["attempts"] >= settings.dbd_max_task_attempts else "retry_wait"
        # Never persist arbitrary transport bodies, tokens, cookies or credentials.
        code = type(exc).__name__
        if archive_id:
            await repo.mark_archive(
                archive_id, "invalid" if isinstance(exc, ContractError) else "failed", code
            )
        await repo.finish(task, status, code)
        TASKS.labels(task["endpoint"], status).inc()
        log.warning(
            "task_failed", task_id=task["id"], endpoint=task["endpoint"], error_code=code, status=status
        )


async def process(
    repo, run_id, *, concurrency=None, rate=None, limit=None, client_factory=AsyncDbdClient, health=None
):
    workers = concurrency if concurrency is not None else settings.dbd_concurrency
    if not 1 <= workers <= 256 or (limit is not None and limit < 1):
        raise ValueError("Concurrency must be 1..256 and limit must be positive")
    owner = uuid4().hex
    writes = asyncio.Semaphore(settings.db_pool_size)
    claims = asyncio.Lock()
    processed = 0
    stop = asyncio.Event()
    remaining_consumers = min(workers, settings.dbd_queue_size)
    maintenance_at = 0.0

    async def heartbeat():
        while not stop.is_set():
            if health:
                await asyncio.to_thread(health)
            await repo.heartbeat(owner)
            try:
                await asyncio.wait_for(stop.wait(), min(10, settings.dbd_lease_seconds / 3))
            except TimeoutError:
                pass

    async def consume(client):
        nonlocal processed, maintenance_at
        while True:
            async with claims:
                if limit is not None and processed >= limit:
                    return
                if time.monotonic() >= maintenance_at:
                    await repo.maintain(run_id)
                    maintenance_at = time.monotonic() + 5
                task = await repo.claim(run_id, owner)
                if task:
                    processed += 1
            if task is None:
                progress = await repo.progress(run_id)
                if not any(progress.get(s, 0) for s in ("pending", "running", "retry_wait")):
                    return
                await asyncio.sleep(1)
                continue
            IN_FLIGHT.inc()
            try:
                await handle_task(repo, client, task, writes)
            finally:
                IN_FLIGHT.dec()

    async def consume_and_signal(client):
        nonlocal remaining_consumers
        try:
            await consume(client)
        finally:
            remaining_consumers -= 1
            if remaining_consumers == 0:
                stop.set()

    try:
        async with client_factory(concurrency=workers, rate_per_second=rate) as client:
            async with asyncio.TaskGroup() as group:
                group.create_task(heartbeat())
                for _ in range(remaining_consumers):
                    group.create_task(consume_and_signal(client))
    finally:
        stop.set()
        # In-progress DB threads may finish during shutdown. Their transaction
        # locks serialize against this release, avoiding false pending outcomes.
        await repo.release(owner)
    await repo.close_if_complete(run_id)
    return await repo.progress(run_id)
