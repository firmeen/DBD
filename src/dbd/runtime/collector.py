from __future__ import annotations

import asyncio
from contextlib import suppress

from ..client.simple_client import SimpleAsyncDbdClient
from ..core.settings import settings
from ..presentation.collector_dashboard import CollectorDashboard
from ..storage.mysql.async_engine import create_engine
from ..storage.mysql.repositories.worker_repository import WorkerRepository
from .worker import process


def _total(progress: dict) -> int:
    return sum(
        int(progress.get(key, 0) or 0)
        for key in (
            "pending",
            "retry_wait",
            "running",
            "failed",
            "done",
            "not_available",
        )
    )


def _live_state(run_id: int, baseline: dict, runtime: dict) -> dict:
    total = _total(baseline)
    done = int(baseline.get("done", 0)) + int(runtime.get("done", 0))
    not_available = int(baseline.get("not_available", 0)) + int(
        runtime.get("not_available", 0)
    )
    failed = int(baseline.get("failed", 0)) + int(runtime.get("failed", 0))
    terminal = done + not_available + failed
    return {
        **runtime,
        "run_id": int(run_id),
        "total": total,
        "done": done,
        "not_available": not_available,
        "failed": failed,
        "remaining": max(0, total - terminal),
        "retry_wait": int(baseline.get("retry_wait", 0))
        + int(runtime.get("retry_wait", 0)),
    }


async def _wait_for_prior_leases(repo, run_id: int) -> None:
    """Let leases from a crashed prior process expire before same-run resume."""
    await repo.maintain(run_id)
    progress = await repo.progress(run_id)
    if int(progress.get("running", 0) or 0) == 0:
        return

    # Avoid repeatedly scanning a million-row run while waiting for one lease TTL.
    await asyncio.sleep(float(settings.dbd_lease_seconds) + 1.0)
    await repo.maintain(run_id)
    progress = await repo.progress(run_id)
    if int(progress.get("running", 0) or 0):
        raise RuntimeError(
            f"Run {run_id} still has active durable leases; "
            "verify no previous collector process remains and retry after lease expiry"
        )


async def collect_run(
    run_id: int,
    *,
    concurrency: int,
    rate: float,
    limit: int | None = None,
    dashboard_enabled: bool = True,
    health=None,
) -> dict:
    """Resume one durable run in a single Python process.

    This path intentionally avoids Guardian, Supervisor, multi-process workers,
    Redis admission, autoscaling and distributed circuit ownership. MySQL remains
    the durable queue and source of truth.
    """
    engine = create_engine()
    repo = WorkerRepository(engine)
    dashboard = CollectorDashboard(run_id, enabled=dashboard_enabled)

    try:
        run = await repo.get_run(run_id)
        scheduler_repo = repo.for_run(run)
        if scheduler_repo.scheduler_mode != "fabric_v2":
            raise ValueError(
                "dbd-data collect requires a fabric_v2 run; "
                "legacy runs remain available through dbd-data bulk"
            )

        await _wait_for_prior_leases(scheduler_repo, run_id)
        baseline = await scheduler_repo.progress(run_id)
        runtime: dict = {}

        def on_runtime(snapshot: dict) -> None:
            runtime.clear()
            runtime.update(snapshot)
            dashboard.render(_live_state(run_id, baseline, runtime))

        with dashboard:
            dashboard.render(_live_state(run_id, baseline, runtime))
            operator_stop = asyncio.Event()
            paused = False
            process_task = asyncio.create_task(
                process(
                    repo,
                    run_id,
                    concurrency=concurrency,
                    rate=rate,
                    limit=limit,
                    health=health,
                    endpoint_rates=None,
                    distributed=False,
                    client_factory=SimpleAsyncDbdClient,
                    runtime_callback=on_runtime,
                    stop_signal=operator_stop,
                )
            )
            try:
                result = await asyncio.shield(process_task)
            except asyncio.CancelledError:
                # First Ctrl+C becomes a bounded graceful drain instead of
                # cancelling active archive/project work immediately.
                paused = True
                operator_stop.set()
                current = asyncio.current_task()
                if current is not None and hasattr(current, "uncancel"):
                    current.uncancel()
                try:
                    result = await asyncio.shield(process_task)
                except asyncio.CancelledError:
                    # A second interrupt is an explicit force stop. The pipeline
                    # still executes its finally block and releases owned leases.
                    process_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await process_task
                    raise

            exact = await scheduler_repo.progress(run_id)
            final = {
                "run_id": int(run_id),
                "total": _total(exact),
                "done": int(exact.get("done", 0)),
                "not_available": int(exact.get("not_available", 0)),
                "failed": int(exact.get("failed", 0)),
                "retry_wait": int(exact.get("retry_wait", 0)),
                "running": int(exact.get("running", 0)),
            }
            final["remaining"] = max(
                0,
                final["total"]
                - final["done"]
                - final["not_available"]
                - final["failed"],
            )
            final.update(
                {
                    "requests": int(runtime.get("requests", 0)),
                    "http_retries": int(runtime.get("http_retries", 0)),
                    "rate_limit_hits": int(runtime.get("rate_limit_hits", 0)),
                    "effective_rate": float(runtime.get("effective_rate") or rate),
                    "target_rate": float(rate),
                    "paused": paused,
                }
            )
            dashboard.render(final)
            return {**result, **final}
    finally:
        await engine.dispose()
