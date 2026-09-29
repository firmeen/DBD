from __future__ import annotations

import asyncio
from contextlib import suppress

from sqlalchemy import text

from ..client.simple_client import SimpleAsyncDbdClient
from ..core.settings import settings
from ..presentation.collector_dashboard import CollectorDashboard
from ..storage.mysql.async_engine import create_engine
from ..storage.mysql.migrations import migration_head_revision
from ..storage.mysql.repositories.worker_repository import WorkerRepository
from .locking import runtime_handoff_lock
from .worker import process

_REQUIRED_SCHEMA = {
    "worker_runs": {"id", "status", "scheduler_mode"},
    "company_collection_state": {
        "run_id",
        "company_id",
        "required_mask",
        "done_mask",
        "not_available_mask",
        "failed_mask",
        "dirty_mask",
        "next_endpoint",
        "stage",
        "status",
        "attempts",
        "project_attempts",
        "available_at",
        "lease_owner",
        "lease_version",
        "lease_expires_at",
        "raw_response_id",
    },
    "response_archive": {"id", "run_id", "endpoint", "processing_status"},
}


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


def _live_state(
    run_id: int,
    baseline: dict,
    runtime: dict,
    *,
    preflight: dict | None = None,
) -> dict:
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
        "retry_wait": max(
            0,
            int(baseline.get("retry_wait", 0))
            - int(runtime.get("retry_wait_claimed", 0))
            + int(runtime.get("retry_wait", 0)),
        ),
        "preflight": dict(preflight or {}),
    }


def _assert_postflight_invariants(baseline: dict, exact: dict) -> None:
    """Protect same-run migration invariants after a bounded or full invocation."""
    violations = []
    baseline_total = _total(baseline)
    exact_total = _total(exact)
    if exact_total != baseline_total:
        violations.append(f"total changed {baseline_total}->{exact_total}")

    for key, label in (
        ("done", "DONE"),
        ("not_available", "N/A"),
        ("failed", "FAILED"),
    ):
        before = int(baseline.get(key, 0) or 0)
        after = int(exact.get(key, 0) or 0)
        if after < before:
            violations.append(f"{label} decreased {before}->{after}")

    running = int(exact.get("running", 0) or 0)
    if running:
        violations.append(f"RUNNING is {running}, expected 0 after collector drain")

    if violations:
        raise RuntimeError(
            "Collector invariant violation: " + "; ".join(violations)
        )


async def _assert_schema_ready(repo) -> str:
    expected_revision = migration_head_revision()
    async with repo.engine.connect() as conn:
        current_revision = (
            await conn.execute(text("SELECT version_num FROM alembic_version LIMIT 1"))
        ).scalar_one_or_none()
        if str(current_revision or "") != expected_revision:
            raise RuntimeError(
                "Collector schema is not current: "
                f"database={current_revision!r}, required={expected_revision!r}. "
                "Run dbd-data migrate before starting the collector."
            )

        rows = (
            await conn.execute(
                text("""
                SELECT table_name,column_name
                FROM information_schema.columns
                WHERE table_schema=DATABASE()
                  AND table_name IN (
                    'worker_runs','company_collection_state','response_archive'
                  )
                """)
            )
        ).all()

    present: dict[str, set[str]] = {}
    for table_name, column_name in rows:
        present.setdefault(str(table_name), set()).add(str(column_name))

    missing = []
    for table_name, required_columns in _REQUIRED_SCHEMA.items():
        absent = sorted(required_columns - present.get(table_name, set()))
        if absent:
            missing.append(f"{table_name}({','.join(absent)})")
    if missing:
        raise RuntimeError(
            "Collector schema contract is incomplete: " + "; ".join(missing)
        )
    return expected_revision


async def _assert_campaign_runtime_offline(run_id: int) -> dict:
    # This is a startup-only diagnostic handoff fence. Keep Campaign/Redis imports
    # out of collector startup when no production Redis control plane is configured.
    if not settings.dbd_redis_url:
        return {
            "redis_configured": False,
            "control_status": "not_configured",
            "supervisor_active": False,
            "workers": 0,
        }

    from .supervisor import collector_handoff_status

    status = await asyncio.to_thread(collector_handoff_status, run_id)
    supervisor_active = bool(status.get("supervisor_active"))
    workers = int(status.get("workers", 0) or 0)
    if supervisor_active or workers:
        raise RuntimeError(
            "Collector handoff refused: Campaign runtime is still active "
            f"(supervisor_active={supervisor_active}, workers={workers}, "
            f"control_status={status.get('control_status', 'unknown')!r})"
        )
    return status


async def _collector_preflight(repo, run_id: int, *, health=None) -> dict:
    if health is not None:
        await asyncio.to_thread(health)

    schema_revision = await _assert_schema_ready(repo)
    # Constructing the backend validates archive configuration before any claim.
    _ = repo.archive_backend

    # Serialize this diagnostic handoff against production distributed-worker startup.
    # The CLI already holds the long-lived manager lock for the collector.
    with runtime_handoff_lock():
        legacy = await _assert_campaign_runtime_offline(run_id)

        leases = await repo.collector_lease_summary(run_id)
        if int(leases.get("invalid", 0) or 0):
            raise RuntimeError(
                "Collector handoff refused: running rows exist without a lease expiry"
            )
        live = int(leases.get("live", 0) or 0)
        if live:
            raise RuntimeError(
                f"Collector handoff refused: Run {run_id} still has {live} live "
                "durable lease(s). Verify the previous process is stopped and retry "
                "after those leases expire."
            )

        reclaimed = await repo.reclaim_expired_collector_leases(run_id)
        after = await repo.collector_lease_summary(run_id)
        if int(after.get("running", 0) or 0):
            raise RuntimeError(
                f"Collector handoff refused: Run {run_id} still has "
                f"{int(after.get('running', 0) or 0)} running durable lease(s)"
            )

    if health is not None:
        await asyncio.to_thread(health)

    return {
        "schema_revision": schema_revision,
        "archive_backend": settings.dbd_archive_backend,
        "campaign_control": str(legacy.get("control_status") or "offline"),
        "campaign_workers": int(legacy.get("workers", 0) or 0),
        "reclaimed_leases": int(reclaimed),
    }


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

        with dashboard:
            preflight = await _collector_preflight(
                scheduler_repo,
                run_id,
                health=health,
            )
            baseline = await scheduler_repo.progress(run_id)
            runtime: dict = {}

            def on_runtime(snapshot: dict) -> None:
                runtime.clear()
                runtime.update(snapshot)
                dashboard.render(
                    _live_state(
                        run_id,
                        baseline,
                        runtime,
                        preflight=preflight,
                    )
                )

            dashboard.render(
                _live_state(
                    run_id,
                    baseline,
                    runtime,
                    preflight=preflight,
                )
            )
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
                    periodic_maintenance=False,
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

            if health is not None:
                await asyncio.to_thread(health)
            exact = await scheduler_repo.progress(run_id)
            _assert_postflight_invariants(baseline, exact)
            final = {
                "run_id": int(run_id),
                "total": _total(exact),
                "done": int(exact.get("done", 0)),
                "not_available": int(exact.get("not_available", 0)),
                "failed": int(exact.get("failed", 0)),
                "retry_wait": int(exact.get("retry_wait", 0)),
                "running": int(exact.get("running", 0)),
                "preflight": preflight,
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
