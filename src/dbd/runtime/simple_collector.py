from __future__ import annotations

import asyncio
import signal

from ..client.simple_client import SimpleDbdClient
from ..core.settings import settings
from ..storage.mysql.async_engine import create_engine
from ..storage.mysql.repositories.worker_repository import WorkerRepository
from .locking import assert_manager_lock, manager_lock
from .worker import process


def _assert_resume_safe(progress) -> None:
    running = int(progress.get("running", 0) or 0)
    if running:
        raise ValueError(
            f"Run still has {running} live RUNNING lease(s); stop the existing worker/campaign "
            "and let its leases release before starting dbd collect"
        )


async def _collect(
    run_id: int,
    *,
    concurrency: int,
    rate: float,
    manager_connection,
    ui: bool,
) -> dict:
    from ..presentation.collector import SimpleCollectorDashboard

    engine = create_engine()
    repo = WorkerRepository(engine)
    stop_requested = asyncio.Event()
    force_stop_requested = asyncio.Event()
    previous_handler = signal.getsignal(signal.SIGINT)
    interrupts = 0
    dashboard = None
    dashboard_closed = False

    try:
        run = await repo.get_run(run_id)
        if str(run.get("scheduler_mode") or "legacy") != "fabric_v2":
            raise ValueError("dbd collect requires an existing fabric_v2 run")

        scheduler_repo = repo.for_run(run)
        await scheduler_repo.maintain(run_id)
        baseline = await scheduler_repo.progress(run_id)
        _assert_resume_safe(baseline)
        dashboard = SimpleCollectorDashboard(
            run_id,
            baseline,
            target_rate=rate,
            concurrency=concurrency,
            enabled=ui,
        )
        dashboard.start()

        loop = asyncio.get_running_loop()

        def handle_interrupt(_signum, _frame) -> None:
            nonlocal interrupts
            interrupts += 1
            loop.call_soon_threadsafe(stop_requested.set)
            if interrupts >= 2:
                loop.call_soon_threadsafe(force_stop_requested.set)

        signal.signal(signal.SIGINT, handle_interrupt)

        def observe(snapshot) -> None:
            dashboard.update(snapshot)

        result = await process(
            repo,
            run_id,
            concurrency=concurrency,
            rate=rate,
            client_factory=SimpleDbdClient,
            health=lambda: assert_manager_lock(manager_connection),
            distributed=False,
            observer=observe,
            stop_requested=stop_requested,
            force_stop_requested=force_stop_requested,
            drain_timeout=float(settings.dbd_collector_drain_timeout_seconds),
        )
        status = "FINISHED" if not await scheduler_repo.has_active_work(run_id) else (
            "PAUSED" if stop_requested.is_set() else "IDLE"
        )

        exact = await scheduler_repo.progress(run_id)
        dashboard.close(exact, status=status)
        dashboard_closed = True
        return {
            "run_id": int(run_id),
            "status": status.lower(),
            "progress": exact,
            "session": result,
        }
    finally:
        if dashboard is not None and not dashboard_closed:
            try:
                dashboard.close(
                    status="PAUSED" if stop_requested.is_set() else "FAILED"
                )
            except Exception:
                # Terminal cleanup must never hide the collector's real failure.
                pass
        signal.signal(signal.SIGINT, previous_handler)
        await engine.dispose()


def collect_run(
    run_id: int,
    *,
    concurrency: int | None = None,
    rate: float | None = None,
    ui: bool = True,
) -> dict:
    """Resume one existing durable run with the simple single-process collector."""
    resolved_concurrency = int(
        settings.dbd_collector_concurrency if concurrency is None else concurrency
    )
    resolved_rate = float(
        settings.dbd_collector_rate_per_second if rate is None else rate
    )
    if not 1 <= resolved_concurrency <= 256:
        raise ValueError("collector concurrency must be 1..256")
    if resolved_rate <= 0:
        raise ValueError("collector rate must be positive")

    with manager_lock() as manager_connection:
        return asyncio.run(
            _collect(
                int(run_id),
                concurrency=resolved_concurrency,
                rate=resolved_rate,
                manager_connection=manager_connection,
                ui=ui,
            )
        )
