from __future__ import annotations

import asyncio
import signal

from ..client.simple_client import SimpleDbdClient
from ..core.settings import settings
from ..storage.mysql.async_engine import create_engine
from ..storage.mysql.repositories.worker_repository import WorkerRepository
from .locking import assert_manager_lock, manager_lock
from .worker import process


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
    processing: asyncio.Task | None = None
    previous_handler = signal.getsignal(signal.SIGINT)
    interrupts = 0

    try:
        run = await repo.get_run(run_id)
        if str(run.get("scheduler_mode") or "legacy") != "fabric_v2":
            raise ValueError("dbd collect requires an existing fabric_v2 run")

        scheduler_repo = repo.for_run(run)
        await scheduler_repo.maintain(run_id)
        baseline = await scheduler_repo.progress(run_id)
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
            if interrupts >= 2 and processing is not None:
                loop.call_soon_threadsafe(processing.cancel)

        signal.signal(signal.SIGINT, handle_interrupt)

        def observe(snapshot) -> None:
            dashboard.update(snapshot)

        processing = asyncio.create_task(
            process(
                repo,
                run_id,
                concurrency=concurrency,
                rate=rate,
                client_factory=SimpleDbdClient,
                health=lambda: assert_manager_lock(manager_connection),
                distributed=False,
                observer=observe,
                stop_requested=stop_requested,
            )
        )

        async def bounded_drain() -> None:
            await stop_requested.wait()
            try:
                await asyncio.wait_for(
                    asyncio.shield(processing),
                    timeout=float(settings.dbd_collector_drain_timeout_seconds),
                )
            except TimeoutError:
                processing.cancel()

        drain_watch = asyncio.create_task(bounded_drain())
        try:
            result = await processing
            status = "FINISHED" if not await scheduler_repo.has_active_work(run_id) else (
                "PAUSED" if stop_requested.is_set() else "IDLE"
            )
        except asyncio.CancelledError:
            if not stop_requested.is_set():
                raise
            result = await scheduler_repo.progress(run_id)
            status = "PAUSED"
        finally:
            drain_watch.cancel()
            try:
                await drain_watch
            except asyncio.CancelledError:
                pass

        exact = await scheduler_repo.progress(run_id)
        dashboard.close(exact, status=status)
        return {
            "run_id": int(run_id),
            "status": status.lower(),
            "progress": exact,
            "session": result,
        }
    finally:
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
