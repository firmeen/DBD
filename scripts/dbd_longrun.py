from __future__ import annotations

import argparse
import asyncio
import math
import multiprocessing as mp
import os
import queue
import sys
import time
from dataclasses import dataclass, field
from typing import Any

from rich.console import Console
from rich.live import Live
from rich.table import Table

from dbd.client.longrun_client import LongRunDbdClient
from dbd.client.redis_admission import FixedRedisAdmission
from dbd.core.settings import settings
from dbd.runtime.collector import _collector_preflight
from dbd.runtime.locking import assert_manager_lock, manager_lock
from dbd.runtime.worker import process as run_pipeline
from dbd.storage.mysql.async_engine import create_engine
from dbd.storage.mysql.repositories.worker_repository import WorkerRepository

_DEFAULT_WORKERS = 4
_DEFAULT_CONCURRENCY = 8
_DEFAULT_WORKER_RATE = 15.0
_DEFAULT_FLEET_RATE = 60.0
_HEALTHY_RESTART_RESET_SECONDS = 300.0
_MANAGER_LOCK_CHECK_SECONDS = 5.0
_HEARTBEAT_TIMEOUT_SECONDS = 60.0
_STALL_DRAIN_SECONDS = 30.0
_RESTART_BACKOFF_SECONDS = (1.0, 2.0, 5.0, 10.0, 30.0)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _positive_rate(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive rate")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Minimal long-run DBD execution engine: fixed multiprocess workers, "
            "curl_cffi HTTP, Redis aggregate admission, existing durable Fabric pipeline."
        )
    )
    parser.add_argument("--run-id", type=_positive_int, required=True)
    parser.add_argument("--workers", type=_positive_int, default=_DEFAULT_WORKERS)
    parser.add_argument(
        "--concurrency",
        type=_positive_int,
        default=_DEFAULT_CONCURRENCY,
        help="Async HTTP concurrency inside each worker process",
    )
    parser.add_argument(
        "--worker-rate",
        type=_positive_rate,
        default=_DEFAULT_WORKER_RATE,
        help="Fixed local request ceiling per worker",
    )
    parser.add_argument(
        "--fleet-rate",
        type=_positive_rate,
        default=_DEFAULT_FLEET_RATE,
        help="Fixed Redis-enforced aggregate request ceiling",
    )
    parser.add_argument(
        "--redis-key",
        default=f"{settings.dbd_redis_rate_key}:longrun:v1",
        help="Dedicated Redis key for the long-run fixed fleet pacer",
    )
    parser.add_argument(
        "--grace-seconds",
        type=_positive_rate,
        default=90.0,
        help="Graceful shutdown window before exact child termination",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=_positive_rate,
        default=1.0,
        help="Terminal refresh interval",
    )
    return parser


async def _preflight(
    run_id: int,
    *,
    fleet_rate: float,
    redis_key: str,
) -> tuple[dict[str, object], dict[str, int]]:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        run = await repo.get_run(run_id)
        scheduler_repo = repo.for_run(run)
        if scheduler_repo.scheduler_mode != "fabric_v2":
            raise ValueError(
                "Long-run execution requires scheduler_mode=fabric_v2"
            )

        result = await _collector_preflight(scheduler_repo, run_id)
        baseline = await scheduler_repo.progress(run_id)

        admission = FixedRedisAdmission(
            rate_per_second=fleet_rate,
            redis_url=settings.dbd_redis_url,
            key=redis_key,
            max_connections=1,
        )
        await admission.open()
        await admission.close()
        return result, {key: int(value or 0) for key, value in baseline.items()}
    finally:
        await engine.dispose()


async def _release_owner(worker_id: str) -> None:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        await repo.release(worker_id)
    finally:
        await engine.dispose()


async def _has_active_work(run_id: int) -> bool:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        run = await repo.get_run(run_id)
        return await repo.for_run(run).has_active_work(run_id)
    finally:
        await engine.dispose()


async def _exact_progress(run_id: int) -> dict[str, int]:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        run = await repo.get_run(run_id)
        progress = await repo.for_run(run).progress(run_id)
        return {key: int(value or 0) for key, value in progress.items()}
    finally:
        await engine.dispose()


async def _worker_async(
    *,
    run_id: int,
    slot: int,
    worker_id: str,
    concurrency: int,
    worker_rate: float,
    fleet_rate: float,
    redis_key: str,
    stop_flag,
    metric_queue,
) -> None:
    os.environ["DBD_WORKER_ID"] = worker_id
    os.environ["DBD_LONGRUN_FLEET_RATE"] = str(fleet_rate)
    os.environ["DBD_LONGRUN_REDIS_KEY"] = redis_key

    engine = create_engine()
    repo = WorkerRepository(engine)
    local_stop = asyncio.Event()

    def publish(snapshot: dict[str, object]) -> None:
        payload = {
            "kind": "metrics",
            "slot": int(slot),
            "worker_id": worker_id,
            "pid": os.getpid(),
            "observed_at": time.time(),
            "metrics": dict(snapshot),
        }
        try:
            metric_queue.put_nowait(payload)
        except (queue.Full, BrokenPipeError, EOFError, OSError):
            pass

    async def watch_parent_and_stop() -> None:
        parent = mp.parent_process()
        while not local_stop.is_set():
            parent_alive = parent is None or parent.is_alive()
            if stop_flag.is_set() or not parent_alive:
                local_stop.set()
                return
            await asyncio.sleep(0.5)

    watcher = asyncio.create_task(watch_parent_and_stop())
    try:
        result = await run_pipeline(
            repo,
            run_id,
            concurrency=concurrency,
            rate=worker_rate,
            limit=None,
            client_factory=LongRunDbdClient,
            endpoint_rates=None,
            distributed=False,
            runtime_callback=publish,
            stop_signal=local_stop,
            periodic_maintenance=slot == 0,
        )
        try:
            metric_queue.put_nowait(
                {
                    "kind": "result",
                    "slot": int(slot),
                    "worker_id": worker_id,
                    "pid": os.getpid(),
                    "observed_at": time.time(),
                    "result": dict(result),
                }
            )
        except (queue.Full, BrokenPipeError, EOFError, OSError):
            pass
    except BaseException as exc:
        try:
            metric_queue.put_nowait(
                {
                    "kind": "error",
                    "slot": int(slot),
                    "worker_id": worker_id,
                    "pid": os.getpid(),
                    "observed_at": time.time(),
                    "error": f"{type(exc).__name__}: {str(exc) or type(exc).__name__}",
                }
            )
        except (queue.Full, BrokenPipeError, EOFError, OSError):
            pass
        raise
    finally:
        local_stop.set()
        try:
            await watcher
        except asyncio.CancelledError:
            pass
        await engine.dispose()


def _worker_entry(
    run_id: int,
    slot: int,
    worker_id: str,
    concurrency: int,
    worker_rate: float,
    fleet_rate: float,
    redis_key: str,
    stop_flag,
    metric_queue,
) -> None:
    asyncio.run(
        _worker_async(
            run_id=run_id,
            slot=slot,
            worker_id=worker_id,
            concurrency=concurrency,
            worker_rate=worker_rate,
            fleet_rate=fleet_rate,
            redis_key=redis_key,
            stop_flag=stop_flag,
            metric_queue=metric_queue,
        )
    )


@dataclass
class _WorkerSlot:
    slot: int
    generation: int = 0
    process: Any = None
    stop_flag: Any = None
    worker_id: str = ""
    started_at: float = 0.0
    restart_count: int = 0
    next_start_at: float = 0.0
    latest: dict[str, object] = field(default_factory=dict)
    last_message: str | None = None
    last_heartbeat_at: float = 0.0
    stall_stop_at: float = 0.0


def _spawn_worker(
    ctx,
    state: _WorkerSlot,
    *,
    run_id: int,
    concurrency: int,
    worker_rate: float,
    fleet_rate: float,
    redis_key: str,
    metric_queue,
) -> None:
    state.generation += 1
    state.worker_id = (
        f"longrun-r{run_id}-p{os.getpid()}-w{state.slot}-g{state.generation}"
    )
    state.stop_flag = ctx.Event()
    state.latest = {}
    state.last_message = None
    process = ctx.Process(
        target=_worker_entry,
        args=(
            run_id,
            state.slot,
            state.worker_id,
            concurrency,
            worker_rate,
            fleet_rate,
            redis_key,
            state.stop_flag,
            metric_queue,
        ),
        name=f"dbd-longrun-{run_id}-{state.slot}",
        daemon=False,
    )
    process.start()
    state.process = process
    state.started_at = time.monotonic()
    state.last_heartbeat_at = state.started_at
    state.stall_stop_at = 0.0
    state.next_start_at = 0.0


def _counter_total(
    states: list[_WorkerSlot],
    retired: dict[str, int],
    key: str,
) -> int:
    return int(retired.get(key, 0)) + sum(
        int(state.latest.get(key, 0) or 0)
        for state in states
    )


def _retire_metrics(state: _WorkerSlot, retired: dict[str, int]) -> None:
    for key in (
        "requests",
        "responses",
        "completed",
        "done",
        "failed",
        "not_available",
        "retry_wait",
        "http_retries",
        "rate_limit_hits",
    ):
        retired[key] = int(retired.get(key, 0)) + int(
            state.latest.get(key, 0) or 0
        )
    state.latest = {}


def _drain_metric_queue(metric_queue, states: list[_WorkerSlot]) -> None:
    by_slot = {state.slot: state for state in states}
    while True:
        try:
            event = metric_queue.get_nowait()
        except queue.Empty:
            return
        except (EOFError, OSError):
            return

        state = by_slot.get(int(event.get("slot", -1)))
        if state is None or event.get("worker_id") != state.worker_id:
            continue

        kind = event.get("kind")
        if kind == "metrics":
            state.latest = dict(event.get("metrics") or {})
            state.latest["observed_at"] = event.get("observed_at")
            state.last_heartbeat_at = time.monotonic()
            state.stall_stop_at = 0.0
        elif kind == "error":
            state.last_message = str(event.get("error") or "worker error")
        elif kind == "result":
            state.last_message = "completed"


def _render(
    *,
    run_id: int,
    states: list[_WorkerSlot],
    retired: dict[str, int],
    baseline: dict[str, int],
    started_at: float,
    fleet_rate: float,
    observed_rate: float,
    stopping: bool,
    preflight: dict[str, object],
) -> Table:
    table = Table(
        title=f"DBD LONG-RUN · RUN {run_id}",
        caption=(
            f"runtime={time.monotonic() - started_at:,.0f}s  "
            f"HTTP={observed_rate:,.1f}/{fleet_rate:,.1f} req/s  "
            f"requests={_counter_total(states, retired, 'requests'):,}  "
            f"completed={_counter_total(states, retired, 'completed'):,}  "
            f"mode={'DRAINING' if stopping else 'RUNNING'}  "
            f"reclaimed={int(preflight.get('reclaimed_leases', 0) or 0):,}"
        ),
    )
    table.add_column("Worker")
    table.add_column("PID", justify="right")
    table.add_column("State")
    table.add_column("Req", justify="right")
    table.add_column("Resp", justify="right")
    table.add_column("Done", justify="right")
    table.add_column("Auth")
    table.add_column("Admission")
    table.add_column("Last")

    for state in states:
        process = state.process
        if process is not None and process.is_alive():
            process_state = "alive"
        elif state.next_start_at > time.monotonic() and not stopping:
            process_state = "backoff"
        else:
            process_state = "stopped"

        error = state.latest.get("last_error") or state.last_message or ""
        if len(str(error)) > 42:
            error = str(error)[:39] + "..."
        table.add_row(
            f"W{state.slot + 1}/g{state.generation}",
            str(process.pid if process is not None else "-"),
            process_state,
            f"{int(state.latest.get('requests', 0) or 0):,}",
            f"{int(state.latest.get('responses', 0) or 0):,}",
            f"{int(state.latest.get('completed', 0) or 0):,}",
            str(state.latest.get("auth_state") or "-"),
            str(state.latest.get("upstream_state") or "-"),
            str(error),
        )

    baseline_terminal = sum(
        int(baseline.get(key, 0) or 0)
        for key in ("done", "not_available", "failed")
    )
    baseline_total = sum(int(value or 0) for value in baseline.values())
    table.add_section()
    table.add_row(
        "Durable start",
        "-",
        f"{baseline_terminal:,}/{baseline_total:,}",
        "-",
        "-",
        "-",
        "-",
        "-",
        "exact at startup",
    )
    return table


def _restart_delay(restart_count: int) -> float:
    index = min(max(0, restart_count - 1), len(_RESTART_BACKOFF_SECONDS) - 1)
    return _RESTART_BACKOFF_SECONDS[index]


def _stop_workers(states: list[_WorkerSlot]) -> None:
    for state in states:
        if state.stop_flag is not None:
            state.stop_flag.set()


def _join_or_terminate(
    states: list[_WorkerSlot],
    *,
    grace_seconds: float,
) -> None:
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        alive = [
            state.process
            for state in states
            if state.process is not None and state.process.is_alive()
        ]
        if not alive:
            break
        for process in alive:
            process.join(timeout=0.2)

    for state in states:
        process = state.process
        if process is None or not process.is_alive():
            continue
        process.terminate()
        process.join(timeout=10.0)
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join(timeout=10.0)


def _release_confirmed_dead(states: list[_WorkerSlot]) -> None:
    for state in states:
        process = state.process
        if process is None or process.is_alive() or not state.worker_id:
            continue
        asyncio.run(_release_owner(state.worker_id))


def _supervise(args, manager_connection, preflight, baseline) -> int:
    ctx = mp.get_context("spawn")
    metric_queue = ctx.Queue(maxsize=max(64, args.workers * 16))
    states = [_WorkerSlot(slot=index) for index in range(args.workers)]
    retired: dict[str, int] = {}

    for state in states:
        _spawn_worker(
            ctx,
            state,
            run_id=args.run_id,
            concurrency=args.concurrency,
            worker_rate=args.worker_rate,
            fleet_rate=args.fleet_rate,
            redis_key=args.redis_key,
            metric_queue=metric_queue,
        )

    console = Console()
    started_at = time.monotonic()
    stopping = False
    exit_code = 0
    last_requests = 0
    last_rate_at = time.monotonic()
    next_manager_check = 0.0
    observed_rate = 0.0

    try:
        with Live(
            _render(
                run_id=args.run_id,
                states=states,
                retired=retired,
                baseline=baseline,
                started_at=started_at,
                fleet_rate=args.fleet_rate,
                observed_rate=0.0,
                stopping=False,
                preflight=preflight,
            ),
            console=console,
            refresh_per_second=4,
        ) as live:
            while True:
                loop_started = time.monotonic()
                _drain_metric_queue(metric_queue, states)

                now = time.monotonic()
                if now >= next_manager_check:
                    try:
                        assert_manager_lock(manager_connection)
                    except Exception:
                        stopping = True
                        exit_code = 1
                        _stop_workers(states)
                    next_manager_check = now + _MANAGER_LOCK_CHECK_SECONDS

                for state in states:
                    process_handle = state.process
                    if process_handle is not None and not process_handle.is_alive():
                        process_handle.join(timeout=0)
                        runtime = now - state.started_at
                        exit_status = process_handle.exitcode

                        _retire_metrics(state, retired)
                        asyncio.run(_release_owner(state.worker_id))
                        state.process = None
                        state.stop_flag = None

                        if stopping:
                            continue

                        active = asyncio.run(_has_active_work(args.run_id))
                        if not active:
                            stopping = True
                            _stop_workers(states)
                            continue

                        if runtime >= _HEALTHY_RESTART_RESET_SECONDS:
                            state.restart_count = 0
                        state.restart_count += 1
                        state.last_message = f"exit={exit_status}"
                        state.next_start_at = now + _restart_delay(
                            state.restart_count
                        )

                    if (
                        not stopping
                        and state.process is not None
                        and state.process.is_alive()
                        and now - state.last_heartbeat_at
                        >= _HEARTBEAT_TIMEOUT_SECONDS
                    ):
                        if state.stall_stop_at <= 0:
                            state.last_message = "heartbeat stale; draining"
                            state.stall_stop_at = now
                            if state.stop_flag is not None:
                                state.stop_flag.set()
                        elif now - state.stall_stop_at >= _STALL_DRAIN_SECONDS:
                            state.last_message = "heartbeat stale; terminating"
                            state.process.terminate()
                            state.process.join(timeout=10.0)

                    if (
                        not stopping
                        and state.process is None
                        and now >= state.next_start_at
                    ):
                        _spawn_worker(
                            ctx,
                            state,
                            run_id=args.run_id,
                            concurrency=args.concurrency,
                            worker_rate=args.worker_rate,
                            fleet_rate=args.fleet_rate,
                            redis_key=args.redis_key,
                            metric_queue=metric_queue,
                        )

                aggregate_requests = _counter_total(states, retired, "requests")
                rate_now = time.monotonic()
                elapsed = rate_now - last_rate_at
                if elapsed >= args.refresh_seconds:
                    observed_rate = max(
                        0.0,
                        (aggregate_requests - last_requests) / elapsed,
                    )
                    last_requests = aggregate_requests
                    last_rate_at = rate_now

                live.update(
                    _render(
                        run_id=args.run_id,
                        states=states,
                        retired=retired,
                        baseline=baseline,
                        started_at=started_at,
                        fleet_rate=args.fleet_rate,
                        observed_rate=observed_rate,
                        stopping=stopping,
                        preflight=preflight,
                    )
                )

                if stopping and not any(
                    state.process is not None and state.process.is_alive()
                    for state in states
                ):
                    break

                delay = max(
                    0.05,
                    args.refresh_seconds - (time.monotonic() - loop_started),
                )
                time.sleep(delay)

    except KeyboardInterrupt:
        stopping = True
        _stop_workers(states)
    finally:
        # Any parent exit path must first request a bounded graceful drain.
        # Do this even for unexpected exceptions so workers are never left
        # running until the hard-termination deadline by accident.
        _stop_workers(states)
        _join_or_terminate(states, grace_seconds=args.grace_seconds)
        _release_confirmed_dead(states)
        try:
            metric_queue.close()
            metric_queue.join_thread()
        except Exception:
            pass

    final = asyncio.run(_exact_progress(args.run_id))
    console.print(
        f"Run {args.run_id} stopped: "
        f"pending={int(final.get('pending', 0)):,} "
        f"retry_wait={int(final.get('retry_wait', 0)):,} "
        f"running={int(final.get('running', 0)):,} "
        f"failed={int(final.get('failed', 0)):,} "
        f"done={int(final.get('done', 0)):,} "
        f"not_available={int(final.get('not_available', 0)):,}"
    )
    if int(final.get("running", 0)) != 0:
        return 1
    return exit_code


def main() -> int:
    args = _parser().parse_args()
    if not settings.dbd_redis_url:
        raise SystemExit("DBD_REDIS_URL is required")
    if args.concurrency > 256:
        raise SystemExit("--concurrency must be <= 256")
    if not args.redis_key.strip():
        raise SystemExit("--redis-key must not be empty")

    if args.workers * args.worker_rate < args.fleet_rate:
        Console().print(
            "[yellow]Warning:[/yellow] configured local worker capacity "
            f"({args.workers * args.worker_rate:.1f}/s) is below the fleet ceiling "
            f"({args.fleet_rate:.1f}/s)."
        )

    # The MySQL manager lock is held for the full parent lifetime. It fences this
    # execution engine from the diagnostic collector and from new legacy Campaign
    # workers that honor the same handoff boundary.
    with manager_lock() as manager_connection:
        preflight, baseline = asyncio.run(
            _preflight(
                args.run_id,
                fleet_rate=args.fleet_rate,
                redis_key=args.redis_key,
            )
        )
        return _supervise(args, manager_connection, preflight, baseline)


if __name__ == "__main__":
    mp.freeze_support()
    sys.exit(main())
