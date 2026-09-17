from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from uuid import uuid4

from redis import Redis

from ..core.progress import ProgressLine
from ..core.settings import PROJECT_ROOT, settings
from ..storage.mysql.engine import connect

_RATE_MODES = {"shared", "multiply", "adaptive"}


@dataclass
class WorkerProcess:
    index: int
    worker_id: str
    process: subprocess.Popen
    restarts: int = 0
    log_handle: object | None = None
    draining: bool = False


@dataclass(frozen=True)
class CampaignRatePlan:
    mode: str
    worker_rate: float
    workers: int
    ceiling: float
    initial: float


def build_rate_plan(
    mode: str,
    worker_rate: float,
    workers: int,
    *,
    initial_workers: int | None = None,
) -> CampaignRatePlan:
    if mode not in _RATE_MODES:
        raise ValueError(f"rate mode must be one of {sorted(_RATE_MODES)}")
    worker_rate = float(worker_rate)
    workers = int(workers)
    initial_workers = workers if initial_workers is None else int(initial_workers)
    if worker_rate <= 0 or workers < 1 or not 1 <= initial_workers <= workers:
        raise ValueError("worker rate/count and initial worker count must be positive and ordered")
    if mode == "shared":
        ceiling = initial = worker_rate
    elif mode == "multiply":
        ceiling = worker_rate * workers
        initial = worker_rate * initial_workers
    else:
        ceiling = worker_rate * workers
        initial = worker_rate
    return CampaignRatePlan(mode, worker_rate, workers, ceiling, initial)


def _next_worker_target(
    *,
    pending: int,
    current: int,
    minimum: int,
    maximum: int,
    worker_concurrency: int,
) -> int:
    """Return one hysteretic autoscale step; scale-down is executed by graceful drain."""
    pending = max(0, int(pending))
    current = max(0, int(current))
    capacity = max(1, int(worker_concurrency))
    if current < minimum:
        return minimum
    if pending > current * capacity * 4 and current < maximum:
        return current + 1
    if current > minimum and pending < (current - 1) * capacity:
        return current - 1
    return current


def _campaign_key(run_id: int) -> str:
    return f"dbd:campaign:{int(run_id)}"


def _task_progress(run_id: int) -> dict[str, int]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status,COUNT(*) n FROM endpoint_tasks WHERE run_id=%s GROUP BY status",
            (run_id,),
        )
        result = {row["status"]: int(row["n"]) for row in cur.fetchall()}
        cur.execute("SELECT COUNT(*) n FROM endpoint_tasks WHERE run_id=%s", (run_id,))
        result["total"] = int(cur.fetchone()["n"])
        return result


def _run_status(run_id: int) -> str:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM worker_runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Run {run_id} not found")
        return str(row["status"])


def _worker_command(run_id: int, *, concurrency: int, rate: float) -> list[str]:
    return [
        sys.executable,
        "-m",
        "dbd",
        "bulk",
        "--run-id",
        str(run_id),
        "--distributed",
        "--concurrency",
        str(concurrency),
        "--rate",
        str(rate),
        "--metrics-port",
        "0",
    ]


def _spawn_worker(
    run_id: int,
    index: int,
    *,
    concurrency: int,
    rate: float,
    open_powershell: bool,
    restart_count: int = 0,
) -> WorkerProcess:
    worker_id = f"local-{run_id}-{index}-{uuid4().hex[:8]}"
    env = os.environ.copy()
    env["DBD_WORKER_ID"] = worker_id
    command = _worker_command(run_id, concurrency=concurrency, rate=rate)
    creationflags = 0
    start_new_session = False
    log_handle = None

    if os.name == "nt":
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        if open_powershell:
            creationflags |= getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
            rendered = subprocess.list2cmdline(command)
            title = f"DBD run {run_id} worker {index}"
            command = [
                "powershell.exe",
                "-NoProfile",
                "-Command",
                f"$Host.UI.RawUI.WindowTitle='{title}'; & {rendered}; exit $LASTEXITCODE",
            ]
    else:
        start_new_session = True

    if not open_powershell:
        log_dir = PROJECT_ROOT / "data" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"campaign-{run_id}-worker-{index}.log"
        log_handle = open(log_path, "a", encoding="utf-8", buffering=1)

    process = subprocess.Popen(
        command,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=None if open_powershell else log_handle,
        stderr=None if open_powershell else subprocess.STDOUT,
        creationflags=creationflags,
        start_new_session=start_new_session,
    )
    return WorkerProcess(index, worker_id, process, restart_count, log_handle)


def _close_worker_handle(worker: WorkerProcess) -> None:
    if worker.log_handle is not None:
        try:
            worker.log_handle.close()
        finally:
            worker.log_handle = None


def _request_stop(redis: Redis, run_id: int, reason: str) -> None:
    redis.set(
        f"{_campaign_key(run_id)}:stop",
        json.dumps({"reason": reason, "requested_at": time.time()}),
        ex=3600,
    )


def _request_worker_drain(redis: Redis, run_id: int, worker_id: str, reason: str) -> None:
    redis.set(
        f"{_campaign_key(run_id)}:worker:{worker_id}:drain",
        json.dumps({"reason": reason, "requested_at": time.time()}),
        ex=3600,
    )


def _clear_stop(redis: Redis, run_id: int) -> None:
    redis.delete(f"{_campaign_key(run_id)}:stop")


def _seed_rate(redis: Redis, rate_key: str, initial_rate: float) -> None:
    """Seed a fresh shared pacer without erasing an active fleet penalty or cooldown."""
    redis.set(f"{rate_key}:effective-rate", str(initial_rate), ex=600, nx=True)
    redis.set(
        f"{rate_key}:last-penalty-ms",
        str(int(time.time() * 1000) - 120_000),
        ex=600,
        nx=True,
    )


def _effective_rate(redis: Redis, rate_key: str, fallback: float) -> float:
    raw = redis.get(f"{rate_key}:effective-rate")
    try:
        return float(raw) if raw is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _registered_workers(redis: Redis, run_id: int) -> int:
    ids = redis.smembers(f"{_campaign_key(run_id)}:workers")
    if not ids:
        return 0
    keys = [f"{_campaign_key(run_id)}:worker:{worker_id}" for worker_id in ids]
    return sum(value is not None for value in redis.mget(keys))


def _force_kill_worker(worker: WorkerProcess) -> None:
    """Force-stop a worker process tree; PowerShell wrappers must not orphan Python children."""
    if worker.process.poll() is not None:
        return
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(worker.process.pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        except OSError:
            pass
    worker.process.kill()


def _terminate_workers(workers: list[WorkerProcess]) -> None:
    if os.name == "nt":
        for worker in workers:
            _force_kill_worker(worker)
            _close_worker_handle(worker)
        return

    for worker in workers:
        if worker.process.poll() is None:
            worker.process.terminate()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and any(w.process.poll() is None for w in workers):
        time.sleep(0.1)
    for worker in workers:
        if worker.process.poll() is None:
            _force_kill_worker(worker)
        _close_worker_handle(worker)


def supervise_campaign(
    run_id: int,
    *,
    workers: int,
    worker_concurrency: int,
    worker_rate: float,
    rate_mode: str = "adaptive",
    open_powershell: bool = False,
    max_restarts: int = 3,
    workers_min: int | None = None,
    workers_max: int | None = None,
) -> int:
    """Run a local worker fleet. First Ctrl+C drains; second forces termination."""
    if not settings.dbd_redis_url:
        raise ValueError("Campaign supervisor requires DBD_REDIS_URL")
    if worker_concurrency < 1 or max_restarts < 0:
        raise ValueError("worker concurrency must be positive and max restarts non-negative")

    minimum = workers if workers_min is None else int(workers_min)
    maximum = workers if workers_max is None else int(workers_max)
    if minimum < 1 or maximum < minimum:
        raise ValueError("workers-min/workers-max must satisfy 1 <= min <= max")
    autoscale = minimum != maximum
    plan = build_rate_plan(rate_mode, worker_rate, maximum, initial_workers=minimum)
    redis = Redis.from_url(settings.dbd_redis_url, decode_responses=True)
    redis.ping()
    _clear_stop(redis, run_id)
    _seed_rate(redis, settings.dbd_redis_rate_key, plan.initial)

    children = [
        _spawn_worker(
            run_id,
            index,
            concurrency=worker_concurrency,
            rate=plan.ceiling,
            open_powershell=open_powershell,
        )
        for index in range(1, minimum + 1)
    ]
    next_index = minimum + 1
    next_scale_at = time.monotonic() + 5
    progress = ProgressLine(f"CAMPAIGN {run_id}", interval=0.75)
    graceful_stop = False

    try:
        while True:
            state = _task_progress(run_id)
            total = state.get("total", 0)
            pending = state.get("pending", 0) + state.get("retry_wait", 0)
            running = state.get("running", 0)
            failed = state.get("failed", 0)
            done = state.get("done", 0) + state.get("not_available", 0)
            effective = _effective_rate(redis, settings.dbd_redis_rate_key, plan.initial)
            registered = _registered_workers(redis, run_id)
            live = [worker for worker in children if worker.process.poll() is None]
            active_live = [worker for worker in live if not worker.draining]
            worker_label = (
                f"{registered} ({minimum}-{maximum})" if autoscale else f"{registered}/{maximum}"
            )
            progress.update(
                done + failed,
                total,
                detail=(
                    f"workers {worker_label} | rate {effective:.2f}/{plan.ceiling:.2f}/s | "
                    f"pending {pending:,} running {running:,} failed {failed:,}"
                ),
            )

            if total > 0 and pending == 0 and running == 0:
                break

            for position, worker in enumerate(list(children)):
                code = worker.process.poll()
                if code is None:
                    continue
                _close_worker_handle(worker)
                if graceful_stop or worker.draining:
                    continue
                current = _task_progress(run_id)
                active = current.get("pending", 0) + current.get("retry_wait", 0) + current.get("running", 0)
                if active and worker.restarts < max_restarts:
                    replacement = _spawn_worker(
                        run_id,
                        worker.index,
                        concurrency=worker_concurrency,
                        rate=plan.ceiling,
                        open_powershell=open_powershell,
                        restart_count=worker.restarts + 1,
                    )
                    children[position] = replacement
                    progress.event(
                        f"worker {worker.index} exited {code}; restarted "
                        f"({replacement.restarts}/{max_restarts})"
                    )
                elif active:
                    progress.event(
                        f"worker {worker.index} exited {code}; restart budget exhausted"
                    )

            if autoscale and time.monotonic() >= next_scale_at and not graceful_stop:
                live = [worker for worker in children if worker.process.poll() is None]
                active_live = [worker for worker in live if not worker.draining]
                current_count = len(active_live)
                target = _next_worker_target(
                    pending=pending,
                    current=current_count,
                    minimum=minimum,
                    maximum=maximum,
                    worker_concurrency=worker_concurrency,
                )
                if target > current_count:
                    worker = _spawn_worker(
                        run_id,
                        next_index,
                        concurrency=worker_concurrency,
                        rate=plan.ceiling,
                        open_powershell=open_powershell,
                    )
                    next_index += 1
                    children.append(worker)
                    progress.event(f"autoscale up: worker {worker.index} started ({current_count}->{target})")
                elif target < current_count:
                    candidate = max(active_live, key=lambda item: item.index)
                    candidate.draining = True
                    _request_worker_drain(redis, run_id, candidate.worker_id, "autoscale_down")
                    progress.event(
                        f"autoscale down: worker {candidate.index} draining ({current_count}->{target})"
                    )
                next_scale_at = time.monotonic() + 5

            if not any(worker.process.poll() is None for worker in children):
                current = _task_progress(run_id)
                active = current.get("pending", 0) + current.get("retry_wait", 0) + current.get("running", 0)
                if active:
                    progress.event("all workers stopped while work remains")
                    return 2
                break
            time.sleep(0.25)
    except KeyboardInterrupt:
        graceful_stop = True
        progress.event("Ctrl+C received: requesting graceful drain; Ctrl+C again forces stop")
        _request_stop(redis, run_id, "supervisor_keyboard_interrupt")
        try:
            while any(worker.process.poll() is None for worker in children):
                state = _task_progress(run_id)
                running = state.get("running", 0)
                registered = _registered_workers(redis, run_id)
                progress.update(
                    state.get("done", 0) + state.get("not_available", 0) + state.get("failed", 0),
                    state.get("total", 0),
                    detail=f"draining | workers {registered} | running {running:,}",
                    force=True,
                )
                time.sleep(0.5)
        except KeyboardInterrupt:
            progress.event("Second Ctrl+C received: forcing worker termination")
            _terminate_workers(children)
        progress.close(
            message=f"Campaign {run_id} paused safely; resume with dbd-data campaign resume {run_id}"
        )
        redis.close()
        return 130
    finally:
        for worker in children:
            _close_worker_handle(worker)

    state = _task_progress(run_id)
    failed = state.get("failed", 0)
    progress.close(
        message=(
            f"Campaign {run_id} complete: done={state.get('done', 0):,} "
            f"not_available={state.get('not_available', 0):,} failed={failed:,}"
        )
    )
    _clear_stop(redis, run_id)
    redis.close()
    return 1 if failed else 0


def campaign_status(run_id: int) -> dict:
    state = _task_progress(run_id)
    state["run_id"] = int(run_id)
    state["run_status"] = _run_status(run_id)
    if settings.dbd_redis_url:
        redis = Redis.from_url(settings.dbd_redis_url, decode_responses=True)
        try:
            redis.ping()
            state["workers"] = _registered_workers(redis, run_id)
            state["effective_rate"] = _effective_rate(
                redis, settings.dbd_redis_rate_key, settings.dbd_rate_per_second
            )
            state["stop_requested"] = bool(redis.exists(f"{_campaign_key(run_id)}:stop"))
        finally:
            redis.close()
    return state


def request_campaign_stop(run_id: int) -> None:
    if not settings.dbd_redis_url:
        raise ValueError("Campaign stop requires DBD_REDIS_URL")
    redis = Redis.from_url(settings.dbd_redis_url, decode_responses=True)
    try:
        redis.ping()
        _request_stop(redis, run_id, "operator_command")
    finally:
        redis.close()
