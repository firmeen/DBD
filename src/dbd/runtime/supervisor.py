from __future__ import annotations

import json
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
_SUPERVISOR_LEASE_SECONDS = 30
_SUPERVISOR_REFRESH_SECONDS = 10.0
_RUNTIME_STATE_TTL_SECONDS = 86400
_AUTOSCALE_UP_COOLDOWN_SECONDS = 5.0
_AUTOSCALE_DOWN_COOLDOWN_SECONDS = 15.0
_AUTOSCALE_LOW_PRESSURE_OBSERVATIONS = 2
_FAILURE_DRAIN_SECONDS = 10.0
_COMPLETE_DRAIN_SECONDS = 10.0


@dataclass
class WorkerProcess:
    index: int
    worker_id: str
    process: subprocess.Popen
    restarts: int = 0
    log_handle: object | None = None
    draining: bool = False
    drain_requested_at: float | None = None


@dataclass(frozen=True)
class CampaignRatePlan:
    mode: str
    worker_rate: float
    workers: int
    ceiling: float
    initial: float


@dataclass
class AutoscaleController:
    minimum: int
    maximum: int
    worker_concurrency: int
    next_decision_at: float = 0.0
    low_pressure_observations: int = 0

    def target(
        self,
        *,
        pending: int,
        running: int,
        current: int,
        effective_maximum: int,
        now: float,
    ) -> int:
        """Return at most one safe fleet-size step with asymmetric hysteresis."""
        current = max(0, int(current))
        pending = max(0, int(pending))
        running = max(0, int(running))
        effective_maximum = max(0, min(int(effective_maximum), self.maximum))
        minimum = min(self.minimum, effective_maximum)
        capacity = max(1, int(self.worker_concurrency))

        if current < minimum:
            self.low_pressure_observations = 0
            self.next_decision_at = now + _AUTOSCALE_UP_COOLDOWN_SECONDS
            return min(current + 1, minimum)
        if now < self.next_decision_at:
            return current

        if current < effective_maximum and pending > max(capacity, current * capacity * 4):
            self.low_pressure_observations = 0
            self.next_decision_at = now + _AUTOSCALE_UP_COOLDOWN_SECONDS
            return current + 1

        low_pending = pending <= max(1, ((current - 1) * capacity) // 2)
        bounded_running = running <= max(capacity, current * capacity * 2)
        if current > minimum and low_pending and bounded_running:
            self.low_pressure_observations += 1
            if self.low_pressure_observations >= _AUTOSCALE_LOW_PRESSURE_OBSERVATIONS:
                self.low_pressure_observations = 0
                self.next_decision_at = now + _AUTOSCALE_DOWN_COOLDOWN_SECONDS
                return current - 1
        else:
            self.low_pressure_observations = 0
        return current


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
    """Compatibility helper for one immediate autoscale decision."""
    controller = AutoscaleController(minimum, maximum, worker_concurrency)
    controller.low_pressure_observations = _AUTOSCALE_LOW_PRESSURE_OBSERVATIONS - 1
    return controller.target(
        pending=pending,
        running=0,
        current=current,
        effective_maximum=maximum,
        now=time.monotonic(),
    )


def _campaign_key(run_id: int) -> str:
    return f"dbd:campaign:{int(run_id)}"


def _supervisor_lease_key(run_id: int) -> str:
    return f"{_campaign_key(run_id)}:supervisor:lease"


def _runtime_state_key(run_id: int) -> str:
    return f"{_campaign_key(run_id)}:supervisor:state"


def _worker_key(run_id: int, worker_id: str) -> str:
    return f"{_campaign_key(run_id)}:worker:{worker_id}"


def _worker_drain_key(run_id: int, worker_id: str) -> str:
    return f"{_worker_key(run_id, worker_id)}:drain"


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
    """Spawn one worker atomically; no log handle survives a failed Popen."""
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

    try:
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
    except BaseException:
        if log_handle is not None:
            log_handle.close()
        raise
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


def _stop_requested(redis: Redis, run_id: int) -> bool:
    return bool(redis.exists(f"{_campaign_key(run_id)}:stop"))


def _request_worker_drain(redis: Redis, run_id: int, worker_id: str, reason: str) -> None:
    redis.set(
        _worker_drain_key(run_id, worker_id),
        json.dumps({"reason": reason, "requested_at": time.time()}),
        ex=3600,
    )


def _cleanup_worker_control(redis: Redis, run_id: int, worker_id: str) -> None:
    """Forget an exited local worker immediately instead of waiting for heartbeat TTLs."""
    try:
        pipe = redis.pipeline(transaction=False)
        pipe.delete(_worker_key(run_id, worker_id), _worker_drain_key(run_id, worker_id))
        pipe.srem(f"{_campaign_key(run_id)}:workers", worker_id)
        pipe.execute()
    except Exception:
        pass


def _clear_stop(redis: Redis, run_id: int) -> None:
    redis.delete(f"{_campaign_key(run_id)}:stop")


def _seed_rate(redis: Redis, rate_key: str, initial_rate: float) -> None:
    """Seed a missing shared rate without erasing an active fleet penalty/cooldown."""
    pipe = redis.pipeline(transaction=False)
    pipe.set(f"{rate_key}:effective-rate", str(initial_rate), ex=600, nx=True)
    pipe.set(
        f"{rate_key}:last-penalty-ms",
        str(int(time.time() * 1000) - 120_000),
        ex=600,
        nx=True,
    )
    pipe.execute()


def _effective_rate(redis: Redis, rate_key: str, fallback: float) -> float:
    raw = redis.get(f"{rate_key}:effective-rate")
    try:
        return float(raw) if raw is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _registered_worker_states(redis: Redis, run_id: int) -> list[dict]:
    set_key = f"{_campaign_key(run_id)}:workers"
    ids = sorted(redis.smembers(set_key))
    if not ids:
        return []
    keys = [_worker_key(run_id, worker_id) for worker_id in ids]
    values = redis.mget(keys)
    result = []
    stale = []
    for worker_id, value in zip(ids, values, strict=True):
        if value is None:
            stale.append(worker_id)
            continue
        try:
            row = json.loads(value)
            result.append(row if isinstance(row, dict) else {"worker_id": worker_id, "status": "unknown"})
        except (TypeError, ValueError):
            result.append({"worker_id": worker_id, "status": "unknown"})
    if stale:
        pipe = redis.pipeline(transaction=False)
        pipe.srem(set_key, *stale)
        for worker_id in stale:
            pipe.delete(_worker_drain_key(run_id, worker_id))
        pipe.execute()
    return result


def _registered_workers(redis: Redis, run_id: int) -> int:
    return len(_registered_worker_states(redis, run_id))


def _acquire_supervisor_lease(redis: Redis, run_id: int, token: str) -> None:
    acquired = redis.set(
        _supervisor_lease_key(run_id),
        token,
        ex=_SUPERVISOR_LEASE_SECONDS,
        nx=True,
    )
    if not acquired:
        raise RuntimeError(
            f"Campaign {run_id} already has an active supervisor; use campaign status/stop instead"
        )


def _refresh_supervisor_lease(redis: Redis, run_id: int, token: str) -> None:
    script = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
      return redis.call('EXPIRE', KEYS[1], ARGV[2])
    end
    return 0
    """
    refreshed = redis.eval(
        script,
        1,
        _supervisor_lease_key(run_id),
        token,
        str(_SUPERVISOR_LEASE_SECONDS),
    )
    if int(refreshed or 0) != 1:
        raise RuntimeError(f"Campaign {run_id} supervisor lease was lost")


def _release_supervisor_lease(redis: Redis, run_id: int, token: str) -> None:
    script = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
      return redis.call('DEL', KEYS[1])
    end
    return 0
    """
    try:
        redis.eval(script, 1, _supervisor_lease_key(run_id), token)
    except Exception:
        pass


def _write_runtime_state(redis: Redis, run_id: int, **state) -> None:
    payload = {"run_id": int(run_id), "updated_at": time.time(), **state}
    redis.set(
        _runtime_state_key(run_id),
        json.dumps(payload, default=str),
        ex=_RUNTIME_STATE_TTL_SECONDS,
    )


def _read_runtime_state(redis: Redis, run_id: int) -> dict | None:
    raw = redis.get(_runtime_state_key(run_id))
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


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


def _request_local_drain(
    redis: Redis,
    run_id: int,
    workers: list[WorkerProcess],
    reason: str,
) -> int:
    requested = 0
    now = time.monotonic()
    for worker in workers:
        if worker.process.poll() is None and not worker.draining:
            try:
                _request_worker_drain(redis, run_id, worker.worker_id, reason)
                worker.draining = True
                worker.drain_requested_at = now
                requested += 1
            except Exception:
                pass
    return requested


def _contain_local_fleet(
    redis: Redis,
    run_id: int,
    workers: list[WorkerProcess],
    reason: str,
    *,
    grace_seconds: float,
) -> None:
    """Drain local workers for a bounded period, then force only remaining process trees."""
    live = [worker for worker in workers if worker.process.poll() is None]
    if not live:
        return
    already_draining = sum(worker.draining for worker in live)
    requested = _request_local_drain(redis, run_id, live, reason)
    if requested + already_draining < len(live):
        _terminate_workers(live)
        return

    deadline = time.monotonic() + max(0.0, grace_seconds)
    while time.monotonic() < deadline:
        remaining = [worker for worker in live if worker.process.poll() is None]
        if not remaining:
            break
        time.sleep(0.1)
    remaining = [worker for worker in live if worker.process.poll() is None]
    if remaining:
        _terminate_workers(remaining)


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
    """Run one lease-protected local fleet with graceful autoscaling and durable work recovery."""
    if not settings.dbd_redis_url:
        raise ValueError("Campaign supervisor requires DBD_REDIS_URL")
    if worker_concurrency < 1 or max_restarts < 0:
        raise ValueError("worker concurrency must be positive and max restarts non-negative")

    requested_workers = int(workers)
    minimum = requested_workers if workers_min is None else int(workers_min)
    maximum = requested_workers if workers_max is None else int(workers_max)
    if requested_workers < 1 or minimum < 1 or maximum < minimum or not minimum <= requested_workers <= maximum:
        raise ValueError("workers/workers-min/workers-max must satisfy 1 <= min <= workers <= max")

    initial_workers = requested_workers
    autoscale = minimum != maximum
    plan = build_rate_plan(rate_mode, worker_rate, maximum, initial_workers=initial_workers)
    controller = AutoscaleController(minimum, maximum, worker_concurrency)

    redis = Redis.from_url(settings.dbd_redis_url, decode_responses=True)
    token = f"{os.getpid()}-{uuid4().hex}"
    children: list[WorkerProcess] = []
    next_index = 1
    exhausted_capacity = 0
    graceful_stop = False
    exit_code = 0
    lease_acquired = False
    supervisor_error = False
    next_lease_refresh = 0.0
    next_state_write = 0.0
    progress = ProgressLine(f"CAMPAIGN {run_id}", interval=0.75)

    def spawn_worker(*, restart_index: int | None = None, restarts: int = 0) -> WorkerProcess:
        nonlocal next_index
        index = next_index if restart_index is None else restart_index
        worker = _spawn_worker(
            run_id,
            index,
            concurrency=worker_concurrency,
            rate=plan.ceiling,
            open_powershell=open_powershell,
            restart_count=restarts,
        )
        if restart_index is None:
            next_index += 1
        return worker

    try:
        redis.ping()
        _acquire_supervisor_lease(redis, run_id, token)
        lease_acquired = True
        next_lease_refresh = time.monotonic() + _SUPERVISOR_REFRESH_SECONDS
        _clear_stop(redis, run_id)
        _seed_rate(redis, settings.dbd_redis_rate_key, plan.initial)

        for _ in range(initial_workers):
            children.append(spawn_worker())

        _write_runtime_state(
            redis,
            run_id,
            supervisor_pid=os.getpid(),
            status="running",
            autoscale=autoscale,
            workers_initial=initial_workers,
            workers_min=minimum,
            workers_max=maximum,
            workers_target=initial_workers,
            workers_active=initial_workers,
            workers_draining=0,
            exhausted_capacity=0,
            worker_concurrency=worker_concurrency,
            worker_rate=worker_rate,
            rate_mode=rate_mode,
            rate_ceiling=plan.ceiling,
        )

        while True:
            now = time.monotonic()
            if now >= next_lease_refresh:
                _refresh_supervisor_lease(redis, run_id, token)
                next_lease_refresh = now + _SUPERVISOR_REFRESH_SECONDS

            state = _task_progress(run_id)
            total = state.get("total", 0)
            pending = state.get("pending", 0) + state.get("retry_wait", 0)
            running = state.get("running", 0)
            failed = state.get("failed", 0)
            done = state.get("done", 0) + state.get("not_available", 0)
            active_work = pending + running

            if _stop_requested(redis, run_id) and not graceful_stop:
                graceful_stop = True
                _request_local_drain(redis, run_id, children, "campaign_stop")
                progress.event("campaign stop requested: draining workers without claiming new work")

            survivors: list[WorkerProcess] = []
            for worker in children:
                code = worker.process.poll()
                if code is None:
                    survivors.append(worker)
                    continue

                _close_worker_handle(worker)
                _cleanup_worker_control(redis, run_id, worker.worker_id)
                if graceful_stop or worker.draining or not active_work:
                    continue

                if worker.restarts < max_restarts:
                    try:
                        replacement = spawn_worker(
                            restart_index=worker.index,
                            restarts=worker.restarts + 1,
                        )
                    except OSError as exc:
                        exhausted_capacity += 1
                        progress.event(
                            f"worker {worker.index} restart failed ({type(exc).__name__}); "
                            "fleet capacity reduced by 1"
                        )
                    else:
                        survivors.append(replacement)
                        progress.event(
                            f"worker {worker.index} exited {code}; restarted "
                            f"({replacement.restarts}/{max_restarts})"
                        )
                else:
                    exhausted_capacity += 1
                    progress.event(
                        f"worker {worker.index} exited {code}; restart budget exhausted; "
                        "fleet capacity reduced by 1"
                    )
            children = survivors

            live = [worker for worker in children if worker.process.poll() is None]
            active_live = [worker for worker in live if not worker.draining]
            draining_live = [worker for worker in live if worker.draining]
            effective_maximum = max(0, maximum - exhausted_capacity)

            if active_work and effective_maximum < minimum:
                progress.event(
                    f"available fleet capacity {effective_maximum} fell below minimum {minimum}"
                )
                exit_code = 2
                _contain_local_fleet(
                    redis,
                    run_id,
                    children,
                    "capacity_exhausted",
                    grace_seconds=_FAILURE_DRAIN_SECONDS,
                )
                break

            target = len(active_live)
            if autoscale and not graceful_stop:
                target = controller.target(
                    pending=pending,
                    running=running,
                    current=len(active_live),
                    effective_maximum=effective_maximum,
                    now=now,
                )
                if target > len(active_live) and len(live) < effective_maximum:
                    try:
                        worker = spawn_worker()
                    except OSError as exc:
                        exhausted_capacity += 1
                        progress.event(
                            f"autoscale up failed ({type(exc).__name__}); fleet capacity reduced by 1"
                        )
                    else:
                        children.append(worker)
                        active_live.append(worker)
                        live.append(worker)
                        progress.event(
                            f"autoscale up: worker {worker.index} started "
                            f"({len(active_live) - 1}->{len(active_live)})"
                        )
                elif target < len(active_live):
                    candidate = max(active_live, key=lambda item: item.index)
                    _request_worker_drain(redis, run_id, candidate.worker_id, "autoscale_down")
                    candidate.draining = True
                    candidate.drain_requested_at = now
                    active_live.remove(candidate)
                    draining_live.append(candidate)
                    progress.event(
                        f"autoscale down: worker {candidate.index} draining "
                        f"({len(active_live) + 1}->{len(active_live)})"
                    )

            effective_maximum = max(0, maximum - exhausted_capacity)
            required_floor = min(minimum, effective_maximum)
            while (
                active_work
                and not graceful_stop
                and len(active_live) < required_floor
                and len(live) < effective_maximum
            ):
                try:
                    worker = spawn_worker()
                except OSError as exc:
                    exhausted_capacity += 1
                    progress.event(
                        f"fleet floor repair failed ({type(exc).__name__}); fleet capacity reduced by 1"
                    )
                    break
                children.append(worker)
                active_live.append(worker)
                live.append(worker)
                progress.event(
                    f"fleet floor repair: worker {worker.index} started "
                    f"({len(active_live)}/{required_floor})"
                )

            effective = _effective_rate(redis, settings.dbd_redis_rate_key, plan.initial)
            registered_states = _registered_worker_states(redis, run_id)
            registered = len(registered_states)
            registered_draining = sum(
                str(worker.get("status", "")).lower() == "draining"
                for worker in registered_states
            )
            worker_label = (
                f"{registered} active={max(0, registered - registered_draining)} "
                f"draining={registered_draining} range={minimum}-{maximum}"
                if autoscale
                else f"{registered}/{initial_workers}"
            )
            progress.update(
                done + failed,
                total,
                detail=(
                    f"workers {worker_label} | rate {effective:.2f}/{plan.ceiling:.2f}/s | "
                    f"pending {pending:,} running {running:,} failed {failed:,}"
                ),
            )

            if now >= next_state_write:
                _write_runtime_state(
                    redis,
                    run_id,
                    supervisor_pid=os.getpid(),
                    status="draining" if graceful_stop else "running",
                    autoscale=autoscale,
                    workers_initial=initial_workers,
                    workers_min=minimum,
                    workers_max=maximum,
                    workers_target=target,
                    workers_active=len(active_live),
                    workers_draining=len(draining_live),
                    workers_registered=registered,
                    exhausted_capacity=exhausted_capacity,
                    effective_worker_max=effective_maximum,
                    worker_concurrency=worker_concurrency,
                    worker_rate=worker_rate,
                    rate_mode=rate_mode,
                    rate_effective=effective,
                    rate_ceiling=plan.ceiling,
                    pending=pending,
                    running=running,
                    done=done,
                    failed=failed,
                    total=total,
                )
                next_state_write = now + 1.0

            if pending == 0 and running == 0:
                _contain_local_fleet(
                    redis,
                    run_id,
                    children,
                    "campaign_complete",
                    grace_seconds=_COMPLETE_DRAIN_SECONDS,
                )
                break

            live = [worker for worker in children if worker.process.poll() is None]
            if not live and active_work:
                if graceful_stop:
                    exit_code = 130
                    break
                progress.event("all local workers stopped while work remains")
                exit_code = 2
                break

            time.sleep(0.25)

    except KeyboardInterrupt:
        graceful_stop = True
        progress.event("Ctrl+C received: requesting graceful drain; Ctrl+C again forces stop")
        try:
            _request_stop(redis, run_id, "supervisor_keyboard_interrupt")
            _request_local_drain(redis, run_id, children, "supervisor_keyboard_interrupt")
            _write_runtime_state(
                redis,
                run_id,
                supervisor_pid=os.getpid(),
                status="draining",
                autoscale=autoscale,
                workers_initial=initial_workers,
                workers_min=minimum,
                workers_max=maximum,
                worker_concurrency=worker_concurrency,
                worker_rate=worker_rate,
                rate_mode=rate_mode,
                rate_ceiling=plan.ceiling,
            )
            try:
                while any(worker.process.poll() is None for worker in children):
                    state = _task_progress(run_id)
                    running = state.get("running", 0)
                    registered = _registered_workers(redis, run_id)
                    progress.update(
                        state.get("done", 0)
                        + state.get("not_available", 0)
                        + state.get("failed", 0),
                        state.get("total", 0),
                        detail=f"draining | workers {registered} | running {running:,}",
                        force=True,
                    )
                    time.sleep(0.5)
            except KeyboardInterrupt:
                progress.event("Second Ctrl+C received: forcing worker termination")
                _terminate_workers(children)
        finally:
            exit_code = 130

    except Exception:
        supervisor_error = True
        _contain_local_fleet(
            redis,
            run_id,
            children,
            "supervisor_failure",
            grace_seconds=_FAILURE_DRAIN_SECONDS,
        )
        progress.close(message=f"Campaign {run_id} supervisor failed; local workers are stopped")
        raise

    finally:
        for worker in children:
            _close_worker_handle(worker)
            if worker.process.poll() is not None:
                _cleanup_worker_control(redis, run_id, worker.worker_id)

        if lease_acquired:
            try:
                snapshot = _task_progress(run_id)
                remaining = (
                    snapshot.get("pending", 0)
                    + snapshot.get("retry_wait", 0)
                    + snapshot.get("running", 0)
                )
                if supervisor_error:
                    runtime_status = "supervisor_error"
                elif exit_code == 130:
                    runtime_status = "paused"
                elif exit_code == 2:
                    runtime_status = "capacity_exhausted"
                elif remaining == 0 and snapshot.get("failed", 0):
                    runtime_status = "completed_with_failures"
                elif remaining == 0:
                    runtime_status = "completed"
                else:
                    runtime_status = "stopped"
                _write_runtime_state(
                    redis,
                    run_id,
                    supervisor_pid=os.getpid(),
                    status=runtime_status,
                    autoscale=autoscale,
                    workers_initial=initial_workers,
                    workers_min=minimum,
                    workers_max=maximum,
                    exhausted_capacity=exhausted_capacity,
                    worker_concurrency=worker_concurrency,
                    worker_rate=worker_rate,
                    rate_mode=rate_mode,
                    rate_ceiling=plan.ceiling,
                    pending=snapshot.get("pending", 0) + snapshot.get("retry_wait", 0),
                    running=snapshot.get("running", 0),
                    done=snapshot.get("done", 0) + snapshot.get("not_available", 0),
                    failed=snapshot.get("failed", 0),
                    total=snapshot.get("total", 0),
                )
                if remaining == 0 and not supervisor_error:
                    _clear_stop(redis, run_id)
            except Exception:
                pass
            _release_supervisor_lease(redis, run_id, token)

        try:
            redis.close()
        except Exception:
            pass

    state = _task_progress(run_id)
    failed = state.get("failed", 0)
    if exit_code == 130:
        progress.close(
            message=f"Campaign {run_id} paused safely; resume with dbd-data campaign resume {run_id}"
        )
        return 130

    final_code = exit_code or (1 if failed else 0)
    progress.close(
        message=(
            f"Campaign {run_id} complete: done={state.get('done', 0):,} "
            f"not_available={state.get('not_available', 0):,} failed={failed:,}"
            if final_code != 2
            else f"Campaign {run_id} stopped because local fleet capacity was exhausted"
        )
    )
    return final_code


def campaign_status(run_id: int) -> dict:
    state = _task_progress(run_id)
    state["run_id"] = int(run_id)
    state["run_status"] = _run_status(run_id)
    if settings.dbd_redis_url:
        redis = Redis.from_url(settings.dbd_redis_url, decode_responses=True)
        try:
            redis.ping()
            workers = _registered_worker_states(redis, run_id)
            state["workers"] = len(workers)
            state["worker_states"] = {
                "active": sum(str(row.get("status", "")).lower() == "active" for row in workers),
                "draining": sum(
                    str(row.get("status", "")).lower() == "draining" for row in workers
                ),
                "unknown": sum(
                    str(row.get("status", "")).lower() not in {"active", "draining"}
                    for row in workers
                ),
            }
            state["effective_rate"] = _effective_rate(
                redis, settings.dbd_redis_rate_key, settings.dbd_rate_per_second
            )
            state["stop_requested"] = _stop_requested(redis, run_id)
            state["supervisor_active"] = bool(redis.exists(_supervisor_lease_key(run_id)))
            runtime = _read_runtime_state(redis, run_id)
            if runtime is not None:
                state["supervisor"] = runtime
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
