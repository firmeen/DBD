from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from uuid import uuid4

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

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
_DURABLE_WORK_CHECK_SECONDS = 1.0
_WORKER_STATE_REFRESH_SECONDS = 2.0
_REDIS_RETRY_SECONDS = 1.0
_SUPERVISOR_LEASE_FAILURE_BUDGET_SECONDS = 20.0
_REDIS_SOCKET_TIMEOUT_SECONDS = 5.0
_REDIS_CONNECT_TIMEOUT_SECONDS = 5.0
_WORKER_STARTUP_GRACE_SECONDS = 20.0
_WORKER_HEARTBEAT_TIMEOUT_SECONDS = 15.0
_WORKER_DRAIN_TIMEOUT_SECONDS = 90.0
_WORKER_KILL_WAIT_SECONDS = 5.0
_RATE_STATE_TTL_SECONDS = 600


@dataclass
class WorkerProcess:
    index: int
    worker_id: str
    process: subprocess.Popen
    restarts: int = 0
    log_handle: object | None = None
    draining: bool = False
    drain_requested_at: float | None = None
    started_at: float = field(default_factory=time.time)
    last_heartbeat_at: float | None = None
    registered_once: bool = False
    capacity_lost: bool = False


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


def _rate_target_key(rate_key: str) -> str:
    return f"{rate_key}:target-rate"


def _task_progress(run_id: int) -> dict[str, int]:
    """Read one durable queue snapshot with a single aggregate scan."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS total,
              COALESCE(SUM(status='pending'),0) AS pending,
              COALESCE(SUM(status='retry_wait'),0) AS retry_wait,
              COALESCE(SUM(status='running'),0) AS running,
              COALESCE(SUM(status='failed'),0) AS failed,
              COALESCE(SUM(status='done'),0) AS done,
              COALESCE(SUM(status='not_available'),0) AS not_available
            FROM endpoint_tasks WHERE run_id=%s
            """,
            (run_id,),
        )
        row = cur.fetchone() or {}
        return {
            key: int(row.get(key) or 0)
            for key in (
                "total",
                "pending",
                "retry_wait",
                "running",
                "failed",
                "done",
                "not_available",
            )
        }


def _has_active_work(run_id: int) -> bool:
    """Use the queue index for a cheap completion probe instead of aggregating the run."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM endpoint_tasks
            WHERE run_id=%s AND status IN ('pending','running','retry_wait')
            LIMIT 1
            """,
            (run_id,),
        )
        return cur.fetchone() is not None


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
    fleet_rate_ceiling: float | None,
    open_powershell: bool,
    restart_count: int = 0,
) -> WorkerProcess:
    """Spawn one worker atomically; no log handle survives a failed Popen."""
    worker_id = f"local-{run_id}-{index}-{uuid4().hex[:8]}"
    env = os.environ.copy()
    env["DBD_WORKER_ID"] = worker_id
    if fleet_rate_ceiling is not None:
        env["DBD_FLEET_RATE_CEILING"] = str(float(fleet_rate_ceiling))
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
    """Forget an exited local worker immediately instead of waiting for TTLs."""
    try:
        pipe = redis.pipeline(transaction=False)
        pipe.delete(_worker_key(run_id, worker_id), _worker_drain_key(run_id, worker_id))
        pipe.srem(f"{_campaign_key(run_id)}:workers", worker_id)
        pipe.execute()
    except Exception:
        pass


def _clear_stop(redis: Redis, run_id: int) -> None:
    redis.delete(f"{_campaign_key(run_id)}:stop")


def _seed_rate(
    redis: Redis,
    rate_key: str,
    initial_rate: float,
    target_rate: float | None = None,
) -> None:
    """Seed shared safety state without erasing an active penalty/cooldown."""
    target = float(initial_rate if target_rate is None else target_rate)
    pipe = redis.pipeline(transaction=False)
    pipe.set(f"{rate_key}:effective-rate", str(initial_rate), ex=_RATE_STATE_TTL_SECONDS, nx=True)
    pipe.set(
        f"{rate_key}:last-penalty-ms",
        str(int(time.time() * 1000) - 120_000),
        ex=_RATE_STATE_TTL_SECONDS,
        nx=True,
    )
    pipe.set(_rate_target_key(rate_key), str(target), ex=_RATE_STATE_TTL_SECONDS)
    pipe.execute()


def _set_rate_target(redis: Redis, rate_key: str, target_rate: float) -> float:
    """Retarget the fleet atomically; scale-down clamps immediately, scale-up recovers gradually."""
    script = """
    local target = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])
    local current = tonumber(redis.call('GET', KEYS[1]) or target)
    redis.call('SET', KEYS[2], tostring(target), 'EX', ttl)
    if current > target then
      current = target
      redis.call('SET', KEYS[1], tostring(current), 'EX', ttl)
    elseif redis.call('EXISTS', KEYS[1]) == 1 then
      redis.call('EXPIRE', KEYS[1], ttl)
    end
    return tostring(current)
    """
    result = redis.eval(
        script,
        2,
        f"{rate_key}:effective-rate",
        _rate_target_key(rate_key),
        str(float(target_rate)),
        str(_RATE_STATE_TTL_SECONDS),
    )
    return float(result)


def _effective_rate(redis: Redis, rate_key: str, fallback: float) -> float:
    raw = redis.get(f"{rate_key}:effective-rate")
    try:
        return float(raw) if raw is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _fleet_rate_target(plan: CampaignRatePlan, rate_workers: int) -> float:
    if plan.mode == "shared":
        return plan.worker_rate
    return min(plan.ceiling, plan.worker_rate * max(1, int(rate_workers)))


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
        # Missing heartbeat visibility may remove set membership, but targeted drain
        # is control state and must survive until confirmed process exit/unregister.
        redis.srem(set_key, *stale)
    return result


def _registered_workers(redis: Redis, run_id: int) -> int:
    return len(_registered_worker_states(redis, run_id))


def _redis_client() -> Redis:
    return Redis.from_url(
        settings.dbd_redis_url,
        decode_responses=True,
        socket_timeout=_REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_connect_timeout=_REDIS_CONNECT_TIMEOUT_SECONDS,
        health_check_interval=15,
    )


def _is_transient_redis_error(exc: BaseException) -> bool:
    return isinstance(exc, (RedisConnectionError, RedisTimeoutError, OSError))


def _worker_metrics(states: list[dict]) -> dict[str, int]:
    summed = (
        "claimed",
        "completed",
        "failed",
        "retry_wait",
        "not_available",
        "fetch_active",
        "archive_active",
        "project_active",
        "fetch_queue",
        "archive_queue",
        "project_queue",
        "inflight",
        "requests",
        "http_retries",
        "rate_limit_hits",
    )
    return {
        key: sum(int(row.get(key, 0) or 0) for row in states)
        for key in summed
    }


def _with_retired_metrics(live: dict[str, int], retired: dict[str, int]) -> dict[str, int]:
    result = dict(live)
    for key in (
        "claimed",
        "completed",
        "failed",
        "retry_wait",
        "not_available",
        "requests",
        "http_retries",
        "rate_limit_hits",
    ):
        result[key] = int(result.get(key, 0)) + int(retired.get(key, 0))
    return result


def _live_progress(baseline: dict[str, int], metrics: dict[str, int]) -> dict[str, int]:
    """Overlay worker heartbeat counters on one durable lifecycle baseline."""
    total = int(baseline.get("total", 0))
    baseline_done = int(baseline.get("done", 0)) + int(
        baseline.get("not_available", 0)
    )
    done = baseline_done + int(metrics.get("completed", 0))
    failed = int(baseline.get("failed", 0)) + int(metrics.get("failed", 0))
    terminal = min(total, done + failed)
    remaining = max(0, total - terminal)
    inflight = min(max(0, int(metrics.get("inflight", 0))), remaining)
    return {
        "total": total,
        "done": min(done, total),
        "failed": min(failed, total),
        "terminal": terminal,
        "remaining": remaining,
        "inflight": inflight,
        "pending_estimate": max(0, remaining - inflight),
    }


def _runtime_progress_baseline(redis: Redis, run_id: int) -> dict[str, int] | None:
    """Reuse the last supervisor snapshot on resume to avoid a multi-million-row scan."""
    runtime = _read_runtime_state(redis, run_id)
    if not runtime:
        return None
    try:
        total = int(runtime.get("total", 0) or 0)
        done = int(runtime.get("done", 0) or 0)
        failed = int(runtime.get("failed", 0) or 0)
        pending = int(runtime.get("pending", 0) or 0)
        running = int(runtime.get("running", 0) or 0)
    except (TypeError, ValueError):
        return None
    if total <= 0:
        return None
    return {
        "total": total,
        "pending": pending,
        "retry_wait": 0,
        "running": running,
        "failed": failed,
        "done": done,
        "not_available": 0,
    }


def _refresh_lease_with_grace(
    redis: Redis,
    run_id: int,
    token: str,
    *,
    now: float,
    last_confirmed: float,
) -> tuple[float, BaseException | None]:
    """Tolerate a short Redis outage, but fail closed before the 30s lease can expire."""
    try:
        _refresh_supervisor_lease(redis, run_id, token)
    except Exception as exc:
        if not _is_transient_redis_error(exc):
            raise
        unavailable_for = max(0.0, now - last_confirmed)
        if unavailable_for >= _SUPERVISOR_LEASE_FAILURE_BUDGET_SECONDS:
            raise RuntimeError(
                f"Campaign {run_id} Redis control plane unavailable for "
                f"{unavailable_for:.1f}s; stopping before supervisor lease expiry"
            ) from exc
        return last_confirmed, exc
    return now, None


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


def _wait_worker_exit(worker: WorkerProcess, timeout: float = _WORKER_KILL_WAIT_SECONDS) -> bool:
    if worker.process.poll() is not None:
        return True
    try:
        worker.process.wait(timeout=max(0.0, float(timeout)))
    except (subprocess.TimeoutExpired, OSError):
        return worker.process.poll() is not None
    return True


def _terminate_workers(workers: list[WorkerProcess]) -> None:
    if os.name == "nt":
        for worker in workers:
            _force_kill_worker(worker)
        for worker in workers:
            _wait_worker_exit(worker)
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
            _wait_worker_exit(worker)
        _close_worker_handle(worker)


def _release_dead_worker_leases(run_id: int, worker_id: str) -> int:
    """Immediately reclaim only after the local process tree is confirmed dead."""
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE endpoint_tasks
                SET status='pending',lease_owner=NULL,lease_expires_at=NULL,
                  attempts=CASE
                    WHEN stage='fetch' AND attempts>0 THEN attempts-1
                    ELSE attempts
                  END,
                  project_attempts=CASE
                    WHEN stage='project' AND project_attempts>0 THEN project_attempts-1
                    ELSE project_attempts
                  END,
                  available_at=UTC_TIMESTAMP(6),last_error='worker_process_lost'
                WHERE run_id=%s AND lease_owner=%s AND status='running'
                """,
                (run_id, worker_id),
            )
            reclaimed = int(cur.rowcount or 0)
            conn.commit()
            return reclaimed
    except Exception:
        # Lease expiry remains the durable fallback if immediate reclamation fails.
        return 0


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
    else:
        deadline = time.monotonic() + max(0.0, grace_seconds)
        while time.monotonic() < deadline:
            remaining = [worker for worker in live if worker.process.poll() is None]
            if not remaining:
                break
            time.sleep(0.1)
        remaining = [worker for worker in live if worker.process.poll() is None]
        if remaining:
            _terminate_workers(remaining)

    for worker in live:
        if worker.process.poll() is not None:
            _release_dead_worker_leases(run_id, worker.worker_id)
            _cleanup_worker_control(redis, run_id, worker.worker_id)
            _close_worker_handle(worker)


def _observe_worker_heartbeat(worker: WorkerProcess, state: dict | None) -> None:
    if not state:
        return
    try:
        heartbeat_at = float(state.get("heartbeat_at") or 0.0)
    except (TypeError, ValueError):
        heartbeat_at = 0.0
    if heartbeat_at > 0:
        worker.last_heartbeat_at = heartbeat_at
        worker.registered_once = True


def _worker_unresponsive_reason(
    worker: WorkerProcess,
    state: dict | None,
    *,
    now_wall: float,
    now_mono: float,
) -> str | None:
    if worker.process.poll() is not None or worker.capacity_lost:
        return None
    _observe_worker_heartbeat(worker, state)
    if worker.draining and worker.drain_requested_at is not None:
        if now_mono - worker.drain_requested_at > _WORKER_DRAIN_TIMEOUT_SECONDS:
            return "drain_timeout"
    if state:
        heartbeat_at = worker.last_heartbeat_at
        if heartbeat_at is not None and now_wall - heartbeat_at > _WORKER_HEARTBEAT_TIMEOUT_SECONDS:
            return "heartbeat_timeout"
        return None
    if worker.registered_once and worker.last_heartbeat_at is not None:
        if now_wall - worker.last_heartbeat_at > _WORKER_HEARTBEAT_TIMEOUT_SECONDS:
            return "heartbeat_missing"
        return None
    if now_wall - worker.started_at > _WORKER_STARTUP_GRACE_SECONDS:
        return "startup_timeout"
    return None


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
    """Run one lease-protected local fleet with graceful autoscaling and durable recovery."""
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

    redis = _redis_client()
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
    next_work_check = 0.0
    next_worker_refresh = 0.0
    last_rate_target: float | None = None
    last_effective_rate = plan.initial
    last_lease_confirmed = 0.0
    baseline: dict[str, int] = {}
    has_work = True
    registered_states: list[dict] = []
    retired_metrics: dict[str, int] = {}
    progress = ProgressLine(f"CAMPAIGN {run_id}", interval=0.75)

    def spawn_worker(*, restart_index: int | None = None, restarts: int = 0) -> WorkerProcess:
        nonlocal next_index
        index = next_index if restart_index is None else restart_index
        worker = _spawn_worker(
            run_id,
            index,
            concurrency=worker_concurrency,
            rate=plan.worker_rate,
            fleet_rate_ceiling=plan.ceiling,
            open_powershell=open_powershell,
            restart_count=restarts,
        )
        if restart_index is None:
            next_index += 1
        return worker

    def retire_metrics(row: dict | None) -> None:
        if not row:
            return
        for key in (
            "claimed",
            "completed",
            "failed",
            "retry_wait",
            "not_available",
            "requests",
            "http_retries",
            "rate_limit_hits",
        ):
            retired_metrics[key] = int(retired_metrics.get(key, 0)) + int(row.get(key, 0) or 0)

    def replacement_for(worker: WorkerProcess, reason: str) -> WorkerProcess | None:
        nonlocal exhausted_capacity
        if worker.restarts >= max_restarts:
            exhausted_capacity += 1
            progress.event(
                f"worker {worker.index} {reason}; restart budget exhausted; fleet capacity reduced by 1"
            )
            return None
        try:
            replacement = spawn_worker(
                restart_index=worker.index,
                restarts=worker.restarts + 1,
            )
        except OSError as exc:
            exhausted_capacity += 1
            progress.event(
                f"worker {worker.index} restart failed ({type(exc).__name__}); fleet capacity reduced by 1"
            )
            return None
        progress.event(
            f"worker {worker.index} {reason}; restarted ({replacement.restarts}/{max_restarts})"
        )
        return replacement

    try:
        redis.ping()
        _acquire_supervisor_lease(redis, run_id, token)
        lease_acquired = True

        # Resume from the last supervisor snapshot when available. This avoids
        # re-aggregating a multi-million-row endpoint queue just to redraw progress.
        baseline = _runtime_progress_baseline(redis, run_id) or _task_progress(run_id)
        has_work = _has_active_work(run_id)

        now = time.monotonic()
        last_lease_confirmed = now
        next_lease_refresh = now + _SUPERVISOR_REFRESH_SECONDS
        next_work_check = now + _DURABLE_WORK_CHECK_SECONDS
        next_worker_refresh = now
        _clear_stop(redis, run_id)
        initial_rate_target = _fleet_rate_target(plan, initial_workers)
        _seed_rate(redis, settings.dbd_redis_rate_key, plan.initial, initial_rate_target)
        last_rate_target = initial_rate_target

        if has_work:
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
            workers_target=initial_workers if has_work else 0,
            workers_active=len(children),
            workers_draining=0,
            exhausted_capacity=0,
            worker_concurrency=worker_concurrency,
            worker_rate=worker_rate,
            rate_mode=rate_mode,
            rate_target=initial_rate_target,
            rate_ceiling=plan.ceiling,
        )

        while True:
            now = time.monotonic()
            now_wall = time.time()
            control_plane_degraded = False

            if now >= next_lease_refresh:
                last_lease_confirmed, lease_error = _refresh_lease_with_grace(
                    redis,
                    run_id,
                    token,
                    now=now,
                    last_confirmed=last_lease_confirmed,
                )
                if lease_error is not None:
                    control_plane_degraded = True
                    next_lease_refresh = now + _REDIS_RETRY_SECONDS
                    progress.event(
                        f"Redis control plane timeout; preserving workers and retrying lease "
                        f"({now - last_lease_confirmed:.1f}s degraded)"
                    )
                else:
                    next_lease_refresh = now + _SUPERVISOR_REFRESH_SECONDS

            if now >= next_worker_refresh:
                try:
                    registered_states = _registered_worker_states(redis, run_id)
                except Exception as exc:
                    if not _is_transient_redis_error(exc):
                        raise
                    control_plane_degraded = True
                    next_worker_refresh = now + _REDIS_RETRY_SECONDS
                else:
                    next_worker_refresh = now + _WORKER_STATE_REFRESH_SECONDS

            live_metrics = _worker_metrics(registered_states)
            campaign_metrics = _with_retired_metrics(live_metrics, retired_metrics)
            live_progress = _live_progress(baseline, campaign_metrics)
            total = live_progress["total"]
            pending = live_progress["pending_estimate"]
            running = live_progress["inflight"]
            failed = live_progress["failed"]
            done = live_progress["done"]

            if now >= next_work_check:
                try:
                    has_work = _has_active_work(run_id)
                except Exception:
                    # Completion observation must not kill a healthy fleet.
                    pass
                next_work_check = now + _DURABLE_WORK_CHECK_SECONDS
            active_work = has_work

            state_by_id = {
                str(row.get("worker_id")): row
                for row in registered_states
                if row.get("worker_id")
            }

            stop_requested = False
            if not control_plane_degraded:
                try:
                    stop_requested = _stop_requested(redis, run_id)
                except Exception as exc:
                    if not _is_transient_redis_error(exc):
                        raise
                    control_plane_degraded = True
            if stop_requested and not graceful_stop:
                graceful_stop = True
                _request_local_drain(redis, run_id, children, "campaign_stop")
                progress.event("campaign stop requested: draining workers without claiming new work")

            survivors: list[WorkerProcess] = []
            for worker in children:
                row = state_by_id.get(worker.worker_id)
                code = worker.process.poll()
                if code is None:
                    reason = (
                        None
                        if control_plane_degraded
                        else _worker_unresponsive_reason(
                            worker,
                            row,
                            now_wall=now_wall,
                            now_mono=now,
                        )
                    )
                    if reason is None:
                        survivors.append(worker)
                        continue

                    progress.event(f"worker {worker.index} unresponsive ({reason}); stopping process tree")
                    _force_kill_worker(worker)
                    if not _wait_worker_exit(worker):
                        if not worker.capacity_lost:
                            exhausted_capacity += 1
                            worker.capacity_lost = True
                        worker.draining = True
                        worker.drain_requested_at = worker.drain_requested_at or now
                        survivors.append(worker)
                        progress.event(
                            f"worker {worker.index} could not be confirmed stopped; capacity quarantined"
                        )
                        continue
                    code = worker.process.poll()
                    retire_metrics(row)
                    reclaimed = _release_dead_worker_leases(run_id, worker.worker_id)
                    _close_worker_handle(worker)
                    _cleanup_worker_control(redis, run_id, worker.worker_id)
                    if reclaimed:
                        progress.event(
                            f"worker {worker.index} dead leases reclaimed: {reclaimed} task(s)"
                        )
                    if graceful_stop or worker.draining or not active_work:
                        continue
                    replacement = replacement_for(worker, f"stopped after {reason}")
                    if replacement is not None:
                        survivors.append(replacement)
                    continue

                retire_metrics(row)
                _release_dead_worker_leases(run_id, worker.worker_id)
                _close_worker_handle(worker)
                _cleanup_worker_control(redis, run_id, worker.worker_id)
                if graceful_stop or worker.draining or not active_work:
                    continue
                replacement = replacement_for(worker, f"exited {code}")
                if replacement is not None:
                    survivors.append(replacement)
            children = survivors

            live = [worker for worker in children if worker.process.poll() is None]
            active_live = [worker for worker in live if not worker.draining and not worker.capacity_lost]
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
            if autoscale and not graceful_stop and not control_plane_degraded:
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

            # Draining workers can still finish already-owned HTTP work, so they
            # remain part of the upstream budget until their process exits.
            rate_workers = len([worker for worker in live if not worker.capacity_lost])
            rate_target = _fleet_rate_target(plan, rate_workers)
            effective = last_effective_rate
            if not control_plane_degraded:
                try:
                    if rate_target != last_rate_target or now >= next_state_write:
                        effective = _set_rate_target(
                            redis,
                            settings.dbd_redis_rate_key,
                            rate_target,
                        )
                        last_rate_target = rate_target
                    else:
                        effective = _effective_rate(
                            redis,
                            settings.dbd_redis_rate_key,
                            last_effective_rate,
                        )
                    last_effective_rate = effective
                except Exception as exc:
                    if not _is_transient_redis_error(exc):
                        raise
                    control_plane_degraded = True

            registered = len(registered_states)
            registered_draining = sum(
                str(worker.get("status", "")).lower() == "draining"
                for worker in registered_states
            )
            stale_registered = sum(
                now_wall - float(worker.get("heartbeat_at") or now_wall)
                > _WORKER_HEARTBEAT_TIMEOUT_SECONDS
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
                    f"workers {worker_label} | rate {effective:.2f}/{rate_target:.2f}/{plan.ceiling:.2f}/s | "
                    f"q {live_metrics.get('fetch_queue', 0)}/{live_metrics.get('archive_queue', 0)}/"
                    f"{live_metrics.get('project_queue', 0)} | http {campaign_metrics.get('requests', 0):,} "
                    f"429/503 {campaign_metrics.get('rate_limit_hits', 0):,} | "
                    f"pending~ {pending:,} running~ {running:,} failed~ {failed:,}"
                ),
            )

            if now >= next_state_write and not control_plane_degraded:
                try:
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
                        workers_stale=stale_registered,
                        exhausted_capacity=exhausted_capacity,
                        effective_worker_max=effective_maximum,
                        worker_concurrency=worker_concurrency,
                        worker_rate=worker_rate,
                        rate_mode=rate_mode,
                        rate_effective=effective,
                        rate_target=rate_target,
                        rate_ceiling=plan.ceiling,
                        worker_metrics=campaign_metrics,
                        progress_source="redis_worker_heartbeat",
                        pending=pending,
                        running=running,
                        done=done,
                        failed=failed,
                        total=total,
                    )
                except Exception as exc:
                    if not _is_transient_redis_error(exc):
                        raise
                next_state_write = now + 1.0

            if not active_work:
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
                    time.sleep(0.75)
            except KeyboardInterrupt:
                progress.event("Second Ctrl+C received: forcing worker termination")
                _terminate_workers(children)
                for worker in children:
                    if worker.process.poll() is not None:
                        _release_dead_worker_leases(run_id, worker.worker_id)
                        _cleanup_worker_control(redis, run_id, worker.worker_id)
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
                _release_dead_worker_leases(run_id, worker.worker_id)
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
                    rate_target=last_rate_target,
                    rate_ceiling=plan.ceiling,
                    worker_metrics=_with_retired_metrics(
                        _worker_metrics(registered_states), retired_metrics
                    ),
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
            now = time.time()
            worker_details = []
            for row in workers:
                item = dict(row)
                try:
                    heartbeat_at = float(item.get("heartbeat_at") or 0.0)
                except (TypeError, ValueError):
                    heartbeat_at = 0.0
                item["heartbeat_age_seconds"] = (
                    max(0.0, now - heartbeat_at) if heartbeat_at else None
                )
                worker_details.append(item)
            state["workers"] = len(workers)
            state["worker_states"] = {
                "active": sum(str(row.get("status", "")).lower() == "active" for row in workers),
                "draining": sum(
                    str(row.get("status", "")).lower() == "draining" for row in workers
                ),
                "stale": sum(
                    (now - float(row.get("heartbeat_at") or now))
                    > _WORKER_HEARTBEAT_TIMEOUT_SECONDS
                    for row in workers
                ),
                "unknown": sum(
                    str(row.get("status", "")).lower() not in {"active", "draining"}
                    for row in workers
                ),
            }
            state["worker_metrics"] = _worker_metrics(workers)
            state["worker_details"] = worker_details
            state["effective_rate"] = _effective_rate(
                redis, settings.dbd_redis_rate_key, settings.dbd_rate_per_second
            )
            target_raw = redis.get(_rate_target_key(settings.dbd_redis_rate_key))
            try:
                state["rate_target"] = float(target_raw) if target_raw is not None else None
            except (TypeError, ValueError):
                state["rate_target"] = None
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
