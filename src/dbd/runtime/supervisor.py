from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from uuid import uuid4

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ..core.progress import LiveCampaignDashboard, TerminalSnapshot
from ..core.settings import PROJECT_ROOT, settings
from ..storage.mysql.engine import connect
from .locking import assert_manager_lock_free, runtime_handoff_lock

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
_ORPHAN_DRAIN_TIMEOUT_SECONDS = 30.0
_ORPHAN_DRAIN_POLL_SECONDS = 0.25
_REDIS_RETRY_SECONDS = 1.0
_SUPERVISOR_LEASE_FAILURE_BUDGET_SECONDS = 20.0
_REDIS_SOCKET_TIMEOUT_SECONDS = 5.0
_REDIS_CONNECT_TIMEOUT_SECONDS = 5.0
_WORKER_STARTUP_GRACE_SECONDS = 20.0
_WORKER_HEARTBEAT_TIMEOUT_SECONDS = 15.0
_WORKER_DRAIN_TIMEOUT_SECONDS = 90.0
_WORKER_KILL_WAIT_SECONDS = 5.0
_REQUEST_STALL_TIMEOUT_SECONDS = 90.0
_REQUEST_STALL_EXIT_CODE = 76
_RATE_STATE_TTL_SECONDS = 600
_DB_POOLS_PER_WORKER = 2
_REDIS_CLIENTS_PER_WORKER = 1
_REDIS_SUPERVISOR_CLIENTS = 1


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


@dataclass
class WorkerProgressWatchdog:
    """Detect request-path stagnation per worker without freezing healthy peers."""

    timeout_seconds: float = _REQUEST_STALL_TIMEOUT_SECONDS
    samples: dict[str, tuple[tuple[int, int], float]] = field(default_factory=dict)

    def forget(self, worker_id: str) -> None:
        self.samples.pop(str(worker_id), None)

    def observe(
        self,
        worker_id: str,
        state: dict | None,
        *,
        now: float,
        active_work: bool,
        control_healthy: bool,
        draining: bool,
    ) -> float | None:
        worker_id = str(worker_id)
        if not state:
            self.forget(worker_id)
            return None
        fetch_active = int(state.get("fetch_active", 0) or 0)
        eligible = (
            active_work
            and control_healthy
            and not draining
            and fetch_active > 0
        )
        # Admission and downstream completion are diagnostic signals, but they
        # must not mask a dead HTTP path. Only actual request/response movement
        # resets this worker's request-progress timeout.
        marker = (
            int(state.get("requests", 0) or 0),
            int(state.get("responses", 0) or 0),
        )
        previous = self.samples.get(worker_id)
        deliberate_shared_wait = (
            bool(state.get("shared_admission_waiting"))
            and int(state.get("shared_admission_waiters", 0) or 0) >= fetch_active
        )
        if not eligible or deliberate_shared_wait:
            # Explicit Redis pacing/cooldown is not a dead request path. Reset
            # the worker-local timer while every active fetch is intentionally
            # parked behind shared admission so Retry-After remains authoritative.
            self.samples[worker_id] = (marker, now)
            return None
        if previous is None or previous[0] != marker:
            self.samples[worker_id] = (marker, now)
            return None
        stalled_for = max(0.0, now - previous[1])
        if stalled_for < float(self.timeout_seconds):
            return None
        return stalled_for


@dataclass(frozen=True)
class CampaignRatePlan:
    mode: str
    worker_rate: float
    workers: int
    ceiling: float
    initial: float
    independent: bool = False


@dataclass(frozen=True)
class CampaignResourcePlan:
    max_workers: int
    supervisor_db_pool: int
    db_connections_budget: int
    db_pool_per_worker: int
    archive_workers_budget: int
    archive_workers_per_worker: int
    projection_workers_budget: int
    projection_workers_per_worker: int
    redis_connections_budget: int
    redis_connections_per_client: int

    @property
    def db_connections_effective_max(self) -> int:
        return self.supervisor_db_pool + (
            self.db_pool_per_worker * _DB_POOLS_PER_WORKER * self.max_workers
        )

    @property
    def archive_workers_effective_max(self) -> int:
        return self.archive_workers_per_worker * self.max_workers

    @property
    def projection_workers_effective_max(self) -> int:
        return self.projection_workers_per_worker * self.max_workers

    @property
    def redis_connections_effective_max(self) -> int:
        return self.redis_connections_per_client * (
            _REDIS_SUPERVISOR_CLIENTS + _REDIS_CLIENTS_PER_WORKER * self.max_workers
        )


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
        fetch_queue: int = 0,
        fetch_active: int = 0,
        archive_queue: int = 0,
        project_queue: int = 0,
        observed_http_rate: float | None = None,
        rate_target: float | None = None,
        worker_rate: float | None = None,
        hard_fleet_cap: bool = False,
        healthy_workers: int | None = None,
        fleet_availability_degraded: bool = False,
        rate_limit_delta: int = 0,
    ) -> int:
        """Scale only when the pipeline needs capacity and downstream is healthy."""
        current = max(0, int(current))
        pending = max(0, int(pending))
        running = max(0, int(running))
        fetch_queue = max(0, int(fetch_queue))
        fetch_active = max(0, int(fetch_active))
        archive_queue = max(0, int(archive_queue))
        project_queue = max(0, int(project_queue))
        effective_maximum = max(0, min(int(effective_maximum), self.maximum))
        minimum = min(self.minimum, effective_maximum)
        capacity = max(1, int(self.worker_concurrency))

        if current < minimum:
            self.low_pressure_observations = 0
            self.next_decision_at = now + _AUTOSCALE_UP_COOLDOWN_SECONDS
            return min(current + 1, minimum)
        if now < self.next_decision_at:
            return current

        # Multiple independent worker availability failures are evidence of a
        # fleet/upstream incident, not missing local capacity. Freeze autoscale
        # rather than creating a replacement storm. A single unhealthy worker
        # may receive one spare process so minimum healthy capacity is preserved.
        if fleet_availability_degraded:
            self.low_pressure_observations = 0
            self.next_decision_at = now + _AUTOSCALE_UP_COOLDOWN_SECONDS
            return current
        if (
            healthy_workers is not None
            and int(healthy_workers) < minimum
            and current < effective_maximum
        ):
            self.low_pressure_observations = 0
            self.next_decision_at = now + _AUTOSCALE_UP_COOLDOWN_SECONDS
            return current + 1

        downstream_limit = max(capacity * 2, current * capacity * 2)
        downstream_backlog = (
            archive_queue > downstream_limit or project_queue > downstream_limit
        )
        if rate_limit_delta > 0 or downstream_backlog:
            self.low_pressure_observations = 0
            self.next_decision_at = now + _AUTOSCALE_UP_COOLDOWN_SECONDS
            return current

        high_pending = pending > max(capacity, current * capacity * 4)
        queue_starved = fetch_queue <= max(capacity, current * capacity)
        under_rate = (
            observed_http_rate is not None
            and rate_target is not None
            and observed_http_rate < (float(rate_target) * 0.90)
        )
        if hard_fleet_cap:
            local_capacity = (
                float(worker_rate) * current
                if worker_rate is not None and float(worker_rate) > 0
                else 0.0
            )
            capacity_shortfall = (
                rate_target is not None
                and local_capacity > 0
                and local_capacity < float(rate_target) * 0.98
            )
            saturated_fetchers = fetch_active >= max(
                1, int(current * capacity * 0.75)
            )
            scale_pressure = capacity_shortfall or (
                saturated_fetchers and under_rate
            )
        else:
            scale_pressure = queue_starved or under_rate
        if current < effective_maximum and high_pending and scale_pressure:
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
    fleet_rate_initial: float | None = None,
    fleet_rate_max: float | None = None,
) -> CampaignRatePlan:
    if mode not in _RATE_MODES:
        raise ValueError(f"rate mode must be one of {sorted(_RATE_MODES)}")
    worker_rate = float(worker_rate)
    workers = int(workers)
    initial_workers = workers if initial_workers is None else int(initial_workers)
    if worker_rate <= 0 or workers < 1 or not 1 <= initial_workers <= workers:
        raise ValueError("worker rate/count and initial worker count must be positive and ordered")

    independent = fleet_rate_initial is not None or fleet_rate_max is not None
    if independent:
        ceiling = float(fleet_rate_max if fleet_rate_max is not None else fleet_rate_initial)
        requested_initial = float(
            fleet_rate_initial if fleet_rate_initial is not None else ceiling
        )
        if requested_initial <= 0 or ceiling <= 0 or requested_initial > ceiling:
            raise ValueError("fleet rate must satisfy 0 < initial <= max")
        # Explicit fleet configuration means each worker keeps its own local
        # hard cap while Redis enforces one aggregate hard cap. The old
        # implementation accidentally replaced worker_rate with the fleet
        # ceiling and treated initial as adaptive fleet state.
        return CampaignRatePlan(mode, worker_rate, workers, ceiling, ceiling, True)

    if mode == "shared":
        ceiling = initial = worker_rate
    elif mode == "multiply":
        ceiling = worker_rate * workers
        initial = worker_rate * initial_workers
    else:
        ceiling = worker_rate * workers
        initial = worker_rate
    return CampaignRatePlan(mode, worker_rate, workers, ceiling, initial, False)


def build_resource_plan(
    max_workers: int,
    *,
    db_pool_size: int,
    archive_workers: int,
    projection_workers: int,
    redis_max_connections: int,
    db_connections_max: int | None = None,
    archive_workers_max: int | None = None,
    projection_workers_max: int | None = None,
    redis_connections_max: int | None = None,
) -> CampaignResourcePlan:
    max_workers = int(max_workers)
    db_pool_size = int(db_pool_size)
    archive_workers = int(archive_workers)
    projection_workers = int(projection_workers)
    redis_max_connections = int(redis_max_connections)
    if min(max_workers, db_pool_size, archive_workers, projection_workers, redis_max_connections) < 1:
        raise ValueError("worker/resource defaults must be positive")

    supervisor_db_pool = db_pool_size
    db_budget = int(
        db_connections_max
        if db_connections_max is not None
        else supervisor_db_pool + (max_workers * _DB_POOLS_PER_WORKER * db_pool_size)
    )
    db_per_worker = min(
        db_pool_size,
        (db_budget - supervisor_db_pool) // (max_workers * _DB_POOLS_PER_WORKER),
    )
    if db_per_worker < 1:
        minimum = supervisor_db_pool + (max_workers * _DB_POOLS_PER_WORKER)
        raise ValueError(
            f"fleet DB connection budget {db_budget} is too small; "
            f"need at least {minimum} for workers-max={max_workers}"
        )

    archive_budget = int(
        archive_workers_max if archive_workers_max is not None else max_workers * archive_workers
    )
    archive_per_worker = min(archive_workers, archive_budget // max_workers, db_per_worker)
    if archive_per_worker < 1:
        raise ValueError(
            f"fleet archive-worker budget {archive_budget} is too small; need at least {max_workers}"
        )

    projection_budget = int(
        projection_workers_max
        if projection_workers_max is not None
        else max_workers * min(projection_workers, db_pool_size)
    )
    projection_per_worker = min(
        projection_workers, projection_budget // max_workers, db_per_worker
    )
    if projection_per_worker < 1:
        raise ValueError(
            f"fleet projection-worker budget {projection_budget} is too small; "
            f"need at least {max_workers}"
        )

    redis_clients = _REDIS_SUPERVISOR_CLIENTS + (_REDIS_CLIENTS_PER_WORKER * max_workers)
    redis_budget = int(
        redis_connections_max
        if redis_connections_max is not None
        else redis_clients * redis_max_connections
    )
    redis_per_client = min(redis_max_connections, redis_budget // redis_clients)
    if redis_per_client < 1:
        raise ValueError(
            f"fleet Redis connection budget {redis_budget} is too small; "
            f"need at least {redis_clients} for workers-max={max_workers}"
        )

    return CampaignResourcePlan(
        max_workers=max_workers,
        supervisor_db_pool=supervisor_db_pool,
        db_connections_budget=db_budget,
        db_pool_per_worker=db_per_worker,
        archive_workers_budget=archive_budget,
        archive_workers_per_worker=archive_per_worker,
        projection_workers_budget=projection_budget,
        projection_workers_per_worker=projection_per_worker,
        redis_connections_budget=redis_budget,
        redis_connections_per_client=redis_per_client,
    )


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


def _scheduler_mode(run_id: int) -> str:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT scheduler_mode FROM worker_runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Run {run_id} not found")
    return str(row["scheduler_mode"] or "legacy")


def _task_progress(run_id: int) -> dict[str, int]:
    """Exact logical-resource reconciliation; never use this as the live hot path."""
    mode = _scheduler_mode(run_id)
    with connect() as conn, conn.cursor() as cur:
        if mode == "fabric_v2":
            cur.execute(
                """
                SELECT
                  COALESCE(SUM(BIT_COUNT(required_mask)),0) AS total,
                  COALESCE(SUM(BIT_COUNT(done_mask)),0) AS done,
                  COALESCE(SUM(BIT_COUNT(not_available_mask)),0) AS not_available,
                  COALESCE(SUM(BIT_COUNT(failed_mask & ~dirty_mask)),0) AS failed,
                  COALESCE(SUM(status='running'),0) AS running,
                  COALESCE(SUM(status='retry_wait'),0) AS retry_wait
                FROM company_collection_state WHERE run_id=%s
                """,
                (run_id,),
            )
            row = cur.fetchone() or {}
            result = {
                key: int(row.get(key) or 0)
                for key in (
                    "total",
                    "retry_wait",
                    "running",
                    "failed",
                    "done",
                    "not_available",
                )
            }
            terminal = result["done"] + result["not_available"] + result["failed"]
            result["pending"] = max(
                0,
                result["total"] - terminal - result["running"] - result["retry_wait"],
            )
            return result
        if mode != "legacy":
            raise ValueError(f"Unsupported scheduler mode {mode!r}")
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


def _failure_diagnostics(run_id: int, limit: int = 8) -> dict:
    """Read-only diagnostics for explicit status; never infer per-endpoint error causes."""
    mode = _scheduler_mode(run_id)
    with connect() as conn, conn.cursor() as cur:
        if mode == "fabric_v2":
            cur.execute(
                """
                SELECT e.endpoint,COUNT(DISTINCT s.id) AS states,COUNT(*) AS resources
                FROM company_collection_state s
                JOIN (
                  SELECT 'profile' AS endpoint, 1 AS bit
                  UNION ALL SELECT 'financial_submissions' AS endpoint, 2 AS bit
                  UNION ALL SELECT 'financial_statements' AS endpoint, 4 AS bit
                  UNION ALL SELECT 'industry_benchmark' AS endpoint, 8 AS bit
                  UNION ALL SELECT 'committees' AS endpoint, 16 AS bit
                  UNION ALL SELECT 'committee_signs' AS endpoint, 32 AS bit
                  UNION ALL SELECT 'transformations' AS endpoint, 64 AS bit
                  UNION ALL SELECT 'nations' AS endpoint, 128 AS bit
                  UNION ALL SELECT 'nation_history' AS endpoint, 256 AS bit
                  UNION ALL SELECT 'name_history' AS endpoint, 512 AS bit
                  UNION ALL SELECT 'capital_history' AS endpoint, 1024 AS bit
                ) e ON (s.failed_mask & ~s.dirty_mask & e.bit)<>0
                WHERE s.run_id=%s
                GROUP BY e.endpoint
                ORDER BY resources DESC,e.endpoint
                LIMIT %s
                """,
                (run_id, max(1, int(limit))),
            )
            by_endpoint = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT COALESCE(NULLIF(last_error,''),'unknown') AS error_code,
                       stage,status,COUNT(*) AS states
                FROM company_collection_state
                WHERE run_id=%s AND last_error IS NOT NULL AND last_error<>''
                GROUP BY error_code,stage,status
                ORDER BY states DESC,error_code
                LIMIT %s
                """,
                (run_id, max(1, int(limit))),
            )
            latest_errors = [dict(row) for row in cur.fetchall()]
            scope = "state_latest_error_not_per_endpoint"
        else:
            cur.execute(
                """
                SELECT endpoint,COUNT(*) AS states,COUNT(*) AS resources
                FROM endpoint_tasks
                WHERE run_id=%s AND status='failed'
                GROUP BY endpoint
                ORDER BY resources DESC,endpoint
                LIMIT %s
                """,
                (run_id, max(1, int(limit))),
            )
            by_endpoint = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT COALESCE(NULLIF(last_error,''),'unknown') AS error_code,
                       stage,status,COUNT(*) AS states
                FROM endpoint_tasks
                WHERE run_id=%s AND status='failed'
                GROUP BY error_code,stage,status
                ORDER BY states DESC,error_code
                LIMIT %s
                """,
                (run_id, max(1, int(limit))),
            )
            latest_errors = [dict(row) for row in cur.fetchall()]
            scope = "task_error_exact"

        cur.execute(
            """
            SELECT error_code,http_status,stage,outcome,COUNT(*) AS events,
                   MAX(error_message) AS sample_message
            FROM collection_failure_events
            WHERE run_id=%s
            GROUP BY error_code,http_status,stage,outcome
            ORDER BY events DESC,error_code
            LIMIT %s
            """,
            (run_id, max(1, int(limit))),
        )
        failure_events = [dict(row) for row in cur.fetchall()]

    return {
        "by_endpoint": by_endpoint,
        "latest_errors": latest_errors,
        "failure_events": failure_events,
        "error_scope": scope,
    }


def _has_active_work(run_id: int) -> bool:
    """Use scheduler indexes for a cheap completion probe without aggregating the run."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM worker_runs r
            WHERE r.id=%s AND (
              (r.scheduler_mode='legacy' AND EXISTS(
                SELECT 1 FROM endpoint_tasks t
                WHERE t.run_id=r.id AND t.status IN ('pending','running','retry_wait')
                LIMIT 1
              ))
              OR
              (r.scheduler_mode='fabric_v2' AND EXISTS(
                SELECT 1 FROM company_collection_state s
                WHERE s.run_id=r.id AND s.status IN ('pending','running','retry_wait')
                LIMIT 1
              ))
            )
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


def _worker_registry_path(run_id: int):
    return PROJECT_ROOT / "data" / "guardian" / f"run-{int(run_id)}.workers.json"


def _write_worker_registry(
    run_id: int,
    supervisor_generation: str | None,
    workers: list[WorkerProcess],
) -> None:
    """Persist exact local child PIDs for bounded hard-loss recovery.

    This registry is operational metadata only. Redis remains the live control
    plane and MySQL remains durable work truth. A hard-killed Supervisor leaves
    the last registry behind so the outer watchdog can inspect exact PIDs before
    falling back to a full Win32 process inventory.
    """
    path = _worker_registry_path(run_id)
    live = [
        {
            "worker_id": worker.worker_id,
            "index": int(worker.index),
            "pid": int(worker.process.pid),
            "started_at": float(worker.started_at),
        }
        for worker in workers
        if worker.process.poll() is None
    ]
    payload = {
        "run_id": int(run_id),
        "supervisor_pid": os.getpid(),
        "supervisor_generation": _normalize_supervisor_generation(
            supervisor_generation
        )
        or None,
        "updated_at": time.time(),
        "workers": live,
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
        temp.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temp, path)
    except OSError:
        return


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


def _normalize_supervisor_generation(value: str | None) -> str:
    generation = str(value or "").strip().lower()
    if not generation:
        return ""
    if len(generation) != 16 or any(
        char not in "0123456789abcdef" for char in generation
    ):
        raise ValueError("supervisor_generation must be a 16-character lowercase hex token")
    return generation


def _spawn_worker(
    run_id: int,
    index: int,
    *,
    concurrency: int,
    rate: float,
    fleet_rate_ceiling: float | None,
    open_powershell: bool,
    resource_plan: CampaignResourcePlan | None = None,
    restart_count: int = 0,
    redis_rate_key: str | None = None,
    supervisor_generation: str | None = None,
) -> WorkerProcess:
    """Spawn one worker atomically; no log handle survives a failed Popen."""
    generation = _normalize_supervisor_generation(supervisor_generation)
    worker_id = (
        f"local-{run_id}-g{generation}-{index}-{uuid4().hex[:8]}"
        if generation
        else f"local-{run_id}-{index}-{uuid4().hex[:8]}"
    )
    env = os.environ.copy()
    env["DBD_WORKER_ID"] = worker_id
    # Headless workers write Rich/plain output into UTF-8 log files. On Thai
    # Windows the inherited console code page may otherwise be cp874, which
    # cannot encode presentation glyphs and can turn a successful worker exit
    # into UnicodeEncodeError/exit 1.
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    if fleet_rate_ceiling is not None:
        env["DBD_FLEET_RATE_CEILING"] = str(float(fleet_rate_ceiling))
    if redis_rate_key:
        env["DBD_REDIS_RATE_KEY"] = str(redis_rate_key)
    if resource_plan is not None:
        env["DB_POOL_SIZE"] = str(resource_plan.db_pool_per_worker)
        env["DBD_ARCHIVE_WORKERS"] = str(resource_plan.archive_workers_per_worker)
        env["DBD_PROJECTION_WORKERS"] = str(resource_plan.projection_workers_per_worker)
        env["DBD_REDIS_MAX_CONNECTIONS"] = str(resource_plan.redis_connections_per_client)
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
    *,
    hard_fleet_cap: bool = False,
) -> None:
    """Seed shared pacing state.

    Legacy derived-rate modes preserve an active adaptive penalty. Explicit
    fleet-rate mode is different: Redis is the aggregate hard ceiling, so stale
    global penalty/cooldown state from the previous architecture is normalized
    in place without deleting coordination keys.
    """
    target = float(initial_rate if target_rate is None else target_rate)
    pipe = redis.pipeline(transaction=False)
    if hard_fleet_cap:
        # Supervisor activation is fenced from the previous fleet, so reset the
        # shared reservation cursor as well as adaptive state. This prevents an
        # old low-rate/cooldown schedule from delaying a new hard-cap fleet.
        pipe.set(
            rate_key,
            str(int(time.time() * 1000)),
            ex=_RATE_STATE_TTL_SECONDS,
        )
        pipe.set(
            f"{rate_key}:effective-rate",
            str(target),
            ex=_RATE_STATE_TTL_SECONDS,
        )
        pipe.set(
            f"{rate_key}:cooldown-until-ms",
            "0",
            ex=_RATE_STATE_TTL_SECONDS,
        )
        pipe.set(
            f"{rate_key}:last-penalty-ms",
            str(int(time.time() * 1000) - 120_000),
            ex=_RATE_STATE_TTL_SECONDS,
        )
    else:
        pipe.set(
            f"{rate_key}:effective-rate",
            str(initial_rate),
            ex=_RATE_STATE_TTL_SECONDS,
            nx=True,
        )
        pipe.set(
            f"{rate_key}:last-penalty-ms",
            str(int(time.time() * 1000) - 120_000),
            ex=_RATE_STATE_TTL_SECONDS,
            nx=True,
        )
    pipe.set(_rate_target_key(rate_key), str(target), ex=_RATE_STATE_TTL_SECONDS)
    pipe.execute()


def _set_rate_target(
    redis: Redis,
    rate_key: str,
    target_rate: float,
    *,
    hard_fleet_cap: bool = False,
) -> float:
    """Retarget shared fleet pacing without coupling worker failures."""
    if hard_fleet_cap:
        pipe = redis.pipeline(transaction=False)
        pipe.set(
            f"{rate_key}:effective-rate",
            str(float(target_rate)),
            ex=_RATE_STATE_TTL_SECONDS,
        )
        pipe.set(
            _rate_target_key(rate_key),
            str(float(target_rate)),
            ex=_RATE_STATE_TTL_SECONDS,
        )
        pipe.execute()
        return float(target_rate)

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
    if plan.independent:
        return plan.ceiling
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


def _redis_client(max_connections: int | None = None) -> Redis:
    kwargs = {
        "decode_responses": True,
        "socket_timeout": _REDIS_SOCKET_TIMEOUT_SECONDS,
        "socket_connect_timeout": _REDIS_CONNECT_TIMEOUT_SECONDS,
        "health_check_interval": 15,
    }
    if max_connections is not None:
        kwargs["max_connections"] = max(1, int(max_connections))
    return Redis.from_url(settings.dbd_redis_url, **kwargs)


def _is_transient_redis_error(exc: BaseException) -> bool:
    return isinstance(exc, (RedisConnectionError, RedisTimeoutError, OSError))


def _worker_metrics(states: list[dict]) -> dict[str, int]:
    summed = (
        "claimed",
        "completed",
        "done",
        "failed",
        "retry_wait",
        "not_available",
        "fetch_active",
        "archive_active",
        "project_active",
        "fetch_queue",
        "fetch_claim_window",
        "archive_queue",
        "project_queue",
        "inflight",
        "requests",
        "responses",
        "auth_failures",
        "http_retries",
        "rate_limit_hits",
        "redis_commands",
        "redis_admissions",
        "redis_pool_in_use",
        "redis_pool_created",
        "redis_pool_max",
    )
    return {
        key: sum(int(row.get(key, 0) or 0) for row in states)
        for key in summed
    }


def _fleet_circuit_state(states: list[dict]) -> str:
    """Return the most restrictive global availability state visible in the fleet."""
    priority = {"unknown": 0, "closed": 1, "half_open": 2, "open": 3}
    resolved = "unknown"
    for row in states:
        state = str(row.get("circuit_global") or "unknown").lower()
        if priority.get(state, 0) > priority.get(resolved, 0):
            resolved = state
    return resolved


def _with_retired_metrics(live: dict[str, int], retired: dict[str, int]) -> dict[str, int]:
    result = dict(live)
    for key in (
        "claimed",
        "completed",
        "done",
        "failed",
        "retry_wait",
        "not_available",
        "requests",
        "responses",
        "auth_failures",
        "http_retries",
        "rate_limit_hits",
        "redis_commands",
        "redis_admissions",
    ):
        result[key] = int(result.get(key, 0)) + int(retired.get(key, 0))
    return result


def _live_progress(baseline: dict[str, int], metrics: dict[str, int]) -> dict[str, int]:
    """Overlay ephemeral worker counters on one durable lifecycle baseline."""
    total = int(baseline.get("total", 0))
    done_only = int(baseline.get("done", 0)) + int(metrics.get("done", 0))
    not_available = int(baseline.get("not_available", 0)) + int(
        metrics.get("not_available", 0)
    )
    failed = int(baseline.get("failed", 0)) + int(metrics.get("failed", 0))
    terminal = min(total, done_only + not_available + failed)
    remaining = max(0, total - terminal)
    inflight = min(max(0, int(metrics.get("inflight", 0))), remaining)
    return {
        "total": total,
        "done_only": min(done_only, total),
        "not_available": min(not_available, total),
        "failed": min(failed, total),
        "terminal": terminal,
        "remaining": remaining,
        "inflight": inflight,
        "pending_estimate": max(0, remaining - inflight),
    }


def _campaign_terminal_snapshot(
    run_id: int,
    *,
    status: str,
    live_progress: dict[str, int],
    live_metrics: dict[str, int],
    campaign_metrics: dict[str, int],
    workers_registered: int,
    workers_target: int,
    workers_active: int,
    workers_draining: int,
    workers_stale: int,
    workers_min: int,
    workers_max: int,
    restarts: int,
    exhausted_capacity: int,
    observed_http_rate: float | None,
    effective_rate: float,
    target_rate: float,
    rate_ceiling: float,
    control_healthy: bool,
    heartbeat_age_seconds: float | None,
    elapsed_seconds: float,
    workers_unhealthy: int = 0,
    fleet_availability_degraded: bool = False,
) -> TerminalSnapshot:
    """Build presentation state from values already present in the supervisor loop."""
    return TerminalSnapshot(
        run_id=run_id,
        status=status,
        current=int(live_progress.get("terminal", 0)),
        total=int(live_progress.get("total", 0)),
        done=int(live_progress.get("done_only", 0)),
        not_available=int(live_progress.get("not_available", 0)),
        failed=int(live_progress.get("failed", 0)),
        remaining=int(live_progress.get("remaining", 0)),
        pending=int(live_progress.get("pending_estimate", 0)),
        running=int(live_progress.get("inflight", 0)),
        fetch_queue=int(live_metrics.get("fetch_queue", 0)),
        archive_queue=int(live_metrics.get("archive_queue", 0)),
        project_queue=int(live_metrics.get("project_queue", 0)),
        queue_capacity=settings.dbd_pipeline_queue_size * max(1, workers_registered),
        fetch_active=int(live_metrics.get("fetch_active", 0)),
        archive_active=int(live_metrics.get("archive_active", 0)),
        project_active=int(live_metrics.get("project_active", 0)),
        workers_registered=workers_registered,
        workers_target=workers_target,
        workers_active=workers_active,
        workers_draining=workers_draining,
        workers_stale=workers_stale,
        workers_unhealthy=workers_unhealthy,
        workers_min=workers_min,
        workers_max=workers_max,
        restarts=restarts,
        exhausted_capacity=exhausted_capacity,
        http_rate=observed_http_rate,
        rate_effective=effective_rate,
        rate_target=target_rate,
        rate_ceiling=rate_ceiling,
        requests=int(campaign_metrics.get("requests", 0)),
        responses=int(campaign_metrics.get("responses", 0)),
        auth_failures=int(campaign_metrics.get("auth_failures", 0)),
        http_retries=int(campaign_metrics.get("http_retries", 0)),
        rate_limit_hits=int(campaign_metrics.get("rate_limit_hits", 0)),
        redis_pool_in_use=int(live_metrics.get("redis_pool_in_use", 0)),
        redis_pool_max=int(live_metrics.get("redis_pool_max", 0)),
        redis_commands=int(campaign_metrics.get("redis_commands", 0)),
        redis_admissions=int(campaign_metrics.get("redis_admissions", 0)),
        control_healthy=control_healthy,
        fleet_availability_degraded=fleet_availability_degraded,
        heartbeat_age_seconds=heartbeat_age_seconds,
        elapsed_seconds=elapsed_seconds,
    )


def _runtime_progress_baseline(redis: Redis, run_id: int) -> dict[str, int] | None:
    """Reuse the last supervisor snapshot on resume to avoid a multi-million-row scan."""
    try:
        runtime = _read_runtime_state(redis, run_id)
    except Exception as exc:
        if _is_transient_redis_error(exc):
            return None
        raise
    if not runtime:
        return None
    try:
        total = int(runtime.get("total", 0) or 0)
        done = int(runtime.get("done_only", runtime.get("done", 0)) or 0)
        not_available = int(runtime.get("not_available", 0) or 0)
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
        "not_available": not_available,
    }


def _drain_registered_workers(
    redis: Redis,
    run_id: int,
    *,
    timeout_seconds: float = _ORPHAN_DRAIN_TIMEOUT_SECONDS,
) -> int:
    """Fence a replacement supervisor from workers left by a previous supervisor."""
    states = _registered_worker_states(redis, run_id)
    if not states:
        return 0
    worker_ids = [str(row.get("worker_id")) for row in states if row.get("worker_id")]
    for worker_id in worker_ids:
        _request_worker_drain(redis, run_id, worker_id, "supervisor_replacement")

    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    while time.monotonic() < deadline:
        states = _registered_worker_states(redis, run_id)
        if not states:
            return len(worker_ids)
        time.sleep(_ORPHAN_DRAIN_POLL_SECONDS)

    remaining = _registered_worker_states(redis, run_id)
    if remaining:
        ids = ", ".join(str(row.get("worker_id") or "unknown") for row in remaining)
        raise RuntimeError(
            f"Campaign {run_id} still has registered worker(s) after supervisor handoff: {ids}"
        )
    return len(worker_ids)


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


def _activate_supervisor_lease(redis: Redis, run_id: int, token: str) -> None:
    """Fence production Supervisor activation against explicit collector startup."""
    with runtime_handoff_lock():
        assert_manager_lock_free()
        redis.ping()
        _acquire_supervisor_lease(redis, run_id, token)


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


def _suppress_sigint():
    """Ignore further Ctrl+C only while bounded force-stop reconciliation runs."""
    try:
        previous = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        return previous
    except (AttributeError, OSError, ValueError):
        return None


def _restore_sigint(previous) -> None:
    if previous is None:
        return
    try:
        signal.signal(signal.SIGINT, previous)
    except (AttributeError, OSError, ValueError):
        pass


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


def _force_stop_and_reconcile(
    redis: Redis,
    run_id: int,
    workers: list[WorkerProcess],
) -> int:
    """Force local process trees, then reclaim only leases of confirmed-dead workers."""
    _terminate_workers(workers)
    reclaimed = 0
    for worker in workers:
        if worker.process.poll() is None:
            continue
        reclaimed += _release_dead_worker_leases(run_id, worker.worker_id)
        _cleanup_worker_control(redis, run_id, worker.worker_id)
        _close_worker_handle(worker)
    return reclaimed


def _reclaim_running_leases(
    run_id: int,
    *,
    worker_id: str | None = None,
    worker_prefix: str | None = None,
    reason: str,
    raise_errors: bool = False,
) -> int:
    """Return confirmed-dead worker work to pending without charging a process-loss attempt."""
    if (worker_id is None) == (worker_prefix is None):
        raise ValueError("exactly one worker_id or worker_prefix is required")
    try:
        with connect() as conn, conn.cursor() as cur:
            reclaimed = 0
            if worker_id is not None:
                owner_clause = " AND lease_owner=%s"
                owner_value = worker_id
            else:
                owner_clause = " AND lease_owner LIKE %s"
                owner_value = f"{worker_prefix}%"
            params = (reason, run_id, owner_value)
            for table in ("endpoint_tasks", "company_collection_state"):
                cur.execute(
                    f"""
                    UPDATE {table}
                    SET status='pending',lease_owner=NULL,lease_expires_at=NULL,
                      attempts=CASE
                        WHEN stage='fetch' AND attempts>0 THEN attempts-1
                        ELSE attempts
                      END,
                      project_attempts=CASE
                        WHEN stage='project' AND project_attempts>0
                        THEN project_attempts-1 ELSE project_attempts
                      END,
                      available_at=UTC_TIMESTAMP(6),last_error=%s
                    WHERE run_id=%s{owner_clause} AND status='running'
                    """,
                    params,
                )
                reclaimed += int(cur.rowcount or 0)
            conn.commit()
            return reclaimed
    except Exception:
        if raise_errors:
            raise
        # Lease expiry remains the durable fallback if immediate reconciliation fails.
        return 0


def _release_dead_worker_leases(run_id: int, worker_id: str) -> int:
    """Immediately reclaim leases owned by one confirmed-dead worker process."""
    return _reclaim_running_leases(
        run_id,
        worker_id=worker_id,
        reason="worker_process_lost",
    )


def reclaim_guardian_process_leases(run_id: int, supervisor_generation: str) -> int:
    """Reclaim leases owned by one confirmed-dead Guardian generation only."""
    generation = _normalize_supervisor_generation(supervisor_generation)
    if not generation:
        raise ValueError("supervisor_generation is required for Guardian lease recovery")
    return _reclaim_running_leases(
        run_id,
        worker_prefix=f"local-{int(run_id)}-g{generation}-",
        reason="guardian_process_recovery",
        raise_errors=True,
    )


def _reconcile_stopped_generation_leases(
    run_id: int,
    supervisor_generation: str | None,
    workers: list[WorkerProcess],
) -> int:
    """Second-chance attempt-neutral cleanup after all local processes stop.

    Workers normally release their durable leases during normal worker cleanup.
    Windows interruption or a shutdown-time database error can prevent that.
    Once every process from this Supervisor generation is confirmed stopped,
    the generation prefix is an exact ownership fence for safe reclamation.
    """
    generation = _normalize_supervisor_generation(supervisor_generation)
    if not generation:
        return 0
    if any(worker.process.poll() is None for worker in workers):
        return 0
    return _reclaim_running_leases(
        run_id,
        worker_prefix=f"local-{int(run_id)}-g{generation}-",
        reason="supervisor_shutdown_recovery",
    )

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
    fleet_rate_initial: float | None = None,
    fleet_rate_max: float | None = None,
    redis_rate_key: str | None = None,
    supervisor_generation: str | None = None,
    fleet_db_connections_max: int | None = None,
    fleet_archive_workers_max: int | None = None,
    fleet_projection_workers_max: int | None = None,
    fleet_redis_connections_max: int | None = None,
    open_powershell: bool = False,
    max_restarts: int = 3,
    request_stall_seconds: float = _REQUEST_STALL_TIMEOUT_SECONDS,
    workers_min: int | None = None,
    workers_max: int | None = None,
) -> int:
    """Run one lease-protected local fleet with graceful autoscaling and durable recovery."""
    if not settings.dbd_redis_url:
        raise ValueError("Campaign supervisor requires DBD_REDIS_URL")
    assert_manager_lock_free()
    if worker_concurrency < 1 or max_restarts < 0:
        raise ValueError("worker concurrency must be positive and max restarts non-negative")
    request_stall_seconds = float(request_stall_seconds)
    if request_stall_seconds < 30.0:
        raise ValueError("request stall watchdog must be at least 30 seconds")

    rate_key = str(redis_rate_key or settings.dbd_redis_rate_key)
    if not rate_key.strip():
        raise ValueError("Redis rate key must not be empty")
    generation = _normalize_supervisor_generation(supervisor_generation)

    requested_workers = int(workers)
    minimum = requested_workers if workers_min is None else int(workers_min)
    maximum = requested_workers if workers_max is None else int(workers_max)
    if requested_workers < 1 or minimum < 1 or maximum < minimum or not minimum <= requested_workers <= maximum:
        raise ValueError("workers/workers-min/workers-max must satisfy 1 <= min <= workers <= max")

    initial_workers = requested_workers
    autoscale = minimum != maximum
    plan = build_rate_plan(
        rate_mode,
        worker_rate,
        maximum,
        initial_workers=initial_workers,
        fleet_rate_initial=fleet_rate_initial,
        fleet_rate_max=fleet_rate_max,
    )
    resource_plan = build_resource_plan(
        maximum,
        db_pool_size=settings.db_pool_size,
        archive_workers=settings.dbd_archive_workers,
        projection_workers=settings.dbd_projection_workers,
        redis_max_connections=settings.dbd_redis_max_connections,
        db_connections_max=(
            settings.dbd_fleet_db_connections_max
            if fleet_db_connections_max is None
            else fleet_db_connections_max
        ),
        archive_workers_max=(
            settings.dbd_fleet_archive_workers_max
            if fleet_archive_workers_max is None
            else fleet_archive_workers_max
        ),
        projection_workers_max=(
            settings.dbd_fleet_projection_workers_max
            if fleet_projection_workers_max is None
            else fleet_projection_workers_max
        ),
        redis_connections_max=(
            settings.dbd_fleet_redis_connections_max
            if fleet_redis_connections_max is None
            else fleet_redis_connections_max
        ),
    )
    controller = AutoscaleController(minimum, maximum, worker_concurrency)

    redis = _redis_client(resource_plan.redis_connections_per_client)
    token = f"{os.getpid()}-{uuid4().hex}"
    children: list[WorkerProcess] = []
    next_index = 1
    exhausted_capacity = 0
    restart_events = 0
    graceful_stop = False
    exit_code = 0
    interrupt_handler = None
    lease_acquired = False
    supervisor_error = False
    next_lease_refresh = 0.0
    next_state_write = 0.0
    next_work_check = 0.0
    next_worker_refresh = 0.0
    last_rate_target: float | None = None
    last_effective_rate = plan.initial
    last_lease_confirmed = 0.0
    last_redis_warning_at = 0.0
    baseline: dict[str, int] = {}
    has_work = True
    reconciled_at: float | None = None
    final_snapshot: dict[str, int] | None = None
    registered_states: list[dict] = []
    retired_metrics: dict[str, int] = {}
    http_sample_at = 0.0
    http_sample_requests = 0
    http_sample_rate_limit_hits = 0
    observed_http_rate: float | None = None
    recent_rate_limit_delta = 0
    dashboard_started_at = time.monotonic()
    progress = LiveCampaignDashboard(run_id, interval=0.5)
    worker_watchdog = WorkerProgressWatchdog(request_stall_seconds)
    registry_signature: tuple[tuple[str, int], ...] | None = None

    def sync_worker_registry() -> None:
        nonlocal registry_signature
        signature = tuple(
            sorted(
                (worker.worker_id, int(worker.process.pid))
                for worker in children
                if worker.process.poll() is None
            )
        )
        if signature == registry_signature:
            return
        _write_worker_registry(run_id, generation or None, children)
        registry_signature = signature

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
            resource_plan=resource_plan,
            restart_count=restarts,
            redis_rate_key=rate_key,
            supervisor_generation=generation or None,
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
            "done",
            "failed",
            "retry_wait",
            "not_available",
            "requests",
            "responses",
            "auth_failures",
            "http_retries",
            "rate_limit_hits",
            "redis_commands",
            "redis_admissions",
        ):
            retired_metrics[key] = int(retired_metrics.get(key, 0)) + int(row.get(key, 0) or 0)

    def replacement_for(worker: WorkerProcess, reason: str) -> WorkerProcess | None:
        nonlocal exhausted_capacity, restart_events
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
        restart_events += 1
        progress.event(
            f"worker {worker.index} {reason}; restarted ({replacement.restarts}/{max_restarts})"
        )
        return replacement

    try:
        _activate_supervisor_lease(redis, run_id, token)
        lease_acquired = True

        orphaned = _drain_registered_workers(redis, run_id)
        if orphaned:
            progress.event(
                f"supervisor handoff: drained {orphaned} previously registered worker(s)"
            )

        # Reconcile durable MySQL truth once at Supervisor startup. Redis runtime
        # snapshots are ephemeral and may be stale when the same run was advanced
        # while no Supervisor was active. The live hot path still uses worker
        # heartbeats after this one exact baseline.
        try:
            baseline = _task_progress(run_id)
            reconciled_at = time.time()
        except Exception as exc:
            raise RuntimeError(
                f"Campaign {run_id} startup durable progress reconciliation failed: "
                f"{type(exc).__name__}"
            ) from exc
        has_work = _has_active_work(run_id)

        now = time.monotonic()
        http_sample_at = now
        if autoscale:
            controller.next_decision_at = now + _AUTOSCALE_UP_COOLDOWN_SECONDS
        last_lease_confirmed = now
        next_lease_refresh = now + _SUPERVISOR_REFRESH_SECONDS
        next_work_check = now + _DURABLE_WORK_CHECK_SECONDS
        next_worker_refresh = now
        _clear_stop(redis, run_id)
        initial_rate_target = _fleet_rate_target(plan, initial_workers)
        _seed_rate(
            redis,
            rate_key,
            plan.initial,
            initial_rate_target,
            hard_fleet_cap=plan.independent,
        )
        last_rate_target = initial_rate_target

        if has_work:
            for _ in range(initial_workers):
                children.append(spawn_worker())
        sync_worker_registry()

        _write_runtime_state(
            redis,
            run_id,
            supervisor_pid=os.getpid(),
            supervisor_generation=generation or None,
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
            worker_rate=plan.worker_rate,
            rate_mode=rate_mode,
            rate_independent=plan.independent,
            fleet_rate_initial=plan.initial,
            fleet_rate_max=plan.ceiling,
            redis_rate_key=rate_key,
            max_restarts=max_restarts,
            request_stall_seconds=request_stall_seconds,
            rate_target=initial_rate_target,
            rate_ceiling=plan.ceiling,
            db_connections_budget=resource_plan.db_connections_budget,
            db_connections_effective_max=resource_plan.db_connections_effective_max,
            db_pool_per_worker=resource_plan.db_pool_per_worker,
            archive_workers_budget=resource_plan.archive_workers_budget,
            archive_workers_per_worker=resource_plan.archive_workers_per_worker,
            projection_workers_budget=resource_plan.projection_workers_budget,
            projection_workers_per_worker=resource_plan.projection_workers_per_worker,
            redis_connections_budget=resource_plan.redis_connections_budget,
            redis_connections_effective_max=resource_plan.redis_connections_effective_max,
            redis_connections_per_client=resource_plan.redis_connections_per_client,
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
                    if now - last_redis_warning_at >= 5.0:
                        progress.event(
                            f"Redis control plane timeout; preserving workers and retrying lease "
                            f"({now - last_lease_confirmed:.1f}s degraded)"
                        )
                        last_redis_warning_at = now
                else:
                    next_lease_refresh = now + _SUPERVISOR_REFRESH_SECONDS

            worker_state_refreshed = False
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
                    worker_state_refreshed = True

            live_metrics = _worker_metrics(registered_states)
            campaign_metrics = _with_retired_metrics(live_metrics, retired_metrics)
            if worker_state_refreshed:
                elapsed = max(0.001, now - http_sample_at)
                current_requests = int(campaign_metrics.get("requests", 0))
                current_rate_limit_hits = int(campaign_metrics.get("rate_limit_hits", 0))
                observed_http_rate = max(0, current_requests - http_sample_requests) / elapsed
                recent_rate_limit_delta = max(
                    0, current_rate_limit_hits - http_sample_rate_limit_hits
                )
                http_sample_at = now
                http_sample_requests = current_requests
                http_sample_rate_limit_hits = current_rate_limit_hits
            live_progress = _live_progress(baseline, campaign_metrics)
            total = live_progress["total"]
            pending = live_progress["pending_estimate"]
            running = live_progress["inflight"]
            failed = live_progress["failed"]
            done = live_progress["done_only"] + live_progress["not_available"]

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
            active_health_rows = [
                row
                for row in registered_states
                if str(row.get("status") or "").lower() == "active"
            ]
            active_health_ids = {
                str(row.get("worker_id"))
                for row in active_health_rows
                if row.get("worker_id")
            }
            unhealthy_workers = sum(
                str(row.get("circuit_global") or "").lower() == "open"
                or str(row.get("auth_state") or "").lower() == "cooldown"
                for row in active_health_rows
            )
            healthy_workers = max(0, len(active_health_rows) - unhealthy_workers)
            fleet_unhealthy_threshold = max(
                2,
                (max(1, len(active_health_rows)) + 1) // 2,
            )
            fleet_availability_degraded = (
                unhealthy_workers >= fleet_unhealthy_threshold
            )

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
                    if reason is None and row is not None:
                        stalled_for = worker_watchdog.observe(
                            worker.worker_id,
                            row,
                            now=now,
                            active_work=bool(active_work),
                            control_healthy=(
                                not control_plane_degraded
                                and not fleet_availability_degraded
                            ),
                            draining=bool(worker.draining or graceful_stop),
                        )
                        if stalled_for is not None:
                            reason = f"request_progress_stall_{stalled_for:.0f}s"
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
                    worker_watchdog.forget(worker.worker_id)
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
                worker_watchdog.forget(worker.worker_id)
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

            circuit_state = _fleet_circuit_state(registered_states)
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
            current_effective_rate = None
            fleet_visibility_complete = active_health_ids == {
                worker.worker_id for worker in active_live
            }
            if (
                autoscale
                and not graceful_stop
                and not control_plane_degraded
                and fleet_visibility_complete
            ):
                if plan.independent:
                    current_effective_rate = _effective_rate(
                        redis, rate_key, plan.initial
                    )
                target = controller.target(
                    pending=pending,
                    running=running,
                    current=len(active_live),
                    effective_maximum=effective_maximum,
                    now=now,
                    fetch_queue=live_metrics.get("fetch_queue", 0),
                    fetch_active=live_metrics.get("fetch_active", 0),
                    archive_queue=live_metrics.get("archive_queue", 0),
                    project_queue=live_metrics.get("project_queue", 0),
                    observed_http_rate=observed_http_rate if plan.independent else None,
                    rate_target=current_effective_rate if plan.independent else None,
                    worker_rate=plan.worker_rate,
                    hard_fleet_cap=plan.independent,
                    healthy_workers=healthy_workers,
                    fleet_availability_degraded=fleet_availability_degraded,
                    rate_limit_delta=recent_rate_limit_delta,
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

            sync_worker_registry()

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
                            rate_key,
                            rate_target,
                            hard_fleet_cap=plan.independent,
                        )
                        last_rate_target = rate_target
                    else:
                        effective = _effective_rate(
                            redis,
                            rate_key,
                            last_effective_rate,
                        )
                    last_effective_rate = effective
                except Exception as exc:
                    if not _is_transient_redis_error(exc):
                        raise
                    control_plane_degraded = True

            registered = len(registered_states)
            stale_registered = sum(
                now_wall - float(worker.get("heartbeat_at") or now_wall)
                > _WORKER_HEARTBEAT_TIMEOUT_SECONDS
                for worker in registered_states
            )
            heartbeat_ages = [
                max(0.0, now_wall - float(worker.get("heartbeat_at") or now_wall))
                for worker in registered_states
                if worker.get("heartbeat_at")
            ]
            heartbeat_age = max(heartbeat_ages) if heartbeat_ages else None
            progress.render(
                _campaign_terminal_snapshot(
                    run_id,
                    status=(
                        "draining"
                        if graceful_stop
                        else "degraded"
                        if control_plane_degraded or fleet_availability_degraded
                        else "running"
                    ),
                    live_progress=live_progress,
                    live_metrics=live_metrics,
                    campaign_metrics=campaign_metrics,
                    workers_registered=registered,
                    workers_target=target,
                    workers_active=len(active_live),
                    workers_draining=len(draining_live),
                    workers_stale=stale_registered,
                    workers_min=minimum,
                    workers_max=maximum,
                    restarts=restart_events,
                    exhausted_capacity=exhausted_capacity,
                    observed_http_rate=observed_http_rate,
                    effective_rate=effective,
                    target_rate=rate_target,
                    rate_ceiling=plan.ceiling,
                    control_healthy=not control_plane_degraded,
                    heartbeat_age_seconds=heartbeat_age,
                    elapsed_seconds=max(0.0, now - dashboard_started_at),
                    workers_unhealthy=unhealthy_workers,
                    fleet_availability_degraded=fleet_availability_degraded,
                )
            )

            if now >= next_state_write and not control_plane_degraded:
                try:
                    _write_runtime_state(
                        redis,
                        run_id,
                        supervisor_pid=os.getpid(),
            supervisor_generation=generation or None,
                        status=(
                            "draining"
                            if graceful_stop
                            else "degraded"
                            if fleet_availability_degraded
                            else "running"
                        ),
                        autoscale=autoscale,
                        workers_initial=initial_workers,
                        workers_min=minimum,
                        workers_max=maximum,
                        workers_target=target,
                        workers_active=len(active_live),
                        workers_draining=len(draining_live),
                        workers_registered=registered,
                        workers_stale=stale_registered,
                        workers_unhealthy=unhealthy_workers,
                        fleet_availability_degraded=fleet_availability_degraded,
                        exhausted_capacity=exhausted_capacity,
                        effective_worker_max=effective_maximum,
                        worker_concurrency=worker_concurrency,
                        worker_rate=plan.worker_rate,
                        rate_mode=rate_mode,
                        rate_independent=plan.independent,
                        fleet_rate_initial=plan.initial,
                        fleet_rate_max=plan.ceiling,
                        redis_rate_key=rate_key,
                        max_restarts=max_restarts,
                        request_stall_seconds=request_stall_seconds,
                        rate_effective=effective,
                        rate_target=rate_target,
                        rate_ceiling=plan.ceiling,
                        http_rate_observed=observed_http_rate,
                        rate_limit_delta=recent_rate_limit_delta,
                        db_connections_budget=resource_plan.db_connections_budget,
                        db_connections_effective_max=resource_plan.db_connections_effective_max,
                        db_pool_per_worker=resource_plan.db_pool_per_worker,
                        archive_workers_budget=resource_plan.archive_workers_budget,
                        archive_workers_per_worker=resource_plan.archive_workers_per_worker,
                        projection_workers_budget=resource_plan.projection_workers_budget,
                        projection_workers_per_worker=resource_plan.projection_workers_per_worker,
                        redis_connections_budget=resource_plan.redis_connections_budget,
                        redis_connections_effective_max=resource_plan.redis_connections_effective_max,
                        redis_connections_per_client=resource_plan.redis_connections_per_client,
                        worker_metrics=campaign_metrics,
                        progress_source="redis_worker_heartbeat",
                        reconciled_at=reconciled_at,
                        pending=pending,
                        retry_wait=None,
                        pending_includes_retry_wait=True,
                        circuit_state=_fleet_circuit_state(registered_states),
                        running=running,
                        done=done,
                        done_only=live_progress["done_only"],
                        not_available=live_progress["not_available"],
                        failed=failed,
                        remaining=live_progress["remaining"],
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
            supervisor_generation=generation or None,
                status="draining",
                autoscale=autoscale,
                workers_initial=initial_workers,
                workers_min=minimum,
                workers_max=maximum,
                worker_concurrency=worker_concurrency,
                worker_rate=plan.worker_rate,
                rate_mode=rate_mode,
                rate_independent=plan.independent,
                fleet_rate_initial=plan.initial,
                fleet_rate_max=plan.ceiling,
                redis_rate_key=rate_key,
                max_restarts=max_restarts,
                request_stall_seconds=request_stall_seconds,
                rate_ceiling=plan.ceiling,
                db_pool_per_worker=resource_plan.db_pool_per_worker,
                archive_workers_per_worker=resource_plan.archive_workers_per_worker,
                projection_workers_per_worker=resource_plan.projection_workers_per_worker,
                redis_connections_per_client=resource_plan.redis_connections_per_client,
            )
            try:
                while any(worker.process.poll() is None for worker in children):
                    states = _registered_worker_states(redis, run_id)
                    metrics = _with_retired_metrics(_worker_metrics(states), retired_metrics)
                    current = _live_progress(baseline, metrics)
                    progress.render(
                        _campaign_terminal_snapshot(
                            run_id,
                            status="draining",
                            live_progress=current,
                            live_metrics=_worker_metrics(states),
                            campaign_metrics=metrics,
                            workers_registered=len(states),
                            workers_target=0,
                            workers_active=sum(
                                str(row.get("status", "")).lower() == "active"
                                for row in states
                            ),
                            workers_draining=sum(
                                str(row.get("status", "")).lower() == "draining"
                                for row in states
                            ),
                            workers_stale=0,
                            workers_min=minimum,
                            workers_max=maximum,
                            restarts=restart_events,
                            exhausted_capacity=exhausted_capacity,
                            observed_http_rate=observed_http_rate,
                            effective_rate=last_effective_rate,
                            target_rate=float(last_rate_target or plan.initial),
                            rate_ceiling=plan.ceiling,
                            control_healthy=True,
                            heartbeat_age_seconds=None,
                            elapsed_seconds=max(0.0, time.monotonic() - dashboard_started_at),
                        ),
                        force=True,
                    )
                    time.sleep(0.75)
            except KeyboardInterrupt:
                progress.event(
                    "Second Ctrl+C received: forcing worker termination; "
                    "further interrupts are shielded until lease reconciliation completes"
                )
                interrupt_handler = _suppress_sigint()
                reclaimed = _force_stop_and_reconcile(redis, run_id, children)
                if reclaimed:
                    progress.event(
                        f"force-stop reclaimed {reclaimed} confirmed-dead worker lease(s)"
                    )
        finally:
            exit_code = 130

    except Exception as exc:
        supervisor_error = True
        _contain_local_fleet(
            redis,
            run_id,
            children,
            "supervisor_failure",
            grace_seconds=_FAILURE_DRAIN_SECONDS,
        )
        progress.close(
            message=(
                f"Campaign {run_id} supervisor failed; local workers are stopped "
                f"({type(exc).__name__})"
            )
        )
        detail = str(exc).strip()
        detail = f": {detail[:300]}" if detail else ""
        raise RuntimeError(
            f"Campaign {run_id} supervisor failure [{type(exc).__name__}]{detail}"
        ) from exc

    finally:
        for worker in children:
            _close_worker_handle(worker)
            if worker.process.poll() is not None:
                _release_dead_worker_leases(run_id, worker.worker_id)
                _cleanup_worker_control(redis, run_id, worker.worker_id)

        generation_reclaimed = _reconcile_stopped_generation_leases(
            run_id,
            generation or None,
            children,
        )
        _write_worker_registry(run_id, generation or None, [])
        if generation_reclaimed:
            progress.event(
                f"shutdown reconciliation reclaimed {generation_reclaimed} "
                "generation-owned durable lease(s)"
            )

        if lease_acquired:
            try:
                snapshot = _task_progress(run_id)
                final_snapshot = snapshot
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
                elif exit_code == _REQUEST_STALL_EXIT_CODE:
                    runtime_status = "request_stalled"
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
            supervisor_generation=generation or None,
                    status=runtime_status,
                    autoscale=autoscale,
                    workers_initial=initial_workers,
                    workers_min=minimum,
                    workers_max=maximum,
                    exhausted_capacity=exhausted_capacity,
                    worker_concurrency=worker_concurrency,
                    worker_rate=plan.worker_rate,
                    rate_mode=rate_mode,
                    rate_independent=plan.independent,
                    fleet_rate_initial=plan.initial,
                    fleet_rate_max=plan.ceiling,
                    redis_rate_key=rate_key,
                    max_restarts=max_restarts,
                    request_stall_seconds=request_stall_seconds,
                    rate_target=last_rate_target,
                    rate_ceiling=plan.ceiling,
                    db_connections_budget=resource_plan.db_connections_budget,
                    db_connections_effective_max=resource_plan.db_connections_effective_max,
                    db_pool_per_worker=resource_plan.db_pool_per_worker,
                    archive_workers_budget=resource_plan.archive_workers_budget,
                    archive_workers_per_worker=resource_plan.archive_workers_per_worker,
                    projection_workers_budget=resource_plan.projection_workers_budget,
                    projection_workers_per_worker=resource_plan.projection_workers_per_worker,
                    redis_connections_budget=resource_plan.redis_connections_budget,
                    redis_connections_effective_max=resource_plan.redis_connections_effective_max,
                    redis_connections_per_client=resource_plan.redis_connections_per_client,
                    worker_metrics=_with_retired_metrics(
                        _worker_metrics(registered_states), retired_metrics
                    ),
                    progress_source="mysql_reconciled",
                    reconciled_at=time.time(),
                    pending=snapshot.get("pending", 0),
                    retry_wait=snapshot.get("retry_wait", 0),
                    pending_includes_retry_wait=False,
                    circuit_state=_fleet_circuit_state(registered_states),
                    running=snapshot.get("running", 0),
                    done=snapshot.get("done", 0) + snapshot.get("not_available", 0),
                    done_only=snapshot.get("done", 0),
                    not_available=snapshot.get("not_available", 0),
                    failed=snapshot.get("failed", 0),
                    remaining=remaining,
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
        _restore_sigint(interrupt_handler)

    state = final_snapshot or _task_progress(run_id)
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


def campaign_status(run_id: int, *, diagnostics: bool = False) -> dict:
    """Prefer live Redis state; only scan durable failure diagnostics on request."""
    state: dict | None = None
    redis = None
    workers: list[dict] = []
    runtime = None
    supervisor_active = False

    if settings.dbd_redis_url:
        try:
            redis = _redis_client(settings.dbd_redis_max_connections)
            redis.ping()
            runtime = _read_runtime_state(redis, run_id)
            supervisor_active = bool(redis.exists(_supervisor_lease_key(run_id)))
            workers = _registered_worker_states(redis, run_id)
            if runtime is not None and supervisor_active and runtime.get("total") is not None:
                state = {
                    "total": int(runtime.get("total", 0) or 0),
                    "pending": int(runtime.get("pending", 0) or 0),
                    "retry_wait": (
                        None
                        if runtime.get("retry_wait") is None
                        else int(runtime.get("retry_wait", 0) or 0)
                    ),
                    "running": int(runtime.get("running", 0) or 0),
                    "failed": int(runtime.get("failed", 0) or 0),
                    "done": int(runtime.get("done_only", runtime.get("done", 0)) or 0),
                    "not_available": int(runtime.get("not_available", 0) or 0),
                    "progress_source": runtime.get(
                        "progress_source", "redis_worker_heartbeat"
                    ),
                    "progress_exact": False,
                    "pending_includes_retry_wait": bool(
                        runtime.get("pending_includes_retry_wait", True)
                    ),
                }
        except Exception:
            if redis is not None:
                try:
                    redis.close()
                except Exception:
                    pass
            redis = None

    if state is None:
        state = _task_progress(run_id)
        state["progress_source"] = "mysql_reconciled"
        state["progress_exact"] = True
        state["pending_includes_retry_wait"] = False

    state["run_id"] = int(run_id)
    state["run_status"] = _run_status(run_id)
    runtime_status = str((runtime or {}).get("status") or "").lower()
    if supervisor_active:
        state["control_status"] = runtime_status or "active"
    elif workers:
        state["control_status"] = "workers_without_supervisor"
    elif runtime_status in {
        "paused",
        "capacity_exhausted",
        "completed",
        "completed_with_failures",
        "supervisor_error",
        "stopped",
    }:
        state["control_status"] = runtime_status
    else:
        state["control_status"] = "offline"
    state["circuit_state"] = str(
        (runtime or {}).get("circuit_state")
        or _fleet_circuit_state(workers)
        or "unknown"
    )
    if diagnostics:
        state["failure_diagnostics"] = _failure_diagnostics(run_id)

    if redis is not None:
        try:
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
            status_rate_key = str(
                (runtime or {}).get("redis_rate_key") or settings.dbd_redis_rate_key
            )
            state["effective_rate"] = _effective_rate(
                redis, status_rate_key, settings.dbd_rate_per_second
            )
            target_raw = redis.get(_rate_target_key(status_rate_key))
            try:
                state["rate_target"] = float(target_raw) if target_raw is not None else None
            except (TypeError, ValueError):
                state["rate_target"] = None
            state["stop_requested"] = _stop_requested(redis, run_id)
            state["supervisor_active"] = supervisor_active
            if runtime is not None:
                state["supervisor"] = runtime
        finally:
            redis.close()
    return state


def collector_handoff_status(run_id: int) -> dict:
    """Fail-closed startup view for the explicit diagnostic collector handoff.

    Campaign is the production runtime. Before a diagnostic collector starts,
    Redis must prove that no production Supervisor or registered distributed
    worker can overlap the single-process diagnostic path.
    """
    if not settings.dbd_redis_url:
        return {
            "redis_configured": False,
            "control_status": "not_configured",
            "supervisor_active": False,
            "workers": 0,
        }

    redis = None
    try:
        redis = _redis_client(settings.dbd_redis_max_connections)
        redis.ping()
        runtime = _read_runtime_state(redis, run_id)
        workers = _registered_worker_states(redis, run_id)
        supervisor_active = bool(redis.exists(_supervisor_lease_key(run_id)))
        runtime_status = str((runtime or {}).get("status") or "").lower()
        if supervisor_active:
            control_status = runtime_status or "active"
        elif workers:
            control_status = "workers_without_supervisor"
        else:
            control_status = runtime_status or "offline"
        return {
            "redis_configured": True,
            "control_status": control_status,
            "supervisor_active": supervisor_active,
            "workers": len(workers),
        }
    except Exception as exc:
        raise RuntimeError(
            "Cannot prove the Campaign runtime is offline because its Redis "
            "control plane is unavailable"
        ) from exc
    finally:
        if redis is not None:
            try:
                redis.close()
            except Exception:
                pass


def request_campaign_stop(run_id: int) -> None:
    if not settings.dbd_redis_url:
        raise ValueError("Campaign stop requires DBD_REDIS_URL")
    redis = _redis_client(settings.dbd_redis_max_connections)
    try:
        redis.ping()
        _request_stop(redis, run_id, "operator_command")
    finally:
        redis.close()
