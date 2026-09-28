import json
import time
from types import SimpleNamespace

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

import dbd.runtime.supervisor as supervisor
from dbd.runtime.supervisor import (
    AutoscaleController,
    CampaignResourcePlan,
    WorkerProcess,
    _campaign_terminal_snapshot,
    _drain_registered_workers,
    _fleet_circuit_state,
    _fleet_rate_target,
    _force_kill_worker,
    _force_stop_and_reconcile,
    _live_progress,
    _refresh_lease_with_grace,
    _restore_sigint,
    _runtime_progress_baseline,
    _seed_rate,
    _spawn_worker,
    _suppress_sigint,
    _terminate_workers,
    _with_retired_metrics,
    _worker_command,
    build_rate_plan,
    build_resource_plan,
)


def test_shared_rate_is_fleet_cap() -> None:
    plan = build_rate_plan("shared", 15, 4)
    assert plan.ceiling == 15
    assert plan.initial == 15


def test_multiply_rate_scales_with_workers() -> None:
    plan = build_rate_plan("multiply", 15, 4)
    assert plan.ceiling == 60
    assert plan.initial == 60


def test_adaptive_rate_starts_low_with_scaled_ceiling() -> None:
    plan = build_rate_plan("adaptive", 15, 4)
    assert plan.ceiling == 60
    assert plan.initial == 15


def test_child_workers_disable_prometheus_listener_collision() -> None:
    command = _worker_command(42, concurrency=8, rate=15)
    assert "--distributed" in command
    assert command[command.index("--metrics-port") + 1] == "0"


def test_seed_rate_does_not_clear_existing_shared_penalty_state() -> None:
    class FakePipeline:
        def __init__(self, calls):
            self.calls = calls

        def set(self, key, value, **kwargs):
            self.calls.append((key, value, kwargs))
            return self

        def execute(self):
            return [True] * len(self.calls)

    class FakeRedis:
        def __init__(self):
            self.set_calls = []

        def pipeline(self, *, transaction=False):
            assert transaction is False
            return FakePipeline(self.set_calls)

        def delete(self, *keys):
            raise AssertionError(f"must not clear active pacing state: {keys}")

    redis = FakeRedis()
    _seed_rate(redis, "dbd:rate", 15)

    assert [call[0] for call in redis.set_calls] == [
        "dbd:rate:effective-rate",
        "dbd:rate:last-penalty-ms",
        "dbd:rate:target-rate",
    ]
    assert redis.set_calls[0][2].get("nx") is True
    assert redis.set_calls[1][2].get("nx") is True
    assert redis.set_calls[2][2].get("nx") is None
    assert all(call[2].get("ex") == 600 for call in redis.set_calls)
    assert float(redis.set_calls[2][1]) == 15.0


def test_windows_force_stop_kills_entire_worker_tree(monkeypatch) -> None:
    calls = []
    process = SimpleNamespace(pid=4321, poll=lambda: None, kill=lambda: calls.append("kill"))
    worker = WorkerProcess(1, "worker-1", process)

    monkeypatch.setattr(supervisor.os, "name", "nt")
    monkeypatch.setattr(
        supervisor.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )

    _force_kill_worker(worker)

    command, kwargs = calls[0]
    assert command == ["taskkill", "/PID", "4321", "/T", "/F"]
    assert kwargs["check"] is False
    assert "kill" not in calls


def test_windows_terminate_workers_skips_wrapper_only_terminate(monkeypatch) -> None:
    calls = []
    process = SimpleNamespace(
        pid=9876,
        poll=lambda: None,
        wait=lambda timeout=None: calls.append(("wait", timeout)),
        terminate=lambda: calls.append("terminate"),
        kill=lambda: calls.append("kill"),
    )
    worker = WorkerProcess(1, "worker-1", process)

    monkeypatch.setattr(supervisor.os, "name", "nt")
    monkeypatch.setattr(
        supervisor.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )

    _terminate_workers([worker])

    assert "terminate" not in calls
    assert "kill" not in calls
    assert calls[0][0] == ["taskkill", "/PID", "9876", "/T", "/F"]
    assert calls[1] == ("wait", 5.0)



def test_live_progress_uses_heartbeat_delta_from_durable_baseline() -> None:
    baseline = {
        "total": 1000,
        "pending": 820,
        "retry_wait": 0,
        "running": 28,
        "failed": 2,
        "done": 100,
        "not_available": 50,
    }
    live = _live_progress(
        baseline,
        {"done": 15, "not_available": 5, "failed": 1, "inflight": 30},
    )

    assert live["terminal"] == 173
    assert live["done_only"] == 115
    assert live["not_available"] == 55
    assert live["failed"] == 3
    assert live["inflight"] == 30
    assert live["pending_estimate"] == 797


def test_transient_redis_lease_timeout_is_tolerated_inside_safety_budget(monkeypatch) -> None:
    def timeout(*args, **kwargs):
        raise RedisTimeoutError("temporary")

    monkeypatch.setattr(supervisor, "_refresh_supervisor_lease", timeout)

    confirmed, error = _refresh_lease_with_grace(
        object(),
        7,
        "token",
        now=15.0,
        last_confirmed=10.0,
    )

    assert confirmed == 10.0
    assert isinstance(error, RedisTimeoutError)


def test_redis_lease_timeout_fails_closed_before_lease_expiry(monkeypatch) -> None:
    def timeout(*args, **kwargs):
        raise RedisTimeoutError("temporary")

    monkeypatch.setattr(supervisor, "_refresh_supervisor_lease", timeout)

    with pytest.raises(RuntimeError, match="stopping before supervisor lease expiry"):
        _refresh_lease_with_grace(
            object(),
            7,
            "token",
            now=31.0,
            last_confirmed=10.0,
        )


def test_resume_progress_falls_back_when_redis_snapshot_times_out() -> None:
    class TimeoutRedis:
        def get(self, key):
            raise RedisTimeoutError("temporary")

    assert _runtime_progress_baseline(TimeoutRedis(), 7) is None



def test_replacement_supervisor_refuses_to_spawn_over_registered_workers(monkeypatch) -> None:
    states = [{"worker_id": "old-worker", "status": "active"}]
    drains = []

    monkeypatch.setattr(
        supervisor,
        "_registered_worker_states",
        lambda redis, run_id: list(states),
    )
    monkeypatch.setattr(
        supervisor,
        "_request_worker_drain",
        lambda redis, run_id, worker_id, reason: drains.append(
            (run_id, worker_id, reason)
        ),
    )

    with pytest.raises(RuntimeError, match="old-worker"):
        _drain_registered_workers(object(), 7, timeout_seconds=0)

    assert drains == [(7, "old-worker", "supervisor_replacement")]


def test_campaign_status_prefers_live_runtime_without_exact_scan(monkeypatch) -> None:
    runtime = {
        "total": 1000,
        "pending": 700,
        "running": 8,
        "done_only": 180,
        "not_available": 100,
        "failed": 12,
        "retry_wait": None,
        "pending_includes_retry_wait": True,
        "status": "running",
        "circuit_state": "open",
        "progress_source": "redis_worker_heartbeat",
    }
    worker = {
        "worker_id": "worker-a",
        "status": "active",
        "heartbeat_at": 1.0,
        "done": 10,
        "not_available": 2,
        "failed": 0,
    }

    class FakeRedis:
        def ping(self):
            return True

        def get(self, key):
            if key.endswith(":supervisor:state"):
                return json.dumps(runtime)
            if key.endswith(":effective-rate"):
                return "11.5"
            if key.endswith(":target-rate"):
                return "12"
            return None

        def exists(self, key):
            return key.endswith(":supervisor:lease")

        def smembers(self, key):
            return {"worker-a"}

        def mget(self, keys):
            return [json.dumps(worker)]

        def srem(self, *args):
            return 0

        def close(self):
            pass

    fake = FakeRedis()
    monkeypatch.setattr(supervisor, "_redis_client", lambda *args, **kwargs: fake)
    monkeypatch.setattr(
        supervisor,
        "settings",
        SimpleNamespace(
            dbd_redis_url="redis://test",
            dbd_redis_rate_key="dbd:test:rate",
            dbd_rate_per_second=1.0,
            dbd_redis_max_connections=4,
        ),
    )
    monkeypatch.setattr(
        supervisor,
        "_task_progress",
        lambda run_id: (_ for _ in ()).throw(
            AssertionError("active campaign status must not run an exact aggregate scan")
        ),
    )
    monkeypatch.setattr(supervisor, "_run_status", lambda run_id: "ready")

    state = supervisor.campaign_status(7)

    assert state["progress_source"] == "redis_worker_heartbeat"
    assert state["progress_exact"] is False
    assert state["done"] == 180
    assert state["not_available"] == 100
    assert state["failed"] == 12
    assert state["retry_wait"] is None
    assert state["pending_includes_retry_wait"] is True
    assert state["control_status"] == "running"
    assert state["circuit_state"] == "open"
    assert state["workers"] == 1
    assert state["supervisor_active"] is True


def test_retired_terminal_metrics_preserve_done_progress() -> None:
    merged = _with_retired_metrics(
        {"done": 3, "not_available": 1, "failed": 0},
        {"done": 7, "not_available": 2, "failed": 1},
    )
    assert merged["done"] == 10
    assert merged["not_available"] == 3
    assert merged["failed"] == 1



def test_explicit_fleet_rate_is_independent_of_worker_count() -> None:
    plan = build_rate_plan("adaptive", 2, 20, initial_workers=4, fleet_rate_initial=12, fleet_rate_max=20)
    assert plan.independent is True
    assert plan.initial == 12
    assert plan.ceiling == 20
    assert plan.worker_rate == 20
    assert _fleet_rate_target(plan, 4) == 20
    assert _fleet_rate_target(plan, 20) == 20


def test_explicit_fleet_rate_rejects_initial_above_ceiling() -> None:
    with pytest.raises(ValueError, match="0 < initial <= max"):
        build_rate_plan("adaptive", 2, 20, fleet_rate_initial=21, fleet_rate_max=20)


def test_resource_plan_bounds_maximum_fleet() -> None:
    plan = build_resource_plan(
        20, db_pool_size=8, archive_workers=2, projection_workers=8,
        redis_max_connections=4, db_connections_max=96, archive_workers_max=20,
        projection_workers_max=40, redis_connections_max=128,
    )
    assert plan.db_pool_per_worker == 2
    assert plan.db_connections_effective_max <= 96
    assert plan.archive_workers_per_worker == 1
    assert plan.archive_workers_effective_max == 20
    assert plan.projection_workers_per_worker == 2
    assert plan.projection_workers_effective_max == 40
    # PR3 shares one Redis pool per worker, so the same fleet budget can
    # preserve the configured 4-connection pool instead of splitting it across
    # two independent worker clients.
    assert plan.redis_connections_per_client == 4
    assert plan.redis_connections_effective_max == 84
    assert plan.redis_connections_effective_max <= 128


def test_resource_plan_rejects_budget_that_cannot_support_workers_max() -> None:
    with pytest.raises(ValueError, match="fleet DB connection budget"):
        build_resource_plan(
            20, db_pool_size=8, archive_workers=2, projection_workers=8,
            redis_max_connections=4, db_connections_max=40,
        )


def test_spawn_worker_applies_derived_resource_caps(monkeypatch) -> None:
    captured = {}

    class FakeProcess:
        pid = 1234

        def poll(self):
            return None

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return FakeProcess()

    monkeypatch.setattr(supervisor.subprocess, "Popen", fake_popen)
    plan = CampaignResourcePlan(
        max_workers=20, supervisor_db_pool=8, db_connections_budget=96,
        db_pool_per_worker=2, archive_workers_budget=20, archive_workers_per_worker=1,
        projection_workers_budget=40, projection_workers_per_worker=2,
        redis_connections_budget=128, redis_connections_per_client=3,
    )
    worker = _spawn_worker(
        7, 1, concurrency=12, rate=20, fleet_rate_ceiling=20,
        open_powershell=True, resource_plan=plan,
    )
    assert worker.process.pid == 1234
    assert captured["env"]["DB_POOL_SIZE"] == "2"
    assert captured["env"]["DBD_ARCHIVE_WORKERS"] == "1"
    assert captured["env"]["DBD_PROJECTION_WORKERS"] == "2"
    assert captured["env"]["DBD_REDIS_MAX_CONNECTIONS"] == "3"


def test_autoscale_adds_worker_when_http_capacity_is_underutilized() -> None:
    controller = AutoscaleController(2, 8, 12)
    assert controller.target(
        pending=5000, running=24, current=4, effective_maximum=8, now=10.0,
        fetch_queue=10, archive_queue=0, project_queue=0,
        observed_http_rate=6.0, rate_target=12.0, rate_limit_delta=0,
    ) == 5


def test_autoscale_holds_when_rate_is_saturated_and_fetch_queue_is_deep() -> None:
    controller = AutoscaleController(2, 8, 12)
    assert controller.target(
        pending=5000, running=24, current=4, effective_maximum=8, now=10.0,
        fetch_queue=500, archive_queue=0, project_queue=0,
        observed_http_rate=11.5, rate_target=12.0, rate_limit_delta=0,
    ) == 4


def test_autoscale_freezes_on_rate_limit_pressure() -> None:
    controller = AutoscaleController(2, 8, 12)
    assert controller.target(
        pending=5000, running=24, current=4, effective_maximum=8, now=10.0,
        fetch_queue=0, archive_queue=0, project_queue=0,
        observed_http_rate=4.0, rate_target=12.0, rate_limit_delta=1,
    ) == 4


def test_autoscale_freezes_on_downstream_backlog() -> None:
    controller = AutoscaleController(2, 8, 12)
    assert controller.target(
        pending=5000, running=24, current=4, effective_maximum=8, now=10.0,
        fetch_queue=0, archive_queue=0, project_queue=500,
        observed_http_rate=4.0, rate_target=12.0, rate_limit_delta=0,
    ) == 4



def test_campaign_terminal_snapshot_uses_existing_loop_values_only() -> None:
    snap = _campaign_terminal_snapshot(
        7,
        status="running",
        live_progress={
            "terminal": 250,
            "total": 1000,
            "done_only": 200,
            "not_available": 40,
            "failed": 10,
            "remaining": 750,
            "pending_estimate": 700,
            "inflight": 50,
        },
        live_metrics={
            "fetch_queue": 300,
            "archive_queue": 20,
            "project_queue": 10,
            "fetch_active": 24,
            "archive_active": 4,
            "project_active": 8,
            "redis_pool_in_use": 6,
            "redis_pool_max": 24,
        },
        campaign_metrics={
            "requests": 10000,
            "http_retries": 12,
            "rate_limit_hits": 0,
            "redis_commands": 10000,
            "redis_admissions": 10000,
        },
        workers_registered=6,
        workers_target=6,
        workers_active=6,
        workers_draining=0,
        workers_stale=0,
        workers_min=3,
        workers_max=6,
        restarts=1,
        exhausted_capacity=0,
        observed_http_rate=11.8,
        effective_rate=12.0,
        target_rate=12.0,
        rate_ceiling=20.0,
        control_healthy=True,
        heartbeat_age_seconds=0.4,
        elapsed_seconds=3600,
    )

    assert snap.current == 250
    assert snap.fetch_queue == 300
    assert snap.queue_capacity == supervisor.settings.dbd_pipeline_queue_size * 6
    assert snap.redis_pool_in_use == 6
    assert snap.redis_commands == 10000
    assert snap.http_rate == 11.8
    assert snap.control_healthy is True



def test_fleet_circuit_state_prefers_most_restrictive_worker_state() -> None:
    assert _fleet_circuit_state([]) == "unknown"
    assert _fleet_circuit_state(
        [
            {"circuit_global": "closed"},
            {"circuit_global": "half_open"},
            {"circuit_global": "open"},
        ]
    ) == "open"


def test_force_stop_reconciles_only_confirmed_dead_workers(monkeypatch) -> None:
    released = []
    cleaned = []
    closed = []

    dead_process = SimpleNamespace(poll=lambda: 1)
    live_process = SimpleNamespace(poll=lambda: None)
    dead = WorkerProcess(1, "dead-worker", dead_process)
    live = WorkerProcess(2, "live-worker", live_process)

    monkeypatch.setattr(supervisor, "_terminate_workers", lambda workers: None)
    monkeypatch.setattr(
        supervisor,
        "_release_dead_worker_leases",
        lambda run_id, worker_id: released.append((run_id, worker_id)) or 3,
    )
    monkeypatch.setattr(
        supervisor,
        "_cleanup_worker_control",
        lambda redis, run_id, worker_id: cleaned.append((run_id, worker_id)),
    )
    monkeypatch.setattr(
        supervisor,
        "_close_worker_handle",
        lambda worker: closed.append(worker.worker_id),
    )

    reclaimed = _force_stop_and_reconcile(object(), 9, [dead, live])

    assert reclaimed == 3
    assert released == [(9, "dead-worker")]
    assert cleaned == [(9, "dead-worker")]
    assert closed == ["dead-worker"]


def test_campaign_status_offline_exposes_exact_retry_wait_and_paused_control(
    monkeypatch,
) -> None:
    runtime = {
        "status": "paused",
        "circuit_state": "open",
        "pending": 7551822,
        "retry_wait": 2100,
    }

    class FakeRedis:
        def ping(self):
            return True

        def get(self, key):
            if key.endswith(":supervisor:state"):
                return json.dumps(runtime)
            return None

        def exists(self, key):
            return False

        def smembers(self, key):
            return set()

        def mget(self, keys):
            return []

        def srem(self, *args):
            return 0

        def close(self):
            pass

    monkeypatch.setattr(supervisor, "_redis_client", lambda *args, **kwargs: FakeRedis())
    monkeypatch.setattr(
        supervisor,
        "settings",
        SimpleNamespace(
            dbd_redis_url="redis://test",
            dbd_redis_rate_key="dbd:test:rate",
            dbd_rate_per_second=60.0,
            dbd_redis_max_connections=4,
        ),
    )
    monkeypatch.setattr(
        supervisor,
        "_task_progress",
        lambda run_id: {
            "total": 9206715,
            "pending": 7551822,
            "retry_wait": 2100,
            "running": 0,
            "failed": 12562,
            "done": 907249,
            "not_available": 732982,
        },
    )
    monkeypatch.setattr(supervisor, "_run_status", lambda run_id: "ready")

    state = supervisor.campaign_status(9)

    assert state["progress_exact"] is True
    assert state["pending_includes_retry_wait"] is False
    assert state["retry_wait"] == 2100
    assert state["pending"] == 7551822
    assert state["run_status"] == "ready"
    assert state["control_status"] == "paused"
    assert state["circuit_state"] == "open"



def test_force_stop_sigint_shield_restores_previous_handler(monkeypatch) -> None:
    calls = []
    previous = object()

    monkeypatch.setattr(supervisor.signal, "getsignal", lambda sig: previous)
    monkeypatch.setattr(
        supervisor.signal,
        "signal",
        lambda sig, handler: calls.append((sig, handler)),
    )

    captured = _suppress_sigint()
    _restore_sigint(captured)

    assert captured is previous
    assert calls == [
        (supervisor.signal.SIGINT, supervisor.signal.SIG_IGN),
        (supervisor.signal.SIGINT, previous),
    ]



def test_collector_handoff_status_skips_redis_when_not_configured(monkeypatch) -> None:
    monkeypatch.setattr(
        supervisor,
        "settings",
        SimpleNamespace(
            dbd_redis_url=None,
            dbd_redis_max_connections=4,
        ),
    )
    monkeypatch.setattr(
        supervisor,
        "_redis_client",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Redis must not be touched when it is not configured")
        ),
    )

    state = supervisor.collector_handoff_status(9)

    assert state == {
        "redis_configured": False,
        "control_status": "not_configured",
        "supervisor_active": False,
        "workers": 0,
    }


def test_collector_handoff_status_reports_registered_legacy_worker(monkeypatch) -> None:
    class FakeRedis:
        def ping(self):
            return True

        def get(self, key):
            return None

        def exists(self, key):
            return False

        def smembers(self, key):
            return {"legacy-worker"}

        def mget(self, keys):
            return [
                json.dumps(
                    {
                        "worker_id": "legacy-worker",
                        "status": "active",
                        "heartbeat_at": time.time(),
                    }
                )
            ]

        def srem(self, *args):
            return 0

        def close(self):
            pass

    monkeypatch.setattr(supervisor, "_redis_client", lambda *args, **kwargs: FakeRedis())
    monkeypatch.setattr(
        supervisor,
        "settings",
        SimpleNamespace(
            dbd_redis_url="redis://test",
            dbd_redis_max_connections=4,
        ),
    )

    state = supervisor.collector_handoff_status(9)

    assert state["supervisor_active"] is False
    assert state["workers"] == 1
    assert state["control_status"] == "workers_without_supervisor"


def test_collector_handoff_status_fails_closed_when_redis_is_unavailable(
    monkeypatch,
) -> None:
    class BrokenRedis:
        def ping(self):
            raise OSError("redis unavailable")

        def close(self):
            pass

    monkeypatch.setattr(supervisor, "_redis_client", lambda *args, **kwargs: BrokenRedis())
    monkeypatch.setattr(
        supervisor,
        "settings",
        SimpleNamespace(
            dbd_redis_url="redis://test",
            dbd_redis_max_connections=4,
        ),
    )

    with pytest.raises(RuntimeError, match="Cannot prove the legacy runtime is offline"):
        supervisor.collector_handoff_status(9)
