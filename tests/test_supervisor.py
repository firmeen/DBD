import json
from types import SimpleNamespace

import dbd.runtime.supervisor as supervisor
from dbd.runtime.supervisor import (
    AutoscaleController,
    CampaignResourcePlan,
    WorkerProcess,
    _drain_registered_workers,
    _fleet_rate_target,
    _force_kill_worker,
    _live_progress,
    _seed_rate,
    _spawn_worker,
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


def test_live_progress_overlays_worker_metrics_without_rescanning_durable_queue() -> None:
    baseline = {
        "total": 1000,
        "pending": 780,
        "retry_wait": 10,
        "running": 10,
        "failed": 5,
        "done": 150,
        "not_available": 45,
    }
    metrics = {
        "done": 20,
        "not_available": 5,
        "failed": 2,
        "inflight": 8,
    }

    state = _live_progress(baseline, metrics)

    assert state == {
        "total": 1000,
        "done_only": 170,
        "not_available": 50,
        "failed": 7,
        "terminal": 227,
        "remaining": 773,
        "inflight": 8,
        "pending_estimate": 765,
    }


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

    try:
        _drain_registered_workers(object(), 7, timeout_seconds=0)
    except RuntimeError as exc:
        assert "old-worker" in str(exc)
    else:
        raise AssertionError("handoff must fail closed while an old worker remains registered")

    assert drains == [(7, "old-worker", "supervisor_replacement")]


def test_campaign_status_prefers_live_runtime_without_exact_scan(monkeypatch) -> None:
    runtime = {
        "total": 1000,
        "pending": 700,
        "running": 8,
        "done_only": 180,
        "not_available": 100,
        "failed": 12,
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

    class FakeRedisFactory:
        @staticmethod
        def from_url(*args, **kwargs):
            return fake

    monkeypatch.setattr(supervisor, "Redis", FakeRedisFactory)
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
    plan = build_rate_plan(
        "adaptive",
        2,
        20,
        initial_workers=4,
        fleet_rate_initial=12,
        fleet_rate_max=20,
    )
    assert plan.independent is True
    assert plan.initial == 12
    assert plan.ceiling == 20
    assert plan.worker_rate == 20
    assert _fleet_rate_target(plan, 4) == 20
    assert _fleet_rate_target(plan, 20) == 20


def test_explicit_fleet_rate_rejects_initial_above_ceiling() -> None:
    try:
        build_rate_plan(
            "adaptive",
            2,
            20,
            fleet_rate_initial=21,
            fleet_rate_max=20,
        )
    except ValueError as exc:
        assert "0 < initial <= max" in str(exc)
    else:
        raise AssertionError("invalid explicit fleet rate must fail")


def test_resource_plan_bounds_maximum_fleet() -> None:
    plan = build_resource_plan(
        20,
        db_pool_size=8,
        archive_workers=2,
        projection_workers=8,
        redis_max_connections=4,
        db_connections_max=96,
        archive_workers_max=20,
        projection_workers_max=40,
        redis_connections_max=128,
    )
    assert plan.db_pool_per_worker == 2
    assert plan.db_connections_effective_max <= 96
    assert plan.archive_workers_per_worker == 1
    assert plan.archive_workers_effective_max == 20
    assert plan.projection_workers_per_worker == 2
    assert plan.projection_workers_effective_max == 40
    assert plan.redis_connections_per_client == 3
    assert plan.redis_connections_effective_max <= 128


def test_resource_plan_rejects_budget_that_cannot_support_workers_max() -> None:
    try:
        build_resource_plan(
            20,
            db_pool_size=8,
            archive_workers=2,
            projection_workers=8,
            redis_max_connections=4,
            db_connections_max=40,
        )
    except ValueError as exc:
        assert "fleet DB connection budget" in str(exc)
    else:
        raise AssertionError("undersized DB budget must fail closed")


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
        max_workers=20,
        supervisor_db_pool=8,
        db_connections_budget=96,
        db_pool_per_worker=2,
        archive_workers_budget=20,
        archive_workers_per_worker=1,
        projection_workers_budget=40,
        projection_workers_per_worker=2,
        redis_connections_budget=128,
        redis_connections_per_client=3,
    )

    worker = _spawn_worker(
        7,
        1,
        concurrency=12,
        rate=20,
        fleet_rate_ceiling=20,
        open_powershell=True,
        resource_plan=plan,
    )
    assert worker.process.pid == 1234
    assert captured["env"]["DB_POOL_SIZE"] == "2"
    assert captured["env"]["DBD_ARCHIVE_WORKERS"] == "1"
    assert captured["env"]["DBD_PROJECTION_WORKERS"] == "2"
    assert captured["env"]["DBD_REDIS_MAX_CONNECTIONS"] == "3"



def test_autoscale_adds_worker_when_http_capacity_is_underutilized() -> None:
    controller = AutoscaleController(2, 8, 12)
    target = controller.target(
        pending=5000,
        running=24,
        current=4,
        effective_maximum=8,
        now=10.0,
        fetch_queue=10,
        archive_queue=0,
        project_queue=0,
        observed_http_rate=6.0,
        rate_target=12.0,
        rate_limit_delta=0,
    )
    assert target == 5


def test_autoscale_holds_when_rate_is_saturated_and_fetch_queue_is_deep() -> None:
    controller = AutoscaleController(2, 8, 12)
    target = controller.target(
        pending=5000,
        running=24,
        current=4,
        effective_maximum=8,
        now=10.0,
        fetch_queue=500,
        archive_queue=0,
        project_queue=0,
        observed_http_rate=11.5,
        rate_target=12.0,
        rate_limit_delta=0,
    )
    assert target == 4


def test_autoscale_freezes_on_rate_limit_pressure() -> None:
    controller = AutoscaleController(2, 8, 12)
    target = controller.target(
        pending=5000,
        running=24,
        current=4,
        effective_maximum=8,
        now=10.0,
        fetch_queue=0,
        archive_queue=0,
        project_queue=0,
        observed_http_rate=4.0,
        rate_target=12.0,
        rate_limit_delta=1,
    )
    assert target == 4


def test_autoscale_freezes_on_downstream_backlog() -> None:
    controller = AutoscaleController(2, 8, 12)
    target = controller.target(
        pending=5000,
        running=24,
        current=4,
        effective_maximum=8,
        now=10.0,
        fetch_queue=0,
        archive_queue=0,
        project_queue=500,
        observed_http_rate=4.0,
        rate_target=12.0,
        rate_limit_delta=0,
    )
    assert target == 4
