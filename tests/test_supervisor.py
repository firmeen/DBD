from types import SimpleNamespace

import dbd.runtime.supervisor as supervisor
from dbd.runtime.supervisor import (
    WorkerProcess,
    _force_kill_worker,
    _seed_rate,
    _worker_command,
    build_rate_plan,
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
    class FakeRedis:
        def __init__(self):
            self.set_calls = []

        def set(self, key, value, **kwargs):
            self.set_calls.append((key, value, kwargs))

        def delete(self, *keys):
            raise AssertionError(f"must not clear active pacing state: {keys}")

    redis = FakeRedis()
    _seed_rate(redis, "dbd:rate", 15)

    assert [call[0] for call in redis.set_calls] == [
        "dbd:rate:effective-rate",
        "dbd:rate:last-penalty-ms",
    ]
    assert all(call[2].get("nx") is True for call in redis.set_calls)
    assert all(call[2].get("ex") == 600 for call in redis.set_calls)


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
