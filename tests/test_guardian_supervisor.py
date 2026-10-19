from __future__ import annotations

import dbd.runtime.supervisor as supervisor


def test_guardian_rate_key_is_forwarded_to_worker_environment(monkeypatch) -> None:
    captured = {}

    class FakeProcess:
        pid = 4242

        def poll(self):
            return None

    def popen(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return FakeProcess()

    monkeypatch.setattr(supervisor.subprocess, "Popen", popen)

    worker = supervisor._spawn_worker(
        8,
        1,
        concurrency=12,
        rate=16.0,
        fleet_rate_ceiling=20.0,
        open_powershell=True,
        redis_rate_key="dbd:production:global-rate",
    )

    assert worker.process.pid == 4242
    assert captured["env"]["DBD_REDIS_RATE_KEY"] == "dbd:production:global-rate"
    assert captured["env"]["DBD_FLEET_RATE_CEILING"] == "20.0"
    # Windows wraps the worker argv in a PowerShell -Command string.
    # Unix passes the same worker argv directly to Popen.
    rendered = " ".join(captured["command"])
    assert "--run-id 8" in rendered
    assert "--distributed" in rendered


def test_guardian_worker_id_contains_supervisor_generation(monkeypatch) -> None:
    captured = {}

    class FakeProcess:
        pid = 9001

        def poll(self):
            return None

    def popen(command, **kwargs):
        captured["env"] = kwargs["env"]
        return FakeProcess()

    monkeypatch.setattr(supervisor.subprocess, "Popen", popen)
    generation = "a1b2c3d4e5f60718"
    worker = supervisor._spawn_worker(
        8,
        3,
        concurrency=4,
        rate=10.0,
        fleet_rate_ceiling=20.0,
        open_powershell=True,
        supervisor_generation=generation,
    )

    assert worker.worker_id.startswith(f"local-8-g{generation}-3-")
    assert len(worker.worker_id) <= 64
    assert captured["env"]["DBD_WORKER_ID"] == worker.worker_id


def test_guardian_generation_reclaim_is_prefix_scoped_and_attempt_neutral(monkeypatch) -> None:
    executed = []
    commits = []

    class FakeCursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            executed.append((sql, params))
            self.rowcount = 3 if "endpoint_tasks" in sql else 2

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            commits.append(True)

    monkeypatch.setattr(supervisor, "connect", lambda: FakeConnection())

    generation = "a1b2c3d4e5f60718"
    reclaimed = supervisor.reclaim_guardian_process_leases(8, generation)

    assert reclaimed == 5
    assert commits == [True]
    assert len(executed) == 2
    for sql, params in executed:
        assert "lease_owner LIKE %s" in sql
        assert "attempts=CASE" in sql
        assert "project_attempts=CASE" in sql
        assert params == ("guardian_process_recovery", 8, f"local-8-g{generation}-%")
