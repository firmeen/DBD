"""Focused tests for the no-limit 50-second Discovery process supervisor."""
import importlib.util
import json
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_discover_auto.py"
spec = importlib.util.spec_from_file_location("run_discover_auto", SCRIPT)
auto = importlib.util.module_from_spec(spec)
spec.loader.exec_module(auto)


def args(test_db=None):
    return Namespace(test_db=test_db, concurrency=8, rate=15.0)


def status(open_buckets=1, done=100, failed=0):
    return {
        "buckets_open": open_buckets, "buckets_failed": failed,
        "partitions_lossy": 0, "pages_failed": 0, "pages_partial": 0,
        "unresolved_rejections": 0, "pages_done": done, "buckets_done": 20,
        "buckets_split": 5, "unique_companies": 1000,
    }


def test_test_db_is_validated_without_accepting_production():
    assert auto.parse_args(["--test-db", "dbd_test_discovery"]).test_db == "dbd_test_discovery"
    with pytest.raises(SystemExit):
        auto.parse_args(["--test-db", "dbd"])
    with pytest.raises(SystemExit):
        auto.parse_args(["--test-db", "production_copy"])


def test_selected_db_is_passed_only_to_child_environment(monkeypatch):
    monkeypatch.setenv("DB_NAME", "dbd")
    env = auto.build_environment(args("dbd_test_discovery"))
    assert env["DB_NAME"] == "dbd_test_discovery"
    assert auto.build_environment(args())["DB_NAME"] == "dbd"


def test_readonly_database_preflight_rejects_wrong_db(monkeypatch):
    calls = []
    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        data = {
            "database": "dbd", "revision": auto.EXPECTED_REVISION,
            "missing_tables": [], "numeric_column": True,
            "target_manager_active": False, "production_manager_active": False,
        }
        return SimpleNamespace(returncode=0, stdout=json.dumps(data), stderr="")
    monkeypatch.setattr(auto.subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="mismatch"):
        auto.preflight({"DB_NAME": "dbd_test_discovery"}, test_db="dbd_test_discovery")
    assert calls and calls[0][0][1] == "-c"
    assert "ALTER " not in auto.PREFLIGHT_CODE.upper()
    assert "CREATE " not in auto.PREFLIGHT_CODE.upper()


@pytest.mark.parametrize("field", ["revision", "missing_tables", "numeric_column", "target_manager_active", "production_manager_active"])
def test_preflight_fail_closed_on_schema_or_manager_lock(monkeypatch, field):
    data = {
        "database": "dbd_test_discovery", "revision": auto.EXPECTED_REVISION,
        "missing_tables": [], "numeric_column": True,
        "target_manager_active": False, "production_manager_active": False,
    }
    changes = {
        "revision": "0012_run9_data_repair",
        "missing_tables": ["discovery_pages"],
        "numeric_column": False,
        "target_manager_active": True,
        "production_manager_active": True,
    }
    data[field] = changes[field]
    monkeypatch.setattr(
        auto.subprocess, "run",
        lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps(data), stderr=""),
    )
    with pytest.raises(RuntimeError):
        auto.preflight({"DB_NAME": "dbd_test_discovery"}, test_db="dbd_test_discovery")


def test_no_limit_is_ever_added_to_full_workload():
    cmd = auto.discovery_command("dbd-data.exe", args("dbd_test_discovery"))
    assert cmd == ["dbd-data.exe", "discover", "--concurrency", "8", "--rate", "15.0"]
    assert "--max-buckets" not in cmd and "--limit" not in cmd


@pytest.mark.parametrize("before,after,rc,expected", [
    (status(1, 1), status(0, 2), 0, "complete"),
    (status(3, 1), status(2, 2), 0, "resume"),
    (status(2, 2), status(2, 2), 0, "blocked"),
    (status(1, 2), status(0, 2, 34), 0, "incomplete"),
    (status(2, 2), status(2, 2), 1, "retry"),
    (status(2, 2), status(2, 2), 130, "interrupted"),
])
def test_status_driven_restart_decisions(before, after, rc, expected):
    assert auto.evaluate_progress(before, after, rc) == expected


def test_supervisor_waits_50_seconds_before_restart(monkeypatch):
    states = iter([status(2, 10), status(1, 11), status(1, 11), status(0, 12)])
    runs = []
    sleeps = []
    monkeypatch.setattr(auto, "resolve_executable", lambda: "dbd-data.exe")
    monkeypatch.setattr(auto, "preflight", lambda *a, **kw: {"database": "dbd_test_discovery"})
    monkeypatch.setattr(auto, "read_status", lambda *a, **kw: next(states))
    monkeypatch.setattr(auto, "run_discovery", lambda command, env: runs.append((command,env)) or 0)
    monkeypatch.setattr(auto.time, "sleep", sleeps.append)
    assert auto.main(["--test-db", "dbd_test_discovery", "--concurrency", "8", "--rate", "15"]) == 0
    assert len(runs) == 2
    assert sleeps == [50]
    assert all(env["DB_NAME"] == "dbd_test_discovery" for _, env in runs)


def test_ctrl_c_terminates_child_and_never_restarts(monkeypatch):
    class Child:
        def __init__(self):
            self.stopped = False
        def wait(self, timeout=None):
            if timeout is None:
                raise KeyboardInterrupt()
            return 130
        def poll(self):
            return None
        def terminate(self):
            self.stopped = True

    child = Child()
    monkeypatch.setattr(auto.subprocess, "Popen", lambda *a, **kw: child)
    with pytest.raises(KeyboardInterrupt):
        auto.run_discovery(["dbd-data.exe", "discover"], {})
    assert child.stopped


def test_status_read_requires_structured_json(monkeypatch):
    monkeypatch.setattr(auto.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout=json.dumps(status()), stderr="",
    ))
    assert auto.read_status("dbd-data.exe", {})["buckets_open"] == 1
    monkeypatch.setattr(auto.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="not-json", stderr="",
    ))
    with pytest.raises(RuntimeError, match="not JSON"):
        auto.read_status("dbd-data.exe", {})
