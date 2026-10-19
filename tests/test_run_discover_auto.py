"""Focused unit tests for the 50-second Discovery supervisor; no real HTTP/DB."""
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


def status(open_buckets=1, done=100, failed=0, buckets=25):
    return {
        "buckets": buckets, "buckets_open": open_buckets,
        "buckets_uncounted": 0, "buckets_failed": failed,
        "partitions_lossy": 0, "pages_failed": 0, "pages_partial": 0,
        "unresolved_rejections": 0, "remaining_counted_pages": 0,
        "pages_done": done, "buckets_done": 20, "buckets_split": 5,
        "unique_companies": 1000,
    }


def preflight_result(db="dbd_test_discovery"):
    return {
        "database": db, "revision": auto.EXPECTED_REVISION,
        "missing_tables": [], "numeric_column": True,
        "production_database": "dbd",
        "target_manager_active": False, "target_deep_active": False,
        "production_manager_active": False, "production_deep_active": False,
    }


def test_test_db_name_and_rate_are_validated():
    assert auto.parse_args(["--test-db", "dbd_test_discovery"]).test_db == "dbd_test_discovery"
    for value in ("dbd", "production_copy", "dbd_test_.."):
        with pytest.raises(SystemExit):
            auto.parse_args(["--test-db", value])
    for value in ("0", "-1", "inf", "nan"):
        with pytest.raises(SystemExit):
            auto.parse_args(["--rate", value])
    with pytest.raises(SystemExit):
        auto.parse_args(["--concurrency", "0"])


def test_build_environment_uses_explicit_production_db_from_env_file(monkeypatch, tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text("DB_NAME=dbd\n", encoding="utf-8")
    monkeypatch.setattr(auto, "ENV_FILE", dotenv)
    monkeypatch.setenv("DB_NAME", "dbd")
    selected = auto.build_environment(args("dbd_test_discovery"))
    assert selected["DB_NAME"] == "dbd_test_discovery"
    assert selected["DBD_AUTO_PRODUCTION_DB"] == "dbd"
    assert auto.build_environment(args())["DB_NAME"] == "dbd"
    assert "DBD_AUTO_PRODUCTION_DB" not in auto.build_environment(args())
    assert auto.TEST_DB_PATTERN.fullmatch(selected["DB_NAME"])


def test_test_mode_refuses_ambiguous_production_database(monkeypatch, tmp_path):
    dotenv = tmp_path / ".env"
    monkeypatch.setattr(auto, "ENV_FILE", dotenv)
    with pytest.raises(RuntimeError, match="explicit DB_NAME"):
        auto.build_environment(args("dbd_test_discovery"))
    dotenv.write_text("DB_NAME=dbd_test_other\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="distinct production"):
        auto.build_environment(args("dbd_test_discovery"))


def test_resolve_executable_stays_inside_interpreter_environment(monkeypatch, tmp_path):
    exe = tmp_path / "python.exe"
    exe.touch()
    monkeypatch.setattr(auto.sys, "executable", str(exe))
    monkeypatch.setenv("PATH", str(tmp_path / "another"))
    with pytest.raises(RuntimeError, match="not installed next to"):
        auto.resolve_executable()
    (tmp_path / "dbd-data.exe").touch()
    assert auto.resolve_executable() == str(tmp_path / "dbd-data.exe")


@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", "0012_run9_data_repair"),
        ("missing_tables", ["discovery_pages"]),
        ("numeric_column", False),
        ("target_manager_active", True),
        ("target_deep_active", True),
        ("production_manager_active", True),
        ("production_deep_active", True),
    ],
)
def test_preflight_fails_closed_on_schema_or_all_manager_locks(monkeypatch, field, value):
    data = preflight_result()
    data[field] = value
    monkeypatch.setattr(
        auto.subprocess, "run",
        lambda *a, **kw: SimpleNamespace(
            returncode=0, stdout=json.dumps(data), stderr=""
        ),
    )
    with pytest.raises(RuntimeError):
        auto.preflight(
            {"DB_NAME": "dbd_test_discovery", "DBD_AUTO_PRODUCTION_DB": "dbd"},
            test_db="dbd_test_discovery",
        )


def test_preflight_rejects_wrong_database_and_missing_lock_evidence(monkeypatch):
    data = preflight_result(db="dbd")
    monkeypatch.setattr(
        auto.subprocess, "run",
        lambda *a, **kw: SimpleNamespace(
            returncode=0, stdout=json.dumps(data), stderr=""
        ),
    )
    with pytest.raises(RuntimeError, match="mismatch"):
        auto.preflight(
            {"DB_NAME": "dbd_test_discovery", "DBD_AUTO_PRODUCTION_DB": "dbd"},
            test_db="dbd_test_discovery",
        )
    data.update(preflight_result())
    data.pop("target_deep_active")
    with pytest.raises(RuntimeError, match="missing manager lock state"):
        auto.preflight(
            {"DB_NAME": "dbd_test_discovery", "DBD_AUTO_PRODUCTION_DB": "dbd"},
            test_db="dbd_test_discovery",
        )
    assert "CREATE TABLE" not in auto.PREFLIGHT_CODE.upper()
    assert "ALTER TABLE" not in auto.PREFLIGHT_CODE.upper()
    assert "DROP TABLE" not in auto.PREFLIGHT_CODE.upper()
    assert "IS_USED_LOCK(%s)" in auto.PREFLIGHT_CODE


def test_preflight_reads_mysql_information_schema_table_name_with_stable_alias(
    monkeypatch, capsys,
):
    """MySQL returns TABLE_NAME unless SQL specifies a lowercase result alias."""
    from dbd.core import settings as settings_module
    from dbd.storage.mysql import engine as engine_module

    required = {
        "discovery_buckets", "discovery_pages", "discovery_sightings",
        "discovery_rejections", "response_archive", "company_master",
    }
    queries = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            queries.append(sql)
            if "SELECT DATABASE()" in sql:
                self.rows = [{"db": "dbd_test_discovery"}]
            elif "FROM alembic_version" in sql:
                self.rows = [{"version_num": auto.EXPECTED_REVISION}]
            elif "FROM information_schema.tables" in sql:
                key = "table_name" if "TABLE_NAME AS table_name" in sql else "TABLE_NAME"
                self.rows = [{key: name} for name in sorted(required)]
            elif "FROM information_schema.columns" in sql:
                self.rows = [{"n": 1}]
            elif "IS_USED_LOCK" in sql:
                self.rows = [{"holder": None}]
            else:
                raise AssertionError("Unexpected preflight SQL: " + sql)

        def fetchone(self):
            return self.rows[0]

        def fetchall(self):
            return self.rows

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(engine_module, "connect", lambda: Connection())
    monkeypatch.setattr(
        settings_module, "settings",
        SimpleNamespace(db_name="dbd_test_discovery"),
    )
    monkeypatch.setenv("DBD_AUTO_PRODUCTION_DB", "dbd")
    exec(auto.PREFLIGHT_CODE, {})
    output = json.loads(capsys.readouterr().out)

    assert output["database"] == "dbd_test_discovery"
    assert output["missing_tables"] == []
    assert output["numeric_column"] is True
    assert not any(value for key, value in output.items() if key.endswith("_active"))
    assert any("TABLE_NAME AS table_name" in sql for sql in queries)


def test_preflight_temporary_connection_failure_is_retriable(monkeypatch):
    monkeypatch.setattr(
        auto.subprocess, "run",
        lambda *a, **kw: SimpleNamespace(returncode=75, stdout="", stderr="temporary"),
    )
    with pytest.raises(ConnectionError, match="temporarily"):
        auto.preflight({}, test_db=None)


def test_full_workload_command_has_no_limit_or_distributed_flag():
    command = auto.discovery_command("dbd-data.exe", args("dbd_test_discovery"))
    assert command == [
        "dbd-data.exe", "discover", "--concurrency", "8", "--rate", "15.0"
    ]
    assert not {"--max-buckets", "--limit", "--distributed"} & set(command)


@pytest.mark.parametrize("before,after,code,expected", [
    (status(1, 1), status(0, 2), 0, "complete"),
    (status(3, 1), status(2, 2), 0, "resume"),
    (status(2, 2), status(2, 2), 0, "blocked"),
    (status(1, 2), status(0, 2, 34), 0, "incomplete"),
    (status(2, 2), status(2, 2), 1, "retry_stalled"),
    (status(2, 2), status(2, 3), 1, "retry"),
    (status(2, 2), status(2, 2), 130, "interrupted"),
    (status(0, 0, buckets=0), status(0, 0, buckets=0), 0, "blocked"),
    (status(0, 0, buckets=0), status(1, 0, buckets=2), 0, "resume"),
])
def test_evaluate_progress(before, after, code, expected):
    assert auto.evaluate_progress(before, after, code) == expected


def test_status_json_converts_integer_strings_without_accepting_invalid_types(monkeypatch):
    data = status()
    data["remaining_counted_pages"] = "226293"
    data["buckets"] = "25"
    data["pages_done"] = 333119
    monkeypatch.setattr(
        auto.subprocess, "run",
        lambda *a, **kw: SimpleNamespace(
            returncode=0, stdout=json.dumps(data), stderr=""
        ),
    )
    result = auto.read_status("dbd-data.exe", {})
    assert result["remaining_counted_pages"] == 226293
    assert result["buckets"] == 25
    for invalid in (None, -1, "NaN", 1.5, True):
        data["buckets"] = invalid
        with pytest.raises(RuntimeError, match="integer status"):
            auto.read_status("dbd-data.exe", {})


def test_status_json_rejects_missing_fields_and_bad_json(monkeypatch):
    data = status()
    data.pop("remaining_counted_pages")
    monkeypatch.setattr(
        auto.subprocess, "run",
        lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps(data), stderr=""),
    )
    with pytest.raises(RuntimeError, match="missing"):
        auto.read_status("dbd-data.exe", {})
    monkeypatch.setattr(
        auto.subprocess, "run",
        lambda *a, **kw: SimpleNamespace(returncode=0, stdout="not-json", stderr=""),
    )
    with pytest.raises(RuntimeError, match="not JSON"):
        auto.read_status("dbd-data.exe", {})


def setup_main(monkeypatch, states, codes, *, test_db=False):
    from collections import deque

    states = deque(states)
    codes = deque(codes)
    runs = []
    sleeps = []
    monkeypatch.setattr(auto, "resolve_executable", lambda: "dbd-data.exe")
    monkeypatch.setattr(
        auto, "build_environment",
        lambda args: {
            "DB_NAME": "dbd_test_discovery" if test_db else "dbd",
            "DBD_AUTO_PRODUCTION_DB": "dbd",
        },
    )
    monkeypatch.setattr(
        auto, "preflight", lambda *a, **kw: {"database": "dbd_test_discovery"}
    )
    monkeypatch.setattr(auto, "read_status", lambda *a, **kw: states.popleft())
    monkeypatch.setattr(
        auto, "run_discovery",
        lambda command, env: runs.append((command, env)) or codes.popleft(),
    )
    monkeypatch.setattr(auto.time, "sleep", sleeps.append)
    return runs, sleeps


def test_supervisor_waits_50_seconds_between_processes(monkeypatch):
    runs, sleeps = setup_main(
        monkeypatch,
        [status(2, 10), status(1, 11), status(1, 11), status(0, 12)],
        [0, 0],
        test_db=True,
    )
    assert auto.main(["--test-db", "dbd_test_discovery"]) == 0
    assert len(runs) == 2
    assert sleeps == [50]


def test_complete_database_never_starts_child(monkeypatch):
    runs, sleeps = setup_main(monkeypatch, [status(0, 120)], [])
    assert auto.main([]) == 0
    assert runs == [] and sleeps == []


def test_empty_database_is_not_complete(monkeypatch):
    runs, sleeps = setup_main(
        monkeypatch,
        [status(0, 0, buckets=0), status(0, 0, buckets=0)], [0],
    )
    assert auto.main([]) == 2
    assert len(runs) == 1 and sleeps == []


def test_repeated_stalled_nonzero_failures_stop_after_three_runs(monkeypatch):
    states = [status(2, 10)] * 6
    runs, sleeps = setup_main(monkeypatch, states, [1, 1, 1])
    assert auto.main([]) == 2
    assert len(runs) == 3
    assert sleeps == [50, 50]


def test_progress_resets_stalled_error_count(monkeypatch):
    states = [
        status(2, 10), status(2, 10),  # stalled 1
        status(2, 10), status(2, 11),  # progress -> reset
        status(2, 11), status(2, 11),  # stalled 1
        status(2, 11), status(0, 12),  # complete
    ]
    runs, sleeps = setup_main(monkeypatch, states, [1, 1, 1, 0])
    assert auto.main([]) == 0
    assert len(runs) == 4
    assert sleeps == [50, 50, 50]


def test_transient_status_failures_do_not_start_extra_children(monkeypatch):
    calls = 0
    def read(*a):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("database temporarily down")
        return status(0, 100)
    monkeypatch.setattr(auto, "resolve_executable", lambda: "dbd-data.exe")
    monkeypatch.setattr(auto, "build_environment", lambda args: {"DB_NAME": "dbd"})
    monkeypatch.setattr(auto, "preflight", lambda *a, **kw: {"database": "dbd"})
    monkeypatch.setattr(auto, "read_status", read)
    monkeypatch.setattr(auto, "run_discovery", lambda *a: pytest.fail("child must not start"))
    sleeps = []
    monkeypatch.setattr(auto.time, "sleep", sleeps.append)
    assert auto.main([]) == 0
    assert calls == 3 and sleeps == [50, 50]


def test_preflight_transient_failure_retries_before_child(monkeypatch):
    calls = 0
    def preflight(*a, **kw):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("temporary")
        return {"database": "dbd"}
    monkeypatch.setattr(auto, "resolve_executable", lambda: "dbd-data.exe")
    monkeypatch.setattr(auto, "build_environment", lambda args: {"DB_NAME": "dbd"})
    monkeypatch.setattr(auto, "preflight", preflight)
    monkeypatch.setattr(auto, "read_status", lambda *a: status(0, 100))
    monkeypatch.setattr(auto, "run_discovery", lambda *a: pytest.fail("child must not start"))
    sleeps = []
    monkeypatch.setattr(auto.time, "sleep", sleeps.append)
    assert auto.main([]) == 0
    assert calls == 3 and sleeps == [50, 50]


def test_child_ctrl_c_is_closed_without_restart(monkeypatch):
    class Child:
        def __init__(self):
            self.terminated = False
            self.signaled = False
        def wait(self, timeout=None):
            if timeout is None:
                raise KeyboardInterrupt()
            return 130
        def poll(self):
            return None
        def terminate(self):
            self.terminated = True
        def send_signal(self, signal):
            self.signaled = True

    child = Child()
    monkeypatch.setattr(auto.subprocess, "Popen", lambda *a, **kw: child)
    with pytest.raises(KeyboardInterrupt):
        auto.run_discovery(["dbd-data.exe", "discover"], {})
    assert child.terminated or child.signaled
