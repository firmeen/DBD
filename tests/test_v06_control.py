"""Monthly SQL Server control-plane regression tests (no live SQL required)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dbd.control import (
    ControlError,
    can_claim,
    choose_monthly_generation,
    effective_start,
    ensure_transition,
    month_key,
    recommend_gap,
    scheduled_utc,
)
from dbd.presentation import setup as setup_ui
from dbd.storage.sqlserver.control_repository import ADMIT_DDL, CONTROL_DDL, ControlRepository


def _now(day=10, hour=9) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=timezone.utc)


def _schedule(day=15) -> dict:
    return {"day": day, "hour": 9, "minute": 0, "timezone": "Asia/Bangkok"}


def _state(gen=2) -> dict:
    return {"next_generation": gen, "active_generation": None}


def _choose(*, day=15, now=None, current=None, active=None):
    return choose_monthly_generation(
        now=now or _now(), generation_state=_state(),
        schedule=_schedule(day), current_month_record=current,
        active_record=active,
    )


def test_same_month_not_due_and_due() -> None:
    assert _choose(day=15, now=_now(10)).action == "NOT_DUE"
    assert _choose(day=15, now=_now(20)).action == "START"


def test_single_generation_per_month_even_after_success() -> None:
    previous = {"generation": 2, "cycle_month": "2026-10", "state": "COMPLETED"}
    decision = _choose(day=15, now=_now(24), current=previous)
    assert decision.action == "ALREADY_COMPLETED"
    assert decision.generation == 2


def test_complete_with_failures_is_terminal_for_month() -> None:
    previous = {
        "generation": 2, "cycle_month": "2026-10",
        "state": "COMPLETE_WITH_FAILURES",
    }
    assert _choose(now=_now(25), current=previous).action == "ALREADY_COMPLETED"


def test_restart_resumes_old_month_before_new_generation() -> None:
    old = {
        "generation": 2, "cycle_month": "2026-10", "state": "DRAINING",
        "scheduled_at": "2026-10-15T02:00:00+00:00",
    }
    result = choose_monthly_generation(
        now=datetime(2026, 11, 20, tzinfo=timezone.utc),
        generation_state=_state(gen=3), schedule=_schedule(), 
        current_month_record=None, active_record=old,
    )
    assert result.action == "RESUME"
    assert result.generation == 2
    assert result.cycle_month == "2026-10"


def test_31st_uses_last_day_of_short_month() -> None:
    assert scheduled_utc("2026-02", 31, 9, 0).astimezone(
        __import__("zoneinfo").ZoneInfo("Asia/Bangkok")
    ).day == 28
    assert scheduled_utc("2028-02", 31, 9, 0).astimezone(
        __import__("zoneinfo").ZoneInfo("Asia/Bangkok")
    ).day == 29


def test_bangkok_calendar_month_not_utc_month() -> None:
    utc_edge = datetime(2026, 9, 30, 18, tzinfo=timezone.utc)
    assert month_key(utc_edge) == "2026-10"


@pytest.mark.parametrize("day,hour,minute", [(0, 9, 0), (32, 9, 0), (15, 24, 0), (15, 9, 60)])
def test_invalid_schedule_is_rejected(day, hour, minute) -> None:
    with pytest.raises(ControlError):
        scheduled_utc("2026-10", day, hour, minute)


def test_pending_change_only_applies_to_next_generation() -> None:
    policy = {**_schedule(), "pending": {
        "day": 20, "hour": 8, "minute": 0, "generation": 3,
    }}
    assert effective_start(policy, 2) == (15, 9, 0)
    assert effective_start(policy, 3) == (20, 8, 0)


def test_10_day_spacing_is_advice_not_a_rejection() -> None:
    start = _now()
    assert recommend_gap(start, start + timedelta(days=9)) is not None
    assert recommend_gap(start, start + timedelta(days=10)) is None


def test_generation_transitions_and_resume_are_fenced() -> None:
    ensure_transition(None, "SCHEDULED")
    ensure_transition("SCHEDULED", "DISCOVERING")
    ensure_transition("COLLECTING", "DRAINING")
    ensure_transition("DRAINING", "DISCOVERING")  # safe replay after interruption
    with pytest.raises(ControlError):
        ensure_transition("COMPLETED", "DISCOVERING")
    with pytest.raises(ControlError):
        ensure_transition("DISCOVERING", "COMPLETED")


def test_claim_only_during_collection_and_when_approved() -> None:
    assert can_claim(role="ADMIN", approved=True, state="COLLECTING", generation=2, current=2)
    assert can_claim(role="WORKER", approved=True, state="COLLECTING", generation=2, current=2)
    assert not can_claim(role="WORKER", approved=True, state="DRAINING", generation=2, current=2)
    assert not can_claim(role="WORKER", approved=False, state="COLLECTING", generation=2, current=2)
    assert not can_claim(role="WORKER", approved=True, state="COLLECTING", generation=1, current=2)


def test_control_schema_has_only_one_extra_table_and_unique_record_key() -> None:
    from dbd.storage.sqlserver.schema import MANAGED_TABLES
    assert len(MANAGED_TABLES) == 5
    assert "dbo.DBD_CONTROL" not in MANAGED_TABLES
    assert "CREATE TABLE dbo.DBD_CONTROL" in CONTROL_DDL
    assert "UNIQUE(record_type, record_key)" in CONTROL_DDL
    assert "ROWVERSION" in CONTROL_DDL
    assert "CHECK (ISJSON(payload) = 1)" in CONTROL_DDL


def test_rate_procedure_requires_authenticated_node_and_fences_generation() -> None:
    assert "WITH EXECUTE AS OWNER" in ADMIT_DDL
    assert "ORIGINAL_LOGIN()" in ADMIT_DDL
    assert "UPDLOCK, HOLDLOCK" in ADMIT_DDL
    assert "@active_generation IS NULL" in ADMIT_DDL
    assert "JSON_MODIFY(payload, '$.next_ms'" in ADMIT_DDL
    assert "THROW 51001" in ADMIT_DDL
    assert "THROW 51002" in ADMIT_DDL


def test_local_machine_id_is_stable_and_not_an_env_secret(tmp_path, monkeypatch) -> None:
    target = tmp_path / "machine.json"
    monkeypatch.setattr(setup_ui, "NODE_PATH", target)
    assert setup_ui.local_node_id() is None
    node = setup_ui.local_node_id(create=True)
    assert node.startswith("node-")
    assert setup_ui.local_node_id(create=True) == node


def test_env_update_is_atomic_and_preserves_unrelated_values(tmp_path, monkeypatch) -> None:
    target = tmp_path / ".env"
    target.write_text("# keep me\nMSSQL_SERVER=example\nUNKNOWN_KEY=yes\n")
    monkeypatch.setattr(setup_ui, "ENV_FILE", target)
    setup_ui._save_env({"DBD_OPERATOR_NAME": "operator", "MSSQL_SERVER": "sql01"})
    text = target.read_text()
    assert "# keep me" in text
    assert "UNKNOWN_KEY=yes" in text
    assert "MSSQL_SERVER=sql01" in text
    assert text.count("MSSQL_SERVER=") == 1


def test_setup_task_is_windows_only(monkeypatch) -> None:
    monkeypatch.setattr(setup_ui.sys, "platform", "linux")
    with pytest.raises(ControlError, match="only on Windows"):
        setup_ui.register_windows_task()


def test_optimistic_sql_updates_have_rowversion_guard() -> None:
    class Cursor:
        rowcount = 1
        def __init__(self):
            self.statements = []
        def execute(self, sql, params=()):
            self.statements.append(sql)
        def fetchone(self):
            return (b"old-version",)
        def close(self):
            pass
    class Conn:
        def __init__(self):
            self.cursor_instance = Cursor()
        def cursor(self):
            return self.cursor_instance

    conn = Conn()
    ControlRepository._put(conn, "SCHEDULE", "core", _schedule())
    assert any("AND row_version=%s" in sql for sql in conn.cursor_instance.statements)


def test_recovery_reconciles_crash_after_business_finalize(monkeypatch) -> None:
    """Never reopen HTTP work just because Control missed its terminal commit."""
    from contextlib import contextmanager
    import dbd.storage.sqlserver.control_repository as control_sql

    class Cursor:
        def execute(self, query, params=()):
            assert "required_mask & failed_mask" in query
            assert tuple(params) == (2,)
        def fetchone(self):
            return (0,)
        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    @contextmanager
    def fake_transaction():
        yield Connection()

    row = {
        "generation": 2, "cycle_month": "2026-10", "state": "FINALIZING",
        "scheduled_at": "2026-10-15T02:00:00+00:00",
        "actual_started_at": "2026-10-15T02:00:00+00:00",
    }
    writes = []
    repo = ControlRepository()
    monkeypatch.setattr(control_sql, "transaction", fake_transaction)
    monkeypatch.setattr(repo, "require_admin", lambda _node: None)
    monkeypatch.setattr(
        repo, "_read",
        lambda _conn, kind, key, lock=False: (
            _schedule() if kind == "SCHEDULE" else
            row if kind == "GENERATION" and key == "core:2026-10" else None
        ),
    )
    monkeypatch.setattr(repo, "active_generation", lambda _conn: row if row["state"] == "FINALIZING" else None)
    monkeypatch.setattr(repo, "_put", lambda _conn, kind, key, value: writes.append((kind, key, dict(value))))
    monkeypatch.setattr(repo, "_audit", lambda _conn, action, detail: writes.append(("AUDIT", action, detail)))

    action, record = repo.select_cycle(
        "node-admin", {
            "active_generation": None, "finalized_generation": 2,
            "last_seen_generation": 2, "next_generation": 3,
        }, auto=True, manual_confirmed=False,
        now=datetime(2026, 10, 25, tzinfo=timezone.utc),
    )
    assert action == "ALREADY_COMPLETED"
    assert record["state"] == "COMPLETED"
    assert record["reconciled_from_durable_generation"] is True
    assert any(kind == "GENERATION" and key == "core:2026-10" for kind, key, _ in writes)
    assert any(kind == "AUDIT" and key == "generation_reconciled" for kind, key, _ in writes)


def test_distinct_machine_sql_login_required_in_worker_approval(monkeypatch) -> None:
    from contextlib import contextmanager
    import dbd.storage.sqlserver.control_repository as control_sql

    @contextmanager
    def fake_transaction():
        yield object()

    repo = ControlRepository()
    monkeypatch.setattr(control_sql, "transaction", fake_transaction)
    monkeypatch.setattr(repo, "require_node", lambda *_args, **_kwargs: {"role": "ADMIN"})
    monkeypatch.setattr(repo, "original_login", lambda *_args: "admin-login")
    with pytest.raises(ControlError, match="distinct from the Admin"):
        repo.approve_worker("admin-node", "node-worker", "admin-login", "operator")
