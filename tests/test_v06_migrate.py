"""Command #04 regression checks: no production database is contacted."""
from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from dbd.storage import migration
from dbd.storage.mysql.production_schema import (
    _matches_industry_scope_expression, _normalize_check_expression,
)


@pytest.mark.parametrize("column", [
    "business_size_code", "location_type", "location_no",
])
@pytest.mark.parametrize("render", [
    "coalesce({column},'')",
    "COALESCE(  \u0060{column}\u0060 , '' )",
    "(coalesce({column},_utf8mb4''))",
    "COALESCE({column},_utf8mb3'')",
])
def test_mysql_functional_index_equivalent_rendering(column, render):
    assert _matches_industry_scope_expression(
        render.format(column=column), column
    )


@pytest.mark.parametrize("wrong", [
    "coalesce(other_column,'')",
    "coalesce(business_size_code,'1')",
    "coalesce(business_size_code,0)",
    "business_size_code",
    "ifnull(business_size_code,'')",
    "coalesce(business_size_code, '') + 1",
])
def test_mysql_functional_index_does_not_accept_wrong_semantics(wrong):
    assert not _matches_industry_scope_expression(wrong, "business_size_code")


def test_mysql_check_normalization_recognizes_catalog_regexp():
    a = _normalize_check_expression("\u0060branch_no\u0060 REGEXP '^[0-9]{5}$'")
    b = _normalize_check_expression(
        "(regexp_like(\u0060branch_no\u0060,_utf8mb4'^[0-9]{5}$'))"
    )
    assert a == b
    assert a != _normalize_check_expression(
        "regexp_like(branch_no,'^[0-9]{4}$')"
    )


def test_sqlserver_control_signature_rejects_different_admission_logic():
    from dbd.storage.sqlserver.control_repository import ADMIT_DDL
    correct = migration._sql_procedure_signature(ADMIT_DDL)
    assert correct == migration._sql_procedure_signature(
        ADMIT_DDL.replace("CREATE OR ALTER PROCEDURE", "CREATE PROCEDURE")
    )
    assert correct != migration._sql_procedure_signature(
        ADMIT_DDL.replace("WITH EXECUTE AS OWNER", "")
    )


def test_migration_plan_is_fail_closed():
    result = migration._final({
        "blockers": ["unsafe", "unsafe"], "actions": ["CREATE x", "CREATE x"],
    })
    assert result["state"] == "BLOCKED"
    assert result["apply_permitted"] is False
    assert result["actions"] == ["CREATE x"]


def test_mysql_preview_uses_select_only(monkeypatch):
    from dbd.storage.mysql import production_engine
    from dbd.core.settings import settings
    monkeypatch.setattr(production_engine, "_configuration", lambda: None)
    sql_seen = []

    class Cursor:
        def execute(self, sql, params=()):
            if not sql.lstrip().upper().startswith(("SELECT", "SHOW GRANTS")):
                raise AssertionError("Preview attempted a write: " + sql)
            sql_seen.append(sql)
            self.sql = sql

        def fetchone(self):
            sql = self.sql
            if "DATABASE(), CURRENT_USER()" in sql:
                return (settings.prod_mysql_database, "test_user@localhost")
            if "SUBSTRING_INDEX" in sql:
                return (9,)
            if "IS_USED_LOCK" in sql:
                return (None,)
            return (None,)

        def fetchall(self):
            if self.sql.strip().upper() == "SHOW GRANTS":
                return [("GRANT ALL PRIVILEGES ON *.* TO 'test_user'@'localhost'",)]
            return []

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    result = migration._mysql_plan(Connection())
    assert result["state"] == "CHANGES_REQUIRED"
    assert result["apply_permitted"] is True
    assert len(result["missing_tables"]) == 6
    assert any("DISCOVERY_WORK" in op for op in result["actions"])
    assert sql_seen
    assert all(s.lstrip().upper().startswith(("SELECT", "SHOW GRANTS")) for s in sql_seen)


def test_apply_denies_blocked_plan_before_acquiring_lock(monkeypatch):
    class Repo:
        def manager_lock(self):
            raise AssertionError("lock must not be acquired for a blocked plan")

    monkeypatch.setattr(migration, "production_repository", lambda _: Repo())
    with pytest.raises(migration.MigrationBlocked):
        migration.apply_migration("mysql", expected={
            "backend": "mysql", "apply_permitted": False,
        })


class Terminal:
    def __init__(self, *args, **kwargs):
        self.finished = []

    def start(self, **kwargs):
        return self

    def finish(self, payload):
        self.finished.append(payload)

    def close(self):
        pass

    def interrupt(self):
        pass

    def fail(self, message):
        pass


def _invoke(monkeypatch, argv, preview, apply=None):
    from dbd.cli import main as cli
    monkeypatch.setattr(cli, "V06Terminal", Terminal)
    monkeypatch.setattr(sys, "argv", ["dbd-data", "migrate"] + argv)
    monkeypatch.setattr(migration, "inspect_migration", preview)
    if apply is not None:
        monkeypatch.setattr(migration, "apply_migration", apply)
    return cli.main()


@pytest.mark.parametrize("flag", ["--check", "--dry-run", ""])
def test_cli_preview_never_applies(monkeypatch, flag):
    seen = []
    def inspect(backend):
        seen.append(backend)
        return {"backend": backend, "state": "CHANGES_REQUIRED",
                "apply_permitted": True, "actions": ["CREATE DBD_BRANCH"],
                "blockers": []}
    _invoke(monkeypatch, (["--backend", "mysql"] + ([flag] if flag else [])), inspect)
    assert seen == ["mysql"]


def test_cli_apply_requires_yes_noninteractive(monkeypatch):
    def inspect(backend):
        return {"backend": backend, "state": "CHANGES_REQUIRED",
                "apply_permitted": True, "actions": [], "blockers": []}
    called = []
    def apply(backend, *, expected):
        called.append(backend)
        return {**expected, "state": "APPLIED"}
    with pytest.raises(SystemExit) as fail:
        _invoke(monkeypatch, ["-d", "mysql", "--apply"], inspect, apply)
    assert fail.value.code == 1
    assert called == []
    _invoke(monkeypatch, ["-d", "mysql", "--apply", "--yes"], inspect, apply)
    assert called == ["mysql"]


def test_cli_rejects_conflicting_modes(monkeypatch):
    from dbd.cli import main as cli
    monkeypatch.setattr(sys, "argv", [
        "dbd-data", "migrate", "--check", "--apply",
    ])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


def test_cli_ctrl_c_preserves_exit_130(monkeypatch):
    with pytest.raises(SystemExit) as exc:
        _invoke(monkeypatch, ["--check"], lambda _: (_ for _ in ()).throw(
            KeyboardInterrupt()
        ))
    assert exc.value.code == 130
