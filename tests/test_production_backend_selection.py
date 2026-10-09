from __future__ import annotations

import pytest

from dbd import workflow
from dbd.deep.cli import parse_args
from dbd.storage import production


def test_production_factory_selects_one_backend(monkeypatch):
    import dbd.storage.mysql.production_repository as mysql_mod
    import dbd.storage.sqlserver.repository as sql_mod

    my = object()
    sql = object()
    monkeypatch.setattr(mysql_mod, "ProductionMySqlRepository", lambda: my)
    monkeypatch.setattr(sql_mod, "ProductionSqlServerRepository", lambda: sql)

    assert production.production_repository("mysql") is my
    assert production.production_repository("sqlserver") is sql


@pytest.mark.asyncio
async def test_status_reads_only_the_requested_backend(monkeypatch):
    called = []

    class Repository:
        def generation_state(self):
            return {
                "active_generation": 6,
                "finalized_generation": 5,
                "last_seen_generation": 6,
                "next_generation": 6,
            }

        def status_summary(self, *, phase_mask, generation):
            assert generation == 6
            return {"total": 10, "unresolved": 3}

        def coverage_summary(self, *, generation):
            return {"companies": 10, "relations": 2}

    def choose(backend):
        called.append(backend)
        return Repository()

    monkeypatch.setattr(workflow, "production_repository", choose)
    result = await workflow.status(backend="mysql", coverage=True)
    assert result.generation == 6
    assert result.summary["unresolved"] == 3
    assert result.coverage["relations"] == 2
    assert called == ["mysql"]


def test_backend_generation_progress_never_uses_other_target():
    class Fake:
        def __init__(self, generation):
            self.generation = generation

        def generation_state(self):
            return {
                "active_generation": self.generation,
                "finalized_generation": self.generation - 1,
                "next_generation": self.generation,
            }

    assert workflow._cycle_generation(Fake(3), phase="core") == (3, True)
    assert workflow._cycle_generation(Fake(6), phase="core") == (6, True)


def test_deep_cli_chooses_mysql_without_changing_existing_default():
    args = parse_args(["collect", "--backend", "mysql", "--limit", "20"])
    assert args.backend == "mysql"
    assert args.limit == 20
    default_args = parse_args(["collect"])
    assert default_args.backend is None


def test_backend_selection_does_not_change_legacy_db_settings():
    from dbd.core.settings import settings

    assert settings.dbd_production_backend in {"sqlserver", "mysql"}
    assert hasattr(settings, "db_name")  # legacy source still separately configured
    assert hasattr(settings, "prod_mysql_database")


def test_sqlserver_lease_lost_is_shared_with_mysql_collector():
    from dbd.storage.production import ProductionLeaseLost
    from dbd.storage.sqlserver.repository import SqlServerLeaseLost

    assert issubclass(SqlServerLeaseLost, ProductionLeaseLost)
