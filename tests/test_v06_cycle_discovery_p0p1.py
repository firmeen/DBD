"""Regression contracts for the focused v0.6 Discover/Cycle P0-P1 changes.

No network or Production database access: only pure policy and fakes.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.core.exceptions import ContractError
from dbd.pipelines import discovery
from dbd.storage.sqlserver import discovery_repository as sql_work
from dbd.storage.mysql import discovery_repository as mysql_work
from dbd import workflow


def test_full_universe_never_certified_from_only_a_registered_tree():
    root = discovery.root_coverage_contract()
    assert root["scope"] == "full"
    assert root["root_strategy_known"]
    assert not root["certified"]
    assert not root["exhaustive"]
    assert not root["search_semantics_certified"]
    assert "exhaustive" in root["reason"]
    with pytest.raises(ContractError, match="not proven exhaustive"):
        discovery.require_certified_full_universe_discovery(
            seeds=None, search_type="", max_buckets=None,
        )


def test_scoped_keyword_never_becomes_global_coverage():
    root = discovery.root_coverage_contract(seeds=["010"])
    assert root["scope"] == "diagnostic"
    assert not root["certified"]
    discovery.require_certified_full_universe_discovery(
        seeds=["010"], search_type="", max_buckets=None,
    )


@pytest.mark.parametrize("keyword", ["010", "0100", "010000000000"])
def test_keyword_candidates_are_ten_bounded_distinct_descendants(keyword):
    parent = SearchFilters(keyword=keyword, jp_types=["5"], pv_codes=["10"],
                           jp_statuses=["1"], business_sizes=["S"])
    candidates = discovery._keyword_expansion_candidates(parent)
    assert len(candidates) == 10
    assert [c.keyword for c in candidates] == [keyword + str(n) for n in range(10)]
    assert len({c.key() for c in candidates}) == 10
    assert all(c.pv_codes == parent.pv_codes and c.jp_types == parent.jp_types
               and c.jp_statuses == parent.jp_statuses
               and c.business_sizes == parent.business_sizes for c in candidates)
    assert parent.key() not in {c.key() for c in candidates}


@pytest.mark.parametrize("kwargs", [
    {"keyword": "0100000000000"},
    {"keyword": "abc"},
    {"keyword": "01"},
    {"keyword": "010", "search_type": "biz"},
])
def test_unsupported_keyword_expansion_is_not_retried(kwargs):
    assert discovery._keyword_expansion_candidates(SearchFilters(**kwargs)) == []


def test_keyword_count_parity_cannot_certify_semantics():
    parent = {
        "status": "COUNTED", "total_items": 20_000,
        "filters_json": json.dumps({"keyword": "010", "search_type": ""}),
    }
    children = discovery._keyword_expansion_candidates(SearchFilters(keyword="010"))
    assert len(children) == 10
    with pytest.raises(ContractError, match="no certified search semantics"):
        discovery.validate_certified_split_shape(parent, children, "keyword")


@pytest.mark.parametrize("dimension,field,values", [
    ("juristic_type", "jp_types", ["2", "5"]),
    ("province", "pv_codes", ["10", "20"]),
    ("status", "jp_statuses", ["1", "A"]),
    ("business_size", "business_sizes", ["S", "L"]),
])
def test_valid_categorical_filter_partition_preserves_parent(dimension, field, values):
    from dataclasses import replace

    original = SearchFilters(keyword="010", search_type="", cap_amt_min=100)
    parent = {
        "status": "COUNTED",
        "filters_json": json.dumps({
            "keyword": original.keyword, "search_type": original.search_type,
            "cap_amt_min": "100",
        }),
    }
    children = [replace(original, **{field: [value]}) for value in values]
    discovery.validate_certified_split_shape(parent, children, dimension)


def test_categorical_split_rejects_changed_filters_and_duplicate_values():
    from dataclasses import replace

    root = SearchFilters(keyword="010", jp_types=[])
    parent = {
        "status": "COUNTED",
        "filters_json": json.dumps({"keyword": "010", "search_type": ""}),
    }
    with pytest.raises(ContractError, match="preserve"):
        discovery.validate_certified_split_shape(
            parent,
            [replace(root, keyword="011", jp_types=["2"]),
             replace(root, jp_types=["5"])],
            "juristic_type",
        )
    with pytest.raises(ContractError, match="unique"):
        discovery.validate_certified_split_shape(
            parent,
            [replace(root, jp_types=["2"]), replace(root, jp_types=["2"])],
            "juristic_type",
        )


@pytest.mark.parametrize("backend", [sql_work, mysql_work])
def test_keyword_blocked_recovery_requires_certified_semantics(backend):
    assert backend.reopen_blocked_keyword(
        None, 2, "full", semantics_certified=False
    ) == 0
    # An untrusted caller boolean cannot override the source-policy evidence.
    assert backend.reopen_blocked_keyword(
        None, 2, "full", semantics_certified=True
    ) == 0


@pytest.mark.parametrize("backend", [sql_work, mysql_work])
def test_repository_rejects_keyword_certification_without_db_write(backend):
    parent = {
        "status": "COUNTED", "generation": 2, "scope_key": "full",
        "total_items": 20_000, "work_id": 17,
        "filters_json": json.dumps({"keyword": "010", "search_type": ""}),
    }
    children = discovery._keyword_expansion_candidates(SearchFilters(keyword="010"))
    with pytest.raises(ContractError, match="no certified search semantics"):
        backend.split(
            None, parent, children, "keyword",
            [(2_000, 200)] * 10,
        )


@pytest.mark.asyncio
async def test_discover_root_guard_precedes_database_selection(monkeypatch):
    def never_create_repository(*_args, **_kwargs):
        raise AssertionError("Production repository must not be opened")

    monkeypatch.setattr(workflow, "production_repository", never_create_repository)
    with pytest.raises(ContractError, match="not proven exhaustive"):
        await workflow.discover()


@pytest.mark.asyncio
async def test_branch_partial_preserves_finalized_core_but_degrades_overall(monkeypatch):
    from dbd.pipelines import branch

    async def fake_core(**_kwargs):
        return workflow.CycleResult(
            status="COMPLETE", generation=2, phase="core", resumed=True,
            discovery={}, prepare={}, collection={}, verification={},
            finalized_rows=0,
        )

    monkeypatch.setattr(workflow, "_cycle_run_core", fake_core)
    monkeypatch.setattr(
        branch, "collect", lambda **_kw: {
            "stage": "PARTIAL - DEFERRED", "deferred": 2,
            "review": 0, "checked": 3, "verified": 1,
        },
    )
    result = await workflow.cycle_run(phase="core")
    assert result.status == "COMPLETE_WITH_FAILURES"
    assert result.branch["core_status"] == "COMPLETE"
    assert result.branch["branch_status"] == "PARTIAL"
    assert result.branch["deferred"] == 2


@pytest.mark.asyncio
async def test_branch_caught_up_preserves_already_completed(monkeypatch):
    from dbd.pipelines import branch

    async def fake_core(**_kwargs):
        return workflow.CycleResult(
            status="ALREADY_COMPLETED", generation=2, phase="core", resumed=True,
            discovery={}, prepare={}, collection={}, verification={},
            finalized_rows=0,
        )

    monkeypatch.setattr(workflow, "_cycle_run_core", fake_core)
    monkeypatch.setattr(
        branch, "collect", lambda **_kw: {
            "stage": "CAUGHT_UP", "deferred": 0, "review": 0,
            "checked": 0, "verified": 0,
        },
    )
    result = await workflow.cycle_run(phase="core")
    assert result.status == "ALREADY_COMPLETED"
    assert result.branch["branch_status"] == "COMPLETE"


@pytest.mark.asyncio
async def test_extended_reuses_core_generation_without_discovery(monkeypatch):
    class FakeExtended:
        backend = "mysql"
        @contextmanager
        def manager_lock(self):
            yield object()
        def assert_manager_lock(self, _lock):
            pass
        def check_database(self):
            return {"schema": "READY"}
        def generation_state(self):
            return {"finalized_generation": 2, "active_generation": None}
        def status_summary(self, **_kwargs):
            return {"unresolved": 0, "active_leases": 0}

    repo = FakeExtended()
    calls = []
    monkeypatch.setattr(
        workflow, "_certified_full_coverage",
        lambda _repo, gen: calls.append(("coverage", gen)) or {"certified": True},
    )
    monkeypatch.setattr(
        workflow, "_prepare_tx", lambda _repo, **kw:
        calls.append(("prepare", kw["generation"], kw["phase"]))
        or workflow.PrepareResult(2, "extended", 2),
    )
    monkeypatch.setattr(
        workflow, "_refresh_extended_tx",
        lambda _repo, gen: calls.append(("refresh", gen)) or 2,
    )

    async def fake_collect(**kwargs):
        calls.append(("collect", kwargs["generation"], kwargs["phase"]))
        return {"companies_finished": 2}

    async def fake_verify(**kwargs):
        calls.append(("verify", kwargs["generation"], kwargs["phase"]))
        return workflow.VerifyResult(2, "extended", "COMPLETE",
                                     {"ok": True, "status": "COMPLETE"})

    monkeypatch.setattr(workflow, "collect", fake_collect)
    monkeypatch.setattr(workflow, "verify", fake_verify)
    monkeypatch.setattr(workflow, "_recover_expired", lambda _repo: 0)
    monkeypatch.setattr(workflow.discovery, "run",
                        lambda *_a, **_kw: (_ for _ in ()).throw(
                            AssertionError("extended must not discover")
                        ))
    result = await workflow.cycle_run(
        phase="extended", refresh=True, repository=repo,
    )
    assert result.status == "COMPLETE"
    assert result.finalized_rows == 0
    assert calls == [
        ("coverage", 2), ("prepare", 2, "extended"),
        ("refresh", 2), ("collect", 2, "extended"), ("verify", 2, "extended"),
    ]


def test_deferred_branch_waits_for_recorded_retry_window():
    from datetime import datetime, timedelta, timezone
    from dbd.pipelines.branch import _deferred_retry_ready

    now = datetime(2026, 10, 20, 10, tzinfo=timezone.utc)
    item = {
        "collection_status": "DEFERRED_NETWORK",
        "last_checked_at": now - timedelta(minutes=30),
    }
    assert not _deferred_retry_ready(item, now)
    assert _deferred_retry_ready(
        {**item, "last_checked_at": now - timedelta(hours=2)}, now
    )
    assert _deferred_retry_ready({**item, "last_checked_at": None}, now)
    assert _deferred_retry_ready(
        {"collection_status": "NO_BRANCH", "last_checked_at": now}, now
    )


def test_deferred_branch_rejects_invalid_durable_timestamps():
    from dbd.pipelines.branch import BranchError, _deferred_retry_ready

    with pytest.raises(BranchError, match="timestamp"):
        _deferred_retry_ready({
            "collection_status": "DEFERRED_PARSE",
            "last_checked_at": "not-a-timestamp",
        })
