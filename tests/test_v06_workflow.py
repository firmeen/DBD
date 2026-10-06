from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import dbd.workflow as workflow
from dbd.pipelines.discovery import DiscoveryResult
from dbd.workflow import VerifyResult


class _WorkflowRepo:
    def __init__(
        self,
        *,
        finalized: int = 1,
        active: int | None = None,
        unfinished: int = 0,
        unresolved: int = 0,
    ) -> None:
        self.finalized = finalized
        self.active = active
        self.unfinished = unfinished
        self.unresolved = unresolved
        self.finalize_calls = 0
        self.prepared = 0
        self.recovered = 0
        self.lock_checks = 0

    def generation_state(self):
        last_seen = self.active or self.finalized
        return {
            "finalized_generation": self.finalized,
            "last_seen_generation": last_seen,
            "active_generation": self.active,
            "unfinished_rows": self.unfinished,
            "next_generation": self.active or self.finalized + 1,
        }

    def status_summary(self, **_kwargs):
        return {
            "total": 10,
            "discovered": 0,
            "pending": self.unresolved,
            "collecting": 0,
            "retry_wait": 0,
            "complete": 10 - self.unresolved,
            "partial": 0,
            "failed": 0,
            "unresolved": self.unresolved,
            "active_leases": 0,
            "expired_leases": 0,
        }

    @contextmanager
    def manager_lock(self):
        yield SimpleNamespace(lock=True)

    def assert_manager_lock(self, _conn):
        self.lock_checks += 1

    def check_database(self):
        return {"schema": "READY"}

    @contextmanager
    def transaction(self):
        yield self

    def prepare_companies(self, _conn, **_kwargs):
        self.prepared += 1
        return 10

    def recover_expired_leases(self, _conn):
        self.recovered += 1
        return 0

    def finalize_generation(self, _conn, *, generation, batch_size, company_id=None):
        del generation, batch_size, company_id
        self.finalize_calls += 1
        return 10 if self.finalize_calls == 1 else 0


def test_cycle_generation_resumes_unfinished_generation() -> None:
    repo = _WorkflowRepo(finalized=5, active=5, unfinished=17)
    assert workflow._cycle_generation(repo, phase="core") == (5, True)


def test_cycle_generation_reuses_latest_when_retry_reopened_work() -> None:
    repo = _WorkflowRepo(finalized=5, active=None, unresolved=3)
    assert workflow._cycle_generation(repo, phase="core") == (5, True)


def test_cycle_generation_advances_only_after_finalized_scope_is_resolved() -> None:
    repo = _WorkflowRepo(finalized=5, active=None, unresolved=0)
    assert workflow._cycle_generation(repo, phase="core") == (6, False)


@pytest.mark.asyncio
async def test_cycle_verifies_before_bounded_finalization(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1, active=None, unresolved=0)
    order = []

    async def fake_discovery_run(*_args, **kwargs):
        order.append(("discover", kwargs["generation"]))
        return DiscoveryResult(1, 1, 10, 1, 0)

    async def fake_collect(**kwargs):
        order.append(("collect", kwargs["generation"]))
        return {
            "companies_claimed": 10,
            "companies_finished": 10,
            "endpoints_failed": 0,
        }

    verify_calls = 0

    async def fake_verify(**kwargs):
        nonlocal verify_calls
        verify_calls += 1
        order.append(("verify", kwargs["generation"], verify_calls))
        return VerifyResult(
            generation=kwargs["generation"],
            phase=kwargs["phase"],
            status="COMPLETE",
            details={
                "ok": True,
                "status": "COMPLETE",
                "terminal_failures": 0,
            },
        )

    original_finalize = workflow._finalize_all

    async def tracked_finalize(*args, **kwargs):
        order.append(("finalize", kwargs["generation"]))
        return await original_finalize(*args, **kwargs)

    monkeypatch.setattr(workflow.discovery, "run", fake_discovery_run)
    monkeypatch.setattr(workflow, "collect", fake_collect)
    monkeypatch.setattr(workflow, "verify", fake_verify)
    monkeypatch.setattr(workflow, "_finalize_all", tracked_finalize)

    result = await workflow.cycle_run(
        phase="core",
        repository=repo,
        verbose=False,
    )

    assert result.status == "COMPLETE"
    assert result.generation == 2
    assert result.resumed is False
    assert repo.prepared == 1
    assert repo.recovered == 1
    assert repo.finalize_calls == 2
    assert order.index(("verify", 2, 1)) < order.index(("finalize", 2))
    assert order.index(("finalize", 2)) < order.index(("verify", 2, 2))


@pytest.mark.asyncio
async def test_cycle_verify_failure_never_finalizes_generation(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1)

    async def fake_discovery_run(*_args, **_kwargs):
        return DiscoveryResult(1, 1, 10, 1, 0)

    async def fake_collect(**_kwargs):
        return {
            "companies_claimed": 10,
            "companies_finished": 9,
            "endpoints_failed": 0,
        }

    async def failed_verify(**kwargs):
        return VerifyResult(
            generation=kwargs["generation"],
            phase=kwargs["phase"],
            status="FAILED",
            details={
                "ok": False,
                "status": "FAILED",
                "terminal_failures": 0,
                "unresolved": 1,
            },
        )

    monkeypatch.setattr(workflow.discovery, "run", fake_discovery_run)
    monkeypatch.setattr(workflow, "collect", fake_collect)
    monkeypatch.setattr(workflow, "verify", failed_verify)

    result = await workflow.cycle_run(
        phase="core",
        repository=repo,
        verbose=False,
    )

    assert result.status == "FAILED"
    assert result.finalized_rows == 0
    assert repo.finalize_calls == 0


@pytest.mark.asyncio
async def test_cycle_loop_is_simple_foreground_repetition(monkeypatch) -> None:
    calls = []

    async def fake_cycle_run(**kwargs):
        calls.append(kwargs["phase"])
        return workflow.CycleResult(
            status="COMPLETE",
            generation=2,
            phase=kwargs["phase"],
            resumed=False,
            discovery={},
            prepare={},
            collection={},
            verification={},
            finalized_rows=0,
        )

    monkeypatch.setattr(workflow, "cycle_run", fake_cycle_run)

    result = await workflow.cycle_loop(
        interval_seconds=1,
        phase="extended",
        max_iterations=1,
    )

    assert result.status == "COMPLETE"
    assert calls == ["extended"]
