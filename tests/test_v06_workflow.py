from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import dbd.workflow as workflow
from dbd.contracts.identity import parse_code
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
        self.discovered_generations = []
        self.finalize_company_ids = []

    def generation_state(self):
        last_seen = self.active or self.finalized
        return {
            "finalized_generation": self.finalized,
            "last_seen_generation": last_seen,
            "active_generation": self.active,
            "unfinished_rows": self.unfinished,
            "next_generation": self.active or self.finalized + 1,
        }

    def status_summary(self, **kwargs):
        self.last_status_phase_mask = kwargs.get("phase_mask")
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

    def upsert_discovered_companies(self, _conn, _rows, *, generation, **_kwargs):
        self.discovered_generations.append(generation)
        return {"inserted": 1, "updated": 0, "refresh_reopened": 0}

    def company_ids(self, _conn, identities):
        identity = list(identities)[0]
        return {identity: 42}

    def company_state(self, _company_id):
        return {
            "company_id": 42,
            "juristic_type_code": "7",
            "registration_no": "0107556000051",
            "latest_fiscal_year": None,
            "required_mask": 113,
            "done_mask": 113,
            "not_available_mask": 0,
            "failed_mask": 0,
            "collect_status": "COMPLETE",
            "lease_owner": None,
            "lease_expires_at": None,
            "retry_at": None,
            "dataset_generation": self.finalized,
            "first_seen_generation": self.finalized,
            "last_seen_generation": self.finalized,
            "last_endpoint": "transformations",
            "last_http_status": 200,
            "last_error": None,
        }

    def prepare_companies(self, _conn, **_kwargs):
        self.prepared += 1
        return 10

    def recover_expired_leases(self, _conn):
        self.recovered += 1
        return 0

    def finalize_generation(self, _conn, *, generation, batch_size, company_id=None):
        del generation, batch_size
        self.finalize_company_ids.append(company_id)
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


@pytest.mark.asyncio
async def test_cycle_reports_complete_with_terminal_failures(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1)

    async def fake_discovery_run(*_args, **_kwargs):
        return DiscoveryResult(1, 1, 10, 1, 0)

    async def fake_collect(**_kwargs):
        return {
            "companies_claimed": 0,
            "companies_finished": 0,
            "endpoints_failed": 0,
        }

    async def verified_with_failures(**kwargs):
        return VerifyResult(
            generation=kwargs["generation"],
            phase=kwargs["phase"],
            status="COMPLETE_WITH_FAILURES",
            details={
                "ok": True,
                "status": "COMPLETE_WITH_FAILURES",
                "terminal_failures": 2,
                "global_terminal_failures": 2,
            },
        )

    monkeypatch.setattr(workflow.discovery, "run", fake_discovery_run)
    monkeypatch.setattr(workflow, "collect", fake_collect)
    monkeypatch.setattr(workflow, "verify", verified_with_failures)

    result = await workflow.cycle_run(
        phase="core",
        repository=repo,
        verbose=False,
    )

    assert result.status == "COMPLETE_WITH_FAILURES"
    assert result.collection["companies_claimed"] == 0
    assert repo.finalize_calls == 2


@pytest.mark.asyncio
async def test_interrupted_cycle_never_finalizes_generation(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1)
    started = asyncio.Event()
    blocked = asyncio.Event()

    async def blocked_discovery(*_args, **_kwargs):
        started.set()
        await blocked.wait()
        raise AssertionError("cancelled discovery unexpectedly resumed")

    monkeypatch.setattr(workflow.discovery, "run", blocked_discovery)

    task = asyncio.create_task(
        workflow.cycle_run(
            phase="core",
            repository=repo,
            verbose=False,
        )
    )
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert repo.prepared == 0
    assert repo.finalize_calls == 0


@pytest.mark.asyncio
async def test_fetch_company_reuses_finalized_generation_and_shared_collect(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=5, active=None, unresolved=0)
    calls = []

    async def fake_collect(**kwargs):
        calls.append(
            (
                kwargs["generation"],
                kwargs["phase"],
                kwargs["company_id"],
            )
        )
        return {
            "companies_claimed": 1,
            "companies_finished": 1,
            "endpoints_failed": 0,
        }

    monkeypatch.setattr(workflow, "collect", fake_collect)

    result = await workflow.fetch_company(
        parse_code("70107556000051"),
        phase="core",
        repository=repo,
    )

    assert result.generation == 5
    assert result.company_id == 42
    assert repo.discovered_generations == [5]
    assert calls == [(5, "core", 42)]
    assert repo.finalize_company_ids == [42]


@pytest.mark.asyncio
async def test_cycle_fails_if_rows_remain_unfinalized_after_bounded_finalize(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1)
    verify_calls = 0

    async def fake_discovery_run(*_args, **_kwargs):
        return DiscoveryResult(1, 1, 10, 1, 0)

    async def fake_collect(**_kwargs):
        return {
            "companies_claimed": 0,
            "companies_finished": 0,
            "endpoints_failed": 0,
        }

    async def fake_verify(**kwargs):
        nonlocal verify_calls
        verify_calls += 1
        return VerifyResult(
            generation=kwargs["generation"],
            phase=kwargs["phase"],
            status="COMPLETE",
            details={
                "ok": True,
                "status": "COMPLETE",
                "terminal_failures": 0,
                "unfinished_rows": 10 if verify_calls == 1 else 1,
            },
        )

    monkeypatch.setattr(workflow.discovery, "run", fake_discovery_run)
    monkeypatch.setattr(workflow, "collect", fake_collect)
    monkeypatch.setattr(workflow, "verify", fake_verify)

    result = await workflow.cycle_run(
        phase="core",
        repository=repo,
        verbose=False,
    )

    assert result.status == "FAILED"
    assert result.verification["unfinished_rows"] == 1


def test_cycle_generation_uses_global_unresolved_work_not_requested_phase() -> None:
    repo = _WorkflowRepo(finalized=5, active=None, unresolved=3)
    assert workflow._cycle_generation(repo, phase="extended") == (5, True)
    assert repo.last_status_phase_mask is None


@pytest.mark.asyncio
async def test_cycle_rejects_scoped_discovery_inputs() -> None:
    with pytest.raises(ValueError, match="full-universe discovery"):
        await workflow.cycle_run(
            phase="core",
            seeds=["010"],
            repository=_WorkflowRepo(),
            verbose=False,
        )


@pytest.mark.asyncio
async def test_fetch_company_does_not_finalize_when_other_required_scope_is_unresolved(
    monkeypatch,
) -> None:
    repo = _WorkflowRepo(finalized=5, active=None, unresolved=0)

    def company_state(_company_id):
        return {
            "company_id": 42,
            "juristic_type_code": "7",
            "registration_no": "0107556000051",
            "latest_fiscal_year": None,
            "required_mask": 2047,
            "done_mask": 1920,
            "not_available_mask": 0,
            "failed_mask": 0,
            "collect_status": "PENDING",
            "lease_owner": None,
            "lease_expires_at": None,
            "retry_at": None,
            "dataset_generation": 4,
            "first_seen_generation": 5,
            "last_seen_generation": 5,
            "last_endpoint": "capital_history",
            "last_http_status": 200,
            "last_error": None,
        }

    repo.company_state = company_state

    async def fake_collect(**_kwargs):
        return {
            "companies_claimed": 1,
            "companies_finished": 1,
            "endpoints_failed": 0,
        }

    monkeypatch.setattr(workflow, "collect", fake_collect)
    result = await workflow.fetch_company(
        parse_code("70107556000051"),
        phase="extended",
        repository=repo,
    )

    assert result.status == "PENDING"
    assert repo.finalize_company_ids == []
