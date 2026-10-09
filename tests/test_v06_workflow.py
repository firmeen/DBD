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

    def prepare_companies(self, _conn, **kwargs):
        self.prepared += 1
        after_company_id = int(kwargs.get("after_company_id") or 0)
        return 10, after_company_id + 10

    def recover_expired_leases(self, _conn):
        self.recovered += 1
        return 0

    def finalize_generation(
        self,
        _conn,
        *,
        generation,
        batch_size,
        company_id=None,
        after_company_id=0,
    ):
        del generation, batch_size
        self.finalize_company_ids.append(company_id)
        self.finalize_calls += 1
        if self.finalize_calls == 1:
            return 10, max(int(after_company_id) + 10, int(company_id or 0))
        return 0, None


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
async def test_cycle_loop_stops_immediately_after_failed_cycle(monkeypatch) -> None:
    calls = 0

    async def fake_cycle_run(**kwargs):
        nonlocal calls
        calls += 1
        return workflow.CycleResult(
            status="FAILED",
            generation=2,
            phase=kwargs["phase"],
            resumed=False,
            discovery={},
            prepare={},
            collection={},
            verification={"ok": False},
            finalized_rows=0,
        )

    monkeypatch.setattr(workflow, "cycle_run", fake_cycle_run)
    result = await workflow.cycle_loop(
        interval_seconds=1,
        phase="core",
        max_iterations=5,
    )

    assert result.status == "FAILED"
    assert calls == 1


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
                "terminal_failures": 0,
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



@pytest.mark.asyncio
async def test_discover_forwards_event_and_reports_real_final_counters(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1, unresolved=0)
    events = []
    captured = {}

    def event(name, payload):
        events.append((name, payload))

    async def fake_discovery_run(*_args, **kwargs):
        captured["event"] = kwargs.get("event")
        kwargs["event"](
            "discovery_progress",
            {
                "buckets_processed": 3,
                "pages_fetched": 4,
                "companies_written": 5,
                "requests_made": 6,
                "invalid_rows": 0,
            },
        )
        return DiscoveryResult(3, 4, 5, 6, 0)

    monkeypatch.setattr(workflow.discovery, "run", fake_discovery_run)

    generation, result = await workflow.discover(
        ["010"],
        phase="core",
        repository=repo,
        verbose=False,
        event=event,
    )

    assert generation == 2
    assert result == DiscoveryResult(3, 4, 5, 6, 0)
    assert captured["event"] is event
    completed = [
        payload
        for name, payload in events
        if name == "phase_completed" and payload.get("phase") == "DISCOVER"
    ]
    assert completed == [
        {
            "phase": "DISCOVER",
            "generation": 2,
            "companies": 5,
            "partitions": 3,
            "pages": 4,
            "requests": 6,
            "invalid_rows": 0,
        }
    ]


@pytest.mark.asyncio
async def test_cycle_forwards_discovery_event_and_preserves_phase_order(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1, unresolved=0)
    events = []
    captured = {}

    def event(name, payload):
        events.append((name, payload))

    async def fake_discovery_run(*_args, **kwargs):
        captured["event"] = kwargs.get("event")
        return DiscoveryResult(3, 4, 5, 6, 0)

    async def fake_collect(**_kwargs):
        return {
            "companies_claimed": 5,
            "companies_finished": 5,
            "endpoints_failed": 0,
        }

    async def fake_verify(**kwargs):
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

    monkeypatch.setattr(workflow.discovery, "run", fake_discovery_run)
    monkeypatch.setattr(workflow, "collect", fake_collect)
    monkeypatch.setattr(workflow, "verify", fake_verify)

    result = await workflow.cycle_run(
        phase="core",
        repository=repo,
        verbose=False,
        event=event,
    )

    assert result.status == "COMPLETE"
    assert captured["event"] is event
    started = [payload["phase"] for name, payload in events if name == "phase_started"]
    assert started == [
        "PRECHECK",
        "DISCOVER",
        "PREPARE",
        "COLLECT",
        "VERIFY",
        "FINALIZE",
    ]
    discovery_completed = [
        payload
        for name, payload in events
        if name == "phase_completed" and payload.get("phase") == "DISCOVER"
    ]
    assert discovery_completed == [
        {
            "phase": "DISCOVER",
            "generation": 2,
            "companies": 5,
            "partitions": 3,
            "pages": 4,
            "requests": 6,
            "invalid_rows": 0,
        }
    ]


@pytest.mark.asyncio
async def test_core_cycle_finalizes_when_all_discovered_entities_are_ineligible(
    monkeypatch,
) -> None:
    # Real eligibility lives in the two repository implementations. This
    # guards the shared Cycle contract: zero eligible HTTP claims is valid.
    repo = _WorkflowRepo(finalized=1, active=None, unresolved=0)
    observed = []

    async def fake_discovery(*_args, **kwargs):
        observed.append(("discover", kwargs["generation"]))
        return DiscoveryResult(1, 1, 10, 1, 0)

    async def fake_collect(**kwargs):
        observed.append(("collect", kwargs["generation"]))
        return {"companies_claimed": 0, "companies_finished": 0}

    async def fake_verify(**kwargs):
        observed.append(("verify", kwargs["generation"]))
        return VerifyResult(
            generation=kwargs["generation"], phase="core", status="COMPLETE",
            details={"ok": True, "unfinished_rows": 0,
                     "global_terminal_failures": 0},
        )

    monkeypatch.setattr(workflow.discovery, "run", fake_discovery)
    monkeypatch.setattr(workflow, "collect", fake_collect)
    monkeypatch.setattr(workflow, "verify", fake_verify)
    result = await workflow.cycle_run(
        phase="core", repository=repo, verbose=False
    )

    assert result.status == "COMPLETE"
    assert result.generation == 2
    assert result.collection["companies_claimed"] == 0
    assert result.finalized_rows > 0
    assert [name for name, _ in observed] == [
        "discover", "collect", "verify", "verify"
    ]


def test_core_cycle_resumes_same_generation_without_skipping_ineligible_rows() -> None:
    repo = _WorkflowRepo(finalized=1, active=2, unfinished=17, unresolved=0)
    assert workflow._cycle_generation(repo, phase="core") == (2, True)
    repo.active = None
    repo.finalized = 2
    repo.unfinished = 0
    assert workflow._cycle_generation(repo, phase="core") == (3, False)


@pytest.mark.asyncio
async def test_sqlserver_auto_cycle_not_due_skips_all_http_and_finalize(monkeypatch) -> None:
    from dbd.presentation import setup as setup_module
    from dbd.storage.sqlserver import control_repository

    class FakeControl:
        def select_cycle(self, _node, _state, **kwargs):
            assert kwargs["auto"] is True
            return "NOT_DUE", None

    repo = _WorkflowRepo(finalized=1)
    repo.backend = "sqlserver"
    monkeypatch.setattr(setup_module, "local_node_id", lambda: "node-test")
    monkeypatch.setattr(control_repository, "ControlRepository", FakeControl)

    async def never_discover(*_args, **_kwargs):
        raise AssertionError("NOT_DUE must never issue upstream Search")

    monkeypatch.setattr(workflow.discovery, "run", never_discover)
    result = await workflow.cycle_run(
        phase="core", auto=True, repository=repo, verbose=False,
    )
    assert result.status == "NOT_DUE"
    assert result.finalized_rows == 0
    assert repo.finalize_calls == 0


@pytest.mark.asyncio
async def test_sqlserver_manual_before_due_requires_confirmation(monkeypatch) -> None:
    from dbd.control import EarlyStartConfirmationRequired
    from dbd.presentation import setup as setup_module
    from dbd.storage.sqlserver import control_repository

    class FakeControl:
        def select_cycle(self, _node, _state, **kwargs):
            assert not kwargs["auto"]
            assert not kwargs["manual_confirmed"]
            return "NOT_DUE", None

    repo = _WorkflowRepo(finalized=1)
    repo.backend = "sqlserver"
    monkeypatch.setattr(setup_module, "local_node_id", lambda: "node-test")
    monkeypatch.setattr(control_repository, "ControlRepository", FakeControl)

    with pytest.raises(EarlyStartConfirmationRequired):
        await workflow.cycle_run(phase="core", repository=repo, verbose=False)
    assert repo.finalize_calls == 0


@pytest.mark.asyncio
async def test_sqlserver_worker_cannot_bypass_core_node_gate_with_extended_collect(monkeypatch) -> None:
    repo = _WorkflowRepo(finalized=1, active=None, unresolved=0)
    repo.backend = "sqlserver"

    def deny(_repository):
        raise RuntimeError("Admin authorization required")

    monkeypatch.setattr(workflow, "_require_sql_admin", deny)
    with pytest.raises(RuntimeError, match="Admin authorization required"):
        await workflow.collect(
            phase="extended", backend="sqlserver",
            repository=repo, continuous=False,
        )
