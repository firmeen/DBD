from __future__ import annotations

import asyncio
import sys
from contextlib import contextmanager
from time import monotonic
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from .contracts.identity import CompanyIdentity
from .core.settings import settings
from .deep.catalog import (
    BASE_REQUIRED_MASK,
    CORE_ENDPOINT_MASK,
    EXTENDED_ENDPOINT_MASK,
    FINANCIAL_MASK,
    phase_mask,
)
from .deep.collector import DeepCollector
from .pipelines import discovery
from .pipelines.discovery import DiscoveryResult
from .storage.production import production_repository

EventCallback = Callable[[str, dict[str, Any]], None]

@contextmanager
def _keep_system_awake():
    """Prevent idle sleep only while a foreground production operation runs."""
    if sys.platform != "win32":
        yield
        return
    import ctypes
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    succeeded = ctypes.windll.kernel32.SetThreadExecutionState(
        ES_CONTINUOUS | ES_SYSTEM_REQUIRED
    )
    try:
        yield
    finally:
        if succeeded:
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)


async def _wait_for_remote_claims(repo, *, generation: int) -> None:
    """Drain active SQL leases before VERIFY without waiting for idle nodes."""
    deadline = monotonic() + max(90, int(settings.dbd_lease_seconds) * 2)
    while True:
        summary = await asyncio.to_thread(
            repo.status_summary, phase_mask=CORE_ENDPOINT_MASK, generation=generation
        )
        if int(summary.get("active_leases") or 0) == 0:
            return
        if monotonic() >= deadline:
            raise RuntimeError("Remote claims did not drain; resume the same generation")
        await asyncio.sleep(2.0)


@dataclass(frozen=True)
class PrepareResult:
    generation: int
    phase: str
    rows_prepared: int


@dataclass(frozen=True)
class RetryResult:
    generation: int
    phase: str
    reopened: int


@dataclass(frozen=True)
class StatusResult:
    generation: int | None
    phase: str
    generation_state: dict[str, Any]
    summary: dict[str, Any]
    coverage: dict[str, int] | None = None


@dataclass(frozen=True)
class VerifyResult:
    generation: int
    phase: str
    status: str
    details: dict[str, Any]


@dataclass(frozen=True)
class FetchCompanyResult:
    company_id: int
    registration_no: str
    juristic_type_code: str
    generation: int
    phase: str
    status: str
    collection: dict[str, Any]
    company: dict[str, Any]


@dataclass(frozen=True)
class CycleResult:
    status: str
    generation: int
    phase: str
    resumed: bool
    discovery: dict[str, Any]
    prepare: dict[str, Any]
    collection: dict[str, Any]
    verification: dict[str, Any]
    finalized_rows: int


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _emit(event: EventCallback | None, name: str, **payload: object) -> None:
    if event is not None:
        event(name, dict(payload))


def _repo(repository: Any | None, backend: str | None = None) -> Any:
    if repository is None:
        return production_repository(backend)
    actual = getattr(repository, "backend", None)
    if backend is not None and actual is not None and backend != actual:
        raise ValueError(
            f"Production repository backend {actual!r} does not match {backend!r}"
        )
    return repository


def _manual_generation(
    repository: Any,
    *,
    phase: str,
) -> int:
    state = repository.generation_state()
    if state["active_generation"] is not None:
        return int(state["active_generation"])
    finalized = state["finalized_generation"]
    if finalized is None:
        return int(state["next_generation"] or 2)
    summary = repository.status_summary(
        phase_mask=phase_mask(phase),
        generation=int(finalized),
    )
    if int(summary["unresolved"] or 0):
        return int(finalized)
    return int(finalized)


def _cycle_generation(
    repository: Any,
    *,
    phase: str,
) -> tuple[int, bool]:
    state = repository.generation_state()
    active = state["active_generation"]
    if active is not None:
        return int(active), True

    finalized = state["finalized_generation"]
    if finalized is not None:
        summary = repository.status_summary(
            phase_mask=None,
            generation=int(finalized),
        )
        if int(summary["unresolved"] or 0):
            return int(finalized), True

    return int(state["next_generation"] or 2), False


async def _guard_application_lock(
    repository: Any,
    lock_conn,
    work: Awaitable[Any],
) -> Any:
    stop = asyncio.Event()

    async def monitor() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=10.0)
                return
            except TimeoutError:
                pass
            # Deliberately use the same session that owns sp_getapplock.
            # This is one bounded one-row query, not a long database operation.
            repository.assert_manager_lock(lock_conn)

    async def run_work() -> Any:
        try:
            return await work
        finally:
            stop.set()

    async with asyncio.TaskGroup() as group:
        monitor_task = group.create_task(monitor())
        work_task = group.create_task(run_work())
    del monitor_task
    return work_task.result()


def _prepare_tx(
    repository: Any,
    *,
    generation: int,
    phase: str,
    company_id: int | None = None,
) -> PrepareResult:
    batch_size = 1 if company_id is not None else max(1, int(settings.dbd_write_batch_size))
    total = 0
    after_company_id = 0
    while True:
        with repository.transaction() as conn:
            changed, last_company_id = repository.prepare_companies(
                conn,
                generation=generation,
                core_mask=CORE_ENDPOINT_MASK,
                base_mask=BASE_REQUIRED_MASK,
                financial_mask=FINANCIAL_MASK,
                extended_mask=EXTENDED_ENDPOINT_MASK,
                phase=phase,
                company_id=company_id,
                limit=batch_size,
                after_company_id=after_company_id,
            )
        total += int(changed)
        if company_id is not None or changed < batch_size:
            break
        if last_company_id is None or last_company_id <= after_company_id:
            raise RuntimeError("Prepare keyset did not advance")
        after_company_id = int(last_company_id)
    return PrepareResult(
        generation=int(generation),
        phase=phase,
        rows_prepared=total,
    )


async def prepare(
    *,
    phase: str = "core",
    generation: int | None = None,
    company_id: int | None = None,
    repository: Any | None = None,
    backend: str | None = None,
    event: EventCallback | None = None,
) -> PrepareResult:
    repo = _repo(repository, backend)
    with repo.manager_lock() as lock_conn:
        selected = int(generation or _manual_generation(repo, phase=phase))
        _emit(event, "phase_started", phase="PREPARE", generation=selected)
        repo.assert_manager_lock(lock_conn)
        result = await asyncio.to_thread(
            _prepare_tx,
            repo,
            generation=selected,
            phase=phase,
            company_id=company_id,
        )
        repo.assert_manager_lock(lock_conn)
        _emit(
            event,
            "phase_completed",
            phase="PREPARE",
            generation=selected,
            rows_prepared=result.rows_prepared,
        )
        return result


def _retry_tx(
    repository: Any,
    *,
    generation: int,
    phase: str,
    limit: int | None,
) -> RetryResult:
    batch_size = max(1, int(settings.dbd_write_batch_size))
    target = None if limit is None else max(0, int(limit))
    total = 0
    after_company_id = 0
    while target is None or total < target:
        current_limit = batch_size if target is None else min(batch_size, target - total)
        if current_limit <= 0:
            break
        with repository.transaction() as conn:
            count, last_company_id = repository.retry_failed(
                conn,
                phase_mask=phase_mask(phase),
                generation=generation,
                limit=current_limit,
                after_company_id=after_company_id,
            )
        total += int(count)
        if count < current_limit:
            break
        if last_company_id is None or last_company_id <= after_company_id:
            raise RuntimeError("Retry keyset did not advance")
        after_company_id = int(last_company_id)
    return RetryResult(
        generation=int(generation),
        phase=phase,
        reopened=total,
    )


async def retry(
    *,
    phase: str = "core",
    limit: int | None = None,
    repository: Any | None = None,
    backend: str | None = None,
    event: EventCallback | None = None,
) -> RetryResult:
    repo = _repo(repository, backend)
    with repo.manager_lock() as lock_conn:
        generation = _manual_generation(repo, phase=phase)
        _emit(event, "phase_started", phase="RETRY", generation=generation)
        repo.assert_manager_lock(lock_conn)
        result = await asyncio.to_thread(
            _retry_tx,
            repo,
            generation=generation,
            phase=phase,
            limit=limit,
        )
        repo.assert_manager_lock(lock_conn)
        _emit(
            event,
            "phase_completed",
            phase="RETRY",
            generation=generation,
            reopened=result.reopened,
        )
        return result


async def status(
    *,
    phase: str = "core",
    coverage: bool = False,
    repository: Any | None = None,
    backend: str | None = None,
) -> StatusResult:
    repo = _repo(repository, backend)
    generation_state = await asyncio.to_thread(repo.generation_state)
    generation = (
        generation_state["active_generation"]
        or generation_state["finalized_generation"]
    )
    summary = await asyncio.to_thread(
        repo.status_summary,
        phase_mask=phase_mask(phase),
        generation=int(generation) if generation is not None else None,
    )
    coverage_result = (
        await asyncio.to_thread(
            repo.coverage_summary,
            generation=int(generation) if generation is not None else None,
        )
        if coverage
        else None
    )
    return StatusResult(
        generation=int(generation) if generation is not None else None,
        phase=phase,
        generation_state=generation_state,
        summary=summary,
        coverage=coverage_result,
    )


async def collect(
    *,
    generation: int | None = None,
    phase: str = "core",
    concurrency: int | None = None,
    rate: float | None = None,
    http_retries: int | None = None,
    limit: int | None = None,
    company_id: int | None = None,
    continuous: bool = False,
    idle_seconds: float = 5.0,
    repository: Any | None = None,
    backend: str | None = None,
    event: EventCallback | None = None,
) -> dict[str, Any]:
    repo = _repo(repository, backend)
    node_id = None
    selected_generation = int(
        generation or await asyncio.to_thread(_manual_generation, repo, phase=phase)
    )
    if getattr(repo, "backend", None) == "sqlserver" and phase == "core":
        from .presentation.setup import local_node_id
        from .storage.sqlserver.control_repository import ControlRepository
        node_id = local_node_id()
        if node_id is None:
            raise RuntimeError("Run dbd-data setup before SQL Server Deep Collection")
        control = ControlRepository()
        active = await asyncio.to_thread(control.active_generation)
        if active is None or active.get("state") != "COLLECTING":
            raise RuntimeError("No COLLECTING monthly generation is available to join")
        if generation is not None and int(generation) != int(active["generation"]):
            raise RuntimeError("Collector generation does not match SQL Server coordinator")
        selected_generation = int(active["generation"])
        node = await asyncio.to_thread(control.node_status, node_id)
        if node is None or not node.get("authorized"):
            raise RuntimeError("Unapproved node or mismatched SQL Server login")
    collector = DeepCollector(
        repository=repo,
        generation=selected_generation,
        phase=phase,
        concurrency=int(concurrency or settings.dbd_concurrency),
        rate_per_second=float(rate or settings.dbd_rate_per_second),
        http_retries=(
            int(settings.dbd_http_retries)
            if http_retries is None
            else int(http_retries)
        ),
        company_limit=limit,
        company_id=company_id,
        node_id=node_id,
        continuous=continuous,
        idle_seconds=idle_seconds,
        event=event,
    )
    return await collector.run()


async def discover(
    seeds: list[str] | None = None,
    *,
    phase: str = "core",
    refresh: bool = False,
    search_type: str = "",
    concurrency: int | None = None,
    rate: float | None = None,
    max_buckets: int | None = None,
    verbose: bool = True,
    repository: Any | None = None,
    backend: str | None = None,
    event: EventCallback | None = None,
) -> tuple[int, DiscoveryResult]:
    repo = _repo(repository, backend)
    with repo.manager_lock() as lock_conn:
        generation, resumed = _cycle_generation(repo, phase=phase)
        repo.assert_manager_lock(lock_conn)
        _emit(
            event,
            "phase_started",
            phase="DISCOVER",
            generation=generation,
            resumed=resumed,
        )
        work = discovery.run(
            list(seeds or []),
            search_type=search_type,
            concurrency=concurrency,
            rate=rate,
            max_buckets=max_buckets,
            verbose=verbose,
            distributed=False,
            generation=generation,
            refresh_mask=phase_mask(phase) if refresh else 0,
            repository=repo,
            event=event,
        )
        result = await _guard_application_lock(repo, lock_conn, work)
        repo.assert_manager_lock(lock_conn)
        _emit(
            event,
            "phase_completed",
            phase="DISCOVER",
            generation=generation,
            companies=result.companies_written,
            partitions=result.buckets_processed,
            pages=result.pages_fetched,
            requests=result.requests_made,
            invalid_rows=result.invalid_rows,
        )
        return generation, result


async def verify(
    *,
    generation: int,
    phase: str = "core",
    repository: Any | None = None,
    backend: str | None = None,
) -> VerifyResult:
    repo = _repo(repository, backend)
    details = await asyncio.to_thread(
        repo.verify_generation,
        generation=int(generation),
        phase_mask=phase_mask(phase),
    )
    return VerifyResult(
        generation=int(generation),
        phase=phase,
        status=str(details["status"]),
        details=details,
    )


def _finalize_batch(
    repository: Any,
    *,
    generation: int,
    batch_size: int,
    company_id: int | None = None,
    after_company_id: int = 0,
) -> tuple[int, int | None]:
    with repository.transaction() as conn:
        return repository.finalize_generation(
            conn,
            generation=int(generation),
            batch_size=int(batch_size),
            company_id=company_id,
            after_company_id=int(after_company_id),
        )


async def _finalize_all(
    repository: Any,
    lock_conn,
    *,
    generation: int,
) -> int:
    total = 0
    after_company_id = 0
    batch_size = max(1, int(settings.dbd_materialize_batch_size))
    while True:
        repository.assert_manager_lock(lock_conn)
        changed, last_company_id = await asyncio.to_thread(
            _finalize_batch,
            repository,
            generation=generation,
            batch_size=batch_size,
            after_company_id=after_company_id,
        )
        total += int(changed)
        if not changed:
            break
        if last_company_id is None or last_company_id <= after_company_id:
            raise RuntimeError("Generation finalization keyset did not advance")
        after_company_id = int(last_company_id)
    repository.assert_manager_lock(lock_conn)
    return total


def _ensure_fetch_company(
    repository: Any,
    company: CompanyIdentity,
    *,
    generation: int,
) -> int:
    with repository.transaction() as conn:
        repository.upsert_discovered_companies(
            conn,
            [
                {
                    "juristic_type_code": company.jp_type_code,
                    "registration_no": company.jp_no,
                    "published_at": _utcnow(),
                }
            ],
            generation=generation,
            financial_mask=FINANCIAL_MASK,
            refresh_mask=0,
        )
        return repository.company_ids(
            conn,
            [(company.jp_type_code, company.jp_no)],
        )[(company.jp_type_code, company.jp_no)]


def _recover_expired(
    repository: Any,
) -> int:
    with repository.transaction() as conn:
        return repository.recover_expired_leases(conn)


async def fetch_company(
    company: CompanyIdentity,
    *,
    phase: str = "core",
    concurrency: int | None = None,
    rate: float | None = None,
    http_retries: int | None = None,
    repository: Any | None = None,
    backend: str | None = None,
    event: EventCallback | None = None,
) -> FetchCompanyResult:
    repo = _repo(repository, backend)

    with repo.manager_lock() as lock_conn:
        state = repo.generation_state()
        has_active_generation = state["active_generation"] is not None
        generation = int(
            state["active_generation"]
            or state["finalized_generation"]
            or 1
        )
        repo.assert_manager_lock(lock_conn)
        company_id = await asyncio.to_thread(
            _ensure_fetch_company,
            repo,
            company,
            generation=generation,
        )
        repo.assert_manager_lock(lock_conn)
        await asyncio.to_thread(
            _prepare_tx,
            repo,
            generation=generation,
            phase=phase,
            company_id=company_id,
        )
        repo.assert_manager_lock(lock_conn)

        collection = await _guard_application_lock(
            repo,
            lock_conn,
            collect(
                generation=generation,
                phase=phase,
                concurrency=concurrency,
                rate=rate,
                http_retries=http_retries,
                company_id=company_id,
                repository=repo,
                event=event,
            ),
        )
        repo.assert_manager_lock(lock_conn)
        company_state = await asyncio.to_thread(repo.company_state, company_id)
        if company_state is None:
            raise RuntimeError(
                f"company_id={company_id} disappeared after collection"
            )

        global_unresolved = int(company_state["required_mask"]) & ~(
            int(company_state["done_mask"])
            | int(company_state["not_available_mask"])
            | int(company_state["failed_mask"])
        )
        if not has_active_generation and global_unresolved == 0:
            await asyncio.to_thread(
                _finalize_batch,
                repo,
                generation=generation,
                batch_size=1,
                company_id=company_id,
                after_company_id=0,
            )
            repo.assert_manager_lock(lock_conn)
            refreshed = await asyncio.to_thread(repo.company_state, company_id)
            if refreshed is not None:
                company_state = refreshed

    return FetchCompanyResult(
        company_id=int(company_id),
        registration_no=company.jp_no,
        juristic_type_code=company.jp_type_code,
        generation=generation,
        phase=phase,
        status=str(company_state["collect_status"]),
        collection=collection,
        company=company_state,
    )


async def cycle_run(
    *,
    phase: str = "core",
    refresh: bool = False,
    seeds: list[str] | None = None,
    search_type: str = "",
    concurrency: int | None = None,
    rate: float | None = None,
    http_retries: int | None = None,
    max_buckets: int | None = None,
    repository: Any | None = None,
    backend: str | None = None,
    event: EventCallback | None = None,
    verbose: bool = True,
    auto: bool = False,
    manual_confirmed: bool = False,
) -> CycleResult:
    if auto and phase != "core":
        raise ValueError("--auto is supported only for the SQL Server Core monthly cycle")
    if seeds or search_type or max_buckets is not None:
        raise ValueError(
            "cycle run requires full-universe discovery; use dbd-data discover for scoped Search"
        )
    repo = _repo(repository, backend)
    with repo.manager_lock() as lock_conn:
        _emit(event, "phase_started", phase="PRECHECK")
        await asyncio.to_thread(repo.check_database)
        repo.assert_manager_lock(lock_conn)

        generation, resumed = _cycle_generation(repo, phase=phase)
        control = None
        control_node = None
        cycle_month = None
        if phase == "core" and getattr(repo, "backend", None) == "sqlserver":
            from .control import EarlyStartConfirmationRequired
            from .presentation.setup import local_node_id
            from .storage.sqlserver.control_repository import ControlRepository
            control_node = local_node_id()
            if control_node is None:
                raise RuntimeError("Run dbd-data setup before the monthly cycle")
            control = ControlRepository()
            action, record = control.select_cycle(
                control_node,
                repo.generation_state(),
                auto=auto,
                manual_confirmed=manual_confirmed,
                now=datetime.now(timezone.utc),
            )
            if action == "NOT_DUE" and not auto:
                raise EarlyStartConfirmationRequired(
                    "Monthly generation is not due yet; confirmation required to start early"
                )
            if action in ("NOT_DUE", "ALREADY_COMPLETED"):
                result = CycleResult(
                    status=action,
                    generation=int(record["generation"]) if record else generation,
                    phase=phase, resumed=False,
                    discovery={}, prepare={}, collection={}, verification={},
                    finalized_rows=0,
                )
                _emit(event, "cycle_completed", **asdict(result))
                return result
            assert record is not None
            generation = int(record["generation"])
            cycle_month = str(record["cycle_month"])
            resumed = action == "RESUME"
            control.transition(control_node, cycle_month, "DISCOVERING")
        elif auto:
            raise ValueError("--auto requires the SQL Server production backend")
        _emit(
            event,
            "phase_completed",
            phase="PRECHECK",
            generation=generation,
            resumed=resumed,
        )

        discover_work = discovery.run(
            list(seeds or []),
            search_type=search_type,
            concurrency=concurrency,
            rate=rate,
            max_buckets=max_buckets,
            verbose=verbose,
            distributed=False,
            generation=generation,
            refresh_mask=phase_mask(phase) if refresh else 0,
            repository=repo,
            event=event,
        )
        _emit(event, "phase_started", phase="DISCOVER", generation=generation)
        with _keep_system_awake():
            discovery_result = await _guard_application_lock(
                repo,
                lock_conn,
                discover_work,
            )
        _emit(
            event,
            "phase_completed",
            phase="DISCOVER",
            generation=generation,
            companies=discovery_result.companies_written,
            partitions=discovery_result.buckets_processed,
            pages=discovery_result.pages_fetched,
            requests=discovery_result.requests_made,
            invalid_rows=discovery_result.invalid_rows,
        )

        if control is not None:
            control.transition(control_node, cycle_month, "PREPARING")
        _emit(event, "phase_started", phase="PREPARE", generation=generation)
        repo.assert_manager_lock(lock_conn)
        prepare_result = await asyncio.to_thread(
            _prepare_tx,
            repo,
            generation=generation,
            phase=phase,
        )
        repo.assert_manager_lock(lock_conn)
        _emit(
            event,
            "phase_completed",
            phase="PREPARE",
            generation=generation,
            rows_prepared=prepare_result.rows_prepared,
        )

        if control is not None:
            control.transition(control_node, cycle_month, "COLLECTING")
        _emit(event, "phase_started", phase="COLLECT", generation=generation)
        collection_work = collect(
            generation=generation,
            phase=phase,
            concurrency=concurrency,
            rate=rate,
            http_retries=http_retries,
            continuous=False,
            repository=repo,
            event=event,
        )
        with _keep_system_awake():
            collection_result = await _guard_application_lock(
                repo,
                lock_conn,
                collection_work,
            )
        _emit(event, "phase_completed", phase="COLLECT", generation=generation)

        if control is not None:
            control.transition(control_node, cycle_month, "DRAINING")
            await _wait_for_remote_claims(repo, generation=generation)
        repo.assert_manager_lock(lock_conn)
        await asyncio.to_thread(_recover_expired, repo)
        repo.assert_manager_lock(lock_conn)
        if control is not None:
            control.transition(control_node, cycle_month, "VERIFYING")

        _emit(event, "phase_started", phase="VERIFY", generation=generation)
        verification = await verify(
            generation=generation,
            phase=phase,
            repository=repo,
        )
        _emit(
            event,
            "phase_completed",
            phase="VERIFY",
            generation=generation,
            status=verification.status,
        )

        if not bool(verification.details["ok"]):
            result = CycleResult(
                status="FAILED",
                generation=generation,
                phase=phase,
                resumed=resumed,
                discovery=asdict(discovery_result),
                prepare=asdict(prepare_result),
                collection=collection_result,
                verification=verification.details,
                finalized_rows=0,
            )
            _emit(event, "cycle_completed", **asdict(result))
            return result

        if control is not None:
            control.transition(control_node, cycle_month, "FINALIZING")
        _emit(event, "phase_started", phase="FINALIZE", generation=generation)
        finalized_rows = await _finalize_all(
            repo,
            lock_conn,
            generation=generation,
        )
        repo.assert_manager_lock(lock_conn)
        final_verification = await verify(
            generation=generation,
            phase=phase,
            repository=repo,
        )
        final_ok = bool(final_verification.details["ok"]) and not int(
            final_verification.details.get("unfinished_rows") or 0
        )
        status_value = (
            "FAILED"
            if not final_ok
            else "COMPLETE_WITH_FAILURES"
            if int(final_verification.details.get("global_terminal_failures") or 0)
            else "COMPLETE"
        )
        result = CycleResult(
            status=status_value,
            generation=generation,
            phase=phase,
            resumed=resumed,
            discovery=asdict(discovery_result),
            prepare=asdict(prepare_result),
            collection=collection_result,
            verification=final_verification.details,
            finalized_rows=finalized_rows,
        )
        if control is not None and final_ok:
            control.transition(
                control_node, cycle_month,
                "COMPLETE_WITH_FAILURES" if status_value == "COMPLETE_WITH_FAILURES" else "COMPLETED",
            )
        _emit(event, "phase_completed", phase="FINALIZE", generation=generation)
        _emit(event, "cycle_completed", **asdict(result))
        return result


async def cycle_loop(
    *,
    interval_seconds: float,
    phase: str = "core",
    refresh: bool = False,
    seeds: list[str] | None = None,
    search_type: str = "",
    concurrency: int | None = None,
    rate: float | None = None,
    http_retries: int | None = None,
    max_buckets: int | None = None,
    repository: Any | None = None,
    backend: str | None = None,
    event: EventCallback | None = None,
    verbose: bool = True,
    max_iterations: int | None = None,
) -> CycleResult:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    iteration = 0
    last: CycleResult | None = None
    while True:
        iteration += 1
        last = await cycle_run(
            phase=phase,
            refresh=refresh,
            seeds=seeds,
            search_type=search_type,
            concurrency=concurrency,
            rate=rate,
            http_retries=http_retries,
            max_buckets=max_buckets,
            repository=repository,
            backend=backend,
            event=event,
            verbose=verbose,
        )
        if last.status == "FAILED":
            return last
        if max_iterations is not None and iteration >= max_iterations:
            return last
        _emit(
            event,
            "cycle_waiting",
            interval_seconds=float(interval_seconds),
            iteration=iteration,
        )
        await asyncio.sleep(float(interval_seconds))
