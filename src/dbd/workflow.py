from __future__ import annotations

import asyncio
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
from .storage.sqlserver.repository import ProductionSqlServerRepository

EventCallback = Callable[[str, dict[str, Any]], None]


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


def _repo(repository: ProductionSqlServerRepository | None) -> ProductionSqlServerRepository:
    return repository or ProductionSqlServerRepository()


def _manual_generation(
    repository: ProductionSqlServerRepository,
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
    repository: ProductionSqlServerRepository,
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
            phase_mask=phase_mask(phase),
            generation=int(finalized),
        )
        if int(summary["unresolved"] or 0):
            return int(finalized), True

    return int(state["next_generation"] or 2), False


async def _guard_application_lock(
    repository: ProductionSqlServerRepository,
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
    repository: ProductionSqlServerRepository,
    *,
    generation: int,
    phase: str,
    company_id: int | None = None,
) -> PrepareResult:
    with repository.transaction() as conn:
        changed = repository.prepare_companies(
            conn,
            generation=generation,
            core_mask=CORE_ENDPOINT_MASK,
            base_mask=BASE_REQUIRED_MASK,
            financial_mask=FINANCIAL_MASK,
            extended_mask=EXTENDED_ENDPOINT_MASK,
            phase=phase,
            company_id=company_id,
        )
    return PrepareResult(
        generation=int(generation),
        phase=phase,
        rows_prepared=int(changed),
    )


async def prepare(
    *,
    phase: str = "core",
    generation: int | None = None,
    company_id: int | None = None,
    repository: ProductionSqlServerRepository | None = None,
    event: EventCallback | None = None,
) -> PrepareResult:
    repo = _repo(repository)
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
    repository: ProductionSqlServerRepository,
    *,
    generation: int,
    phase: str,
    limit: int | None,
) -> RetryResult:
    with repository.transaction() as conn:
        count = repository.retry_failed(
            conn,
            phase_mask=phase_mask(phase),
            generation=generation,
            limit=limit,
        )
    return RetryResult(
        generation=int(generation),
        phase=phase,
        reopened=int(count),
    )


async def retry(
    *,
    phase: str = "core",
    limit: int | None = None,
    repository: ProductionSqlServerRepository | None = None,
    event: EventCallback | None = None,
) -> RetryResult:
    repo = _repo(repository)
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
    repository: ProductionSqlServerRepository | None = None,
) -> StatusResult:
    repo = _repo(repository)
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
    repository: ProductionSqlServerRepository | None = None,
    event: EventCallback | None = None,
) -> dict[str, Any]:
    repo = _repo(repository)
    selected_generation = int(
        generation or await asyncio.to_thread(_manual_generation, repo, phase=phase)
    )
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
    repository: ProductionSqlServerRepository | None = None,
    event: EventCallback | None = None,
) -> tuple[int, DiscoveryResult]:
    repo = _repo(repository)
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
        )
        result = await _guard_application_lock(repo, lock_conn, work)
        repo.assert_manager_lock(lock_conn)
        _emit(
            event,
            "phase_completed",
            phase="DISCOVER",
            generation=generation,
            companies=result.companies_written,
            invalid_rows=result.invalid_rows,
        )
        return generation, result


async def verify(
    *,
    generation: int,
    phase: str = "core",
    repository: ProductionSqlServerRepository | None = None,
) -> VerifyResult:
    repo = _repo(repository)
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
    repository: ProductionSqlServerRepository,
    *,
    generation: int,
    batch_size: int,
) -> int:
    with repository.transaction() as conn:
        return repository.finalize_generation(
            conn,
            generation=int(generation),
            batch_size=int(batch_size),
        )


async def _finalize_all(
    repository: ProductionSqlServerRepository,
    lock_conn,
    *,
    generation: int,
) -> int:
    total = 0
    batch_size = max(1, int(settings.dbd_materialize_batch_size))
    while True:
        repository.assert_manager_lock(lock_conn)
        changed = await asyncio.to_thread(
            _finalize_batch,
            repository,
            generation=generation,
            batch_size=batch_size,
        )
        total += int(changed)
        if not changed:
            break
    repository.assert_manager_lock(lock_conn)
    return total


def _ensure_fetch_company(
    repository: ProductionSqlServerRepository,
    company: CompanyIdentity,
    *,
    generation: int,
    finalize_new: bool,
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
            refresh_mask=0,
            finalize_new=finalize_new,
        )
        return repository.company_ids(
            conn,
            [(company.jp_type_code, company.jp_no)],
        )[(company.jp_type_code, company.jp_no)]


def _recover_expired(
    repository: ProductionSqlServerRepository,
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
    repository: ProductionSqlServerRepository | None = None,
    event: EventCallback | None = None,
) -> FetchCompanyResult:
    repo = _repo(repository)

    with repo.manager_lock() as lock_conn:
        state = repo.generation_state()
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
            finalize_new=state["active_generation"] is None,
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

    collection = await collect(
        generation=generation,
        phase=phase,
        concurrency=concurrency,
        rate=rate,
        http_retries=http_retries,
        company_id=company_id,
        repository=repo,
        event=event,
    )
    company_state = await asyncio.to_thread(repo.company_state, company_id)
    if company_state is None:
        raise RuntimeError(f"company_id={company_id} disappeared after collection")
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
    repository: ProductionSqlServerRepository | None = None,
    event: EventCallback | None = None,
    verbose: bool = True,
) -> CycleResult:
    repo = _repo(repository)
    with repo.manager_lock() as lock_conn:
        _emit(event, "phase_started", phase="PRECHECK")
        await asyncio.to_thread(repo.check_database)
        repo.assert_manager_lock(lock_conn)

        generation, resumed = _cycle_generation(repo, phase=phase)
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
        )
        _emit(event, "phase_started", phase="DISCOVER", generation=generation)
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
            invalid_rows=discovery_result.invalid_rows,
        )

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
        collection_result = await _guard_application_lock(
            repo,
            lock_conn,
            collection_work,
        )
        _emit(event, "phase_completed", phase="COLLECT", generation=generation)

        repo.assert_manager_lock(lock_conn)
        await asyncio.to_thread(_recover_expired, repo)
        repo.assert_manager_lock(lock_conn)

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
        status_value = (
            "COMPLETE_WITH_FAILURES"
            if int(final_verification.details["terminal_failures"] or 0)
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
    repository: ProductionSqlServerRepository | None = None,
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
            event=event,
            verbose=verbose,
        )
        if max_iterations is not None and iteration >= max_iterations:
            return last
        _emit(
            event,
            "cycle_waiting",
            interval_seconds=float(interval_seconds),
            iteration=iteration,
        )
        await asyncio.sleep(float(interval_seconds))
