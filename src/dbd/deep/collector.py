from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from ..contracts.company import normalize as normalize_profile
from ..contracts.financial import normalize_statements, normalize_submissions
from ..contracts.identity import identity
from ..contracts.strict import normalize_collection, normalize_industry_benchmark
from ..core.exceptions import (
    ContractError,
    DbdAuthError,
    DbdForbiddenError,
    IdentityConflict,
)
from .catalog import (
    ACTIVE_ENDPOINT_MASK,
    ACTIVE_ENDPOINTS,
    BASE_REQUIRED_MASK,
    FINANCIAL_ENDPOINTS,
    SUBMIT_FIN_YEAR_MAX_LENGTH,
    EndpointSpec,
    latest_fiscal_year,
    unresolved_mask,
)
from .http import (
    DeepDecodeError,
    DeepNotAvailable,
    DeepPermanentHttpError,
    DeepTransientHttpError,
    FixedRateDbdClient,
)
from .repository import DeepRepository, DeepTransientDatabaseError

EventCallback = Callable[[str, dict[str, Any]], None]


def _normalize_resource(endpoint: str, payload: object):
    if endpoint == "profile":
        return normalize_profile(payload)
    if endpoint == "financial_submissions":
        return normalize_submissions(payload)
    if endpoint == "financial_statements":
        return normalize_statements(payload)
    if endpoint == "industry_benchmark":
        return normalize_industry_benchmark(payload)
    return normalize_collection(endpoint, payload)


@dataclass
class CollectorStats:
    companies_claimed: int = 0
    companies_finished: int = 0
    companies_retry_wait: int = 0
    endpoints_done: int = 0
    endpoints_not_available: int = 0
    endpoints_failed: int = 0
    runtime_recovered: int = 0
    master_promoted: int = 0
    deep_adopted: int = 0
    active_refreshed: int = 0


def _json_payload(value: object) -> str:
    """Serialize logical API JSON while preserving Decimal as JSON numbers."""

    def encode(item: object) -> str:
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, str):
            return json.dumps(item, ensure_ascii=False)
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError("Non-finite Decimal cannot be stored as JSON")
            return str(item)
        if isinstance(item, int):
            return str(item)
        if isinstance(item, float):
            return json.dumps(item, allow_nan=False)
        if isinstance(item, (datetime, date)):
            return json.dumps(item.isoformat(), ensure_ascii=False)
        if isinstance(item, (list, tuple)):
            return "[" + ",".join(encode(child) for child in item) + "]"
        if isinstance(item, dict):
            parts: list[str] = []
            for key, child in item.items():
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                parts.append(
                    json.dumps(key, ensure_ascii=False)
                    + ":"
                    + encode(child)
                )
            return "{" + ",".join(parts) + "}"
        raise TypeError(type(item).__name__)

    return encode(value)


def _meta(
    *,
    status: str,
    http_status: int | None,
    requested_at: datetime | None,
    error: str | None = None,
) -> str:
    payload: dict[str, object] = {
        "status": status,
        "http_status": http_status,
        "requested_at": requested_at.isoformat() if requested_at else None,
    }
    if error:
        payload["error"] = error[:1000]
    return _json_payload(payload)


def _retry_delay(attempts: int) -> int:
    # Company-level retry is deliberately bounded. HTTP already performed its
    # own short retries; this is the durable delay before the company is tried
    # again without turning a transient outage into terminal endpoint failure.
    return min(300, max(5, 5 * (2 ** min(max(attempts - 1, 0), 6))))


class DeepCollector:
    def __init__(
        self,
        *,
        repository: DeepRepository,
        concurrency: int,
        rate_per_second: float,
        http_retries: int,
        buffer_size: int | None = None,
        company_limit: int | None = None,
        continuous: bool = False,
        idle_seconds: float = 5.0,
        event: EventCallback | None = None,
    ) -> None:
        if concurrency < 1 or concurrency > 256:
            raise ValueError("concurrency must be between 1 and 256")
        if rate_per_second <= 0 or rate_per_second > 1000:
            raise ValueError("rate_per_second must be between 0 and 1000")
        if http_retries < 0 or http_retries > 10:
            raise ValueError("http_retries must be between 0 and 10")
        if company_limit is not None and company_limit < 1:
            raise ValueError("company_limit must be positive")
        if idle_seconds < 0.5 or idle_seconds > 3600:
            raise ValueError("idle_seconds must be between 0.5 and 3600")

        self.repo = repository
        self.concurrency = int(concurrency)
        self.rate_per_second = float(rate_per_second)
        self.http_retries = int(http_retries)
        self.buffer_size = max(
            self.concurrency,
            int(buffer_size or max(32, self.concurrency * 4)),
        )
        if self.buffer_size > 10000:
            raise ValueError("buffer_size must be <= 10000")
        self.company_limit = company_limit
        self.continuous = bool(continuous)
        self.idle_seconds = float(idle_seconds)
        self.event = event

        self.stats = CollectorStats()
        self._stats_lock = asyncio.Lock()
        self._claim_lock = asyncio.Lock()
        self._refill_lock = asyncio.Lock()
        self._idle_lock = asyncio.Lock()
        self._idle_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._specs: tuple[EndpointSpec, ...] = ()
        self._cycle = 0
        self._queue_source = "IDLE"
        self._empty_until = 0.0
        self._next_refresh_scan_at = 0.0
        # Endpoint freshness horizons are measured in hours. Discovery may be
        # polled every few seconds, but an empty exact stale scan should not be
        # repeated at that same cadence across a multi-million COMPLETE set.
        self._refresh_scan_seconds = max(60.0, self.idle_seconds)

    def _emit(self, name: str, **payload: object) -> None:
        if self.event is not None:
            self.event(name, dict(payload))

    async def _bump(self, **increments: int) -> None:
        async with self._stats_lock:
            for key, value in increments.items():
                setattr(self.stats, key, int(getattr(self.stats, key)) + int(value))
            snapshot = asdict(self.stats)
        self._emit(
            "progress",
            cycle=self._cycle,
            queue_source=self._queue_source,
            **snapshot,
        )

    async def _remaining_budget(self) -> int | None:
        if self.company_limit is None:
            return None
        async with self._stats_lock:
            return max(0, self.company_limit - self.stats.companies_claimed)

    async def _claim(self) -> dict[str, Any] | None:
        # The lock only serializes the tiny claim/budget section. HTTP and DB
        # endpoint commits remain fully concurrent across companies.
        async with self._claim_lock:
            if self.company_limit is not None:
                async with self._stats_lock:
                    if self.stats.companies_claimed >= self.company_limit:
                        return None

            row = await self.repo.claim()
            if row is None:
                return None

            await self._bump(companies_claimed=1)
            return row

    async def _refill(self) -> dict[str, int]:
        async with self._refill_lock:
            # When many workers observe an empty queue together, only the
            # first worker in an idle window performs the expensive
            # unresolved/Master/stale scan. Others reuse the empty result.
            if self.continuous and time.monotonic() < self._empty_until:
                return {
                    "runtime": 0,
                    "adopted": 0,
                    "promoted": 0,
                    "refreshed": 0,
                    "refresh_scanned": False,
                }

            remaining = await self._remaining_budget()
            if remaining == 0:
                return {
                    "runtime": await self.repo.runtime_size(),
                    "adopted": 0,
                    "promoted": 0,
                    "refreshed": 0,
                    "refresh_scanned": False,
                }

            target = self.buffer_size
            if remaining is not None:
                # Do not promote/reopen more companies than a --limit run can
                # actually process; this keeps canary runs minimally invasive.
                target = min(target, remaining)

            now = time.monotonic()
            include_refresh = now >= self._next_refresh_scan_at
            result = await self.repo.refill(
                max(1, target),
                include_refresh=include_refresh,
            )
            sources = []
            if result["adopted"]:
                sources.append("UNRESOLVED")
                await self._bump(deep_adopted=result["adopted"])
            if result["promoted"]:
                sources.append("NEW COMPANY")
                await self._bump(master_promoted=result["promoted"])
            if result.get("refreshed"):
                sources.append("ACTIVE REFRESH")
                await self._bump(active_refreshed=result["refreshed"])

            self._cycle += 1
            if len(sources) > 1:
                self._queue_source = "MIXED"
            elif sources:
                self._queue_source = sources[0]
            elif int(result["runtime"]) > 0:
                self._queue_source = "RUNTIME"
            else:
                self._queue_source = "IDLE"

            if self.continuous and result.get("refresh_scanned") and not result.get("refreshed"):
                self._next_refresh_scan_at = (
                    time.monotonic() + self._refresh_scan_seconds
                )

            if int(result["runtime"]) == 0 and self.continuous:
                self._empty_until = time.monotonic() + self.idle_seconds
            else:
                self._empty_until = 0.0

            self._emit(
                "cycle",
                cycle=self._cycle,
                queue_source=self._queue_source,
                **result,
            )
            return result

    async def _idle_cycle(self) -> None:
        async with self._idle_lock:
            task = self._idle_task
            if task is None or task.done():
                self._queue_source = "IDLE"
                self._emit(
                    "idle",
                    cycle=self._cycle,
                    idle_seconds=self.idle_seconds,
                    queue_source=self._queue_source,
                )
                task = asyncio.create_task(asyncio.sleep(self.idle_seconds))
                self._idle_task = task
        await task

    @staticmethod
    def _request_for(
        spec: EndpointSpec,
        company: dict[str, Any],
    ) -> tuple[str, dict[str, object] | None]:
        path = spec.path_template.format(
            jp_type_code=company["jp_type_code"],
            jp_no=company["jp_no"],
        )
        params: dict[str, object] | None = None
        if spec.key in FINANCIAL_ENDPOINTS:
            fiscal_year = latest_fiscal_year(company.get("submit_fin_year"))
            if fiscal_year is None:
                raise RuntimeError(
                    f"{spec.key} is required but submit_fin_year is empty"
                )
            params = {"fiscalYear": fiscal_year}
        return path, params

    @staticmethod
    def _validate_payload(
        spec: EndpointSpec,
        company: dict[str, Any],
        payload: object,
    ) -> tuple[str, str | None, bool]:
        normalized = _normalize_resource(spec.key, payload)

        if spec.key == "profile":
            expected = identity(company["jp_no"], company["jp_type_code"])
            actual = identity(
                getattr(normalized, "jp_no", None),
                getattr(normalized, "jp_type_code", None),
            )
            if actual != expected:
                raise IdentityConflict(
                    f"profile identity mismatch: expected {expected.code}, got {actual.code}"
                )
            fiscal_year = getattr(normalized, "submit_fin_year", None)
            fiscal_present = "submit_fin_year" in getattr(
                normalized,
                "present_fields",
                set(),
            )
            fiscal_value = (
                str(fiscal_year).strip()
                if fiscal_year is not None and str(fiscal_year).strip()
                else None
            )
            if (
                fiscal_value is not None
                and len(fiscal_value) > SUBMIT_FIN_YEAR_MAX_LENGTH
            ):
                canonical_year = latest_fiscal_year(fiscal_value)
                if (
                    canonical_year is None
                    or len(canonical_year) > SUBMIT_FIN_YEAR_MAX_LENGTH
                ):
                    raise ContractError(
                        "profile submitFinYear exceeds storage bound and "
                        "cannot be reduced to a usable fiscal year"
                    )
                fiscal_value = canonical_year
            return "done", fiscal_value, fiscal_present

        if spec.key == "industry_benchmark" and normalized is None:
            return "not_available", None, False

        return "done", None, False

    async def _commit_outcome(
        self,
        company: dict[str, Any],
        spec: EndpointSpec,
        *,
        outcome: str,
        payload: object | None,
        http_status: int | None,
        requested_at: datetime | None,
        error: str | None = None,
        profile_submit_fin_year: str | None = None,
        profile_submit_fin_year_present: bool = False,
    ) -> None:
        state = await self.repo.commit_endpoint(
            jp_type_code=str(company["jp_type_code"]),
            jp_no=str(company["jp_no"]),
            spec=spec,
            outcome=outcome,
            payload_json=_json_payload(payload) if outcome == "done" else None,
            meta_json=_meta(
                status=outcome,
                http_status=http_status,
                requested_at=requested_at,
                error=error,
            ),
            http_status=http_status,
            error=error,
            requested_at=requested_at,
            profile_submit_fin_year=profile_submit_fin_year,
            profile_submit_fin_year_present=profile_submit_fin_year_present,
        )
        company.update(state)

        if outcome == "done":
            await self._bump(endpoints_done=1)
        elif outcome == "not_available":
            await self._bump(endpoints_not_available=1)
        else:
            await self._bump(endpoints_failed=1)

    async def _process_company(
        self,
        company: dict[str, Any],
        client: FixedRateDbdClient,
    ) -> None:
        jp_type_code = str(company["jp_type_code"])
        jp_no = str(company["jp_no"])

        required = int(company["required_mask"])
        done = int(company["done_mask"])
        not_available = int(company["not_available_mask"])
        failed = int(company["failed_mask"])
        if required not in {BASE_REQUIRED_MASK, ACTIVE_ENDPOINT_MASK}:
            raise RuntimeError(
                f"{jp_type_code}{jp_no}: unprepared required_mask={required}; "
                "run dbd-deep prepare"
            )
        if (
            (done & not_available)
            or (done & failed)
            or (not_available & failed)
        ):
            raise RuntimeError(
                f"{jp_type_code}{jp_no}: overlapping Deep outcome masks"
            )

        for spec in self._specs:
            unresolved = unresolved_mask(
                int(company["required_mask"]),
                int(company["done_mask"]),
                int(company["not_available_mask"]),
                int(company["failed_mask"]),
            )
            if (unresolved & spec.bit) == 0:
                continue

            path, params = self._request_for(spec, company)

            try:
                response = await client.fetch(
                    spec.method,
                    path,
                    params=params,
                    referer=spec.referer_template,
                )
            except DeepNotAvailable as exc:
                requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
                await self._commit_outcome(
                    company,
                    spec,
                    outcome="not_available",
                    payload=None,
                    http_status=exc.status,
                    requested_at=requested_at,
                )
                if spec.key == "profile":
                    company.update(
                        await self.repo.resolve_profile_dependencies(
                            jp_type_code,
                            jp_no,
                            outcome="not_available",
                            requested_at=requested_at,
                            error="profile: HTTP 404",
                        )
                    )
                continue
            except DeepTransientHttpError as exc:
                delay = _retry_delay(int(company["attempts"]))
                await self.repo.retry_wait(
                    jp_type_code,
                    jp_no,
                    delay_seconds=delay,
                    error=f"{spec.key}: {exc}",
                )
                await self._bump(companies_retry_wait=1)
                self._emit(
                    "retry_wait",
                    jp_type_code=jp_type_code,
                    jp_no=jp_no,
                    endpoint=spec.key,
                    delay_seconds=delay,
                    error=str(exc),
                )
                return
            except (DeepPermanentHttpError, DeepDecodeError) as exc:
                requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
                error = f"{spec.key}: {exc}"
                await self._commit_outcome(
                    company,
                    spec,
                    outcome="failed",
                    payload=None,
                    http_status=exc.status,
                    requested_at=requested_at,
                    error=error,
                )
                if spec.key == "profile":
                    company.update(
                        await self.repo.resolve_profile_dependencies(
                            jp_type_code,
                            jp_no,
                            outcome="failed",
                            requested_at=requested_at,
                            error=error,
                        )
                    )
                continue

            try:
                (
                    outcome,
                    profile_submit_fin_year,
                    profile_submit_fin_year_present,
                ) = self._validate_payload(
                    spec,
                    company,
                    response.data,
                )
            except IdentityConflict as exc:
                error = f"profile: IdentityConflict: {exc}"
                await self._commit_outcome(
                    company,
                    spec,
                    outcome="failed",
                    payload=None,
                    http_status=response.status,
                    requested_at=response.requested_at,
                    error=error,
                )
                company.update(
                    await self.repo.fail_unresolved_identity(
                        jp_type_code,
                        jp_no,
                        error=error,
                    )
                )
                await self.repo.finish_runtime(jp_type_code, jp_no)
                await self._bump(companies_finished=1)
                self._emit(
                    "worker_error",
                    jp_type_code=jp_type_code,
                    jp_no=jp_no,
                    endpoint=spec.key,
                    error=error,
                )
                return
            except (ContractError, TypeError, ValueError) as exc:
                error = f"{spec.key}: {type(exc).__name__}: {exc}"
                await self._commit_outcome(
                    company,
                    spec,
                    outcome="failed",
                    payload=None,
                    http_status=response.status,
                    requested_at=response.requested_at,
                    error=error,
                )
                if spec.key == "profile":
                    company.update(
                        await self.repo.resolve_profile_dependencies(
                            jp_type_code,
                            jp_no,
                            outcome="failed",
                            requested_at=response.requested_at,
                            error=error,
                        )
                    )
                continue

            await self._commit_outcome(
                company,
                spec,
                outcome=outcome,
                payload=response.data if outcome == "done" else None,
                http_status=response.status,
                requested_at=response.requested_at,
                profile_submit_fin_year=profile_submit_fin_year,
                profile_submit_fin_year_present=profile_submit_fin_year_present,
            )

        unresolved = unresolved_mask(
            int(company["required_mask"]),
            int(company["done_mask"]),
            int(company["not_available_mask"]),
            int(company["failed_mask"]),
        )
        if unresolved:
            delay = _retry_delay(int(company["attempts"]))
            await self.repo.retry_wait(
                jp_type_code,
                jp_no,
                delay_seconds=delay,
                error=f"unresolved required mask remains: {unresolved}",
            )
            await self._bump(companies_retry_wait=1)
            return

        await self.repo.finish_runtime(jp_type_code, jp_no)
        await self._bump(companies_finished=1)

    async def _worker(
        self,
        worker_no: int,
        client: FixedRateDbdClient,
    ) -> None:
        while not self._stop.is_set():
            company = await self._claim()
            if company is None:
                remaining = await self._remaining_budget()
                if remaining == 0:
                    self._stop.set()
                    return

                refill = await self._refill()
                company = await self._claim()
                if company is None:
                    runtime = int(refill["runtime"])
                    if runtime == 0:
                        if not self.continuous:
                            self._stop.set()
                            return
                        await self._idle_cycle()
                        continue
                    delay = await self.repo.next_runtime_delay()
                    await asyncio.sleep(delay if delay is not None else 0.5)
                    continue

            try:
                await self._process_company(company, client)
            except (DbdAuthError, DbdForbiddenError):
                raise
            except asyncio.CancelledError:
                raise
            except DeepTransientDatabaseError as exc:
                # Endpoint payload writes are idempotent, so an exhausted
                # transient DB failure can safely return the company to durable
                # retry state. Programming/schema errors are intentionally not
                # caught here and stop the collector.
                delay = _retry_delay(int(company["attempts"]))
                await self.repo.retry_wait(
                    str(company["jp_type_code"]),
                    str(company["jp_no"]),
                    delay_seconds=delay,
                    error=f"{type(exc).__name__}: {exc}",
                )
                await self._bump(companies_retry_wait=1)
                self._emit(
                    "worker_error",
                    worker=worker_no,
                    jp_type_code=company["jp_type_code"],
                    jp_no=company["jp_no"],
                    error=f"{type(exc).__name__}: {exc}",
                )

    async def _monitor_lock(self, lock_conn) -> None:
        while not self._stop.is_set():
            await self.repo.assert_manager_lock(lock_conn)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=10.0)
            except TimeoutError:
                pass

    async def run(self) -> dict[str, Any]:
        self._emit("phase", phase="PREFLIGHT")
        self._specs = await self.repo.preflight(
            require_runtime=True,
            require_prepared=True,
            check_indexes=True,
        )

        async with self.repo.manager_lock() as lock_conn:
            self._emit("phase", phase="RECOVER")
            self.stats.runtime_recovered = await self.repo.recover_runtime()
            await self.repo.assert_no_overlap()

            self._emit("phase", phase="REFILL")
            refill = await self._refill()

            # Continuous mode may start before Discovery has produced work.
            # Keep the manager lock, but do not open/authenticate the HTTP client
            # until a bounded refill actually produces runtime work.
            while int(refill["runtime"]) == 0 and self.continuous:
                remaining = await self._remaining_budget()
                if remaining == 0:
                    self._stop.set()
                    break
                self._emit("phase", phase="IDLE")
                await self._idle_cycle()
                await self.repo.assert_manager_lock(lock_conn)
                refill = await self._refill()

            if int(refill["runtime"]) == 0:
                self._emit("phase", phase="IDLE")
                result = {
                    **asdict(self.stats),
                    "http_requests": 0,
                    "http_retries": 0,
                    "rate_limit_hits": 0,
                    "configured_rate": self.rate_per_second,
                }
            else:
                self._emit("phase", phase="AUTH")
                async with FixedRateDbdClient(
                    concurrency=self.concurrency,
                    rate_per_second=self.rate_per_second,
                    max_retries=self.http_retries,
                ) as client:
                    self._emit(
                        "started",
                        concurrency=self.concurrency,
                        rate_per_second=self.rate_per_second,
                        buffer_size=self.buffer_size,
                        company_limit=self.company_limit,
                        continuous=self.continuous,
                        idle_seconds=self.idle_seconds,
                        active_endpoints=list(ACTIVE_ENDPOINTS),
                        **asdict(self.stats),
                    )
                    self._emit("phase", phase="RUNNING")

                    async with asyncio.TaskGroup() as group:
                        group.create_task(self._monitor_lock(lock_conn))
                        for worker_no in range(1, self.concurrency + 1):
                            group.create_task(self._worker(worker_no, client))

                    result = {
                        **asdict(self.stats),
                        "http_requests": client.requests_made,
                        "http_retries": client.retries_made,
                        "rate_limit_hits": client.rate_limit_hits,
                        "configured_rate": client.rate_per_second,
                    }

        result["cycle"] = self._cycle
        result["queue_source"] = self._queue_source
        result["continuous"] = self.continuous
        result["status"] = await self.repo.status_summary(include_coverage=False)
        return result

