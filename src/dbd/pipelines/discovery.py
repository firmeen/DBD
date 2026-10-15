from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
from uuid import uuid4

from ..client.async_client import AsyncDbdClient
from ..deep.http import FixedRateDbdClient
from contextlib import nullcontext
from ..client.endpoints.search import (
    ITEMS_PER_PAGE,
    MAX_ROWS_PER_QUERY,
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    parse_search_response,
)
from ..contracts.company import normalize
from ..core.exceptions import ContractError
from ..core.progress import ProgressLine
from ..core.settings import settings
from ..runtime.delta import master_fingerprint
from ..storage.mysql.repositories.discovery_repository import (
    DiscoveryLeaseLost,
    bucket_has_pages,
    claim_buckets,
    discovery_summary,
    heartbeat_bucket_claims,
    pending_buckets,
    recoverable_failed_buckets,
    release_bucket_claim,
    release_bucket_claims,
    update_bucket,
    upsert_bucket,
)
from ..storage.mysql.repositories.discovery_split_repository import split_bucket
from .discovery_numeric import (
    NUMERIC_DIMENSIONS,
    PRODUCTION_CERTIFIED_DIMENSIONS,
    NumericObservation,
    numeric_partition_structure,
    plan_numeric_split,
)

JP_TYPES = ("0", "2", "3", "5", "7", "8", "9", "A")
BUSINESS_SIZES = ("S", "M", "L", "X")
EXTRA_PREFIX_SEEDS = ("099",)


@dataclass
class DiscoveryResult:
    buckets_processed: int
    pages_fetched: int
    companies_written: int
    requests_made: int
    recovery_attempts: int = 0
    split_created: int = 0
    remaining_failed: int = 0
    remaining_lossy: int = 0
    remaining_open: int = 0
    remaining_partial: int = 0
    remaining_failed_pages: int = 0
    remaining_quarantine: int = 0


@dataclass
class _BatchController:
    size: int
    minimum: int
    maximum: int

    @classmethod
    def build(cls, concurrency: int) -> _BatchController:
        configured = max(1, int(settings.dbd_archive_batch_size))
        maximum = max(configured, min(64, max(8, concurrency * 2)))
        return cls(size=min(configured, maximum), minimum=1, maximum=maximum)

    def observe(self, queue_depth: int, queue_capacity: int, persist_seconds: float) -> None:
        pressure = queue_depth / max(1, queue_capacity)
        if pressure >= 0.70:
            self.size = min(self.maximum, max(self.size + 1, int(self.size * 1.5)))
        elif pressure <= 0.15 and persist_seconds >= 0.75:
            self.size = max(self.minimum, self.size // 2)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _filters_from_row(row: dict) -> SearchFilters:
    def split(raw: str) -> list[str]:
        return [item for item in (raw or "").split(",") if item]

    persisted = row.get("numeric_filters")
    if isinstance(persisted, str):
        persisted = json.loads(persisted)
    if persisted is None:
        persisted = {}
    if not isinstance(persisted, dict) or set(persisted) - {
        "capAmtMin", "capAmtMax", "totalAssetMin", "totalAssetMax",
        "totalIncomeMin", "totalIncomeMax", "netProfitMin", "netProfitMax",
    }:
        raise ContractError("Unexpected persisted discovery numeric filters")

    return SearchFilters(
        keyword=row["keyword"],
        search_type=row["search_type"],
        pv_codes=split(row["pv_codes"]),
        jp_statuses=split(row["jp_statuses"]),
        jp_types=split(row["jp_types"]),
        business_sizes=split(row["business_sizes"]),
        cap_amt_min=persisted.get("capAmtMin"),
        cap_amt_max=persisted.get("capAmtMax"),
        total_asset_min=persisted.get("totalAssetMin"),
        total_asset_max=persisted.get("totalAssetMax"),
        total_income_min=persisted.get("totalIncomeMin"),
        total_income_max=persisted.get("totalIncomeMax"),
        net_profit_min=persisted.get("netProfitMin"),
        net_profit_max=persisted.get("netProfitMax"),
    )


def _partition_plans(
    filters: SearchFilters, provinces: list[str], status_codes: list[str]
) -> list[list[SearchFilters]]:
    """Suggest categorical filters; API count parity must validate every plan."""
    plans = []
    dimensions = (
        ("jp_types", JP_TYPES),
        ("pv_codes", provinces),
        ("jp_statuses", status_codes),
        ("business_sizes", BUSINESS_SIZES),
    )
    for field, values in dimensions:
        if getattr(filters, field) or not values:
            continue
        # API/observed enumerations can contain duplicate or empty codes.
        # Never produce duplicate child identities or a blank categorical filter.
        distinct_values = tuple(dict.fromkeys(str(value) for value in values if value))
        if not distinct_values:
            continue
        options = [replace(filters, **{field: [value]}) for value in distinct_values]
        plans.append(options)
    return plans


def observed_status_codes() -> list[str]:
    """Candidates, not an authoritative enumeration of all DBD status codes."""
    from ..storage.mysql.engine import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT observed_jp_stat_code AS code
            FROM company_master
            WHERE observed_jp_stat_code IS NOT NULL
              AND observed_jp_stat_code <> ''
            ORDER BY observed_jp_stat_code
            """
        )
        return [str(row["code"]) for row in cur.fetchall()]


async def fetch_provinces(client: AsyncDbdClient) -> list[str]:
    data = await client.get("/api/v1/commons/provinces")
    if not isinstance(data, list):
        raise ContractError("Province response must be a JSON array")
    provinces = [str(row["pvCode"]) for row in data if isinstance(row, dict) and row.get("pvCode")]
    if not provinces:
        raise ContractError("Province response contains no usable pvCode values")
    return provinces


async def province_seeds(client: AsyncDbdClient) -> list[str]:
    provinces = await fetch_provinces(client)
    return sorted(
        {f"0{str(province).zfill(2)}" for province in provinces if str(province).strip()}
        | set(EXTRA_PREFIX_SEEDS)
    )


def coverage_gaps() -> list[str]:
    from ..storage.mysql.engine import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT LEFT(s.jp_no,3) AS prefix
            FROM discovery_sightings s
            WHERE NOT EXISTS (
              SELECT 1 FROM discovery_buckets b WHERE b.keyword=LEFT(s.jp_no,3)
            ) ORDER BY prefix
            """
        )
        return [row["prefix"] for row in cur.fetchall()]


async def _count(client: AsyncDbdClient, bucket_id: int, filters: SearchFilters):
    from ..storage.mysql.repositories.discovery_repository import archive_search, mark_search_archive

    requested = _now()
    data = await client.post(
        SEARCH_PATH,
        filters.payload(page=1),
        referer=SEARCH_REFERER,
        timeout=settings.dbd_count_timeout_seconds,
        rate_key="search",
    )
    archive_id = await asyncio.to_thread(
        archive_search, bucket_id, 1, filters, data, requested, link_page=False
    )
    try:
        page = parse_search_response(data)
    except Exception as exc:
        await asyncio.to_thread(mark_search_archive, archive_id, "invalid", type(exc).__name__)
        raise
    return page, archive_id, data, requested


def _normalize_rows(rows: list[dict]) -> tuple[list, list[dict]]:
    profiles = []
    rejections = []
    for row_index, row in enumerate(rows):
        try:
            profiles.append(normalize(row))
        except ContractError as exc:
            rejections.append(
                {
                    "row_index": row_index,
                    "payload": row,
                    "error_code": type(exc).__name__,
                    "error_message": str(exc),
                }
            )
    return profiles, rejections


def _stage_projection_cache(batch: list[dict], cache: dict[tuple[str, str], str]) -> dict:
    """Select projections against committed cache without mutating it before DB commit."""
    staged: dict[tuple[str, str], str] = {}
    for record in batch:
        selected = []
        fingerprints = []
        for profile in record["profiles"]:
            fingerprint = master_fingerprint(profile)
            key = (profile.jp_type_code, profile.jp_no)
            known = staged.get(key, cache.get(key))
            if known == fingerprint:
                continue
            staged[key] = fingerprint
            selected.append(profile)
            fingerprints.append((key, fingerprint))
        record["projection_profiles"] = selected
        record["projection_fingerprints"] = fingerprints
    return staged


async def _drain_bucket(
    client,
    bucket_id,
    filters,
    pages_total,
    start_page=None,
    *,
    verbose=False,
    first_page=None,
    master_cache: dict[tuple[str, str], str] | None = None,
    progress_line: ProgressLine | None = None,
    lease_owner: str | None = None,
    lease_version: int | None = None,
    expected_items: int | None = None,
):
    """Drain incomplete page checkpoints; start_page is retained for API compatibility."""
    from ..storage.mysql.repositories.discovery_batch_repository import persist_page_batch
    from ..storage.mysql.repositories.discovery_repository import completed_pages, fail_page, page_progress

    del start_page
    local_progress = None
    if progress_line is None and verbose:
        local_progress = ProgressLine("DISCOVERY", interval=0.75)
        progress_line = local_progress

    def owned_update(**fields) -> None:
        if lease_owner is None:
            update_bucket(bucket_id, **fields)
        else:
            update_bucket(
                bucket_id,
                lease_owner=lease_owner,
                lease_version=lease_version,
                **fields,
            )

    owned_update(status="draining", last_error=None)
    completed = await asyncio.to_thread(completed_pages, bucket_id)
    pages = [page for page in range(1, pages_total + 1) if page not in completed]
    if not pages:
        state = await asyncio.to_thread(page_progress, bucket_id)
        owned_update(
            status="done" if state["done"] == pages_total else "failed",
            pages_done=state["done"],
            rows_seen=state["rows"] + state["rejected"],
            last_error=None if state["done"] == pages_total else "Discovery page state incomplete",
        )
        if local_progress is not None:
            local_progress.close()
        return 0, 0

    cache = master_cache if master_cache is not None else {}
    page_queue: asyncio.Queue[int] = asyncio.Queue()
    for page_no in pages:
        page_queue.put_nowait(page_no)
    result_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=max(8, client.concurrency * 2))
    workers_done = asyncio.Event()
    stop_pages = asyncio.Event()
    bucket_error: str | None = None
    controller = _BatchController.build(client.concurrency)
    fetched = 0
    written = 0

    async def fetch_one(page_no: int) -> dict | None:
        nonlocal bucket_error
        if stop_pages.is_set():
            return None
        try:
            archive_id = None
            if page_no == 1 and first_page is not None:
                archive_id, data, requested = first_page
            else:
                requested = _now()
                data = await client.post(
                    SEARCH_PATH,
                    filters.payload(page=page_no),
                    referer=SEARCH_REFERER,
                    rate_key="search",
                )
            page = parse_search_response(data)
            if page.current_page != page_no:
                raise ContractError(
                    f"PAGE_NUMBER_MISMATCH bucket={bucket_id} page={page_no} "
                    f"actual_page={page.current_page}"
                )
            if page.items_per_page != ITEMS_PER_PAGE:
                raise ContractError(
                    f"PAGE_SIZE_CONTRACT_MISMATCH bucket={bucket_id} page={page_no} "
                    f"expected_page_size={ITEMS_PER_PAGE} actual_page_size={page.items_per_page}"
                )
            if not page.rows and page.total_items > 0:
                raise ContractError(
                    f"UNEXPECTED_EMPTY_PAGE bucket={bucket_id} page={page_no} "
                    f"total_items={page.total_items}"
                )
            if page.total_pages != pages_total or (
                expected_items is not None and page.total_items != expected_items
            ):
                raise ContractError(
                    f"PAGINATION_MISMATCH bucket={bucket_id} page={page_no} "
                    f"expected_items={expected_items} actual_items={page.total_items} "
                    f"expected_pages={pages_total} actual_pages={page.total_pages}"
                )
            # A page that claims the same total but has fewer identities is
            # not a complete page. Check the documented fixed page size.
            if expected_items is not None:
                expected_rows = min(
                    ITEMS_PER_PAGE,
                    max(0, int(expected_items) - (page_no - 1) * ITEMS_PER_PAGE),
                )
                if len(page.rows) != expected_rows:
                    raise ContractError(
                        f"PAGE_SIZE_MISMATCH bucket={bucket_id} page={page_no} "
                        f"expected_rows={expected_rows} actual_rows={len(page.rows)}"
                    )
            if filters.numeric_range() is not None:
                from ..contracts.common import money

                for dimension, raw_key in NUMERIC_DIMENSIONS:
                    minimum = getattr(filters, dimension + "_min")
                    maximum = getattr(filters, dimension + "_max")
                    if minimum is None and maximum is None:
                        continue
                    for raw in page.rows:
                        amount = money(raw.get(raw_key))
                        if amount is None or (minimum is not None and amount < minimum) or (
                            maximum is not None and amount > maximum
                        ):
                            raise ContractError(
                                f"NUMERIC_BOUNDARY_MISMATCH bucket={bucket_id} "
                                f"page={page_no} dimension={dimension}"
                            )
            profiles, rejections = _normalize_rows(page.rows)
            canonical_payload = json.dumps(data, ensure_ascii=False, default=str)
            result = {
                "bucket_id": bucket_id,
                "page_no": page_no,
                "filters": filters,
                "data": data,
                "canonical_payload": canonical_payload,
                "payload_sha256": sha256(canonical_payload.encode()).hexdigest(),
                "requested": requested,
                "archive_id": archive_id,
                "profiles": profiles,
                "projection_profiles": [],
                "projection_fingerprints": [],
                "rejections": rejections,
            }
            if lease_owner is not None:
                result["lease_owner"] = lease_owner
                result["lease_version"] = lease_version
            return result
        except DiscoveryLeaseLost:
            raise
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:700]}"
            # Even one exhausted request failure should pause this bucket;
            # the shared client has already applied its bounded HTTP retries.
            # Other buckets continue, without a storm of failed page records.
            if not stop_pages.is_set():
                bucket_error = detail
            stop_pages.set()
            if lease_owner is None:
                await asyncio.to_thread(fail_page, bucket_id, page_no, detail)
            else:
                await asyncio.to_thread(
                    fail_page,
                    bucket_id,
                    page_no,
                    detail,
                    lease_owner=lease_owner,
                    lease_version=lease_version,
                )
            return None

    async def page_worker():
        while not stop_pages.is_set():
            try:
                page_no = page_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                if stop_pages.is_set():
                    return
                result = await fetch_one(page_no)
                if result is not None:
                    await result_queue.put(result)
            finally:
                page_queue.task_done()

    async def fetch_all():
        worker_count = min(client.concurrency, max(1, len(pages)))
        try:
            async with asyncio.TaskGroup() as group:
                for _ in range(worker_count):
                    group.create_task(page_worker())
        finally:
            workers_done.set()

    async def writer():
        nonlocal fetched, written
        while not workers_done.is_set() or not result_queue.empty():
            try:
                first = await asyncio.wait_for(result_queue.get(), 0.05)
            except TimeoutError:
                continue
            batch = [first]
            while len(batch) < controller.size:
                try:
                    batch.append(result_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if stop_pages.is_set():
                # Do not commit buffered results once this bucket's current
                # pagination snapshot has been rejected; committed work stays.
                for _ in batch:
                    result_queue.task_done()
                continue
            staged_cache = _stage_projection_cache(batch, cache)
            started = time.monotonic()
            try:
                outcomes = await asyncio.to_thread(persist_page_batch, batch)
            finally:
                for _ in batch:
                    result_queue.task_done()
            cache.update(staged_cache)
            elapsed = time.monotonic() - started
            fetched += len(outcomes)
            written += sum(int(outcome["written"]) for outcome in outcomes)
            controller.observe(result_queue.qsize(), result_queue.maxsize, elapsed)
            state = await asyncio.to_thread(page_progress, bucket_id)
            owned_update(
                pages_done=state["done"],
                rows_seen=state["rows"] + state["rejected"],
            )
            if progress_line is not None:
                progress_line.update(
                    state["done"],
                    pages_total,
                    detail=(
                        f"bucket {bucket_id} | rows {state['rows']:,} | "
                        f"partial {state['partial']} | batch {controller.size} | q {result_queue.qsize()}"
                    ),
                )

    async with asyncio.TaskGroup() as group:
        group.create_task(fetch_all())
        group.create_task(writer())

    state = await asyncio.to_thread(page_progress, bucket_id)
    complete = state["done"] == pages_total and state["failed"] == 0 and state["partial"] == 0
    if complete:
        error = None
    elif bucket_error is not None:
        error = bucket_error[:700]
    elif state["partial"]:
        error = "One or more pages contain quarantined rows; investigation required"
    else:
        error = "One or more pages failed; safe checkpoint resume required"
    owned_update(
        status="done" if complete else "failed",
        pages_done=state["done"],
        rows_seen=state["rows"] + state["rejected"],
        last_error=error,
    )
    if local_progress is not None:
        local_progress.close()
    return fetched, written


async def _process_bucket(
    client: AsyncDbdClient,
    row: dict,
    provinces: list[str],
    *,
    master_cache: dict[tuple[str, str], str],
    progress_line: ProgressLine | None,
    lease_owner: str | None = None,
    lease_version: int | None = None,
    status_codes: list[str] | None = None,
    numeric_allowed: bool | None = None,
) -> tuple[int, int]:
    from ..storage.mysql.repositories.discovery_repository import (
        completed_page_evidence,
        completed_pages,
        mark_search_archive,
    )

    bucket_id = int(row["id"])
    filters = _filters_from_row(row)
    total_items = row["total_items"]
    pages_total = row["pages_total"]
    first_page = None
    count_archive_id = None
    counted_parent_rows: list[dict] | None = None

    def owned_update(**fields) -> None:
        if lease_owner is None:
            update_bucket(bucket_id, **fields)
        else:
            update_bucket(
                bucket_id,
                lease_owner=lease_owner,
                lease_version=lease_version,
                **fields,
            )

    # Oversized cached counts must also be refreshed: retry() preserves them.
    if total_items is None or int(total_items) > MAX_ROWS_PER_QUERY:
        page, count_archive_id, data, requested = await _count(client, bucket_id, filters)
        if page.current_page != 1:
            raise ContractError("Invalid Discovery count page")
        total_items, pages_total = page.total_items, page.total_pages
        # Use the current population for an untouched oversized leaf.
        # A preexisting page (done, partial or failed) fences its historical
        # count to prevent mixing two result snapshots during resume.
        recorded_count = row.get("total_items")
        if recorded_count is not None and total_items != int(recorded_count):
            # A transient empty result is not enough to conclude an already
            # observed non-empty cohort disappeared. Leave it unresolved.
            if int(recorded_count) > 0 and total_items == 0:
                await asyncio.to_thread(mark_search_archive, count_archive_id, "processed")
                raise ContractError(
                    f"Discovery refused unexpected empty live population for bucket {bucket_id}"
                )
            has_pages = await asyncio.to_thread(bucket_has_pages, bucket_id)
            if int(row.get("pages_done") or 0) or has_pages:
                await asyncio.to_thread(mark_search_archive, count_archive_id, "processed")
                raise ContractError(
                    f"Discovery count drift with checkpoints for bucket {bucket_id}: "
                    f"stored={recorded_count}, live={total_items}"
                )
            if progress_line is not None:
                progress_line.event(
                    f"bucket {bucket_id}: updated untouched search count "
                    f"{recorded_count:,} -> {total_items:,}; historical archive retained"
                )
        elif recorded_count is not None and int(row.get("pages_done") or 0):
            if int(recorded_count) != total_items:
                raise ContractError("Search total changed after pages were committed")
        first_page = (count_archive_id, data, requested)
        counted_parent_rows = page.rows
        owned_update(total_items=total_items, pages_total=pages_total, status="counted")

    if (
        row.get("status") == "failed"
        and total_items is not None
        and 0 < int(total_items) <= MAX_ROWS_PER_QUERY
        and pages_total is not None
        and 0 < int(pages_total) <= 1000
    ):
        # Resume existing failed/missing pages only after checking the current
        # API population AND the identity order of the archived first page.
        # Never mix committed pages from an incompatible live snapshot.
        live, resume_archive, resume_data, resume_requested = await _count(
            client, bucket_id, filters
        )
        await asyncio.to_thread(mark_search_archive, resume_archive, "processed")
        if (
            live.current_page != 1 or live.total_items != int(total_items)
            or live.total_pages != int(pages_total)
        ):
            raise ContractError(
                f"RESUME_SNAPSHOT_MISMATCH bucket={bucket_id} "
                f"expected_items={total_items} actual_items={live.total_items} "
                f"expected_pages={pages_total} actual_pages={live.total_pages}"
            )
        saved_done = await asyncio.to_thread(completed_pages, bucket_id)
        if saved_done:
            evidence = await asyncio.to_thread(completed_page_evidence, bucket_id)
            if evidence is None or int(evidence["page_no"]) not in saved_done:
                raise ContractError(
                    f"RESUME_MISSING_DONE_PAGE_ARCHIVE bucket={bucket_id}"
                )
            evidence_page = int(evidence["page_no"])
            if evidence_page == 1:
                live_evidence = live
            else:
                # Only one extra read when the earliest DONE page is not page 1.
                requested_evidence = _now()
                response = await client.post(
                    SEARCH_PATH, filters.payload(page=evidence_page),
                    referer=SEARCH_REFERER, rate_key="search",
                )
                from ..storage.mysql.repositories.discovery_repository import archive_search

                evidence_archive_id = await asyncio.to_thread(
                    archive_search, bucket_id, evidence_page, filters,
                    response, requested_evidence, link_page=False,
                )
                try:
                    live_evidence = parse_search_response(response)
                except Exception:
                    await asyncio.to_thread(
                        mark_search_archive, evidence_archive_id, "invalid",
                        "checkpoint_resume_evidence_invalid",
                    )
                    raise
                await asyncio.to_thread(
                    mark_search_archive, evidence_archive_id, "processed"
                )
            identities = [
                (str(item.get("jpTypeCode")), str(item.get("jpNo")))
                for item in live_evidence.rows
            ]
            if (
                live_evidence.current_page != evidence_page
                or identities != evidence["identities"]
                or live_evidence.total_items != evidence["total_items"]
                or live_evidence.total_pages != evidence["total_pages"]
            ):
                raise ContractError(
                    f"RESUME_CHECKPOINT_CHANGED bucket={bucket_id} "
                    f"page={evidence_page}: archived identities or "
                    "pagination no longer match"
                )
        else:
            # Reuse this validated count page if page one needs collection.
            first_page = (resume_archive, resume_data, resume_requested)

    if total_items == 0:
        if count_archive_id is not None:
            await asyncio.to_thread(mark_search_archive, count_archive_id, "processed")
        owned_update(status="done")
        return 0, 0

    if total_items > MAX_ROWS_PER_QUERY:
        if count_archive_id is not None:
            await asyncio.to_thread(mark_search_archive, count_archive_id, "processed")
        rejected_plans: list[str] = []
        validation_options = {"rejection_reasons": rejected_plans}
        if counted_parent_rows is not None:
            validation_options["parent_rows"] = counted_parent_rows
        if numeric_allowed is not None:
            validation_options["numeric_allowed"] = numeric_allowed
        children = await _validated_children(
            client, bucket_id, filters, provinces, status_codes or [], int(total_items),
            **validation_options,
        )
        if not children:
            reason = "; ".join(rejected_plans) if rejected_plans else "no_usable_partition"
            owned_update(
                status="failed",
                last_error=(
                    f"{total_items} rows exceeds the {MAX_ROWS_PER_QUERY} ceiling; "
                    f"no count-validated, non-overlapping split is available: {reason}"
                ),
            )
            return 0, 0
        await asyncio.to_thread(
            split_bucket,
            bucket_id,
            children,
            int(row["depth"]) + 1,
            lease_owner=lease_owner,
            lease_version=lease_version,
        )
        if progress_line is not None:
            progress_line.event(
                f"bucket {bucket_id}: {total_items:,} rows -> split into {len(children)} child buckets"
            )
        return 0, 0

    return await _drain_bucket(
        client,
        bucket_id,
        filters,
        pages_total,
        1,
        verbose=False,
        first_page=first_page,
        master_cache=master_cache,
        progress_line=progress_line,
        lease_owner=lease_owner,
        lease_version=lease_version,
        expected_items=int(total_items),
    )


def validate_splits(verbose: bool = True) -> int:
    """Reconcile from the leaves upward, never count unfinished descendants as complete."""
    from ..storage.mysql.engine import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id,parent_id,depth,status,total_items,pages_total,pages_done,
                   partition_lossless,bucket_key
            FROM discovery_buckets
            ORDER BY depth DESC,id
            """
        )
        rows = cur.fetchall()

    children_by_parent: dict[int, list[dict]] = {}
    for row in rows:
        if row["parent_id"] is not None:
            children_by_parent.setdefault(int(row["parent_id"]), []).append(row)

    verified: dict[int, bool] = {}
    checked = 0
    for parent in rows:
        if parent["status"] != "split":
            continue
        parent_id = int(parent["id"])
        children = children_by_parent.get(parent_id, [])
        countable = bool(children) and parent["total_items"] is not None and all(
            child["total_items"] is not None for child in children
        )
        count_equal = countable and sum(int(child["total_items"]) for child in children) == int(
            parent["total_items"]
        )
        def child_ready(child: dict) -> bool:
            if child["status"] == "split":
                return verified.get(int(child["id"]), False)
            if child["status"] != "done" or child["total_items"] is None:
                return False
            count = int(child["total_items"])
            done = int(child["pages_done"] or 0)
            total = int(child["pages_total"] or 0)
            return (count == 0 and total == 0 and done == 0) or (
                count > 0 and 0 < total <= 1000 and done == total
            )

        # Numeric children may match the parent count while silently omitting
        # NULL/fractional identities. Until the backend semantics and exact
        # interval structure are certified, NEVER mark their ancestors lossless.
        parent_key = str(parent.get("bucket_key") or "")
        parent_numeric = {
            part for part in parent_key.split("|")
            if part.startswith(("cap=", "asset=", "income=", "profit="))
        }
        numeric_children = any(
            {
                part for part in str(child.get("bucket_key") or "").split("|")
                if part.startswith(("cap=", "asset=", "income=", "profit="))
            } != parent_numeric for child in children
        )
        numeric_safe = (
            not numeric_children
            or numeric_partition_structure(
                parent_key,
                [str(child.get("bucket_key") or "") for child in children],
                certified=PRODUCTION_CERTIFIED_DIMENSIONS,
            )
        )
        lossless = bool(count_equal) and numeric_safe and all(
            child_ready(c) for c in children
        )
        verified[parent_id] = lossless
        if parent["partition_lossless"] is None or int(parent["partition_lossless"]) != int(lossless):
            update_bucket(parent_id, partition_lossless=int(lossless))
        checked += 1
        if verbose and not lossless:
            from ..presentation import TerminalPresenter
            TerminalPresenter().message(
                f"Bucket {parent_id}: counts or descendant pages are not complete; "
                "completeness blocked",
                kind="error",
            )
    return checked


async def _probe_partition(
    client: AsyncDbdClient,
    bucket_id: int,
    filters: SearchFilters,
    parent_count: int,
    candidates: list[SearchFilters],
    rejection_reasons: list[str] | None = None,
    observations: list[NumericObservation] | None = None,
) -> bool:
    """Validate count/sample constraints without duplicating numeric count probes."""
    from ..storage.mysql.repositories.discovery_repository import mark_search_archive

    def reject(reason: str) -> bool:
        if rejection_reasons is not None:
            rejection_reasons.append(reason)
        return False

    keys = [child.key() for child in candidates]
    if not keys or len(set(keys)) != len(keys) or filters.key() in keys:
        return reject("duplicate_or_unchanged_child_key")

    if observations is not None and len(observations) != len(candidates):
        return reject("numeric_observation_count_mismatch")
    child_counts: list[int] = []
    sampled_identities: set[tuple[str, str]] = set()
    for index, child in enumerate(candidates):
        if observations is None:
            page, archive_id, _, _ = await _count(client, bucket_id, child)
            await asyncio.to_thread(mark_search_archive, archive_id, "processed")
        else:
            page = observations[index]
        if page.current_page != 1 or (page.total_items > 0 and not page.rows):
            raise ContractError("Invalid Discovery split count response")

        # Samples cannot certify the entire partition, but a contradiction
        # must disqualify it immediately. Preserve all parent constraints.
        for raw in page.rows:
            for field, key in (
                ("jp_types", "jpTypeCode"),
                ("pv_codes", "pvCode"),
                ("jp_statuses", "jpStatCode"),
                ("business_sizes", "businessSizeCode"),
            ):
                selected = getattr(child, field)
                if selected and (raw.get(key) is None or str(raw[key]) not in selected):
                    return reject(f"sample_filter_mismatch:{field}")
            jp_type, jp_no = raw.get("jpTypeCode"), raw.get("jpNo")
            if jp_type is not None and jp_no is not None:
                identity = (str(jp_type), str(jp_no))
                if identity in sampled_identities:
                    return reject("sample_identity_overlap")
                sampled_identities.add(identity)

        if child.numeric_range() is not None:
            from ..contracts.common import money

            for dimension, raw_key in (
                ("cap_amt", "capAmt"),
                ("total_asset", "totalAsset"),
                ("total_income", "totalIncome"),
                ("net_profit", "netProfit"),
            ):
                minimum = getattr(child, dimension + "_min")
                maximum = getattr(child, dimension + "_max")
                if minimum is None and maximum is None:
                    continue
                for raw in page.rows:
                    amount = money(raw.get(raw_key))
                    if amount is None:
                        return reject(f"sample_{dimension}_unavailable")
                    if minimum is not None and amount < minimum:
                        return reject(f"sample_{dimension}_below_min")
                    if maximum is not None and amount > maximum:
                        return reject(f"sample_{dimension}_above_max")
        child_counts.append(page.total_items)

    child_total = sum(child_counts)
    if child_total != parent_count:
        return reject(f"count_mismatch:parent={parent_count},children={child_total}")
    if max(child_counts, default=0) >= parent_count:
        return reject("no_progress:child_not_smaller_than_parent")

    # One more parent count, after all children, guards against API drift.
    latest, archive_id, _, _ = await _count(client, bucket_id, filters)
    await asyncio.to_thread(mark_search_archive, archive_id, "processed")
    if latest.current_page != 1 or latest.total_items != parent_count:
        return reject(
            f"parent_count_drift:before={parent_count},after={latest.total_items}"
        )
    return True


async def _validated_children(
    client: AsyncDbdClient,
    bucket_id: int,
    filters: SearchFilters,
    provinces: list[str],
    statuses: list[str],
    parent_count: int,
    *,
    rejection_reasons: list[str] | None = None,
    numeric_allowed: bool | None = None,
    parent_rows: list[dict] | None = None,
) -> list[SearchFilters]:
    """Try categories first, then bounded live-validated numeric candidates.

    A candidate must have disjoint structural bounds, sample membership and
    matching stable API counts. Unresolved ranges remain failed; this does not
    claim global NULL/fractional coverage from count parity alone.
    """
    plans = _partition_plans(filters, provinces, statuses)
    if not plans and rejection_reasons is not None:
        rejection_reasons.append("categorical_dimensions_exhausted_or_unavailable")
    for options in plans:
        dimension = next(
            field for field in ("jp_types", "pv_codes", "jp_statuses", "business_sizes")
            if getattr(options[0], field) and not getattr(filters, field)
        )
        if rejection_reasons is None:
            accepted = await _probe_partition(
                client, bucket_id, filters, parent_count, options
            )
        else:
            failures: list[str] = []
            accepted = await _probe_partition(
                client, bucket_id, filters, parent_count, options, failures
            )
            if not accepted:
                reason = ",".join(failures) if failures else "unverified_split"
                rejection_reasons.append(f"{dimension}:{reason}")
        if accepted:
            return options

    if numeric_allowed is None:
        # Explicit recovery-only opt-in; never activate Numeric automatically
        # through normal 'dbd-data discover' when certification changes.
        numeric_allowed = False
    if not numeric_allowed:
        return []

    from ..storage.mysql.repositories.discovery_repository import numeric_schema_ready

    if not await asyncio.to_thread(numeric_schema_ready):
        if rejection_reasons is not None:
            rejection_reasons.append("numeric_schema_missing_migration_0013")
        return []

    # Numeric is a continuation of normal Discovery work, never a separate
    # operator probe. Use the parent count page already archived by _count().
    from ..contracts.common import money
    from ..storage.mysql.repositories.discovery_repository import mark_search_archive

    if parent_rows is None:
        current, archive_id, _, _ = await _count(client, bucket_id, filters)
        await asyncio.to_thread(mark_search_archive, archive_id, "processed")
    else:
        # _process_bucket already retrieved the parent count and first page.
        current = NumericObservation(parent_count, parent_rows)
    if current.current_page != 1 or current.total_items != parent_count:
        if rejection_reasons is not None:
            rejection_reasons.append("numeric_parent_count_changed_during_split")
        return []

    async def read_numeric_count(candidate: SearchFilters) -> NumericObservation:
        page, item_archive, _, _ = await _count(client, bucket_id, candidate)
        await asyncio.to_thread(mark_search_archive, item_archive, "processed")
        if page.current_page != 1 or (page.total_items and not page.rows):
            raise ContractError("Numeric count response violates page contract")
        return NumericObservation(page.total_items, page.rows)

    # A rejected numeric candidate must not stop the entire bucket: try the
    # next dimension. Never loosen any of the existing parent constraints.
    remaining = [name for name, _ in NUMERIC_DIMENSIONS]
    request_budget = 12
    while remaining and request_budget > 0:
        plan = await plan_numeric_split(
            filters, parent_count, read_numeric_count, current.rows,
            dimensions=tuple(remaining), max_total_trials=request_budget,
        )
        request_budget -= len(plan.attempts)
        if not plan.candidate:
            if rejection_reasons is not None:
                rejection_reasons.append("numeric:" + plan.reason)
            break

        dimension = plan.dimension
        raw_key = dict(NUMERIC_DIMENSIONS)[dimension]
        structurally_sound = numeric_partition_structure(
            filters.key(), [child.key() for child in plan.children],
            certified=frozenset({dimension}),
        )
        # A NULL or out-of-range observed parent row is evidence of a gap.
        parent_samples_covered = structurally_sound
        if parent_samples_covered:
            for raw in current.rows:
                amount = money(raw.get(raw_key))
                if amount is None:
                    parent_samples_covered = False
                    break
                matches = sum(
                    (getattr(child, dimension + "_min") is None
                     or amount >= getattr(child, dimension + "_min"))
                    and (getattr(child, dimension + "_max") is None
                         or amount <= getattr(child, dimension + "_max"))
                    for child in plan.children
                )
                if matches != 1:
                    parent_samples_covered = False
                    break

        failures: list[str] = []
        if parent_samples_covered and await _probe_partition(
            client, bucket_id, filters, parent_count, plan.children, failures,
            observations=plan.child_observations,
        ):
            return plan.children

        if rejection_reasons is not None:
            reason = ",".join(failures) if failures else "unverified_numeric_interval_or_sample"
            rejection_reasons.append(f"{dimension}:{reason}")
        remaining.remove(dimension)

    # Some live API dimensions cannot cover NULL or fractional values. Leave
    # those leaves failed/unresolved and continue processing other buckets.
    return []


async def _probe_capital_boundary(
    row: dict, *, cap_max: str, cap_next_min: str, rate: float, health=None,
) -> dict:
    """Preview a candidate two-way capital split; never write or certify it.

    Numeric bounds have not yet been proven to cover NULLs or every fractional
    value in DBD. Count parity and first-page samples are diagnostics ONLY.
    """
    from ..contracts.common import money

    parent = _filters_from_row(row)
    left_bound = money(cap_max)
    right_bound = money(cap_next_min)
    if left_bound is None or right_bound is None or left_bound >= right_bound:
        raise ValueError("Split probe requires cap_max < cap_next_min")
    if parent.cap_amt_min is not None and left_bound < parent.cap_amt_min:
        raise ValueError("Lower child lies outside persisted parent capital range")
    if parent.cap_amt_max is not None and right_bound > parent.cap_amt_max:
        raise ValueError("Upper child lies outside persisted parent capital range")
    left = replace(parent, cap_amt_max=left_bound)
    right = replace(parent, cap_amt_min=right_bound)
    candidates = (
        ("parent_before", parent),
        ("left", left),
        ("right", right),
        ("parent_after", parent),
    )
    counts: dict[str, int] = {}
    samples: dict[str, set[tuple[str, str]]] = {}
    invalid_capital = 0
    async with AsyncDbdClient(
        concurrency=1,
        rate_per_second=rate,
        endpoint_rates={"search": rate},
    ) as client:
        for label, filters in candidates:
            if health is not None:
                await asyncio.to_thread(health)
            data = await client.post(
                SEARCH_PATH,
                filters.payload(page=1),
                referer=SEARCH_REFERER,
                timeout=settings.dbd_count_timeout_seconds,
                rate_key="search",
            )
            page = parse_search_response(data)
            if page.current_page != 1 or (page.total_items and not page.rows):
                raise ContractError(f"Invalid capital range probe response: {label}")
            counts[label] = page.total_items
            identities: set[tuple[str, str]] = set()
            for raw in page.rows:
                jp_type, jp_no = raw.get("jpTypeCode"), raw.get("jpNo")
                if not jp_type or not jp_no:
                    raise ContractError(f"Missing identity in capital probe: {label}")
                identity = (str(jp_type), str(jp_no))
                if identity in identities:
                    raise ContractError(f"Duplicate sampled identity in capital probe: {label}")
                identities.add(identity)
                if label in ("left", "right"):
                    capital = money(raw.get("capAmt"))
                    if capital is None or (
                        label == "left" and capital > left_bound
                    ) or (
                        label == "right" and capital < right_bound
                    ):
                        invalid_capital += 1
            samples[label] = identities

    stable = counts["parent_before"] == counts["parent_after"]
    parity = counts["left"] + counts["right"] == counts["parent_before"]
    sampled_overlap = len(samples["left"] & samples["right"])
    return {
        "mode": "numeric_plan_preview",
        "bucket_id": int(row["id"]),
        "stored_count": row.get("total_items"),
        "parent_before": counts["parent_before"],
        "parent_after": counts["parent_after"],
        "cap_max": str(left_bound),
        "cap_next_min": str(right_bound),
        "left_count": counts["left"],
        "right_count": counts["right"],
        "count_parity": parity,
        "count_stable": stable,
        "stored_count_drift": (
            row.get("total_items") is not None
            and int(row["total_items"]) != counts["parent_before"]
        ),
        "sample_overlap": sampled_overlap,
        "invalid_sample_capital": invalid_capital,
        "request_count": len(candidates),
        "decision": "OBSERVATION_ONLY",
        "reason": (
            "Count-only evidence cannot establish NULL coverage, fractional "
            "boundary completeness, or historical count reconciliation; "
            "no numeric children created"
        ),
    }


async def _probe_keyword_cohort(
    keyword: str, *, rate: float, health=None,
) -> dict:
    """Read every identity in a small Search cohort without persistent writes.

    These observations can reject an unsafe range plan, but never certify
    numeric partitions for production or the whole DBD population.
    """
    from decimal import Decimal

    from ..contracts.common import money

    max_rows = 100
    max_pages = 10  # Backend returns 10 rows/page.
    variants = (
        ("parent_before", {}),
        ("max_zero", {"capAmtMax": 0}),
        ("min_zero", {"capAmtMin": 0}),
        ("min_one", {"capAmtMin": 1}),
        ("both_zero", {"capAmtMin": 0, "capAmtMax": 0}),
        ("parent_after", {}),
    )
    base = SearchFilters(keyword=keyword).payload(page=1)
    observations: dict[str, dict[tuple[str, str], dict]] = {}
    counts: dict[str, int] = {}
    request_count = 0

    async with AsyncDbdClient(
        concurrency=1,
        rate_per_second=rate,
        endpoint_rates={"search": rate},
    ) as client:
        for label, numeric in variants:
            rows: dict[tuple[str, str], dict] = {}
            expected_count = expected_pages = None
            for page_no in range(1, max_pages + 1):
                if health is not None:
                    await asyncio.to_thread(health)
                data = await client.post(
                    SEARCH_PATH,
                    {**base, **numeric, "currentPage": page_no},
                    referer=SEARCH_REFERER,
                    timeout=settings.dbd_count_timeout_seconds,
                    rate_key="search",
                )
                request_count += 1
                page = parse_search_response(data)
                if page.current_page != page_no:
                    raise ContractError(f"Numeric probe {label}: wrong currentPage")
                if page.total_items > max_rows or page.total_pages > max_pages:
                    raise ContractError(
                        f"Numeric probe {label} exceeds the {max_rows}-row safety limit"
                    )
                if expected_count is None:
                    expected_count, expected_pages = page.total_items, page.total_pages
                elif (page.total_items, page.total_pages) != (expected_count, expected_pages):
                    raise ContractError(f"Numeric probe {label}: page count drift")
                for raw in page.rows:
                    jp_type, jp_no = raw.get("jpTypeCode"), raw.get("jpNo")
                    if not isinstance(jp_type, str) or not isinstance(jp_no, str) or not jp_type or not jp_no:
                        raise ContractError(f"Numeric probe {label}: missing company identity")
                    identity = (jp_type, jp_no)
                    if identity in rows:
                        raise ContractError(f"Numeric probe {label}: duplicate company identity")
                    rows[identity] = raw
                if page_no >= expected_pages:
                    break
            if len(rows) != expected_count:
                raise ContractError(
                    f"Numeric probe {label}: expected {expected_count} identities, got {len(rows)}"
                )
            observations[label] = rows
            counts[label] = expected_count

    parent = set(observations["parent_before"])
    after = set(observations["parent_after"])
    max_zero = set(observations["max_zero"])
    min_zero = set(observations["min_zero"])
    min_one = set(observations["min_one"])
    both_zero = set(observations["both_zero"])
    combined = max_zero | min_one
    missing = parent - combined
    extra = combined - parent
    overlap = max_zero & min_one
    stable = counts["parent_before"] == counts["parent_after"] and parent == after

    capital = {identity: money(raw.get("capAmt")) for identity, raw in observations["parent_before"].items()}
    null_ids = {identity for identity, amount in capital.items() if amount is None}
    fractional_ids = {
        identity for identity, amount in capital.items()
        if amount is not None and Decimal(0) < amount < Decimal(1)
    }
    zero_ids = {identity for identity, amount in capital.items() if amount == 0}
    invalid_amounts = 0
    for label, predicate in (
        ("max_zero", lambda amount: amount <= 0),
        ("min_one", lambda amount: amount >= 1),
    ):
        for raw in observations[label].values():
            amount = money(raw.get("capAmt"))
            if amount is None or not predicate(amount):
                invalid_amounts += 1

    cohort_pass = (
        stable and not missing and not extra and not overlap
        and invalid_amounts == 0 and bool(parent)
    )
    if not null_ids:
        null_coverage = "NOT_TESTED_NO_NULL_IN_PARENT"
    elif null_ids <= combined:
        null_coverage = "OBSERVED_COVERED_ONLY"
    else:
        null_coverage = "OBSERVED_MISSING"
    if not zero_ids:
        zero_boundary = "NOT_TESTED_NO_ZERO_IN_PARENT"
    elif zero_ids <= (max_zero & min_zero & both_zero):
        zero_boundary = "ZERO_PRESENT_IN_ALL_TESTED_ZERO_FILTERS"
    else:
        zero_boundary = "ZERO_FILTERS_DIFFER"

    return {
        "mode": "probe",
        "keyword": keyword,
        "filter": "capAmt",
        "request_count": request_count,
        "parent_count": counts["parent_before"],
        "parent_after": counts["parent_after"],
        "counts": counts,
        "count_stable": stable,
        "null_parent_rows": len(null_ids),
        "fractional_0_to_1_rows": len(fractional_ids),
        "null_coverage": null_coverage,
        "zero_boundary": zero_boundary,
        "min_zero_matches_parent": min_zero == parent,
        "zero_filter_overlap": len(max_zero & min_zero),
        "child_overlap": len(overlap),
        "child_missing": len(missing),
        "child_extra": len(extra),
        "invalid_child_capital": invalid_amounts,
        "cohort_check": "PASS" if cohort_pass else "FAIL",
        "decision": "OBSERVATION_ONLY",
        "reason": (
            "Small-cohort identity checks passed; numeric splitting is NOT approved globally"
            if cohort_pass else
            "Small-cohort identity or capital checks failed; numeric splitting is NOT approved"
        ),
    }


async def probe_discovery_partition(
    *,
    parent_id: int | None = None,
    keyword: str | None = None,
    cap_max: str | None = None,
    cap_next_min: str | None = None,
    rate: float = 0.25,
    health=None,
) -> dict:
    """Observe one numeric filter boundary without altering persisted discovery.

    The probe deliberately cannot approve range splits: numeric boundary
    inclusivity and NULL/unfiltered coverage have not been proven. No archive,
    bucket, page, or checkpoint writes occur here.
    """
    from ..storage.mysql.repositories.discovery_repository import get_bucket

    if rate <= 0:
        raise ValueError("Probe rate must be positive")
    if (parent_id is None) == (keyword is None):
        raise ValueError("Probe requires exactly one of parent_id or keyword")
    if (cap_max is None) != (cap_next_min is None):
        raise ValueError("Capital boundary probe needs both cap_max and cap_next_min")
    if keyword is not None and cap_max is not None:
        raise ValueError("Capital boundary probe requires parent_id, not keyword")
    if keyword is not None:
        keyword = keyword.strip()
        if len(keyword) < 3:
            raise ValueError("Probe keyword must have at least 3 characters")
        return await _probe_keyword_cohort(keyword, rate=rate, health=health)
    row = await asyncio.to_thread(get_bucket, parent_id)
    if row is None:
        raise ValueError(f"Unknown discovery bucket {parent_id}")
    if row["status"] != "failed" or int(row.get("total_items") or 0) <= MAX_ROWS_PER_QUERY:
        raise ValueError("Probe requires a failed bucket exceeding 10000 rows")
    if cap_max is not None:
        return await _probe_capital_boundary(
            row, cap_max=cap_max, cap_next_min=cap_next_min, rate=rate, health=health
        )

    base = _filters_from_row(row).payload(page=1)
    # These intentionally overlap at zero. They test backend filter behavior,
    # not a certified non-overlapping plan.
    variants = (
        ("parent_before", {}),
        ("cap_amt_at_most_zero", {"capAmtMax": 0}),
        ("cap_amt_at_least_zero", {"capAmtMin": 0}),
        ("parent_after", {}),
    )
    observations: dict[str, int] = {}
    samples: dict[str, list[tuple[str, str]]] = {}
    async with AsyncDbdClient(
        concurrency=1,
        rate_per_second=rate,
        endpoint_rates={"search": rate},
    ) as client:
        for label, patch in variants:
            if health is not None:
                await asyncio.to_thread(health)
            data = await client.post(
                SEARCH_PATH,
                {**base, **patch},
                referer=SEARCH_REFERER,
                timeout=settings.dbd_count_timeout_seconds,
                rate_key="search",
            )
            page = parse_search_response(data)
            if page.current_page != 1 or (page.total_items and not page.rows):
                raise ContractError(f"Invalid probe response for {label}")
            observations[label] = page.total_items
            samples[label] = [
                (str(item["jpTypeCode"]), str(item["jpNo"]))
                for item in page.rows
                if item.get("jpTypeCode") is not None and item.get("jpNo") is not None
            ]

    stable = observations["parent_before"] == observations["parent_after"]
    overlap = len(set(samples["cap_amt_at_most_zero"]) & set(samples["cap_amt_at_least_zero"]))
    return {
        "mode": "probe",
        "bucket_id": parent_id,
        "filter": "capAmt",
        "request_count": len(variants),
        "parent_before": observations["parent_before"],
        "parent_after": observations["parent_after"],
        "at_most_zero": observations["cap_amt_at_most_zero"],
        "at_least_zero": observations["cap_amt_at_least_zero"],
        "count_stable": stable,
        "sample_overlap": overlap,
        "null_coverage": "UNKNOWN",
        "boundary_semantics": "UNKNOWN",
        "decision": "OBSERVATION_ONLY",
        "reason": (
            "Parent count drifted; numeric range split is not approved"
            if not stable
            else "Range boundary and NULL coverage are unverified; no split approved"
        ),
    }


async def preview_numeric_discovery_plan(
    parent_id: int, *, rate: float = 0.25, health=None,
) -> dict:
    """Read-only bounded numeric count preview. No archives or checkpoint writes."""
    from ..storage.mysql.repositories.discovery_repository import get_bucket

    if rate <= 0:
        raise ValueError("Numeric preview rate must be positive")
    row = await asyncio.to_thread(get_bucket, parent_id)
    if row is None or row["status"] != "failed" or int(row.get("total_items") or 0) <= MAX_ROWS_PER_QUERY:
        raise ValueError("Numeric preview requires an oversized failed leaf")
    filters = _filters_from_row(row)
    requests = 0
    async with AsyncDbdClient(
        concurrency=1, rate_per_second=rate, endpoint_rates={"search": rate},
    ) as client:

        async def count_one(query: SearchFilters) -> NumericObservation:
            nonlocal requests
            if health is not None:
                await asyncio.to_thread(health)
            data = await client.post(
                SEARCH_PATH, query.payload(page=1), referer=SEARCH_REFERER,
                timeout=settings.dbd_count_timeout_seconds, rate_key="search",
            )
            requests += 1
            page = parse_search_response(data)
            if page.current_page != 1 or (page.total_items and not page.rows):
                raise ContractError("Numeric preview received invalid search page")
            return NumericObservation(page.total_items, page.rows)

        before = await count_one(filters)
        stored_count = int(row["total_items"])
        if before.count != stored_count:
            return {
                "mode": "numeric_plan_preview", "bucket_id": parent_id,
                "stored_count": stored_count, "live_count": before.count,
                "request_count": requests, "decision": "BLOCKED",
                "reason": "historical_count_drift_no_rewrite",
            }

        plan = await plan_numeric_split(
            filters, before.count, count_one, before.rows,
        )
        after = await count_one(filters)

    stable = before.count == after.count
    return {
        "mode": "numeric_plan_preview",
        "bucket_id": parent_id,
        "stored_count": stored_count,
        "parent_before": before.count, "parent_after": after.count,
        "candidate_dimension": plan.dimension,
        "candidate_child_counts": plan.child_counts,
        "candidate_ranges": [
            child.numeric_range() for child in plan.children
        ],
        "attempts": plan.attempts,
        "request_count": requests,
        "count_stable": stable,
        "decision": "OBSERVATION_ONLY" if stable else "BLOCKED",
        "reason": (
            "Count-only plans do not certify NULL, decimal boundary or full identity coverage"
            if stable else "Parent count drifted during numeric preview"
        ),
        "planner_reason": plan.reason,
    }


async def recover_discovery(
    *,
    parent_id: int | None = None,
    limit: int = 10,
    execute: bool = False,
    rate: float = 1.0,
    health=None,
    numeric_split: bool = False,
    shared_client=None,
) -> dict:
    """Preview or selectively recover historical count failures.

    Preview is DB SELECT only. Execution shares the CLI's MySQL manager lock;
    never deletes pages and never resets already collected child pages.
    """
    from ..storage.mysql.repositories.discovery_repository import (
        apply_recovery_recount,
        get_bucket,
        mark_search_archive,
        recovery_candidates,
        recovery_family,
    )

    if numeric_split and (not execute or parent_id is None):
        raise ValueError("Numeric recovery execution requires --execute and --parent-id")

    candidates = await asyncio.to_thread(recovery_candidates, limit, parent_id)
    reason_counts = {
        reason: sum(item["reason"] == reason for item in candidates)
        for reason in sorted({item["reason"] for item in candidates})
    }
    # Previously failed oversized leaves have already exhausted the existing
    # partition planner. Retry them only on explicit --parent-id, not on bulk
    # recovery where they would repeat costly failed Search probes.
    actionable = {"stale_zero"}
    if parent_id is not None:
        actionable.add("overflow")
        if numeric_split:
            actionable.add("partition_exhausted")
    blocked = sum(item["reason"] not in actionable | {"ready_to_validate"} for item in candidates)
    report: dict = {
        "mode": "execute" if execute else "preview",
        "candidates": len(candidates),
        "stale_zero": reason_counts.get("stale_zero", 0),
        "overflow": reason_counts.get("overflow", 0),
        "blocked": blocked,
        "ready_to_validate": reason_counts.get("ready_to_validate", 0),
        "reason_counts": reason_counts,
        "bucket_ids": ",".join(str(item["bucket_id"]) for item in candidates),
    }
    if not execute or not candidates:
        return report
    if rate <= 0:
        raise ValueError("Recovery rate must be positive")
    # No actionable candidates: reconcile existing checkpoints, but do not
    # authenticate or issue even one Search request.
    if not any(item["reason"] in actionable for item in candidates):
        checked = await asyncio.to_thread(validate_splits, False)
        report.update({
            "reopened": 0,
            "split": 0,
            "recovered": 0,
            "partitions_checked": checked,
            "unresolved": blocked,
            "sample_errors": "",
        })
        return report

    reopened = split = recovered = 0
    failures: list[str] = []
    # When invoked inside normal Discovery, reuse the same admitted fixed-rate
    # client. Standalone recovery keeps its existing adaptive transport.
    recovery_context = (
        nullcontext(shared_client) if shared_client is not None
        else AsyncDbdClient(
            concurrency=1, rate_per_second=rate,
            endpoint_rates={"search": rate},
        )
    )
    async with recovery_context as client:
        statuses = await asyncio.to_thread(observed_status_codes)
        provinces = None
        for item in candidates:
            # Blocked children and exhausted categorical dimensions must not
            # trigger repeated expensive API recounts.
            if item["reason"] not in actionable:
                continue
            bucket_id = int(item["bucket_id"])
            if health is not None:
                await asyncio.to_thread(health)
            try:
                if item["reason"] == "stale_zero":
                    parent, children = await asyncio.to_thread(recovery_family, bucket_id)
                    if parent["status"] != "split":
                        raise ContractError("Recovery candidate no longer split")
                    filters = _filters_from_row(parent)
                    page, archive_id, _, _ = await _count(client, bucket_id, filters)
                    await asyncio.to_thread(mark_search_archive, archive_id, "processed")
                    if page.current_page != 1 or not page.rows:
                        raise ContractError("Parent recount returned an invalid page")
                    current_parent = page.total_items
                    observations: dict[int, int] = {}
                    for child in children:
                        child_filters = _filters_from_row(child)
                        cp, child_archive, _, _ = await _count(
                            client, int(child["id"]), child_filters
                        )
                        await asyncio.to_thread(mark_search_archive, child_archive, "processed")
                        if cp.current_page != 1 or (cp.total_items > 0 and not cp.rows):
                            raise ContractError("Recovery child count response is invalid")
                        # Verify the sample actually satisfies the child filter.
                        for raw in cp.rows:
                            for field, raw_key in (
                                ("jp_types", "jpTypeCode"),
                                ("pv_codes", "pvCode"),
                                ("jp_statuses", "jpStatCode"),
                                ("business_sizes", "businessSizeCode"),
                            ):
                                selected = getattr(child_filters, field)
                                if selected and raw_key in raw and str(raw[raw_key]) not in selected:
                                    raise ContractError("Recovery child filter returned mismatched rows")
                        observations[int(child["id"])] = cp.total_items
                    confirm, confirm_archive, _, _ = await _count(client, bucket_id, filters)
                    await asyncio.to_thread(mark_search_archive, confirm_archive, "processed")
                    if confirm.current_page != 1 or confirm.total_items != current_parent:
                        raise ContractError("Parent count drifted while recounting children")
                    if health is not None:
                        await asyncio.to_thread(health)
                    outcome = await asyncio.to_thread(
                        apply_recovery_recount, bucket_id, current_parent, observations
                    )
                    reopened += int(outcome["reopened"])
                    if not outcome["count_consistent"] or outcome["changed_nonzero"]:
                        failures.append(f"{bucket_id}: counts cannot yet be reconciled")
                    elif outcome["parent_updated"] or outcome["reopened"]:
                        recovered += 1
                else:
                    row = await asyncio.to_thread(get_bucket, bucket_id)
                    if row is None or row["status"] != "failed":
                        continue
                    if row.get("lease_owner") is not None and row.get("lease_expires_at") is not None:
                        if row["lease_expires_at"] > _now():
                            raise DiscoveryLeaseLost("Overflow bucket has an active lease")
                    if provinces is None:
                        provinces = await fetch_provinces(client)
                    if health is not None:
                        await asyncio.to_thread(health)
                    await _process_bucket(
                        client, row, provinces, master_cache={},
                        progress_line=None, status_codes=statuses,
                        numeric_allowed=numeric_split,
                    )
                    state = await asyncio.to_thread(get_bucket, bucket_id)
                    if state["status"] == "split":
                        split += 1
                    elif state["status"] == "done":
                        recovered += 1
                    else:
                        failures.append(
                            f"{bucket_id}: {(state.get('last_error') or 'no validated split available')[:250]}"
                        )
            except DiscoveryLeaseLost:
                raise
            except Exception as exc:
                failures.append(f"{bucket_id}: {type(exc).__name__}: {str(exc)[:120]}")
    # Never make the return receipt green based only on sampled count parity.
    checked = await asyncio.to_thread(validate_splits, False)
    report.update({
        "reopened": reopened,
        "split": split,
        "recovered": recovered,
        "partitions_checked": checked,
        "unresolved": blocked + len(failures),
        "sample_errors": "; ".join(failures[:5]),
    })
    return report


async def _run_work(
    client,
    seeds,
    search_type,
    max_buckets,
    *,
    distributed: bool,
    owner: str,
    progress_line: ProgressLine,
    numeric_split: bool = False,
    recovery_rate: float = 1.0,
) -> DiscoveryResult:
    buckets_processed = pages_fetched = companies_written = 0
    provinces = await fetch_provinces(client)
    master_cache: dict[tuple[str, str], str] = {}
    status_codes = await asyncio.to_thread(observed_status_codes)
    attempted_failed: set[int] = set()
    attempted_recovery_count = 0
    stale_zero_checked = False
    baseline = await asyncio.to_thread(discovery_summary)

    if not seeds:
        seeds = sorted(
            {f"0{str(province).zfill(2)}" for province in provinces if str(province).strip()}
            | set(EXTRA_PREFIX_SEEDS)
        )
    for seed in seeds:
        upsert_bucket(SearchFilters(keyword=seed, search_type=search_type))

    base_parallelism = max(1, min(4, client.concurrency // 4 or 1))
    while True:
        remaining = None if max_buckets is None else max_buckets - buckets_processed
        if remaining is not None and remaining <= 0:
            break
        desired = base_parallelism if remaining is None else min(base_parallelism, remaining)
        recovery_batch = False
        if distributed:
            batch = await asyncio.to_thread(
                claim_buckets, owner, desired, settings.dbd_lease_seconds
            )
            if not batch:
                batch = await asyncio.to_thread(
                    claim_buckets, owner, desired, settings.dbd_lease_seconds,
                    failed_only=True, exclude_ids=attempted_failed,
                )
                recovery_batch = bool(batch)
        else:
            batch = (await asyncio.to_thread(pending_buckets, 25))[:desired]
            if not batch:
                batch = await asyncio.to_thread(
                    recoverable_failed_buckets, desired, exclude_ids=attempted_failed,
                )
                recovery_batch = bool(batch)
        if not batch:
            # One bounded pass over existing false-zero children, using the
            # existing transactional recovery logic. No bulk failed reset and
            # no repeated recount loop; any reopened children go to the
            # standard pending queue. Distributed runs leave this reconciliation
            # to the single-manager invocation to avoid competing recounts.
            if not distributed and max_buckets is None and not stale_zero_checked:
                stale_zero_checked = True
                from ..storage.mysql.repositories.discovery_repository import recovery_candidates

                candidates = await asyncio.to_thread(recovery_candidates, 400)
                if any(item["reason"] == "stale_zero" for item in candidates):
                    report = await recover_discovery(
                        limit=400, execute=True, rate=recovery_rate,
                        shared_client=client,
                    )
                    progress_line.event(
                        "discovery: stale-zero reconciliation "
                        f"reopened={report.get('reopened', 0)} "
                        f"unresolved={report.get('unresolved', 0)}"
                    )
                    if int(report.get("reopened") or 0):
                        continue
            break
        if recovery_batch:
            # Each historical failed leaf is attempted at most once per run.
            # Its newly created children enter the original pending queue.
            attempted_failed.update(int(row["id"]) for row in batch)
            attempted_recovery_count += len(batch)
            progress_line.event(
                f"discovery: retrying {len(batch)} oversized failed leaf bucket(s)"
            )

        async def process_one(row):
            bucket_id = int(row["id"])
            lease_version = int(row["lease_version"]) if distributed else None
            try:
                return await _process_bucket(
                    client,
                    row,
                    provinces,
                    master_cache=master_cache,
                    progress_line=progress_line,
                    lease_owner=owner if distributed else None,
                    lease_version=lease_version,
                    status_codes=status_codes,
                    numeric_allowed=numeric_split,
                )
            except DiscoveryLeaseLost:
                progress_line.event(
                    f"bucket {bucket_id}: lease moved to another worker; stale result discarded"
                )
                return 0, 0
            except Exception as exc:
                try:
                    if distributed:
                        update_bucket(
                            bucket_id,
                            lease_owner=owner,
                            lease_version=lease_version,
                            status="failed",
                            last_error=f"{type(exc).__name__}: {str(exc)[:300]}",
                        )
                    else:
                        update_bucket(
                            bucket_id,
                            status="failed",
                            last_error=f"{type(exc).__name__}: {str(exc)[:300]}",
                        )
                except DiscoveryLeaseLost:
                    progress_line.event(
                        f"bucket {bucket_id}: lease changed while handling {type(exc).__name__}; not failed"
                    )
                    return 0, 0
                progress_line.event(
                    f"bucket {bucket_id} FAILED: {type(exc).__name__}: {str(exc)[:120]}"
                )
                return 0, 0
            finally:
                if distributed:
                    await asyncio.to_thread(
                        release_bucket_claim,
                        bucket_id,
                        owner,
                        lease_version,
                    )

        results = await asyncio.gather(*(process_one(row) for row in batch))
        buckets_processed += len(batch)
        pages_fetched += sum(pages for pages, _ in results)
        companies_written += sum(written for _, written in results)

    validate_splits(False)
    latest = await asyncio.to_thread(discovery_summary)
    if attempted_recovery_count:
        progress_line.event(
            f"discovery: attempted {attempted_recovery_count} historical failed leaves "
            "without clearing existing page checkpoints"
        )
    return DiscoveryResult(
        buckets_processed, pages_fetched, companies_written, client.requests_made,
        recovery_attempts=attempted_recovery_count,
        split_created=max(
            0, int(latest.get("buckets_split") or 0) - int(baseline.get("buckets_split") or 0)
        ),
        remaining_failed=int(latest.get("buckets_failed") or 0),
        remaining_lossy=int(latest.get("partitions_lossy") or 0),
        remaining_open=int(latest.get("buckets_open") or 0),
        remaining_partial=int(latest.get("pages_partial") or 0),
        remaining_failed_pages=int(latest.get("pages_failed") or 0),
        remaining_quarantine=int(latest.get("unresolved_rejections") or 0),
    )


async def run(
    seeds: list[str],
    *,
    search_type: str = "",
    concurrency: int | None = None,
    rate: float | None = None,
    max_buckets: int | None = None,
    verbose: bool = True,
    health=None,
    distributed: bool = False,
    numeric_split: bool = True,
) -> DiscoveryResult:
    # Fail before any HTTP or bucket writes if the existing migration 0013
    # has not been applied. No hidden DDL, guessed schema or retry storm.
    numeric_schema_missing = False
    if numeric_split:
        from ..storage.mysql.repositories.discovery_repository import numeric_schema_ready

        if not await asyncio.to_thread(numeric_schema_ready):
            # Numeric-only work is deferred, not permitted to block all
            # categorical children or checkpointed page collection.
            numeric_split = False
            numeric_schema_missing = True
    if distributed and not settings.dbd_redis_url:
        raise ValueError("Distributed discovery requires DBD_REDIS_URL")
    stop = asyncio.Event()
    owner_label = os.environ.get("DBD_WORKER_ID") or "discovery"
    owner = f"{owner_label}-{uuid4().hex}"
    progress_line = ProgressLine("DISCOVERY", interval=0.75, enabled=verbose)
    if numeric_schema_missing:
        progress_line.event(
            "DISCOVERY_NUMERIC_SCHEMA_MISSING: discovery_buckets.numeric_filters "
            "JSON is absent; categorical splits and page resume continue, "
            "numeric fallback is deferred until the existing migration 0013 is applied"
        )
    global_rate = settings.dbd_rate_per_second if rate is None else rate

    # Normal Discovery uses the exact fixed-rate HTTP execution used by
    # Collect. Distributed Discovery retains the Redis-coordinated path.
    http_client = (
        AsyncDbdClient(
            concurrency=concurrency,
            rate_per_second=global_rate,
            endpoint_rates={"search": global_rate},
            redis_url=settings.dbd_redis_url,
            redis_key=settings.dbd_redis_rate_key,
        )
        if distributed else
        FixedRateDbdClient(
            concurrency=settings.dbd_concurrency if concurrency is None else concurrency,
            rate_per_second=global_rate,
            max_retries=settings.dbd_http_retries,
            timeout=settings.request_timeout,
        )
    )
    async with http_client as client:

        async def monitor_health():
            while not stop.is_set():
                if health is not None:
                    await asyncio.to_thread(health)
                if distributed:
                    await asyncio.to_thread(
                        heartbeat_bucket_claims, owner, settings.dbd_lease_seconds
                    )
                try:
                    await asyncio.wait_for(stop.wait(), min(10, settings.dbd_lease_seconds / 3))
                except TimeoutError:
                    pass

        async def work():
            try:
                return await _run_work(
                    client,
                    seeds,
                    search_type,
                    max_buckets,
                    distributed=distributed,
                    owner=owner,
                    progress_line=progress_line,
                    numeric_split=numeric_split,
                    recovery_rate=global_rate,
                )
            finally:
                stop.set()

        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(monitor_health())
                task = group.create_task(work())
        finally:
            if distributed:
                await asyncio.to_thread(release_bucket_claims, owner)

    result = task.result()
    status = discovery_summary()
    progress_line.close(
        message=(
            f"Discovery: companies={int(status.get('unique_companies') or 0):,} "
            f"pages_done={int(status.get('pages_done') or 0):,} "
            f"open_buckets={int(status.get('buckets_open') or 0):,} "
            f"failed={int(status.get('buckets_failed') or 0):,} "
            f"lossy={int(status.get('partitions_lossy') or 0):,}"
        )
    )
    blockers = []
    if int(status.get("buckets_failed") or 0):
        blockers.append(f"{status['buckets_failed']} failed bucket(s)")
    if int(status.get("pages_partial") or 0):
        blockers.append(f"{status['pages_partial']} partial page(s)")
    if int(status.get("pages_failed") or 0):
        blockers.append(f"{status['pages_failed']} failed page(s)")
    if int(status.get("unresolved_rejections") or 0):
        blockers.append(f"{status['unresolved_rejections']} quarantined row(s)")
    if int(status.get("partitions_lossy") or 0):
        blockers.append(f"{status['partitions_lossy']} lossy partition(s)")
    if blockers:
        # Work on all other buckets has already completed. Report residual
        # limitations without turning already committed pages into a failure
        # or falsely claiming full upstream coverage.
        from ..presentation import TerminalPresenter

        TerminalPresenter().message(
            "Discovery INCOMPLETE: " + ", ".join(blockers), kind="warning"
        )
    return result
