from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from uuid import uuid4

from ..client.async_client import AsyncDbdClient
from ..client.endpoints.search import (
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
    claim_buckets,
    discovery_summary,
    heartbeat_bucket_claims,
    pending_buckets,
    release_bucket_claim,
    release_bucket_claims,
    update_bucket,
    upsert_bucket,
)
from ..storage.mysql.repositories.discovery_split_repository import split_bucket

JP_TYPES = ("0", "2", "3", "5", "7", "8", "9", "A")
BUSINESS_SIZES = ("S", "M", "L", "X")
EXTRA_PREFIX_SEEDS = ("099",)


@dataclass
class DiscoveryResult:
    buckets_processed: int
    pages_fetched: int
    companies_written: int
    requests_made: int


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

    return SearchFilters(
        keyword=row["keyword"],
        search_type=row["search_type"],
        pv_codes=split(row["pv_codes"]),
        jp_statuses=split(row["jp_statuses"]),
        jp_types=split(row["jp_types"]),
        business_sizes=split(row["business_sizes"]),
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
        options = []
        for value in values:
            kwargs = {
                "keyword": filters.keyword,
                "search_type": filters.search_type,
                "pv_codes": list(filters.pv_codes),
                "jp_statuses": list(filters.jp_statuses),
                "jp_types": list(filters.jp_types),
                "business_sizes": list(filters.business_sizes),
            }
            kwargs[field] = [value]
            options.append(SearchFilters(**kwargs))
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
    controller = _BatchController.build(client.concurrency)
    fetched = 0
    written = 0

    async def fetch_one(page_no: int) -> dict | None:
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
            if page.current_page != page_no or (not page.rows and page.total_items > 0):
                raise ContractError("Search page number or empty-page contract mismatch")
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
            if lease_owner is None:
                await asyncio.to_thread(fail_page, bucket_id, page_no, type(exc).__name__)
            else:
                await asyncio.to_thread(
                    fail_page,
                    bucket_id,
                    page_no,
                    type(exc).__name__,
                    lease_owner=lease_owner,
                    lease_version=lease_version,
                )
            return None

    async def page_worker():
        while True:
            try:
                page_no = page_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                result = await fetch_one(page_no)
                if result is not None:
                    await result_queue.put(result)
            finally:
                page_queue.task_done()

    async def fetch_all():
        worker_count = min(client.concurrency, max(1, len(pages)))
        async with asyncio.TaskGroup() as group:
            for _ in range(worker_count):
                group.create_task(page_worker())
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
    elif state["partial"]:
        error = "One or more pages contain quarantined rows; fix the contract and run discover-retry"
    else:
        error = "One or more pages failed; run discover-retry"
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
) -> tuple[int, int]:
    from ..storage.mysql.repositories.discovery_repository import mark_search_archive

    bucket_id = int(row["id"])
    filters = _filters_from_row(row)
    total_items = row["total_items"]
    pages_total = row["pages_total"]
    first_page = None
    count_archive_id = None

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
        if int(row.get("pages_done") or 0) and row["total_items"] != total_items:
            raise ContractError("Search total changed after pages were committed")
        first_page = (count_archive_id, data, requested)
        owned_update(total_items=total_items, pages_total=pages_total, status="counted")

    if total_items == 0:
        if count_archive_id is not None:
            await asyncio.to_thread(mark_search_archive, count_archive_id, "processed")
        owned_update(status="done")
        return 0, 0

    if total_items > MAX_ROWS_PER_QUERY:
        if count_archive_id is not None:
            await asyncio.to_thread(mark_search_archive, count_archive_id, "processed")
        children = await _validated_children(
            client, bucket_id, filters, provinces, status_codes or [], int(total_items)
        )
        if not children:
            owned_update(
                status="failed",
                last_error=(
                    f"{total_items} rows exceeds the {MAX_ROWS_PER_QUERY} ceiling; "
                    "no count-validated, non-overlapping split is available"
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
    )


def validate_splits(verbose: bool = True) -> int:
    """Reconcile from the leaves upward, never count unfinished descendants as complete."""
    from ..storage.mysql.engine import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id,parent_id,depth,status,total_items,pages_total,pages_done,
                   partition_lossless
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

        lossless = bool(count_equal) and all(child_ready(c) for c in children)
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
) -> bool:
    """Count every proposed child and recheck parent before admitting a split."""
    from ..storage.mysql.repositories.discovery_repository import mark_search_archive

    child_counts = []
    for child in candidates:
        page, archive_id, _, _ = await _count(client, bucket_id, child)
        await asyncio.to_thread(mark_search_archive, archive_id, "processed")
        if page.current_page != 1 or (page.total_items > 0 and not page.rows):
            raise ContractError("Invalid Discovery split count response")
        # Check sampled search rows against the proposed categorical constraints.
        for raw in page.rows:
            for field, key in (
                ("jp_types", "jpTypeCode"),
                ("pv_codes", "pvCode"),
                ("jp_statuses", "jpStatCode"),
                ("business_sizes", "businessSizeCode"),
            ):
                selected = getattr(child, field)
                if selected and key in raw and str(raw[key]) not in selected:
                    return False
        child_counts.append(page.total_items)

    if sum(child_counts) != parent_count or max(child_counts, default=0) >= parent_count:
        return False
    # Count metadata may drift while children are probed; fail closed.
    latest, archive_id, _, _ = await _count(client, bucket_id, filters)
    await asyncio.to_thread(mark_search_archive, archive_id, "processed")
    return latest.current_page == 1 and latest.total_items == parent_count


async def _validated_children(
    client: AsyncDbdClient,
    bucket_id: int,
    filters: SearchFilters,
    provinces: list[str],
    statuses: list[str],
    parent_count: int,
) -> list[SearchFilters]:
    for options in _partition_plans(filters, provinces, statuses):
        if await _probe_partition(client, bucket_id, filters, parent_count, options):
            return options
    return []


async def probe_discovery_partition(
    *,
    parent_id: int,
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
    row = await asyncio.to_thread(get_bucket, parent_id)
    if row is None:
        raise ValueError(f"Unknown discovery bucket {parent_id}")
    if row["status"] != "failed" or int(row.get("total_items") or 0) <= MAX_ROWS_PER_QUERY:
        raise ValueError("Probe requires a failed bucket exceeding 10000 rows")

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


async def recover_discovery(
    *,
    parent_id: int | None = None,
    limit: int = 10,
    execute: bool = False,
    rate: float = 1.0,
    health=None,
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
    async with AsyncDbdClient(
        concurrency=1,
        rate_per_second=rate,
        endpoint_rates={"search": rate},
    ) as client:
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
                    )
                    state = await asyncio.to_thread(get_bucket, bucket_id)
                    if state["status"] == "split":
                        split += 1
                    elif state["status"] == "done":
                        recovered += 1
                    else:
                        failures.append(f"{bucket_id}: no validated split available")
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
) -> DiscoveryResult:
    buckets_processed = pages_fetched = companies_written = 0
    provinces = await fetch_provinces(client)
    master_cache: dict[tuple[str, str], str] = {}
    status_codes = await asyncio.to_thread(observed_status_codes)

    if not seeds:
        seeds = await province_seeds(client)
    for seed in seeds:
        upsert_bucket(SearchFilters(keyword=seed, search_type=search_type))

    base_parallelism = max(1, min(4, client.concurrency // 4 or 1))
    while True:
        remaining = None if max_buckets is None else max_buckets - buckets_processed
        if remaining is not None and remaining <= 0:
            break
        desired = base_parallelism if remaining is None else min(base_parallelism, remaining)
        if distributed:
            rows = await asyncio.to_thread(
                claim_buckets, owner, desired, settings.dbd_lease_seconds
            )
            batch = rows
        else:
            rows = await asyncio.to_thread(pending_buckets, 25)
            batch = rows[:desired]
        if not batch:
            break

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
    return DiscoveryResult(buckets_processed, pages_fetched, companies_written, client.requests_made)


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
) -> DiscoveryResult:
    if distributed and not settings.dbd_redis_url:
        raise ValueError("Distributed discovery requires DBD_REDIS_URL")
    stop = asyncio.Event()
    owner_label = os.environ.get("DBD_WORKER_ID") or "discovery"
    owner = f"{owner_label}-{uuid4().hex}"
    progress_line = ProgressLine("DISCOVERY", interval=0.75, enabled=verbose)
    global_rate = settings.dbd_rate_per_second if rate is None else rate

    async with AsyncDbdClient(
        concurrency=concurrency,
        rate_per_second=global_rate,
        endpoint_rates={"search": global_rate},
        redis_url=settings.dbd_redis_url if distributed else None,
        redis_key=settings.dbd_redis_rate_key,
    ) as client:

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
            f"open_buckets={int(status.get('buckets_open') or 0):,}"
        )
    )
    blockers = []
    if int(status.get("buckets_failed") or 0):
        blockers.append(f"{status['buckets_failed']} failed bucket(s)")
    if int(status.get("pages_partial") or 0):
        blockers.append(f"{status['pages_partial']} partial page(s)")
    if int(status.get("unresolved_rejections") or 0):
        blockers.append(f"{status['unresolved_rejections']} quarantined row(s)")
    if int(status.get("partitions_lossy") or 0):
        blockers.append(f"{status['partitions_lossy']} lossy partition(s)")
    if blockers and max_buckets is None:
        raise ContractError("Discovery incomplete: " + ", ".join(blockers))
    return result
