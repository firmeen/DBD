from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256

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
    pending_buckets,
    release_bucket_claim,
    update_bucket,
    upsert_bucket,
)
from ..storage.mysql.repositories.discovery_split_repository import split_bucket

JP_TYPES = ("0", "2", "3", "5", "7", "8", "9", "A")
BUSINESS_SIZES = ("S", "M", "L")
EXTRA_PREFIX_SEEDS = ("099",)


@dataclass
class DiscoveryResult:
    buckets_processed: int
    pages_fetched: int
    companies_written: int
    requests_made: int
    invalid_rows: int = 0


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


def _children_for(filters: SearchFilters, provinces: list[str]) -> list[SearchFilters]:
    if not filters.jp_types:
        return [
            SearchFilters(
                keyword=filters.keyword,
                search_type=filters.search_type,
                pv_codes=filters.pv_codes,
                jp_statuses=filters.jp_statuses,
                jp_types=[kind],
                business_sizes=filters.business_sizes,
            )
            for kind in JP_TYPES
        ]
    if not filters.pv_codes and provinces:
        return [
            SearchFilters(
                keyword=filters.keyword,
                search_type=filters.search_type,
                pv_codes=[province],
                jp_statuses=filters.jp_statuses,
                jp_types=filters.jp_types,
                business_sizes=filters.business_sizes,
            )
            for province in provinces
        ]
    if not filters.business_sizes:
        return [
            SearchFilters(
                keyword=filters.keyword,
                search_type=filters.search_type,
                pv_codes=filters.pv_codes,
                jp_statuses=filters.jp_statuses,
                jp_types=filters.jp_types,
                business_sizes=[size],
            )
            for size in BUSINESS_SIZES
        ]
    return []


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

    if total_items is None:
        page, count_archive_id, data, requested = await _count(client, bucket_id, filters)
        total_items, pages_total = page.total_items, page.total_pages
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
        children = _children_for(filters, provinces)
        if not children:
            owned_update(
                status="failed",
                last_error=(
                    f"{total_items} rows exceeds the {MAX_ROWS_PER_QUERY} ceiling and no split dimension remains"
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
    from ..storage.mysql.engine import connect

    checked = 0
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id,p.total_items,COUNT(c.id) AS child_count,
                   SUM(c.total_items IS NULL) AS uncounted,
                   COALESCE(SUM(c.total_items),0) AS child_sum
            FROM discovery_buckets p JOIN discovery_buckets c ON c.parent_id=p.id
            WHERE p.status='split' AND p.total_items IS NOT NULL
            GROUP BY p.id,p.total_items
            """
        )
        rows = cur.fetchall()
    for row in rows:
        if int(row["uncounted"] or 0) > 0:
            continue
        parent_total = int(row["total_items"])
        children = int(row["child_sum"])
        lossless = 1 if children == parent_total else 0
        update_bucket(int(row["id"]), partition_lossless=lossless)
        checked += 1
        if verbose and not lossless:
            from ..presentation import TerminalPresenter

            TerminalPresenter().message(
                f"Bucket {row['id']}: SUM(children)={children:,} vs parent={parent_total:,}; "
                "completeness blocked",
                kind="error",
            )
    return checked


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



def _canonical_company_payload(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _discovery_company_row(profile, raw: dict, *, published_at: datetime) -> dict:
    mapping = {
        "jp_name": "company_name_th",
        "jp_name_e": "company_name_en",
        "jp_search_name": "company_search_name",
        "jp_stat_code": "status_code",
        "jp_stat_desc": "status_description",
        "jp_type_desc": "juristic_type_description",
        "reg_date": "registration_date",
        "stat_change_date": "status_change_date",
        "update_date": "dbd_update_date",
        "address": "address_th",
        "address_e": "address_en",
        "pv_code": "province_code",
        "pv_desc": "province_name",
        "ampur_code": "district_code",
        "ampur_desc": "district_name",
        "tumbon_code": "subdistrict_code",
        "tumbon_desc": "subdistrict_name",
        "zip_code": "postal_code",
        "business_size_code": "business_size_code",
        "business_type_code": "business_type_code",
        "business_type_desc": "business_type_description",
        "setup_obj_code": "setup_objective_code",
        "setup_obj_desc": "setup_objective_description",
        "setup_obj_grp_code": "setup_objective_group_code",
        "setup_obj_grp_desc": "setup_objective_group_description",
        "setup_obj_name_keyin": "setup_objective_keyin",
        "submit_obj_code": "submit_objective_code",
        "submit_obj_desc": "submit_objective_description",
        "submit_obj_name_keyin": "submit_objective_keyin",
        "cap_amt": "registered_capital",
        "paid_amt": "paid_capital",
        "total_asset": "total_assets_latest",
        "total_equity": "total_equity_latest",
        "total_income": "total_income_latest",
        "net_profit": "net_profit_latest",
    }
    result = {
        "juristic_type_code": profile.jp_type_code,
        "registration_no": profile.jp_no,
        "master_payload": _canonical_company_payload(raw),
        "published_at": published_at,
    }
    for field, column in mapping.items():
        if field in profile.present_fields:
            result[column] = getattr(profile, field)
    if "submit_fin_year" in profile.present_fields:
        from ..deep.catalog import latest_fiscal_year

        result["latest_fiscal_year"] = latest_fiscal_year(profile.submit_fin_year)
    if "update_date" in profile.present_fields:
        result["source_updated_at"] = profile.update_date
    return result


def _normalize_direct_rows(
    rows: list[dict],
    *,
    published_at: datetime,
) -> tuple[list[dict], int]:
    valid: list[dict] = []
    invalid = 0
    for raw in rows:
        try:
            profile = normalize(raw)
        except ContractError:
            invalid += 1
            continue
        valid.append(
            _discovery_company_row(profile, raw, published_at=published_at)
        )
    return valid, invalid


def _persist_direct_batch(
    repository,
    rows: list[dict],
    *,
    generation: int,
    refresh_mask: int,
) -> dict[str, int]:
    if not rows:
        return {"inserted": 0, "updated": 0, "refresh_reopened": 0}
    with repository.transaction() as conn:
        return repository.upsert_discovered_companies(
            conn,
            rows,
            generation=generation,
            refresh_mask=refresh_mask,
        )


async def _direct_page(
    client: AsyncDbdClient,
    filters: SearchFilters,
    page_no: int,
) -> tuple[object, datetime]:
    requested = _now()
    data = await client.post(
        SEARCH_PATH,
        filters.payload(page=page_no),
        referer=SEARCH_REFERER,
        timeout=settings.dbd_count_timeout_seconds if page_no == 1 else None,
        rate_key="search",
    )
    page = parse_search_response(data)
    if page.current_page != page_no:
        raise ContractError(
            f"Search returned page {page.current_page} for requested page {page_no}"
        )
    if not page.rows and page.total_items > 0:
        raise ContractError(f"Search page {page_no} is empty for a non-empty partition")
    return data, requested


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
    generation: int | None = None,
    refresh_mask: int = 0,
    repository=None,
) -> DiscoveryResult:
    """Direct v0.6 Search traversal into SQL Server DBD_COMPANY.

    Split/page state is intentionally transient. A crash restarts Search
    traversal; natural-key SQL Server upserts make repeated pages safe.
    """
    if distributed:
        raise ValueError(
            "--distributed is a legacy MySQL discovery mode and is not supported "
            "by the v0.6 direct SQL Server discovery path"
        )
    if generation is None:
        raise ValueError("v0.6 direct discovery requires an explicit generation")

    from ..storage.sqlserver.repository import ProductionSqlServerRepository

    repo = repository or ProductionSqlServerRepository()
    global_rate = settings.dbd_rate_per_second if rate is None else rate
    progress_line = ProgressLine("DISCOVERY", interval=0.75, enabled=verbose)

    buckets_processed = 0
    pages_fetched = 0
    companies_written = 0
    invalid_rows = 0

    async with AsyncDbdClient(
        concurrency=concurrency,
        rate_per_second=global_rate,
        endpoint_rates={"search": global_rate},
        redis_url=None,
        redis_key=settings.dbd_redis_rate_key,
    ) as client:
        provinces = await fetch_provinces(client)
        if not seeds:
            seeds = await province_seeds(client)

        queue = deque(
            (SearchFilters(keyword=seed, search_type=search_type), None)
            for seed in seeds
        )
        limit = None if max_buckets is None else int(max_buckets)

        while queue:
            if limit is not None and buckets_processed >= limit:
                break
            filters, prefetched = queue.popleft()
            if health is not None:
                await asyncio.to_thread(health)

            if prefetched is None:
                data, _requested = await _direct_page(client, filters, 1)
                page = parse_search_response(data)
                pages_fetched += 1
            else:
                data, page = prefetched

            buckets_processed += 1
            if page.total_items == 0:
                continue

            if page.total_items > MAX_ROWS_PER_QUERY:
                children = _children_for(filters, provinces)
                if not children:
                    progress_line.close()
                    raise ContractError(
                        f"{page.total_items} rows exceeds the "
                        f"{MAX_ROWS_PER_QUERY} ceiling and no split dimension remains"
                    )

                counted_children = []
                child_total = 0
                for child in children:
                    child_data, _requested = await _direct_page(client, child, 1)
                    child_page = parse_search_response(child_data)
                    pages_fetched += 1
                    child_total += int(child_page.total_items)
                    counted_children.append((child, (child_data, child_page)))
                if child_total != int(page.total_items):
                    progress_line.close()
                    raise ContractError(
                        "Discovery partition is not lossless: "
                        f"parent={page.total_items:,}, children={child_total:,}"
                    )
                queue.extend(counted_children)
                progress_line.event(
                    f"{filters.key()}: {page.total_items:,} rows -> "
                    f"{len(children)} lossless child partitions"
                )
                continue

            page_numbers = list(range(1, int(page.total_pages) + 1))
            page_chunk = max(
                1,
                min(
                    len(page_numbers),
                    max(1, int(concurrency or settings.dbd_concurrency)) * 2,
                ),
            )
            first_data = data
            for offset in range(0, len(page_numbers), page_chunk):
                chunk = page_numbers[offset : offset + page_chunk]
                tasks = []
                for page_no in chunk:
                    if page_no == 1:
                        tasks.append(None)
                    else:
                        tasks.append(
                            asyncio.create_task(
                                _direct_page(client, filters, page_no)
                            )
                        )

                company_rows: list[dict] = []
                for page_no, task in zip(chunk, tasks, strict=True):
                    if page_no == 1:
                        page_data = first_data
                        page_obj = page
                    else:
                        assert task is not None
                        page_data, _requested = await task
                        page_obj = parse_search_response(page_data)
                        pages_fetched += 1
                    normalized, rejected = _normalize_direct_rows(
                        page_obj.rows,
                        published_at=_now(),
                    )
                    invalid_rows += rejected
                    company_rows.extend(normalized)

                write_size = max(1, int(settings.dbd_write_batch_size))
                for write_offset in range(0, len(company_rows), write_size):
                    batch = company_rows[write_offset : write_offset + write_size]
                    result = await asyncio.to_thread(
                        _persist_direct_batch,
                        repo,
                        batch,
                        generation=int(generation),
                        refresh_mask=int(refresh_mask),
                    )
                    companies_written += int(result["inserted"]) + int(result["updated"])

                progress_line.event(
                    f"{filters.key()}: pages={min(offset + len(chunk), len(page_numbers))}/"
                    f"{len(page_numbers)} companies={companies_written:,}"
                )

    progress_line.close(
        message=(
            f"Discovery: partitions={buckets_processed:,} "
            f"pages={pages_fetched:,} companies={companies_written:,} "
            f"invalid={invalid_rows:,}"
        )
    )
    if invalid_rows and max_buckets is None:
        raise ContractError(
            f"Discovery encountered {invalid_rows:,} contract-invalid company row(s); "
            "generation cannot be finalized"
        )
    return DiscoveryResult(
        buckets_processed=buckets_processed,
        pages_fetched=pages_fetched,
        companies_written=companies_written,
        requests_made=client.requests_made,
        invalid_rows=invalid_rows,
    )
