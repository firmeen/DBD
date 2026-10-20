from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
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
from ..deep.catalog import FINANCIAL_MASK
from ..deep.http import FixedRateDbdClient
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

# The current full-universe root strategy is NOT certified. The repository's
# own source reference documents that normal search (type="") can match names,
# registration numbers and other text, so province-like numeric keywords are
# not proven exhaustive or pairwise disjoint. Never finalize a Generation on
# the strength of a completed tree until a lossless root strategy is proven.
PRODUCTION_ROOT_COVERAGE_CERTIFIED = False


def root_coverage_contract(*, seeds: list[str] | None = None,
                           search_type: str = "", max_buckets: int | None = None) -> dict:
    """Separate a complete registered tree from an authoritative universe proof.

    The normal DBD keyword Search is not documented as an exhaustive/disjoint
    registration-prefix search. No observed page count can certify that fact.
    """
    scoped = bool(seeds or search_type or max_buckets is not None)
    return {
        "scope": "diagnostic" if scoped else "full",
        "root_strategy_known": True,
        "search_semantics_certified": False,
        "exhaustive": False,
        "disjoint": False,
        "certified": False,
        "reason": (
            "Scoped Search is not a complete company universe"
            if scoped else
            "DBD normal keyword Search has no authoritative exhaustive, "
            "disjoint registration-prefix semantics proof"
        ),
    }


def require_certified_full_universe_discovery(
    *, seeds: list[str] | None, search_type: str,
    max_buckets: int | None, recovery_pass: bool = False,
) -> None:
    """Fail closed before monthly state changes when root coverage is unproven."""
    evidence = root_coverage_contract(
        seeds=seeds, search_type=search_type, max_buckets=max_buckets,
    )
    if evidence["scope"] == "full" and not recovery_pass and not evidence["certified"]:
        raise ContractError(
            "Full-universe Discovery is blocked: roots are not proven exhaustive "
            "and disjoint; " + str(evidence["reason"])
        )


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


def _direct_categorical_plans(
    filters: SearchFilters, provinces: list[str],
    statuses: tuple[str, ...] = (),
) -> list[tuple[str, list[SearchFilters]]]:
    """Bounded candidate plans; only counted, validated plans are committed.

    Kept separate from legacy _children_for so Dataset 1 semantics do not
    change. Observed status codes are hypotheses, not an exhaustive catalog.
    """
    from dataclasses import replace

    candidates = []
    for dimension, field, values in (
        ("juristic_type", "jp_types", JP_TYPES if not filters.jp_types else ()),
        ("province", "pv_codes", tuple(provinces) if not filters.pv_codes else ()),
        ("status", "jp_statuses", statuses if not filters.jp_statuses else ()),
        ("business_size", "business_sizes",
         ("S","M","L","X") if not filters.business_sizes else ()),
    ):
        if len(values) >= 2:
            children = [replace(filters, **{field:[value]}) for value in values]
            if len({child.key() for child in children}) == len(children):
                candidates.append((dimension,children))
    return candidates



def _keyword_expansion_candidates(filters: SearchFilters) -> list[SearchFilters]:
    """Bounded, syntactic registration-prefix *candidates*, never a certificate.

    DBD normal Search can also match names and substrings. Advancing a keyword
    is unsafe without independently established prefix/exhaustiveness semantics.
    Preserve all other filters and advance at most to a 13-digit identity.
    """
    from dataclasses import replace

    keyword = filters.keyword
    if (not keyword.isascii() or not keyword.isdecimal()
        or not 3 <= len(keyword) < 13 or filters.search_type):
        return []
    children = [replace(filters, keyword=keyword + str(n)) for n in range(10)]
    keys = [child.key() for child in children]
    if len(set(keys)) != 10 or filters.key() in keys:
        return []
    return children


def validate_search_scope(rows: list[dict], filters: SearchFilters) -> None:
    """Reject company rows outside their requested disjoint categorical filters.

    Count parity alone cannot prove non-overlap if a source silently ignores a
    filter. Verify every returned identity has each constrained attribute.
    This also detects requests whose search filters changed semantics upstream.
    """
    categorical = (
        (filters.jp_types, "jpTypeCode"),
        (filters.pv_codes, "pvCode"),
        (filters.jp_statuses, "jpStatCode"),
        (filters.business_sizes, "businessSizeCode"),
    )
    constraints = [
        ({str(value) for value in expected},field)
        for expected,field in categorical if expected
    ]
    for row in rows:
        if not isinstance(row,dict):
            raise ContractError("Search returned a non-object company row")
        for allowed,field in constraints:
            if str(row.get(field) or "") not in allowed:
                raise ContractError(
                    f"Search response ignored or violated scoped filter {field}"
                )




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
    def encode(item: object) -> str:
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, str):
            return json.dumps(item.replace("\x00", ""), ensure_ascii=False)
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError("Non-finite Decimal cannot be stored as JSON")
            return str(item)
        if isinstance(item, int):
            return str(item)
        if isinstance(item, float):
            return json.dumps(item, allow_nan=False)
        if isinstance(item, list):
            return "[" + ",".join(encode(child) for child in item) + "]"
        if isinstance(item, dict):
            return (
                "{"
                + ",".join(
                    json.dumps(str(key), ensure_ascii=False) + ":" + encode(item[key])
                    for key in sorted(item, key=lambda value: str(value))
                )
                + "}"
            )
        raise TypeError(type(item).__name__)

    return encode(value)


_NESTED_PROFILE_FIELDS = {
    "jp_stat_desc": ("jpStatus", "jpStatDesc"),
    "jp_type_desc": ("jpType", "jpTypeDesc"),
    "pv_desc": ("locationProvince", "pvDesc"),
    "business_type_desc": ("businessType", "businessTypeDesc"),
    "setup_obj_desc": ("setupObjType", "objDesc"),
    "setup_obj_grp_code": ("setupObjType", "objGrpCode"),
    "setup_obj_grp_desc": ("setupObjType", "objGrpDesc"),
    "submit_obj_desc": ("submitObjType", "objDesc"),
    "ampur_desc": ("locationAmpur", "ampurDesc"),
    "tumbon_desc": ("locationTumbon", "tumbonDesc"),
}


def _profile_field_present(profile, field: str) -> bool:
    nested = _NESTED_PROFILE_FIELDS.get(field)
    if nested is None:
        return field in profile.present_fields
    container, key = nested
    source = profile.facts.get(container)
    return isinstance(source, dict) and key in source


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
        if _profile_field_present(profile, field):
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
    profiles, rejections = _normalize_rows(rows)
    rejected = {int(item["row_index"]) for item in rejections}
    valid: list[dict] = []
    profile_index = 0
    for row_index, raw in enumerate(rows):
        if row_index in rejected:
            continue
        profile = profiles[profile_index]
        profile_index += 1
        valid.append(
            _discovery_company_row(profile, raw, published_at=published_at)
        )
    return valid, len(rejections)


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
            financial_mask=FINANCIAL_MASK,
            refresh_mask=refresh_mask,
        )


async def _direct_page(
    client: FixedRateDbdClient,
    filters: SearchFilters,
    page_no: int,
) -> tuple[object, datetime]:
    response = await client.fetch(
        "POST",
        SEARCH_PATH,
        json_body=filters.payload(page=page_no),
        referer=SEARCH_REFERER,
        timeout=settings.dbd_count_timeout_seconds if page_no == 1 else None,
    )
    data = response.data
    meta = data.get("meta") if isinstance(data,dict) else None
    if (not isinstance(meta,dict) or type(meta.get("itemsPerPage")) is not int
        or meta["itemsPerPage"] != 10):
        raise ContractError(
            f"Search page {page_no} is missing the source's integer itemsPerPage=10 contract"
        )
    page = parse_search_response(data)
    if page.current_page != page_no:
        raise ContractError(
            f"Search returned page {page.current_page} for requested page {page_no}"
        )
    if page.items_per_page != 10 or len(page.rows)>10:
        raise ContractError(
            f"Search page {page_no} violated its 10-row pagination contract"
        )
    if page.total_items > 0 and page_no > page.total_pages:
        raise ContractError(
            f"Search page {page_no} is beyond reported totalPages={page.total_pages}"
        )
    if page.total_items <= MAX_ROWS_PER_QUERY and page.total_pages != (page.total_items+9)//10:
        raise ContractError(
            f"Search page {page_no} pagination total does not reconcile"
        )
    expected_rows=min(10,max(0,page.total_items-(page_no-1)*10))
    if len(page.rows) != expected_rows:
        raise ContractError(
            f"Search page {page_no} is incomplete: expected {expected_rows} rows, "
            f"received {len(page.rows)}"
        )
    if page.total_items == 0 and page_no != 1:
        raise ContractError("Empty Search partition cannot have pages after page 1")
    return data, response.requested_at


async def run(
    seeds: list[str],
    *,
    search_type: str = "",
    concurrency: int | None = None,
    rate: float | None = None,
    max_buckets: int | None = None,
    recovery_limit: int | None = None,
    recovery_pass: bool = False,
    verbose: bool = True,
    health=None,
    distributed: bool = False,
    generation: int | None = None,
    refresh_mask: int = 0,
    repository=None,
    event=None,
) -> DiscoveryResult:
    """Direct v0.6 Search traversal into the selected production DBD_COMPANY.

    Both Production backends use a durable Generation-scoped bucket tree.
    Company writes and contiguous page checkpoints share one transaction.
    """
    if distributed:
        raise ValueError(
            "--distributed is a legacy MySQL discovery mode and is not supported "
            "by the v0.6 direct SQL Server discovery path"
        )
    if generation is None:
        raise ValueError("v0.6 direct discovery requires an explicit generation")

    from ..storage.production import production_repository

    repo = repository if repository is not None else production_repository()
    explicitly_scoped = bool(seeds or search_type or max_buckets is not None)
    if getattr(repo, "backend", None) in ("sqlserver", "mysql"):
        require_certified_full_universe_discovery(
            seeds=seeds,search_type=search_type,max_buckets=max_buckets,
            recovery_pass=recovery_pass,
        )
    global_rate = settings.dbd_rate_per_second if rate is None else rate
    admission = None
    if getattr(repo, "backend", None) == "sqlserver":
        from ..presentation.setup import local_node_id
        from ..storage.sqlserver.control_repository import ControlRepository
        control = ControlRepository()
        active = await asyncio.to_thread(control.active_generation)
        if active is None or active.get("state") != "DISCOVERING" or (
            int(active.get("generation") or 0) != int(generation)
        ):
            raise RuntimeError(
                "SQL Server Discovery requires the matching authorized "
                "DISCOVERING monthly Generation; start with dbd-data cycle run"
            )
        node_id = local_node_id()
        if node_id is None:
            raise RuntimeError("Run dbd-data setup on the Admin machine before Discovery")
        await asyncio.to_thread(control.require_admin, node_id)
        admission = lambda: control.admit(node_id, int(generation))

    buckets_processed = 0
    pages_fetched = 0
    companies_written = 0
    invalid_rows = 0

    def emit_progress(client: FixedRateDbdClient) -> None:
        if event is None:
            return
        event(
            "discovery_progress",
            {
                "buckets_processed": buckets_processed,
                "pages_fetched": pages_fetched,
                "companies_written": companies_written,
                "requests_made": client.requests_made,
                "invalid_rows": invalid_rows,
            },
        )

    async with FixedRateDbdClient(
        concurrency=int(concurrency or settings.dbd_concurrency),
        rate_per_second=float(global_rate),
        max_retries=int(settings.dbd_http_retries),
        admission=admission,
    ) as client:
        province_response = await client.fetch(
            "GET",
            "/api/v1/commons/provinces",
        )
        province_data = province_response.data
        if not isinstance(province_data, list):
            raise ContractError("Province response must be a JSON array")
        provinces = [
            str(row["pvCode"])
            for row in province_data
            if isinstance(row, dict) and row.get("pvCode")
        ]
        if not provinces:
            raise ContractError("Province response contains no usable pvCode values")
        if not seeds:
            seeds = sorted(
                {
                    f"0{str(province).zfill(2)}"
                    for province in provinces
                    if str(province).strip()
                }
                | set(EXTRA_PREFIX_SEEDS)
            )

        if getattr(repo, "backend", None) in ("sqlserver", "mysql"):
            return await _run_durable_discovery(
                repo, client, seeds, search_type=search_type,
                provinces=provinces, generation=int(generation),
                refresh_mask=int(refresh_mask),
                concurrency=int(concurrency or settings.dbd_concurrency),
                max_buckets=max_buckets, event=event, health=health,
                explicitly_scoped=explicitly_scoped,
                recovery_limit=recovery_limit,
                recovery_pass=recovery_pass,
            )

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
                emit_progress(client)
                continue

            if page.total_items > MAX_ROWS_PER_QUERY:
                children = _children_for(filters, provinces)
                if not children:
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
                    raise ContractError(
                        "Discovery partition is not lossless: "
                        f"parent={page.total_items:,}, children={child_total:,}"
                    )
                queue.extend(counted_children)
                if event is not None:
                    event(
                        "discovery_split",
                        {
                            "rows": int(page.total_items),
                            "children": len(children),
                            "buckets_processed": buckets_processed,
                            "pages_fetched": pages_fetched,
                            "companies_written": companies_written,
                            "requests_made": client.requests_made,
                            "invalid_rows": invalid_rows,
                        },
                    )
                emit_progress(client)
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
                tasks = {
                    page_no: asyncio.create_task(
                        _direct_page(client, filters, page_no)
                    )
                    for page_no in chunk
                    if page_no != 1
                }

                company_rows: list[dict] = []
                try:
                    for page_no in chunk:
                        if page_no == 1:
                            page_data = first_data
                            page_obj = page
                        else:
                            page_data, _requested = await tasks[page_no]
                            page_obj = parse_search_response(page_data)
                            pages_fetched += 1
                        normalized, rejected = _normalize_direct_rows(
                            page_obj.rows,
                            published_at=_now(),
                        )
                        invalid_rows += rejected
                        company_rows.extend(normalized)
                except BaseException:
                    for task in tasks.values():
                        if not task.done():
                            task.cancel()
                    if tasks:
                        await asyncio.gather(*tasks.values(), return_exceptions=True)
                    raise

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

                emit_progress(client)

    if queue:
        raise ContractError(
            "Discovery stopped before all in-memory partitions were drained; "
            "persisted rows remain safe but generation coverage is incomplete"
        )

    emit_progress(client)
    if invalid_rows:
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


async def _run_durable_discovery(
    repo, client: FixedRateDbdClient, seeds: list[str], *,
    search_type: str, provinces: list[str], generation: int,
    refresh_mask: int, concurrency: int, max_buckets: int | None,
    event=None, health=None, explicitly_scoped: bool = False,
    recovery_limit: int | None = None,
    recovery_pass: bool = False,
) -> DiscoveryResult:
    """Production G2+ Discovery: one durable tree and atomic page checkpoints.

    No second HTTP client, queue service, or company writer. The Core-7
    repository owns business writes and shares the checkpoint transaction.
    Uncertified partition dimensions remain BLOCKED, never marked complete.
    """
    if getattr(repo, "backend", None) == "mysql":
        from ..storage.mysql import discovery_repository as work
    else:
        from ..storage.sqlserver import discovery_repository as work

    # A manually supplied keyword, search type or bound is diagnostic only.
    # These may never share the monthly full-universe durable root set.
    full = not explicitly_scoped
    scope = "full" if full else (
        "diag:" + sha256(
            json.dumps(
                {"seeds": sorted(set(seeds)), "type": search_type},
                sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:40]
    )
    roots = [SearchFilters(keyword=seed, search_type=search_type)
             for seed in sorted(set(seeds))]
    processed = pages_fetched = written = invalid = 0
    saved_pages = 0
    started = time.monotonic()
    active = None
    samples = deque(maxlen=120)
    durable_snapshot = {}
    last_summary_at = 0.0

    def notify(name: str, **values) -> None:
        if event is None:
            return
        try:
            now = time.monotonic()
            if not samples or now - samples[-1][0] >= 0.9:
                samples.append((now,client.requests_made,saved_pages))
            while len(samples)>1 and now - samples[0][0]>60.0:
                samples.popleft()
            rate = None
            page_rate = None
            if len(samples)>=2:
                elapsed = samples[-1][0]-samples[0][0]
                if elapsed >= 5.0:
                    rate = round((samples[-1][1]-samples[0][1])/elapsed,3)
                    page_rate = round((samples[-1][2]-samples[0][2])/elapsed,3)
            cooldown = client.cooldown_remaining()
            state = dict(values)
            if cooldown > 0 and state.get("stage") not in ("BLOCKED","DONE"):
                state["work_stage"] = state.get("stage")
                state["stage"] = "RATE_LIMITED"
            event(name, {
                **state, "buckets_processed": processed,
                "pages_fetched": pages_fetched, "companies_written": written,
                "invalid_rows": invalid, "requests_made": client.requests_made,
                "rate_limit_hits": client.rate_limit_hits,
                "configured_rate": client.rate_per_second,
                "actual_http_rate_1m":rate, "saved_page_rate_1m":page_rate,
                "saved_pages_run":saved_pages,
                "cooldown_seconds": cooldown,
                "elapsed_seconds": max(0, now - started),
                "generation": generation, "scope": scope,
            })
        except Exception:
            # Rendering/telemetry failures never fail persistence.
            pass

    async def atomic(fn, *args, **kwargs):
        def execute():
            with repo.transaction() as conn:
                return fn(conn, *args, **kwargs)
        return await asyncio.to_thread(execute)

    def initialize(conn):
        work.validate_schema(conn)
        work.register_roots(conn, generation, scope, roots)

    await atomic(initialize)

    def observed_statuses(conn):
        cur = conn.cursor()
        try:
            if getattr(repo, "backend", None) == "mysql":
                cur.execute(
                    "SELECT DISTINCT status_code FROM DBD_COMPANY "
                    "WHERE status_code IS NOT NULL AND status_code<>'' "
                    "ORDER BY status_code LIMIT 16"
                )
                return tuple(str(row[0]) for row in cur.fetchall())
            cur.execute(
                "SELECT DISTINCT TOP (16) status_code FROM dbo.DBD_COMPANY "
                "WHERE status_code IS NOT NULL AND status_code<>N'' "
                "ORDER BY status_code"
            )
            return tuple(str(row[0]) for row in cur.fetchall())
        finally:
            cur.close()

    status_candidates = await atomic(observed_statuses)

    if recovery_pass:
        from .discovery_numeric import PRODUCTION_CERTIFIED_DIMENSIONS
        if PRODUCTION_CERTIFIED_DIMENSIONS:
            reopened = await atomic(
                work.reopen_blocked_numeric, generation, scope,
                limit=min(int(recovery_limit or 100), 1000),
            )
            notify(
                "discovery_progress", stage="RECONCILING",
                reopened_buckets=reopened,
            )

    async def pulse() -> None:
        nonlocal durable_snapshot, last_summary_at
        while True:
            now = time.monotonic()
            if now - last_summary_at >= 20.0:
                last_summary_at = now
                try:
                    durable_snapshot = await atomic(work.report,generation,scope)
                except Exception:
                    # Rendering observations may fail, but the durable writer
                    # and its independent SQL errors must never be hidden.
                    pass
            notify("discovery_progress", **{**durable_snapshot, **(active or {})})
            await asyncio.sleep(1.0)

    task = asyncio.create_task(pulse())
    try:
        while True:
            bucket = await atomic(work.next_work, generation, scope)
            if bucket is None:
                break
            if (max_buckets is not None and processed >= max_buckets
                or recovery_limit is not None and processed >= recovery_limit):
                break
            if health is not None:
                await asyncio.to_thread(health)
            filters = work.filters_for(bucket)
            bucket_id = int(bucket["work_id"])
            active = {"work_id": bucket_id, "bucket": filters.key(),
                      "stage": "COUNTING"}
            notify("discovery_progress", **active)
            response, _ = await _direct_page(client, filters, 1)
            parent = parse_search_response(response)
            pages_fetched += 1
            processed += 1
            if bucket["status"] == "PENDING":
                await atomic(work.count, bucket_id, parent.total_items, parent.total_pages)
                bucket["status"] = "COUNTED"
                bucket["total_items"] = parent.total_items
                bucket["total_pages"] = parent.total_pages
            elif (int(bucket["total_items"]) != parent.total_items
                  or int(bucket["total_pages"]) != parent.total_pages):
                await atomic(work.blocked, bucket_id, "count_or_pagination_drift")
                notify("discovery_progress", **active,
                       stage="RECONCILING", error="count_or_pagination_drift")
                continue

            if parent.total_items == 0:
                await atomic(work.done, bucket, empty=True)
                notify("discovery_progress", **active, stage="DONE")
                continue

            if parent.total_items > MAX_ROWS_PER_QUERY:
                active["stage"] = "VALIDATING_SPLIT"
                notify("discovery_progress", **active)
                children = None
                counts = []
                split_dimension = None
                reasons = []

                from ..deep.http import DeepPermanentHttpError
                for dimension, planned in _direct_categorical_plans(
                    filters, provinces, status_candidates,
                ):
                    seen = set()
                    candidate_counts = []
                    candidate_ok = True
                    for child in planned:
                        try:
                            child_data, _ = await _direct_page(client,child,1)
                        except DeepPermanentHttpError as exc:
                            if exc.status == 400 and dimension == "status":
                                candidate_ok = False
                                reasons.append("status_filter_not_supported")
                                break
                            raise
                        observed = parse_search_response(child_data)
                        pages_fetched += 1
                        candidate_counts.append(
                            (observed.total_items, observed.total_pages)
                        )
                        try:
                            validate_search_scope(observed.rows, child)
                        except ContractError as exc:
                            # Only an incompatible *candidate* filter is safe
                            # to skip. Invalid pagination, HTTP, auth and
                            # persistence errors remain fatal.
                            candidate_ok = False
                            reasons.append(dimension + ":filter_not_respected:" + str(exc)[:120])
                            break
                        for row in observed.rows:
                            identity = (row.get("jpTypeCode"),row.get("jpNo"))
                            if all(identity):
                                if identity in seen:
                                    candidate_ok = False
                                seen.add(identity)
                    if not candidate_ok or len(candidate_counts) != len(planned):
                        reasons.append(dimension + ":sample_overlap_or_invalid_filter")
                        continue
                    if sum(item[0] for item in candidate_counts) != parent.total_items:
                        reasons.append(dimension + ":count_parity_failed")
                        continue
                    # Child counts were collected at different times. Verify
                    # the parent has not drifted before certifying the tree.
                    current_data, _ = await _direct_page(client,filters,1)
                    current_parent = parse_search_response(current_data)
                    pages_fetched += 1
                    if (current_parent.total_items != parent.total_items or
                        current_parent.total_pages != parent.total_pages):
                        reasons.append(dimension + ":parent_count_drift")
                        continue
                    children = planned
                    counts = candidate_counts
                    split_dimension = dimension
                    break

                if children is None:
                    # Categorical splitting was exhausted. Decimal keyword
                    # extension is a bounded candidate, NOT a verified prefix
                    # partition for DBD normal-search semantics. Probe only
                    # sample membership/count and always retain a blocked
                    # certificate until an authoritative search contract exists.
                    keyword_children = _keyword_expansion_candidates(filters)
                    if keyword_children:
                        reasons.append("keyword_prefix_semantics_not_certified")
                    else:
                        reasons.append("keyword_expansion_not_applicable")

                if children is None:
                    reason = "no_certified_lossless_split:" + ",".join(reasons)[:900]
                    await atomic(work.blocked,bucket_id,reason)
                    notify("discovery_progress", **active,
                           stage="BLOCKED",error=reason)
                    continue

                active["stage"] = "SPLITTING"
                # Parent transition, child keys and validated counts share one
                # durable transaction. The next loop Fetches the children.
                await atomic(work.split,bucket,children,split_dimension,counts)
                notify("discovery_split", **active,children=len(children))
                continue

            if bucket["status"] == "COUNTED":
                await atomic(work.fetching, bucket)
            previous = int(bucket["last_committed_page"])
            total_pages = int(parent.total_pages)
            active.update(stage="FETCHING", pages_done=previous,
                          pages_total=total_pages)
            chunk_size = max(1, min(32, concurrency * 2))
            # Bounded to at most one source partition (<=10,000 identities).
            # Detect API pages with repeated companies before committing a
            # second copy as though it represented a different page.
            observed_identities: set[tuple[str,str]] = set()
            next_page = previous + 1
            while next_page <= total_pages:
                numbers = list(range(next_page, min(total_pages + 1,
                                                    next_page + chunk_size)))
                tasks = {
                    p: asyncio.create_task(_direct_page(client, filters, p))
                    for p in numbers if p != 1
                }
                batch = []
                try:
                    for p in numbers:
                        if p == 1:
                            obj = parent
                        else:
                            page_data, _ = await tasks[p]
                            obj = parse_search_response(page_data)
                            pages_fetched += 1
                        if (obj.total_items != parent.total_items or
                            obj.total_pages != parent.total_pages or
                            len(obj.rows) == 0):
                            raise ContractError("Discovery pagination drift or empty page")
                        validate_search_scope(obj.rows, filters)
                        rows, rejected = _normalize_direct_rows(
                            obj.rows, published_at=_now())
                        invalid += rejected
                        if rejected:
                            raise ContractError(
                                "Invalid company row; refusing to checkpoint incomplete page")
                        for row in rows:
                            identity=(str(row["juristic_type_code"]),
                                      str(row["registration_no"]))
                            if identity in observed_identities:
                                raise ContractError(
                                    "Search pagination repeated a company identity "
                                    "within the current discovery partition"
                                )
                            observed_identities.add(identity)
                        batch.extend(rows)
                except BaseException:
                    for pending in tasks.values():
                        if not pending.done():
                            pending.cancel()
                    if tasks:
                        await asyncio.gather(*tasks.values(), return_exceptions=True)
                    raise

                active["stage"] = "PERSISTING"
                notify("discovery_progress", **active)
                def commit_chunk(conn):
                    staged_written = 0
                    size = max(1, int(settings.dbd_write_batch_size))
                    for offset in range(0, len(batch), size):
                        result = repo.upsert_discovered_companies(
                            conn, batch[offset:offset + size],
                            generation=generation, financial_mask=FINANCIAL_MASK,
                            refresh_mask=refresh_mask)
                        staged_written += int(result["inserted"]) + int(result["updated"])
                    work.checkpoint(conn, bucket_id, previous, numbers[-1])
                    return staged_written

                written += await atomic(commit_chunk)
                saved_pages += len(numbers)
                previous = numbers[-1]
                next_page = previous + 1
                active.update(stage="FETCHING", pages_done=previous)
                notify("discovery_progress", **active)
            await atomic(work.done, bucket)
            notify("discovery_progress", **active, stage="DONE")

        report = await atomic(work.coverage, generation, scope)
        notify("discovery_progress", **report, stage="RECONCILING")
        if max_buckets is not None or (not recovery_pass and not report["ok"]):
            raise ContractError(
                "Discovery coverage incomplete: " + ", ".join(
                    f"{key}={value}" for key, value in sorted(report.items())
                )
            )
        return DiscoveryResult(
            buckets_processed=processed, pages_fetched=pages_fetched,
            companies_written=written, requests_made=client.requests_made,
            invalid_rows=invalid,
        )
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
