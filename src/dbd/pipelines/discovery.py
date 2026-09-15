from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

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
from ..core.settings import settings
from ..storage.mysql.repositories.discovery_repository import (
    discovery_summary,
    pending_buckets,
    update_bucket,
    upsert_bucket,
)

# Split dimensions, applied in this order. jpType is first because the
# reference implementation validated it as lossless on the observed data.
JP_TYPES = ("0", "2", "3", "5", "7", "8", "9", "A")
BUSINESS_SIZES = ("S", "M", "L")

# 099 was observed in collected company numbers but is not represented by the
# province-prefix seed list. Keep it explicit until an authoritative population
# reconciliation proves a more general seed strategy.
EXTRA_PREFIX_SEEDS = ("099",)


@dataclass
class DiscoveryResult:
    buckets_processed: int
    pages_fetched: int
    companies_written: int
    requests_made: int


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _filters_from_row(row: dict) -> SearchFilters:
    def split(value: str) -> list[str]:
        return [v for v in (value or "").split(",") if v]

    return SearchFilters(
        keyword=row["keyword"],
        search_type=row["search_type"],
        pv_codes=split(row["pv_codes"]),
        jp_statuses=split(row["jp_statuses"]),
        jp_types=split(row["jp_types"]),
        business_sizes=split(row["business_sizes"]),
    )


def _children_for(filters: SearchFilters, provinces: list[str]) -> list[SearchFilters]:
    """Pick the next unused split dimension. Returns [] when exhausted."""
    if not filters.jp_types:
        return [
            SearchFilters(
                keyword=filters.keyword,
                search_type=filters.search_type,
                pv_codes=filters.pv_codes,
                jp_statuses=filters.jp_statuses,
                jp_types=[t],
                business_sizes=filters.business_sizes,
            )
            for t in JP_TYPES
        ]

    if not filters.pv_codes and provinces:
        return [
            SearchFilters(
                keyword=filters.keyword,
                search_type=filters.search_type,
                pv_codes=[pv],
                jp_statuses=filters.jp_statuses,
                jp_types=filters.jp_types,
                business_sizes=filters.business_sizes,
            )
            for pv in provinces
        ]

    if not filters.business_sizes:
        return [
            SearchFilters(
                keyword=filters.keyword,
                search_type=filters.search_type,
                pv_codes=filters.pv_codes,
                jp_statuses=filters.jp_statuses,
                jp_types=filters.jp_types,
                business_sizes=[sz],
            )
            for sz in BUSINESS_SIZES
        ]

    return []


async def fetch_provinces(client: AsyncDbdClient) -> list[str]:
    data = await client.get("/api/v1/commons/provinces")
    if not isinstance(data, list):
        raise ContractError("Province response must be a JSON array")
    provinces = [str(r["pvCode"]) for r in data if isinstance(r, dict) and r.get("pvCode")]
    if not provinces:
        raise ContractError("Province response contains no usable pvCode values")
    return provinces


async def province_seeds(client: AsyncDbdClient) -> list[str]:
    """Return the current registration-prefix seeds plus audited exceptions.

    Province prefixes cover the known 13-digit registration-number format, but
    they are not treated as proof of the whole population: type A shorter IDs,
    future formats and live search behavior require reconciliation separately.
    """
    provinces = await fetch_provinces(client)
    return sorted(
        {f"0{str(pv).zfill(2)}" for pv in provinces if str(pv).strip()} | set(EXTRA_PREFIX_SEEDS)
    )


def coverage_gaps() -> list[str]:
    """Observed company-number prefixes that have no matching seed bucket.

    Zero observed gaps is necessary but not sufficient proof of completeness;
    companies never returned by any search cannot appear in this query.
    """
    from ..storage.mysql.engine import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT LEFT(s.jp_no, 3) AS prefix
            FROM discovery_sightings s
            WHERE NOT EXISTS (
                SELECT 1 FROM discovery_buckets b
                WHERE b.keyword = LEFT(s.jp_no, 3)
            )
            ORDER BY prefix
            """
        )
        return [row["prefix"] for row in cur.fetchall()]


async def _count(client: AsyncDbdClient, bucket_id: int, filters: SearchFilters):
    """Fetch and archive page 1 once; it is reused if the bucket is drainable."""
    from ..storage.mysql.repositories.discovery_repository import archive_search

    requested = _now()
    data = await client.post(
        SEARCH_PATH,
        filters.payload(page=1),
        referer=SEARCH_REFERER,
        timeout=settings.dbd_count_timeout_seconds,
    )
    archive_id = await asyncio.to_thread(
        archive_search, bucket_id, 1, filters, data, requested, link_page=False
    )
    page = parse_search_response(data)
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


async def _drain_bucket(
    client,
    bucket_id,
    filters,
    pages_total,
    start_page,
    *,
    verbose,
    first_page=None,
):
    from ..storage.mysql.repositories.discovery_repository import (
        archive_search,
        completed_pages,
        fail_page,
        finish_page,
        link_search_page,
        page_progress,
    )

    update_bucket(bucket_id, status="draining", last_error=None)
    completed = await asyncio.to_thread(completed_pages, bucket_id)
    pages = [p for p in range(1, pages_total + 1) if p not in completed]

    async def one_page(page_no):
        try:
            if page_no == 1 and first_page is not None:
                archive_id, data, requested = first_page
                await asyncio.to_thread(link_search_page, bucket_id, page_no, archive_id)
            else:
                requested = _now()
                data = await client.post(
                    SEARCH_PATH, filters.payload(page=page_no), referer=SEARCH_REFERER
                )
                archive_id = await asyncio.to_thread(
                    archive_search, bucket_id, page_no, filters, data, requested
                )

            page = parse_search_response(data)
            if page.current_page != page_no or (not page.rows and page.total_items > 0):
                raise ContractError("Search page number or empty-page contract mismatch")
            profiles, rejections = _normalize_rows(page.rows)
            return await asyncio.to_thread(
                finish_page,
                bucket_id,
                page_no,
                archive_id,
                profiles,
                data,
                requested,
                rejections,
            )
        except Exception as exc:
            await asyncio.to_thread(fail_page, bucket_id, page_no, type(exc).__name__)
            return None

    fetched = written = 0
    iterator = iter(pages)
    in_flight: set[asyncio.Task] = set()

    def fill():
        while len(in_flight) < client.concurrency:
            try:
                page_no = next(iterator)
            except StopIteration:
                return
            in_flight.add(asyncio.create_task(one_page(page_no)))

    fill()
    while in_flight:
        done, in_flight = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            result = task.result()
            if result is not None:
                fetched += 1
                written += result
        fill()
        progress = await asyncio.to_thread(page_progress, bucket_id)
        update_bucket(
            bucket_id,
            pages_done=progress["done"],
            rows_seen=progress["rows"] + progress["rejected"],
        )
        if verbose:
            print(
                f"    pages {progress['done']}/{pages_total}, partial {progress['partial']}, "
                f"rows {progress['rows']}, quarantined {progress['rejected']}"
            )

    progress = await asyncio.to_thread(page_progress, bucket_id)
    complete = (
        progress["done"] == pages_total
        and progress["failed"] == 0
        and progress["partial"] == 0
    )
    if complete:
        error = None
    elif progress["partial"]:
        error = "One or more pages contain quarantined rows; fix the contract and run discover-retry"
    else:
        error = "One or more pages failed; run discover-retry"
    update_bucket(
        bucket_id,
        status="done" if complete else "failed",
        pages_done=progress["done"],
        rows_seen=progress["rows"] + progress["rejected"],
        last_error=error,
    )
    return fetched, written


async def _process_bucket(
    client: AsyncDbdClient,
    row: dict,
    provinces: list[str],
    *,
    verbose: bool,
) -> tuple[int, int]:
    bucket_id = int(row["id"])
    filters = _filters_from_row(row)

    if verbose:
        print(f"  bucket {bucket_id}: {filters.key()}")

    total_items = row["total_items"]
    pages_total = row["pages_total"]
    first_page = None

    if total_items is None:
        page, archive_id, data, requested = await _count(client, bucket_id, filters)
        total_items, pages_total = page.total_items, page.total_pages
        first_page = (archive_id, data, requested)
        update_bucket(bucket_id, total_items=total_items, pages_total=pages_total, status="counted")
        if verbose:
            print(f"    totalItems={total_items:,} pages={pages_total}")

    if total_items == 0:
        update_bucket(bucket_id, status="done")
        return 0, 0

    if total_items > MAX_ROWS_PER_QUERY:
        children = _children_for(filters, provinces)
        if not children:
            update_bucket(
                bucket_id,
                status="failed",
                last_error=(
                    f"{total_items} rows exceeds the {MAX_ROWS_PER_QUERY} ceiling "
                    "and no split dimension remains"
                ),
            )
            return 0, 0

        for child in children:
            upsert_bucket(child, parent_id=bucket_id, depth=int(row["depth"]) + 1)

        update_bucket(bucket_id, status="split")
        if verbose:
            print(f"    too large -> split into {len(children)} child bucket(s)")
        return 0, 0

    return await _drain_bucket(
        client,
        bucket_id,
        filters,
        pages_total,
        1,
        verbose=verbose,
        first_page=first_page,
    )


def validate_splits(verbose: bool = True) -> int:
    """Record, for every split bucket, whether SUM(children) == parent."""
    from ..storage.mysql.engine import connect

    checked = 0
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id, p.total_items,
                   COUNT(c.id)                              AS child_count,
                   SUM(c.total_items IS NULL)               AS uncounted,
                   COALESCE(SUM(c.total_items), 0)          AS child_sum
            FROM discovery_buckets p
            JOIN discovery_buckets c ON c.parent_id = p.id
            WHERE p.status = 'split' AND p.total_items IS NOT NULL
            GROUP BY p.id, p.total_items
            """
        )
        rows = cur.fetchall()

    for row in rows:
        if int(row["uncounted"] or 0) > 0:
            if verbose:
                print(
                    f"  bucket {row['id']}: {int(row['uncounted'])}/{int(row['child_count'])} "
                    "child bucket(s) not counted yet - validation deferred"
                )
            continue

        parent_total = int(row["total_items"])
        children = int(row["child_sum"])
        lossless = 1 if children == parent_total else 0
        update_bucket(int(row["id"]), partition_lossless=lossless)
        checked += 1

        if verbose and not lossless:
            delta = children - parent_total
            print(
                f"  bucket {row['id']}: SUM(children)={children:,} vs parent={parent_total:,} "
                f"(delta {delta:+,}); completeness is blocked until reconciled"
            )

    return checked


async def _run_work(
    client,
    seeds,
    search_type,
    max_buckets,
    verbose,
) -> DiscoveryResult:
    buckets_processed = pages_fetched = companies_written = 0
    provinces = await fetch_provinces(client)
    if verbose:
        print(f"provinces available for splitting: {len(provinces)}")

    # With no explicit seed, always upsert the current default seed set. This is
    # idempotent and lets newly audited exceptions such as 099 join an existing
    # checkpointed campaign without a reset.
    if not seeds:
        seeds = await province_seeds(client)
        if verbose:
            print(f"seeding/resuming from {len(seeds)} audited prefixes")

    for seed in seeds:
        upsert_bucket(SearchFilters(keyword=seed, search_type=search_type))

    while True:
        rows = pending_buckets(limit=25)
        if not rows:
            break
        if max_buckets is not None:
            remaining = max_buckets - buckets_processed
            if remaining <= 0:
                if verbose:
                    print(f"stopping at --max-buckets={max_buckets}")
                break
            rows = rows[:remaining]

        # Process a small bounded set of buckets concurrently. The HTTP client
        # still owns the global semaphore/rate limiter, so this removes bucket
        # head-of-line blocking without multiplying actual transport concurrency.
        bucket_parallelism = max(1, min(4, client.concurrency // 4 or 1, len(rows)))
        batch = rows[:bucket_parallelism]

        async def process_one(row):
            try:
                pages, written = await _process_bucket(client, row, provinces, verbose=verbose)
                return pages, written
            except Exception as exc:
                update_bucket(
                    int(row["id"]),
                    status="failed",
                    last_error=f"{type(exc).__name__}: {str(exc)[:300]}",
                )
                if verbose:
                    print(f"    bucket {row['id']} FAILED: {type(exc).__name__}: {str(exc)[:120]}")
                return 0, 0

        results = await asyncio.gather(*(process_one(row) for row in batch))
        buckets_processed += len(batch)
        pages_fetched += sum(pages for pages, _ in results)
        companies_written += sum(written for _, written in results)

    validate_splits(verbose)
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
) -> DiscoveryResult:
    """Enumerate companies with durable page checkpoints and explicit partial state."""
    stop = asyncio.Event()

    async with AsyncDbdClient(concurrency=concurrency, rate_per_second=rate) as client:
        async def monitor_health():
            if health is None:
                await stop.wait()
                return
            while not stop.is_set():
                await asyncio.to_thread(health)
                try:
                    await asyncio.wait_for(stop.wait(), 2)
                except TimeoutError:
                    pass

        async def work():
            try:
                return await _run_work(client, seeds, search_type, max_buckets, verbose)
            finally:
                stop.set()

        async with asyncio.TaskGroup() as group:
            group.create_task(monitor_health())
            task = group.create_task(work())

    result = task.result()
    status = discovery_summary()
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


def summary() -> dict:
    return discovery_summary()
