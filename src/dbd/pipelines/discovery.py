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
from ..core.settings import settings
from ..storage.mysql.repositories.discovery_repository import (
    discovery_summary,
    pending_buckets,
    update_bucket,
    upsert_bucket,
)

# Split dimensions, applied in this order. jpType is first because the
# reference implementation validated it as lossless (the eight type counts
# summed exactly to the unfiltered parent count), whereas numeric-prefix
# splitting provably is not.
JP_TYPES = ("0", "2", "3", "5", "7", "8", "9", "A")
BUSINESS_SIZES = ("S", "M", "L")


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
        return []
    return [str(r["pvCode"]) for r in data if isinstance(r, dict) and r.get("pvCode")]


async def _count(client: AsyncDbdClient, filters: SearchFilters) -> tuple[int, int]:
    """Page 1 doubles as the count probe, so nothing is wasted."""
    data = await client.post(
        SEARCH_PATH,
        filters.payload(page=1),
        referer=SEARCH_REFERER,
        timeout=settings.dbd_count_timeout_seconds,
    )
    page = parse_search_response(data)
    return page.total_items, page.total_pages


async def _drain_bucket(client, bucket_id, filters, pages_total, start_page, *, verbose):
    from ..core.exceptions import ContractError
    from ..storage.mysql.repositories.discovery_repository import (
        archive_search,
        completed_pages,
        fail_page,
        finish_page,
        page_progress,
    )

    update_bucket(bucket_id, status="draining", last_error=None)
    completed = await asyncio.to_thread(completed_pages, bucket_id)
    # Never derive a page number from a count of successful concurrent requests.
    pages = [p for p in range(1, pages_total + 1) if p not in completed]

    async def one_page(page_no):
        try:
            requested = _now()
            data = await client.post(SEARCH_PATH, filters.payload(page=page_no), referer=SEARCH_REFERER)
            archive_id = await asyncio.to_thread(archive_search, bucket_id, page_no, filters, data, requested)
            page = parse_search_response(data)
            if page.current_page != page_no or (not page.rows and page.total_items > 0):
                raise ContractError("Search page number or empty-page contract mismatch")
            profiles = [normalize(row) for row in page.rows]
            return await asyncio.to_thread(
                finish_page, bucket_id, page_no, archive_id, profiles, data, requested
            )
        except Exception as exc:
            await asyncio.to_thread(fail_page, bucket_id, page_no, type(exc).__name__)
            return None

    fetched = written = 0
    for offset in range(0, len(pages), client.concurrency):
        results = await asyncio.gather(*(one_page(p) for p in pages[offset : offset + client.concurrency]))
        fetched += sum(r is not None for r in results)
        written += sum(r for r in results if r is not None)
        progress = await asyncio.to_thread(page_progress, bucket_id)
        update_bucket(bucket_id, pages_done=progress["done"], rows_seen=progress["rows"])
        if verbose:
            print(f"    pages {progress['done']}/{pages_total}, rows {progress['rows']}")
    progress = await asyncio.to_thread(page_progress, bucket_id)
    complete = progress["done"] == pages_total and progress["failed"] == 0
    update_bucket(
        bucket_id,
        status="done" if complete else "failed",
        pages_done=progress["done"],
        rows_seen=progress["rows"],
        last_error=None if complete else "One or more pages failed; run discover-retry",
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

    if total_items is None:
        total_items, pages_total = await _count(client, filters)
        update_bucket(bucket_id, total_items=total_items, pages_total=pages_total, status="counted")
        if verbose:
            print(f"    totalItems={total_items:,} pages={pages_total}")

    if total_items == 0:
        update_bucket(bucket_id, status="done")
        return 0, 0

    if total_items > MAX_ROWS_PER_QUERY:
        children = _children_for(filters, provinces)
        if not children:
            # Out of split dimensions: this bucket cannot be fully read.
            update_bucket(
                bucket_id,
                status="failed",
                last_error=f"{total_items} rows exceeds the {MAX_ROWS_PER_QUERY} ceiling and no split dimension remains",
            )
            return 0, 0

        for child in children:
            upsert_bucket(child, parent_id=bucket_id, depth=int(row["depth"]) + 1)

        update_bucket(bucket_id, status="split")
        if verbose:
            print(f"    too large -> split into {len(children)} child bucket(s)")
        return 0, 0

    return await _drain_bucket(client, bucket_id, filters, pages_total, 1, verbose=verbose)


def validate_splits(verbose: bool = True) -> int:
    """Record, for every split bucket, whether SUM(children) == parent.

    A mismatch does not stop ingestion, but it means that partition is not
    lossless and coverage below it cannot be claimed as complete.
    """
    from ..storage.mysql.engine import connect

    checked = 0
    with connect() as conn, conn.cursor() as cur:
        # A partition can only be judged once every child has been counted.
        # Comparing against a partial sum would report a perfectly good split
        # as lossy, which would discredit the one guarantee this check exists
        # to provide.
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
            # The counts are taken at different moments against live data, so a
            # delta of a row or two usually means a registration changed between
            # the parent count and the child counts, not a broken partition.
            # A large delta means the split really is losing or duplicating rows.
            print(
                f"  bucket {row['id']}: SUM(children)={children:,} vs parent={parent_total:,} "
                f"(delta {delta:+,})"
            )

    return checked


async def run(
    seeds: list[str],
    *,
    search_type: str = "",
    concurrency: int | None = None,
    rate: float | None = None,
    max_buckets: int | None = None,
    verbose: bool = True,
) -> DiscoveryResult:
    """Enumerate companies from seed keywords, splitting oversized buckets.

    One search page carries ten complete company profiles โ€” the same field set
    the per-company profile endpoint returns โ€” so this writes full company rows
    rather than only a list of registration numbers.
    """
    buckets_processed = pages_fetched = companies_written = 0

    async with AsyncDbdClient(concurrency=concurrency, rate_per_second=rate) as client:
        provinces = await fetch_provinces(client)
        if verbose:
            print(f"provinces available for splitting: {len(provinces)}")

        for seed in seeds:
            upsert_bucket(SearchFilters(keyword=seed, search_type=search_type))

        while True:
            rows = pending_buckets(limit=25)
            if not rows:
                break

            for row in rows:
                if max_buckets is not None and buckets_processed >= max_buckets:
                    if verbose:
                        print(f"stopping at --max-buckets={max_buckets}")
                    validate_splits(verbose)
                    return DiscoveryResult(
                        buckets_processed, pages_fetched, companies_written, client.requests_made
                    )

                try:
                    pages, written = await _process_bucket(client, row, provinces, verbose=verbose)
                except Exception as exc:
                    update_bucket(int(row["id"]), status="failed", last_error=f"{type(exc).__name__}: {exc}")
                    if verbose:
                        print(f"    bucket {row['id']} FAILED: {type(exc).__name__}: {str(exc)[:120]}")
                    buckets_processed += 1
                    continue

                buckets_processed += 1
                pages_fetched += pages
                companies_written += written

        validate_splits(verbose)

    return DiscoveryResult(buckets_processed, pages_fetched, companies_written, client.requests_made)


def summary() -> dict:
    return discovery_summary()
