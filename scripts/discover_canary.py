
"""
scripts/discover_canary.py

Targeted Discovery using existing MySQL Discovery Buckets.

- Reads canary-106105.txt.
- Processes only buckets belonging to the supplied juristic numbers.
- Reuses the existing DBD Discovery API client and persistence pipeline.
- Preserves MySQL page checkpoints and supports safe resume.
- Does not create duplicate root buckets.
- Does not reset, truncate, or delete existing data.
- Does not process unrelated Discovery buckets.
- Keeps numeric splitting disabled.
- Allows existing categorical splitting when required.
- Uses DBD keyword-search semantics; API results are not guaranteed
  to be exact matches of the supplied jpNo.

PowerShell:

    python .\scripts\discover_canary.py

    python .\scripts\discover_canary.py `
        --seed-file .\canary-106105.txt `
        --concurrency 8 `
        --rate 15

    python .\scripts\discover_canary.py --dry-run

    python .\scripts\discover_canary.py --retry-failed
"""

from __future__ import annotations

import argparse
import asyncio
import math
import re
import sys
import time

from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))


from dbd.client.endpoints.search import SearchFilters
from dbd.core.settings import settings
from dbd.deep.http import FixedRateDbdClient
from dbd.pipelines import discovery
from dbd.runtime.locking import (
    assert_manager_lock,
    manager_lock,
)
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.discovery_repository import (
    get_bucket,
    update_bucket,
)


OPEN_STATUSES = frozenset({
    "pending",
    "counted",
    "draining",
})

CHUNK_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run targeted Discovery using existing Canary buckets"
        )
    )

    parser.add_argument(
        "--seed-file",
        type=Path,
        default=Path("canary-106105.txt"),
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=15.0,
    )

    parser.add_argument(
        "--bucket-workers",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--retry-failed",
        action="store_true",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = parser.parse_args()

    if args.concurrency < 1:
        parser.error("--concurrency must be positive")

    if args.bucket_workers < 1:
        parser.error("--bucket-workers must be positive")

    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("--rate must be a finite positive number")

    if args.bucket_workers > args.concurrency:
        parser.error(
            "--bucket-workers cannot exceed --concurrency"
        )

    available_connections = max(
        1,
        settings.db_pool_size - 2,
    )

    if args.bucket_workers > available_connections:
        parser.error(
            "--bucket-workers exceeds the safe MySQL pool capacity"
        )

    return args


def read_seed_file(path: Path) -> list[str]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Seed file not found: {path.resolve()}"
        )

    unique = {}

    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:

        for line_number, raw in enumerate(handle, 1):
            number = raw.strip()

            if not number or number.startswith("#"):
                continue

            if not re.fullmatch(
                r"[0-9]{11,13}",
                number,
                flags=re.ASCII,
            ):
                raise ValueError(
                    f"Invalid juristic number at line "
                    f"{line_number}: {number!r}"
                )

            unique[number] = None

    if not unique:
        raise ValueError(
            "The seed file contains no valid juristic numbers"
        )

    return list(unique)


def chunks(
    values: list[str],
    size: int = CHUNK_SIZE,
):
    for position in range(0, len(values), size):
        yield values[position:position + size]


def load_buckets(
    numbers: list[str],
) -> dict[str, list[dict]]:
    if not numbers:
        return {}

    placeholders = ",".join(
        "%s" for _ in numbers
    )

    sql = f"""
        SELECT *
        FROM discovery_buckets
        WHERE keyword IN ({placeholders})
    """

    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                tuple(numbers),
            )

            rows = cursor.fetchall()

    grouped = defaultdict(list)

    for row in rows:
        keyword = str(row["keyword"])
        grouped[keyword].append(row)

    return dict(grouped)


def find_root(
    keyword: str,
    rows: list[dict],
) -> dict | None:
    expected_key = SearchFilters(
        keyword=keyword,
    ).key()

    roots = [
        row
        for row in rows
        if row["parent_id"] is None
        and str(row["bucket_key"]) == expected_key
    ]

    if len(roots) > 1:
        raise RuntimeError(
            f"Duplicate canonical root buckets: {keyword}"
        )

    return roots[0] if roots else None


def build_bucket_tree(
    keyword: str,
    rows: list[dict],
) -> list[dict]:
    root = find_root(
        keyword,
        rows,
    )

    if root is None:
        raise RuntimeError(
            f"Missing Canary root bucket: {keyword}"
        )

    children_by_parent = defaultdict(list)

    for row in rows:
        parent_id = row["parent_id"]

        if parent_id is not None:
            children_by_parent[
                int(parent_id)
            ].append(row)

    result = []
    visited = set()
    visiting = set()

    def traverse(row: dict):
        bucket_id = int(row["id"])

        if bucket_id in visiting:
            raise RuntimeError(
                f"Bucket ancestry cycle: {bucket_id}"
            )

        if bucket_id in visited:
            return

        visiting.add(bucket_id)
        visited.add(bucket_id)

        result.append(row)

        children = children_by_parent.get(
            bucket_id,
            [],
        )

        if row["status"] == "split" and not children:
            raise RuntimeError(
                f"Split bucket {bucket_id} has no children"
            )

        if children and row["status"] != "split":
            raise RuntimeError(
                f"Bucket {bucket_id} has children "
                "but is not marked split"
            )

        for child in sorted(
            children,
            key=lambda item: int(item["id"]),
        ):
            traverse(child)

        visiting.remove(bucket_id)

    traverse(root)

    return result


def preflight(
    numbers: list[str],
) -> dict[str, int]:
    found = set()
    states = defaultdict(int)

    for group in chunks(numbers):
        grouped = load_buckets(group)

        for number in group:
            root = find_root(
                number,
                grouped.get(number, []),
            )

            if root is not None:
                found.add(number)
                states[str(root["status"])] += 1

    missing = [
        number
        for number in numbers
        if number not in found
    ]

    if missing:
        examples = ", ".join(missing[:12])

        raise RuntimeError(
            f"{len(missing):,} Canary root buckets "
            f"are missing. Examples: {examples}. "
            "Aborting without creating or modifying them."
        )

    return dict(states)


def assert_deep_collector_inactive():
    with connect() as conn:
        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT IS_USED_LOCK(
                    CONCAT(
                        DATABASE(),
                        ':dbd-deep-collector'
                    )
                ) AS holder
            """)

            result = cursor.fetchone()

            if result["holder"] is not None:
                raise RuntimeError(
                    "A Deep collector is active "
                    "against the selected database"
                )


@dataclass
class Statistics:
    total: int

    settled: int = 0
    completed: int = 0
    unresolved: int = 0

    buckets_processed: int = 0
    pages_fetched: int = 0
    rows_written: int = 0

    started_at: float = 0.0

    def display(
        self,
        *,
        force: bool = False,
    ):
        if not force and self.settled % 100 != 0:
            return

        elapsed = max(
            0.01,
            time.monotonic() - self.started_at,
        )

        speed = self.settled / elapsed

        remaining = self.total - self.settled

        eta = (
            remaining / speed
            if speed > 0
            else math.inf
        )

        eta_text = (
            f"{eta / 60:.1f}m"
            if math.isfinite(eta)
            else "--"
        )

        percent = (
            self.settled / self.total * 100
            if self.total
            else 0
        )

        print(
            f"CANARY "
            f"{self.settled:,}/{self.total:,} "
            f"({percent:.2f}%) "
            f"| done={self.completed:,} "
            f"| unresolved={self.unresolved:,} "
            f"| buckets={self.buckets_processed:,} "
            f"| pages={self.pages_fetched:,} "
            f"| rows={self.rows_written:,} "
            f"| avg={speed:.2f}/s "
            f"| ETA={eta_text}",
            flush=True,
        )


async def process_keyword(
    keyword: str,
    initial_rows: list[dict],
    *,
    client: FixedRateDbdClient,
    provinces: list[str],
    status_codes: list[str],
    retry_failed: bool,
    statistics: Statistics,
) -> bool:

    attempted = set()
    master_cache = {}

    rows = initial_rows

    while True:
        tree = build_bucket_tree(
            keyword,
            rows,
        )

        leaves = [
            row
            for row in tree
            if row["status"] != "split"
        ]

        candidates = [
            row
            for row in leaves
            if (
                row["status"] in OPEN_STATUSES
                or (
                    retry_failed
                    and row["status"] == "failed"
                )
            )
            and int(row["id"]) not in attempted
        ]

        if not candidates:
            return (
                bool(leaves)
                and all(
                    row["status"] == "done"
                    for row in leaves
                )
            )

        row = candidates[0]

        bucket_id = int(row["id"])

        attempted.add(bucket_id)

        latest = await asyncio.to_thread(
            get_bucket,
            bucket_id,
        )

        if latest is None:
            raise RuntimeError(
                f"Bucket disappeared: {bucket_id}"
            )

        # Do not interfere with active distributed leases.
        lease_owner = latest.get("lease_owner")
        lease_expiry = latest.get("lease_expires_at")

        if lease_owner and lease_expiry is not None:
            from datetime import datetime, timezone

            now = datetime.now(
                timezone.utc
            ).replace(tzinfo=None)

            if lease_expiry > now:
                raise RuntimeError(
                    f"Bucket {bucket_id} is leased "
                    "by another worker"
                )

        if latest["status"] in ("done", "split"):
            refreshed = await asyncio.to_thread(
                load_buckets,
                [keyword],
            )

            rows = refreshed.get(keyword, [])
            continue

        if (
            latest["status"] == "failed"
            and not retry_failed
        ):
            refreshed = await asyncio.to_thread(
                load_buckets,
                [keyword],
            )

            rows = refreshed.get(keyword, [])
            continue

        statistics.buckets_processed += 1

        try:
            pages, written = (
                await discovery._process_bucket(
                    client,
                    latest,
                    provinces,
                    master_cache=master_cache,
                    progress_line=None,
                    status_codes=status_codes,
                    numeric_allowed=False,
                )
            )

            statistics.pages_fetched += pages
            statistics.rows_written += written

        except Exception as exc:
            from pymysql import err as mysql_errors
            from sqlalchemy.exc import DBAPIError

            # Database failures must stop the runner.
            # Checkpoints already committed remain intact.
            if isinstance(
                exc,
                (
                    mysql_errors.Error,
                    DBAPIError,
                    ConnectionError,
                ),
            ):
                raise

            error_message = (
                f"{type(exc).__name__}: {str(exc)[:300]}"
            )

            try:
                await asyncio.to_thread(
                    update_bucket,
                    bucket_id,
                    status="failed",
                    last_error=error_message,
                )

            except Exception:
                raise RuntimeError(
                    f"Unable to safely record failure "
                    f"for bucket {bucket_id}"
                ) from exc

            print(
                f"FAILED "
                f"bucket={bucket_id} "
                f"keyword={keyword} "
                f"| {error_message}",
                flush=True,
            )

        # Refresh from MySQL, including any newly split
        # categorical child buckets belonging to this keyword.
        refreshed = await asyncio.to_thread(
            load_buckets,
            [keyword],
        )

        rows = refreshed.get(keyword, [])


async def monitor_manager_lock(
    connection,
    parent_task: asyncio.Task,
):
    while True:
        await asyncio.sleep(10)

        try:
            await asyncio.to_thread(
                assert_manager_lock,
                connection,
            )

        except Exception as exc:
            print(
                f"FATAL | Manager lock lost: {exc}",
                file=sys.stderr,
                flush=True,
            )

            parent_task.cancel()
            return


async def execute(
    args: argparse.Namespace,
    numbers: list[str],
) -> int:

    print(
        f"CANARY DISCOVERY | "
        f"database={settings.db_name} "
        f"| targets={len(numbers):,} "
        f"| concurrency={args.concurrency} "
        f"| rate={args.rate:g}/s "
        f"| bucket-workers={args.bucket_workers} "
        f"| numeric-split=OFF",
        flush=True,
    )

    if args.dry_run:
        states = await asyncio.to_thread(
            preflight,
            numbers,
        )

        print(
            f"DRY RUN | "
            f"registered={len(numbers):,} "
            f"| states={states}",
            flush=True,
        )

        return 0

    # Exclusive non-distributed DBD API manager.
    # No second normal Discovery process may share this lock.
    with manager_lock() as manager_connection:

        await asyncio.to_thread(
            assert_manager_lock,
            manager_connection,
        )

        await asyncio.to_thread(
            assert_deep_collector_inactive,
        )

        states = await asyncio.to_thread(
            preflight,
            numbers,
        )

        print(
            f"PREFLIGHT | {states}",
            flush=True,
        )

        statistics = Statistics(
            total=len(numbers),
            started_at=time.monotonic(),
        )

        parent_task = asyncio.current_task()

        if parent_task is None:
            raise RuntimeError(
                "Cannot identify the Discovery manager task"
            )

        heartbeat = asyncio.create_task(
            monitor_manager_lock(
                manager_connection,
                parent_task,
            )
        )

        try:
            async with FixedRateDbdClient(
                concurrency=args.concurrency,
                rate_per_second=args.rate,
                max_retries=settings.dbd_http_retries,
                timeout=settings.request_timeout,
            ) as client:

                provinces = (
                    await discovery.fetch_provinces(client)
                )

                status_codes = await asyncio.to_thread(
                    discovery.observed_status_codes,
                )

                for number_group in chunks(numbers):

                    grouped = await asyncio.to_thread(
                        load_buckets,
                        number_group,
                    )

                    queue = asyncio.Queue()

                    for number in number_group:
                        queue.put_nowait(number)

                    async def worker():
                        while True:
                            try:
                                keyword = queue.get_nowait()

                            except asyncio.QueueEmpty:
                                return

                            try:
                                completed = (
                                    await process_keyword(
                                        keyword,
                                        grouped.get(keyword, []),
                                        client=client,
                                        provinces=provinces,
                                        status_codes=status_codes,
                                        retry_failed=args.retry_failed,
                                        statistics=statistics,
                                    )
                                )

                                if completed:
                                    statistics.completed += 1
                                else:
                                    statistics.unresolved += 1

                            except Exception as exc:
                                raise RuntimeError(
                                    f"Canary {keyword}: {exc}"
                                ) from exc

                            finally:
                                statistics.settled += 1

                                statistics.display()

                                queue.task_done()

                    async with asyncio.TaskGroup() as tasks:

                        worker_count = min(
                            args.bucket_workers,
                            len(number_group),
                        )

                        for _ in range(worker_count):
                            tasks.create_task(worker())

        finally:
            heartbeat.cancel()

            with suppress(asyncio.CancelledError):
                await heartbeat

        statistics.display(force=True)

        print(
            f"CANARY FINISHED "
            f"| database={settings.db_name} "
            f"| done={statistics.completed:,} "
            f"| unresolved={statistics.unresolved:,} "
            f"| buckets={statistics.buckets_processed:,} "
            f"| pages={statistics.pages_fetched:,} "
            f"| requests={client.requests_made:,}",
            flush=True,
        )

        return (
            0
            if statistics.unresolved == 0
            else 2
        )


def main() -> int:
    try:
        args = parse_args()

        numbers = read_seed_file(
            args.seed_file
        )

        return asyncio.run(
            execute(
                args,
                numbers,
            )
        )

    except KeyboardInterrupt:
        print(
            "\nCANARY STOPPED | "
            "Committed MySQL checkpoints preserved",
            flush=True,
        )

        return 130

    except (Exception, asyncio.CancelledError) as exc:
        print(
            f"CANARY STOPPED | "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
