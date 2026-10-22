
# scripts/discover_canary_direct.py
#
# Bucket-free, checkpointed DBD Canary Discovery.
#
# DRY RUN:
# python .\scripts\discover_canary_direct.py --seed-file .\canary-7320.txt --dry-run
#
# RUN:
# python .\scripts\discover_canary_direct.py --seed-file .\canary-7320.txt --concurrency 12 --rate 20 --db-writers 3
#
# RETRY FAILED:
# python .\scripts\discover_canary_direct.py --seed-file .\canary-7320.txt --concurrency 12 --rate 20 --retry-failed
#
# CONTINUE:
# Run the same command again.
#
# STOP:
# Ctrl+C. Active requests and database commits drain before exit.
#
# AFTER DISCOVERY:
# dbd-deep prepare
# dbd-deep collect --concurrency 12 --rate 20
#
# Database behavior:
# - Does not modify discovery_buckets or discovery_pages.
# - Does not modify discovery_sightings.
# - Never deletes existing company data.
# - Never overwrites dbd_company_deep.
# - Inserts only missing company/master identities.
# - Persists Search responses in response_archive.
# - Commits each company's records and checkpoint atomically.
# - Creates only dbd_canary_direct_checkpoint.

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import re
import signal
import sys
import time

from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from dbd.client.endpoints.search import (
    ITEMS_PER_PAGE,
    MAX_PAGE,
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    parse_search_response,
)
from dbd.contracts.company import normalize
from dbd.core.settings import settings
from dbd.deep.bridge import MASTER_JSON_SQL
from dbd.deep.http import FixedRateDbdClient
from dbd.runtime.delta import (
    MASTER_HASH_VERSION,
    master_fingerprint,
)
from dbd.runtime.locking import (
    assert_manager_lock,
    manager_lock,
)
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.company_repository import (
    company_upsert_sql,
    company_values,
)


CHECKPOINT_TABLE = "dbd_canary_direct_checkpoint"

TERMINAL_STATES = {
    "DONE",
    "ALREADY_PRESENT",
    "NOT_FOUND",
}

RETRY_STATES = {
    "FAILED",
    "UNRESOLVED",
}

FATAL_HTTP_NAMES = {
    "DbdAuthError",
    "DbdForbiddenError",
    "DeepTransientHttpError",
}

FATAL_DB_CODES = {
    1040,
    1045,
    1205,
    1213,
    2002,
    2003,
    2006,
    2013,
}


class IncompleteSearch(RuntimeError):
    pass


class InfrastructureFailure(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_args():
    p = argparse.ArgumentParser(
        description="DBD Canary Direct Discovery (no buckets)"
    )

    p.add_argument(
        "--seed-file",
        type=Path,
        default=Path("canary-7320.txt"),
    )
    p.add_argument("--concurrency", type=int, default=12)
    p.add_argument("--rate", type=float, default=20.0)
    p.add_argument("--db-writers", type=int, default=3)
    p.add_argument(
        "--http-retries",
        type=int,
        default=settings.dbd_http_retries,
    )
    p.add_argument("--retry-failed", action="store_true")
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--progress-interval",
        type=float,
        default=1.0,
    )

    args = p.parse_args()

    if not 1 <= args.concurrency <= 256:
        p.error("--concurrency must be 1..256")

    if not math.isfinite(args.rate) or not 0 < args.rate <= 1000:
        p.error("--rate must be finite and > 0, <= 1000")

    if not 1 <= args.db_writers <= max(1, settings.db_pool_size - 2):
        p.error("--db-writers exceeds configured DB pool budget")

    if not 0 <= args.http_retries <= 10:
        p.error("--http-retries must be 0..10")

    if not 0.25 <= args.progress_interval <= 60:
        p.error("--progress-interval must be 0.25..60")

    return args


def read_numbers(path):
    if not path.is_file():
        raise FileNotFoundError(path)

    numbers = []
    seen = set()
    duplicates = 0

    with path.open("r", encoding="utf-8-sig") as f:
        for line_no, line in enumerate(f, 1):
            number = line.strip()

            if not number or number.startswith("#"):
                continue

            if not re.fullmatch(r"[0-9]{11,13}", number):
                raise ValueError(
                    f"Invalid juristic number at line {line_no}: "
                    f"{number!r}"
                )

            if number in seen:
                duplicates += 1
                continue

            seen.add(number)
            numbers.append(number)

    if not numbers:
        raise ValueError("Seed file contains no juristic numbers")

    return numbers, duplicates


def chunks(items, size=400):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def get_value(row, key):
    """
    Handle database drivers that preserve MySQL column-name case.

    Examples:
      TABLE_NAME / table_name
      COLUMN_NAME / column_name
      STATUS / status
    """
    if row is None:
        return None

    if key in row:
        return row[key]

    target = key.lower()

    for actual_key, value in row.items():
        if str(actual_key).lower() == target:
            return value

    raise KeyError(
        f"Column {key!r} missing. Available: {list(row)}"
    )


def schema_preflight():
    required_tables = {
        "companies",
        "company_master",
        "dbd_company_master",
        "dbd_company_deep",
        "response_archive",
    }

    requirements = {
        "companies": {
            "jp_type_code",
            "jp_no",
            "latest_archive_id",
        },
        "company_master": {
            "jp_type_code",
            "jp_no",
            "master_hash",
            "master_hash_version",
            "latest_search_archive_id",
        },
        "dbd_company_master": {
            "jp_type_code",
            "jp_no",
            "master_data",
            "first_seen_at",
            "last_seen_at",
        },
        "dbd_company_deep": {
            "jp_type_code",
            "jp_no",
        },
        "response_archive": {
            "id",
            "endpoint",
            "request_path",
            "request_context",
            "payload",
            "payload_sha256",
        },
    }

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    TABLE_NAME AS table_name,
                    COLUMN_NAME AS column_name
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME IN (
                      'companies',
                      'company_master',
                      'dbd_company_master',
                      'dbd_company_deep',
                      'response_archive'
                  )
                """
            )

            columns = {}

            for row in cur.fetchall():
                table = str(get_value(row, "table_name"))
                column = str(get_value(row, "column_name"))

                columns.setdefault(table, set()).add(column)

            missing_tables = required_tables - set(columns)

            if missing_tables:
                raise RuntimeError(
                    "Missing DBD tables: "
                    + ", ".join(sorted(missing_tables))
                )

            for table, required in requirements.items():
                missing = required - columns.get(table, set())

                if missing:
                    raise RuntimeError(
                        f"Missing {table} columns: "
                        + ", ".join(sorted(missing))
                    )

    return columns


def checkpoint_exists():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 AS found
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME=%s
                LIMIT 1
                """,
                (CHECKPOINT_TABLE,),
            )

            return cur.fetchone() is not None


def create_checkpoint():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
        jp_no VARCHAR(20) NOT NULL,

        status ENUM(
            'PENDING',
            'DONE',
            'ALREADY_PRESENT',
            'NOT_FOUND',
            'FAILED',
            'UNRESOLVED'
        ) NOT NULL DEFAULT 'PENDING',

        attempts INT UNSIGNED NOT NULL DEFAULT 0,

        matches_found INT UNSIGNED NOT NULL DEFAULT 0,
        pages_checked INT UNSIGNED NOT NULL DEFAULT 0,

        last_error TEXT NULL,

        created_at DATETIME(6) NOT NULL
            DEFAULT CURRENT_TIMESTAMP(6),

        updated_at DATETIME(6) NOT NULL
            DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),

        completed_at DATETIME(6) NULL,

        PRIMARY KEY (jp_no),

        KEY idx_canary_direct_status (
            status,
            updated_at
        )
    )
    ENGINE=InnoDB
    DEFAULT CHARACTER SET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    """

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)

        conn.commit()


def register_numbers(numbers):
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                for group in chunks(numbers):
                    cur.executemany(
                        f"""
                        INSERT IGNORE INTO {CHECKPOINT_TABLE}
                            (jp_no, status)
                        VALUES (%s, 'PENDING')
                        """,
                        [(number,) for number in group],
                    )

            conn.commit()

        except BaseException:
            conn.rollback()
            raise


def read_checkpoints(numbers):
    result = {}

    if not checkpoint_exists():
        return result

    with connect() as conn:
        with conn.cursor() as cur:
            for group in chunks(numbers):
                marks = ",".join("%s" for _ in group)

                cur.execute(
                    f"""
                    SELECT jp_no, status, attempts,
                           matches_found, pages_checked,
                           last_error
                    FROM {CHECKPOINT_TABLE}
                    WHERE jp_no IN ({marks})
                    """,
                    tuple(group),
                )

                for row in cur.fetchall():
                    number = str(get_value(row, "jp_no"))
                    result[number] = dict(row)

    return result


def existing_numbers(numbers):
    """
    An identity already in Deep or Deep Master needs no new Search
    projection unless --refresh is explicitly requested.
    """
    result = set()

    with connect() as conn:
        with conn.cursor() as cur:
            for group in chunks(numbers):
                marks = ",".join("%s" for _ in group)

                cur.execute(
                    f"""
                    SELECT jp_no
                    FROM dbd_company_master
                    WHERE jp_no IN ({marks})

                    UNION

                    SELECT jp_no
                    FROM dbd_company_deep
                    WHERE jp_no IN ({marks})
                    """,
                    tuple(group) + tuple(group),
                )

                for row in cur.fetchall():
                    result.add(str(get_value(row, "jp_no")))

    return result


def record_existing(number):
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {CHECKPOINT_TABLE}
                    SET status='ALREADY_PRESENT',
                        matches_found=1,
                        last_error=NULL,
                        completed_at=UTC_TIMESTAMP(6)
                    WHERE jp_no=%s
                      AND status NOT IN (
                          'DONE',
                          'ALREADY_PRESENT'
                      )
                    """,
                    (number,),
                )

            conn.commit()

        except BaseException:
            conn.rollback()
            raise


def record_failure(number, message, pages, unresolved):
    state = "UNRESOLVED" if unresolved else "FAILED"

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {CHECKPOINT_TABLE}
                    SET status=%s,
                        attempts=attempts+1,
                        pages_checked=%s,
                        last_error=%s,
                        completed_at=NULL
                    WHERE jp_no=%s
                      AND status NOT IN (
                          'DONE',
                          'ALREADY_PRESENT'
                      )
                    """,
                    (
                        state,
                        pages,
                        str(message)[:2000],
                        number,
                    ),
                )

            conn.commit()

        except BaseException:
            conn.rollback()
            raise


def record_not_found(number, pages):
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {CHECKPOINT_TABLE}
                    SET status='NOT_FOUND',
                        attempts=attempts+1,
                        matches_found=0,
                        pages_checked=%s,
                        last_error=NULL,
                        completed_at=UTC_TIMESTAMP(6)
                    WHERE jp_no=%s
                    """,
                    (pages, number),
                )

            conn.commit()

        except BaseException:
            conn.rollback()
            raise


def archive_search_page(
    cur,
    number,
    page_number,
    data,
    requested_at,
):
    payload = json.dumps(
        data,
        ensure_ascii=False,
        default=str,
    )

    digest = hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()

    body = SearchFilters(
        keyword=number
    ).payload(page=page_number)

    context = json.dumps(
        {
            "method": "POST",
            "body": body,
            "purpose": "direct_canary",
        },
        ensure_ascii=False,
        default=str,
    )

    cur.execute(
        """
        INSERT INTO response_archive (
            endpoint,
            request_path,
            request_context,
            http_status,
            requested_at,
            received_at,
            payload,
            payload_sha256
        )
        VALUES (
            'search_page',
            %s,
            %s,
            200,
            %s,
            UTC_TIMESTAMP(6),
            %s,
            %s
        )
        """,
        (
            SEARCH_PATH,
            context,
            requested_at,
            payload,
            digest,
        ),
    )

    return int(cur.lastrowid)


COMPANY_INSERT_ONLY = (
    company_upsert_sql()
    .split(" ON DUPLICATE KEY UPDATE", 1)[0]
    .replace("INSERT INTO", "INSERT IGNORE INTO", 1)
)


LEGACY_MASTER_INSERT = """
INSERT IGNORE INTO company_master (
    jp_no,
    jp_type_code,
    observed_jp_stat_code,
    observed_status_at,
    master_hash,
    master_hash_version,
    last_master_changed_at,
    latest_search_archive_id,
    first_seen_at,
    last_seen_at
)
VALUES (
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s
)
"""


DEEP_MASTER_INSERT = f"""
INSERT IGNORE INTO dbd_company_master (
    jp_type_code,
    jp_no,
    jp_name,
    jp_name_e,
    jp_stat_code,
    jp_stat_desc,
    jp_type_desc,
    address,
    pv_code,
    pv_desc,
    ampur_code,
    ampur_desc,
    tumbon_code,
    tumbon_desc,
    zip_code,
    business_size_code,
    business_type_code,
    business_type_desc,
    setup_obj_code,
    setup_obj_desc,
    submit_obj_code,
    submit_obj_desc,
    cap_amt,
    paid_amt,
    total_asset,
    total_equity,
    total_income,
    net_profit,
    submit_fin_year,
    master_data,
    first_seen_at,
    last_seen_at
)
SELECT
    c.jp_type_code,
    c.jp_no,
    c.jp_name,
    c.jp_name_e,
    COALESCE(cm.observed_jp_stat_code,c.jp_stat_code),
    c.jp_stat_desc,
    c.jp_type_desc,
    c.address,
    c.pv_code,
    c.pv_desc,
    c.ampur_code,
    c.ampur_desc,
    c.tumbon_code,
    c.tumbon_desc,
    c.zip_code,
    c.business_size_code,
    c.business_type_code,
    c.business_type_desc,
    c.setup_obj_code,
    c.setup_obj_desc,
    c.submit_obj_code,
    c.submit_obj_desc,
    c.cap_amt,
    c.paid_amt,
    c.total_asset,
    c.total_equity,
    c.total_income,
    c.net_profit,
    c.submit_fin_year,
    {MASTER_JSON_SQL},
    %s,
    %s
FROM companies c
JOIN company_master cm
  ON cm.jp_type_code=c.jp_type_code
 AND cm.jp_no=c.jp_no
WHERE c.jp_type_code=%s
  AND c.jp_no=%s
  AND NOT EXISTS (
      SELECT 1
      FROM dbd_company_deep d
      WHERE d.jp_type_code=c.jp_type_code
        AND d.jp_no=c.jp_no
  )
"""


def save_matches(number, matches, pages):
    """
    One transaction:
      1. Archive complete Search results.
      2. Insert missing company projections.
      3. Insert missing legacy Master projections.
      4. Insert missing Deep Master projections.
      5. Verify identity in Master or Deep.
      6. Commit per-number checkpoint.

    No bucket writes.
    No Deep endpoint result writes.
    """

    if not matches:
        raise ValueError("Cannot persist an empty match collection")

    identities = set()

    for profile in matches:
        if str(profile.jp_no) != number:
            raise ValueError(
                f"Identity mismatch for {number}"
            )

        identity = (
            str(profile.jp_type_code),
            str(profile.jp_no),
        )

        if identity in identities:
            raise IncompleteSearch(
                f"Repeated exact identity: {identity}"
            )

        identities.add(identity)

    requested_at = utc_now()

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                archive_ids = []

                for page_number, payload in pages:
                    archive_ids.append(
                        archive_search_page(
                            cur,
                            number,
                            page_number,
                            payload,
                            requested_at,
                        )
                    )

                archive_id = (
                    archive_ids[-1]
                    if archive_ids
                    else None
                )

                for profile in matches:
                    cur.execute(
                        COMPANY_INSERT_ONLY,
                        company_values(
                            profile,
                            None,
                            requested_at,
                            archive_id,
                        ),
                    )

                    cur.execute(
                        LEGACY_MASTER_INSERT,
                        (
                            profile.jp_no,
                            profile.jp_type_code,
                            profile.jp_stat_code,
                            (
                                requested_at
                                if profile.jp_stat_code is not None
                                else None
                            ),
                            master_fingerprint(profile),
                            MASTER_HASH_VERSION,
                            requested_at,
                            archive_id,
                            requested_at,
                            requested_at,
                        ),
                    )

                    cur.execute(
                        DEEP_MASTER_INSERT,
                        (
                            requested_at,
                            requested_at,
                            profile.jp_type_code,
                            profile.jp_no,
                        ),
                    )

                    cur.execute(
                        """
                        SELECT (
                            EXISTS (
                                SELECT 1
                                FROM dbd_company_master
                                WHERE jp_type_code=%s
                                  AND jp_no=%s
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM dbd_company_deep
                                WHERE jp_type_code=%s
                                  AND jp_no=%s
                            )
                        ) AS present
                        """,
                        (
                            profile.jp_type_code,
                            profile.jp_no,
                            profile.jp_type_code,
                            profile.jp_no,
                        ),
                    )

                    present = get_value(
                        cur.fetchone(),
                        "present",
                    )

                    if not bool(present):
                        raise RuntimeError(
                            "Master/Deep insertion postcondition "
                            f"failed: {profile.jp_type_code}/{profile.jp_no}"
                        )

                cur.execute(
                    f"""
                    UPDATE {CHECKPOINT_TABLE}
                    SET status='DONE',
                        attempts=attempts+1,
                        matches_found=%s,
                        pages_checked=%s,
                        last_error=NULL,
                        completed_at=UTC_TIMESTAMP(6)
                    WHERE jp_no=%s
                    """,
                    (
                        len(matches),
                        len(pages),
                        number,
                    ),
                )

                if cur.rowcount != 1:
                    raise RuntimeError(
                        f"Checkpoint update failed: {number}"
                    )

            conn.commit()

        except BaseException:
            conn.rollback()
            raise


async def search_exact(client, number, stop_event):
    """
    Exhaust every reported Search page.

    The API is keyword search, not guaranteed exact lookup.
    Never classify a partial search as NOT_FOUND.
    """

    filters = SearchFilters(keyword=number)

    expected_items = None
    expected_pages = None

    matches = {}
    pages = []

    page_no = 1

    while True:
        if stop_event.is_set():
            raise asyncio.CancelledError()

        response = await client.post(
            SEARCH_PATH,
            filters.payload(page=page_no),
            referer=SEARCH_REFERER,
            rate_key="search",
        )

        page = parse_search_response(response)

        if page.current_page != page_no:
            raise IncompleteSearch(
                f"PAGE_NUMBER_MISMATCH requested={page_no} "
                f"actual={page.current_page}"
            )

        if page.items_per_page != ITEMS_PER_PAGE:
            raise IncompleteSearch(
                f"PAGE_SIZE_CONTRACT_MISMATCH "
                f"expected={ITEMS_PER_PAGE} "
                f"actual={page.items_per_page}"
            )

        if expected_items is None:
            expected_items = page.total_items
            expected_pages = page.total_pages

            if expected_items > ITEMS_PER_PAGE * MAX_PAGE:
                raise IncompleteSearch(
                    f"QUERY_OVER_LIMIT items={expected_items:,}"
                )

            if expected_pages > MAX_PAGE:
                raise IncompleteSearch(
                    f"PAGE_LIMIT_EXCEEDED pages={expected_pages:,}"
                )

            if expected_items == 0:
                if page.rows or expected_pages not in (0, 1):
                    raise IncompleteSearch(
                        "INCONSISTENT_EMPTY_RESPONSE"
                    )

            elif expected_pages != math.ceil(
                expected_items / ITEMS_PER_PAGE
            ):
                raise IncompleteSearch(
                    "INCONSISTENT_PAGINATION_METADATA"
                )

        else:
            if (
                page.total_items != expected_items
                or page.total_pages != expected_pages
            ):
                raise IncompleteSearch(
                    "PAGINATION_CHANGED_DURING_SEARCH"
                )

        expected_count = min(
            ITEMS_PER_PAGE,
            max(
                0,
                expected_items - (page_no - 1) * ITEMS_PER_PAGE,
            ),
        )

        if len(page.rows) != expected_count:
            raise IncompleteSearch(
                f"PAGE_ROWS_MISMATCH page={page_no} "
                f"expected={expected_count} "
                f"actual={len(page.rows)}"
            )

        pages.append((page_no, response))

        for raw in page.rows:
            if not isinstance(raw, dict):
                raise IncompleteSearch(
                    "INVALID_SEARCH_ROW_TYPE"
                )

            if str(raw.get("jpNo", "")).strip() != number:
                continue

            profile = normalize(raw)

            key = (
                str(profile.jp_type_code),
                str(profile.jp_no),
            )

            if key in matches:
                raise IncompleteSearch(
                    f"DUPLICATE_SEARCH_IDENTITY {key}"
                )

            matches[key] = profile

        if expected_items == 0:
            break

        if page_no >= expected_pages:
            break

        page_no += 1

    if expected_items is None:
        raise IncompleteSearch(
            "SEARCH_RESPONSE_MISSING"
        )

    if expected_items > 0 and len(pages) != expected_pages:
        raise IncompleteSearch(
            "INCOMPLETE_PAGE_ENUMERATION"
        )

    return list(matches.values()), pages


class Metrics:
    def __init__(self, total, previously_completed):
        self.total = total
        self.previously_completed = previously_completed
        self.started = time.monotonic()

        self.completed = 0
        self.states = Counter()
        self.inflight = set()

        self.samples = deque(maxlen=30000)
        self.last_number = "-"
        self.last_error = "-"

    def begin(self, number):
        self.inflight.add(number)
        self.last_number = number

    def finish(self, number, status, error=None):
        self.inflight.discard(number)

        self.completed += 1
        self.states[status] += 1

        if error:
            self.last_error = (
                f"{number}: {error}"
            )[:160]

        self.samples.append(
            (time.monotonic(), self.completed)
        )

    def flow(self, seconds):
        now = time.monotonic()
        boundary = now - seconds

        if not self.samples:
            return 0.0

        baseline_count = 0
        baseline_time = self.started

        for stamp, count in reversed(self.samples):
            if stamp <= boundary:
                baseline_time = stamp
                baseline_count = count
                break

        elapsed = max(
            0.001,
            now - baseline_time,
        )

        return (
            self.completed - baseline_count
        ) / elapsed

    def render(self, client, rate, concurrency):
        elapsed = max(
            0.001,
            time.monotonic() - self.started,
        )

        remaining = max(
            0,
            self.total - self.completed,
        )

        overall_total = (
            self.total + self.previously_completed
        )

        overall_done = (
            self.previously_completed + self.completed
        )

        percentage = (
            100 * overall_done / overall_total
            if overall_total
            else 100
        )

        average = self.completed / elapsed

        eta = (
            remaining / average
            if average > 0
            else math.inf
        )

        eta_text = (
            f"{eta / 60:.1f} min"
            if math.isfinite(eta)
            else "--"
        )

        width = 30
        filled = min(
            width,
            round(width * percentage / 100),
        )

        bar = "#" * filled + "-" * (width - filled)

        requests = client.requests_made
        retries = client.retries_made

        print(
            "\n"
            + "=" * 78
            + "\nDBD CANARY DIRECT DISCOVERY | NO BUCKETS\n"
            + "=" * 78
            + f"\nDATABASE        {settings.db_name}"
            + f"\nPROGRESS        [{bar}] {percentage:.2f}%"
            + f"\nOVERALL         {overall_done:,}/{overall_total:,}"
            + f"\nTHIS RUN        {self.completed:,}/{self.total:,}"
            + f"\nREMAINING       {remaining:,}"
            + f"\nIN FLIGHT       {len(self.inflight):,}"
            + f"\n"
            + f"\nDONE            {self.states['DONE']:,}"
            + f"\nALREADY PRESENT {self.states['ALREADY_PRESENT']:,}"
            + f"\nNOT FOUND       {self.states['NOT_FOUND']:,}"
            + f"\nFAILED          {self.states['FAILED']:,}"
            + f"\nUNRESOLVED      {self.states['UNRESOLVED']:,}"
            + f"\n"
            + f"\nRATE LIMIT      {rate:g} req/s"
            + f"\nCONCURRENCY     {concurrency}"
            + f"\nHTTP REQUESTS   {requests:,}"
            + f"\nHTTP RETRIES    {retries:,}"
            + f"\nHTTP FLOW       {requests / elapsed:.2f} req/s"
            + f"\n"
            + f"\nNOW (10s)       {self.flow(10):.2f} companies/s"
            + f"\n1 MIN           {self.flow(60):.2f} companies/s"
            + f"\n5 MIN           {self.flow(300):.2f} companies/s"
            + f"\nAVERAGE         {average:.2f} companies/s"
            + f"\nETA             {eta_text}"
            + f"\nELAPSED         {elapsed / 60:.1f} min"
            + f"\nLAST COMPANY    {self.last_number}"
            + f"\nLAST ERROR      {self.last_error}"
            + "\n"
            + "=" * 78,
            flush=True,
        )


def is_infrastructure_failure(exc):
    name = type(exc).__name__
    message = str(exc).lower()

    if name in FATAL_HTTP_NAMES:
        return True

    if name in {
        "OperationalError",
        "InterfaceError",
        "DBAPIError",
    }:
        return True

    if any(
        phrase in message
        for phrase in (
            "warmup_page failed",
            "http 401",
            "http 403",
            "http 429",
            "http 502",
            "http 503",
            "lost connection",
            "too many connections",
            "access denied",
        )
    ):
        return True

    args = getattr(exc, "args", ())

    if args:
        try:
            code = int(args[0])
            if code in FATAL_DB_CODES:
                return True
        except (TypeError, ValueError):
            pass

    return False


async def database_operation(fn, *args):
    """
    A synchronous MySQL transaction executes in a worker thread.

    The operation is awaited to completion. A graceful Ctrl+C does
    not cancel this task, so there is no abandoned in-flight commit.
    """
    return await asyncio.to_thread(fn, *args)


async def process_company(
    number,
    client,
    db_sem,
    metrics,
    stop_event,
    refresh,
):
    metrics.begin(number)
    pages_checked = 0

    try:
        if not refresh:
            existing = await database_operation(
                existing_numbers,
                [number],
            )

            if number in existing:
                async with db_sem:
                    await database_operation(
                        record_existing,
                        number,
                    )

                metrics.finish(
                    number,
                    "ALREADY_PRESENT",
                )
                return

        matches, pages = await search_exact(
            client,
            number,
            stop_event,
        )

        pages_checked = len(pages)

        if stop_event.is_set():
            # No checkpoint mutation. This number remains retryable.
            return

        if matches:
            async with db_sem:
                await database_operation(
                    save_matches,
                    number,
                    matches,
                    pages,
                )

            metrics.finish(number, "DONE")

        else:
            async with db_sem:
                await database_operation(
                    record_not_found,
                    number,
                    pages_checked,
                )

            metrics.finish(number, "NOT_FOUND")

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        message = (
            f"{type(exc).__name__}: {exc}"
        )

        unresolved = isinstance(
            exc,
            IncompleteSearch,
        )

        async with db_sem:
            await database_operation(
                record_failure,
                number,
                message,
                pages_checked,
                unresolved,
            )

        metrics.finish(
            number,
            "UNRESOLVED" if unresolved else "FAILED",
            message,
        )

        if is_infrastructure_failure(exc):
            stop_event.set()

            raise InfrastructureFailure(
                message
            ) from exc

    finally:
        metrics.inflight.discard(number)


async def worker(
    queue,
    client,
    db_sem,
    metrics,
    stop_event,
    refresh,
):
    while not stop_event.is_set():
        try:
            number = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            await process_company(
                number,
                client,
                db_sem,
                metrics,
                stop_event,
                refresh,
            )
        finally:
            queue.task_done()


async def monitor(
    metrics,
    client,
    args,
    stop_event,
):
    while not stop_event.is_set():
        metrics.render(
            client,
            args.rate,
            args.concurrency,
        )

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=args.progress_interval,
            )
        except TimeoutError:
            pass

    metrics.render(
        client,
        args.rate,
        args.concurrency,
    )


async def watchdog(
    manager_conn,
    stop_event,
):
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=10,
            )
            return
        except TimeoutError:
            pass

        try:
            await database_operation(
                assert_manager_lock,
                manager_conn,
            )
        except Exception as exc:
            stop_event.set()

            raise InfrastructureFailure(
                f"DBD manager lock lost: {exc}"
            ) from exc


async def execute(args, numbers, stop_event):
    with manager_lock() as manager_conn:
        assert_manager_lock(manager_conn)

        create_checkpoint()
        register_numbers(numbers)

        checkpoints = read_checkpoints(numbers)

        selected = []
        skipped = Counter()

        for number in numbers:
            row = checkpoints[number]
            status = str(get_value(row, "status"))

            if not args.refresh and status in TERMINAL_STATES:
                skipped[status] += 1
                continue

            if (
                not args.refresh
                and status in RETRY_STATES
                and not args.retry_failed
            ):
                skipped[status] += 1
                continue

            selected.append(number)

        print(
            f"CHECKPOINTS     : {dict(skipped)}",
            flush=True,
        )
        print(
            f"QUEUED          : {len(selected):,}",
            flush=True,
        )
        print(
            f"SKIPPED         : {sum(skipped.values()):,}",
            flush=True,
        )

        if not selected:
            print(
                "COMPLETE | No eligible work remaining",
                flush=True,
            )
            return 0

        queue = asyncio.Queue()

        for number in selected:
            queue.put_nowait(number)

        metrics = Metrics(
            len(selected),
            sum(skipped.values()),
        )

        db_sem = asyncio.Semaphore(
            args.db_writers
        )

        async with FixedRateDbdClient(
            concurrency=args.concurrency,
            rate_per_second=args.rate,
            max_retries=args.http_retries,
            timeout=settings.request_timeout,
        ) as client:

            dashboard_task = asyncio.create_task(
                monitor(
                    metrics,
                    client,
                    args,
                    stop_event,
                )
            )

            watchdog_task = asyncio.create_task(
                watchdog(
                    manager_conn,
                    stop_event,
                )
            )

            workers = [
                asyncio.create_task(
                    worker(
                        queue,
                        client,
                        db_sem,
                        metrics,
                        stop_event,
                        args.refresh,
                    )
                )
                for _ in range(args.concurrency)
            ]

            error = None

            try:
                pending = set(
                    workers + [watchdog_task]
                )

                while not stop_event.is_set():
                    if all(task.done() for task in workers):
                        break

                    done, _ = await asyncio.wait(
                        pending,
                        timeout=0.5,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in done:
                        pending.discard(task)

                        if task.cancelled():
                            continue

                        exception = task.exception()

                        if exception is not None:
                            error = exception
                            stop_event.set()
                            break

                    if error is not None:
                        break

                # Graceful drain:
                # Never cancel active company workers merely because
                # Ctrl+C or an infrastructure stop was requested.
                # Workers finish or roll back their current company.
                await asyncio.gather(
                    *workers,
                    return_exceptions=True,
                )

                for task in workers:
                    if task.cancelled():
                        continue

                    exception = task.exception()

                    if exception is not None and error is None:
                        error = exception

                if error is not None:
                    raise error

            finally:
                stop_event.set()

                if not watchdog_task.done():
                    watchdog_task.cancel()

                if not dashboard_task.done():
                    dashboard_task.cancel()

                await asyncio.gather(
                    watchdog_task,
                    dashboard_task,
                    return_exceptions=True,
                )

                metrics.render(
                    client,
                    args.rate,
                    args.concurrency,
                )

        if metrics.completed != len(selected):
            return 130

        if metrics.states["FAILED"] or metrics.states["UNRESOLVED"]:
            return 2

        return 0


async def main_async(args):
    numbers, duplicates = read_numbers(
        args.seed_file
    )

    print("=" * 78)
    print("DBD | CANARY DIRECT DISCOVERY")
    print("=" * 78)
    print(f"SOURCE          : {args.seed_file}")
    print(f"DATABASE        : {settings.db_name}")
    print(f"INPUT UNIQUE    : {len(numbers):,}")
    print(f"DUPLICATES      : {duplicates:,}")
    print("NO BUCKET MODE  : ENABLED")
    print("TRANSACTIONAL   : ENABLED")
    print(f"CHECKPOINT      : {CHECKPOINT_TABLE}")
    print(f"RATE            : {args.rate:g} req/s")
    print(f"CONCURRENCY     : {args.concurrency}")
    print(f"DB WRITERS      : {args.db_writers}")
    print("=" * 78)

    schema_preflight()

    if args.dry_run:
        checkpoints = await database_operation(
            read_checkpoints,
            numbers,
        )

        present = await database_operation(
            existing_numbers,
            numbers,
        )

        counts = Counter(
            str(get_value(row, "status"))
            for row in checkpoints.values()
        )

        print("DRY RUN | READ ONLY")
        print(f"REGISTERED      : {len(checkpoints):,}")
        print(
            f"UNREGISTERED    : "
            f"{len(numbers) - len(checkpoints):,}"
        )
        print(f"MASTER / DEEP   : {len(present):,}")
        print(f"CHECKPOINTS     : {dict(counts)}")
        print("SCHEMA          : PASS")
        print("DATABASE WRITES : NONE")

        return 0

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop():
        if not stop_event.is_set():
            print(
                "\nSTOP REQUESTED | Finishing active companies...",
                flush=True,
            )
            stop_event.set()

    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)

        if sig is None:
            continue

        try:
            loop.add_signal_handler(sig, request_stop)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(
                    sig,
                    lambda *_: loop.call_soon_threadsafe(
                        request_stop
                    ),
                )
            except (ValueError, OSError):
                pass

    return await execute(
        args,
        numbers,
        stop_event,
    )


def main():
    args = parse_args()

    try:
        code = asyncio.run(
            main_async(args)
        )

        if code == 0:
            print(
                "\nCANARY COMPLETE | Selected work finished",
                flush=True,
            )

        elif code == 130:
            print(
                "\nCANARY STOPPED | Checkpoints preserved",
                flush=True,
            )

        else:
            print(
                "\nCANARY PARTIAL | Retry unresolved work as needed",
                flush=True,
            )

        return code

    except KeyboardInterrupt:
        print(
            "\nCANARY INTERRUPTED | "
            "Committed checkpoints preserved",
            flush=True,
        )
        return 130

    except BaseException as exc:
        print(
            f"\nCANARY STOPPED | "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
