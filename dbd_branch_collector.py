#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DBD BRANCH COLLECTOR

Run this same command for initial collection and resume within the active
Phase, including newly discovered companies and deferred retries:

    python .\\dbd_branch_collector.py --status-codes 1,A,B

MySQL:
    SOURCE: dbd_company_deep
    TARGET: dbd_company_branch

REQUIREMENTS:
    - Use existing MySQL tables only.
    - No new MySQL tables.
    - No deletion of existing branch records.
    - No timestamp-based work selection.
    - Identify companies by (jp_type_code, jp_no).
    - Discover companies inserted before or after previous cursors.
    - Detect changes in relevant source business information.
    - Find and process previously unchecked identities.
    - Retry unresolved identities on the next invocation.
    - Preserve previous .dbd_branch_checkpoint.json.
    - Preserve existing branch data.
    - Concurrent HTTP workers with one global rate limiter.
    - One MySQL writer.
    - Transactional branch UPSERT.
    - Safe Ctrl+C, crash recovery, and resume.
    - No Redis or additional MySQL tables.
    - One command for normal execution and subsequent resume.

STATE:
    .dbd_branch_results.jsonl

The result journal is necessary because dbd_company_branch contains branch
records, not a complete history of every company checked.

Companies with no branches also need durable company-level results.

The previous checkpoint contains aggregate counters and deferred identities.
It cannot reconstruct missing zero-branch or unavailable results. Never
silently treat a lost result Journal as a fresh installation.

Once a company has a terminal result for the active Phase, its source
fingerprint is an audit value, not a reason to fetch the website again.
Use --new-phase explicitly to refresh even those terminal companies.

If newly retrieved branch data omits previously stored branch numbers,
existing rows are preserved and the result is marked STALE_BRANCHES.

The journal must be backed up together with MySQL.

HTTP 401, 403, and 429 stop collection safely.
Access restrictions are not bypassed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import sys
import threading
import time

from collections import Counter, deque

from concurrent.futures import (
    FIRST_COMPLETED,
    ThreadPoolExecutor,
    wait,
)

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse


try:
    import pymysql
    import requests

    from bs4 import BeautifulSoup
    from dotenv import load_dotenv

    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel

    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    from rich.table import Table

except ImportError as exc:
    raise SystemExit(
        "Install dependencies:\n"
        "python -m pip install "
        "PyMySQL python-dotenv requests beautifulsoup4 rich"
    ) from exc


# ============================================================
# CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parent

load_dotenv(
    ROOT / ".env",
    override=False,
)

SOURCE = "dbd_company_deep"

TARGET = "dbd_company_branch"

SITE = "https://www.dataforthai.com"

CHECKPOINT = (
    ROOT / ".dbd_branch_checkpoint.json"
)

JOURNAL = (
    ROOT / ".dbd_branch_results.jsonl"
)

LOCK = "dbd:branch:collector:exclusive"

RECONNECT = {
    2006,
    2013,
    2014,
    2055,
}

PHASE_TERMINAL_STATUSES = frozenset({
    "OK", "NO_BRANCH", "LEGACY_OK", "NO_BRANCH_LISTED",
    "UNAVAILABLE", "NOT_LISTED",
})
PHASE_REVIEW_STATUSES = frozenset({"STALE_BRANCHES"})
PHASE_FORMAT_VERSION = 2


def result_class(status):
    if status in PHASE_TERMINAL_STATUSES:
        return "TERMINAL"
    if status in PHASE_REVIEW_STATUSES:
        return "REVIEW"
    if status.startswith("DEFERRED_") and re.fullmatch(r"DEFERRED_[A-Z0-9_]+", status):
        return "DEFERRED"
    raise CollectorError(f"Unknown Journal status: {status!r}")

# Relevant source business fields are recorded for audit purposes only.
# A fingerprint change must NOT invalidate a current-Phase terminal result.
# Runtime timestamps, financial endpoints, and worker controls are excluded.

FINGERPRINT_FIELDS = (
    "jp_type_code",
    "jp_no",
    "jp_name",
    "jp_name_e",
    "jp_stat_code",
    "address",
    "pv_code",
    "ampur_code",
    "tumbon_code",
    "zip_code",
)

REQUIRED_SOURCE = {
    "jp_type_code",
    "jp_no",
    "jp_name",
    "jp_name_e",
    "jp_stat_code",
}

REQUIRED_TARGET = {
    "jp_type_code",
    "jp_no",
    "jp_name",
    "jp_name_e",
    "branch_no",
    "branch_count",
    "branch_name",
    "branch_address",
    "province_name",
    "district_name",
    "subdistrict_name",
    "postal_code",
    "source_url",
    "fetched_at",
}


# ============================================================
# EXISTING TARGET TABLE UPSERT
# ============================================================

UPSERT = f"""
INSERT INTO `{TARGET}` (

    jp_type_code,
    jp_no,
    jp_name,
    jp_name_e,

    branch_no,
    branch_name,
    branch_address,

    province_name,
    district_name,
    subdistrict_name,
    postal_code,

    source_url,
    fetched_at,
    branch_count

)
VALUES (
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s
)

ON DUPLICATE KEY UPDATE

    jp_name = VALUES(jp_name),
    jp_name_e = VALUES(jp_name_e),

    branch_name = VALUES(branch_name),
    branch_address = VALUES(branch_address),

    province_name = VALUES(province_name),
    district_name = VALUES(district_name),
    subdistrict_name = VALUES(subdistrict_name),
    postal_code = VALUES(postal_code),

    source_url = VALUES(source_url),
    fetched_at = VALUES(fetched_at),
    branch_count = VALUES(branch_count)
"""


# ============================================================
# EXCEPTIONS
# ============================================================

class CollectorError(RuntimeError):
    pass


class AccessDenied(CollectorError):
    pass


# ============================================================
# HELPERS
# ============================================================

def positive(
    name,
    default,
    kind=float,
):
    try:
        value = kind(
            os.getenv(name, default)
        )

        if value <= 0:
            raise ValueError

        return value

    except (
        TypeError,
        ValueError,
    ) as exc:
        raise CollectorError(
            f"Invalid positive setting: {name}"
        ) from exc


def clean(value):
    return (
        " ".join(str(value or "").split())
        or None
    )


def key(row):
    return (
        str(row["jp_type_code"]),
        str(row["jp_no"]),
    )


def key_text(row):
    jp_type, jp_no = key(row)

    return (
        jp_type + ":" + jp_no
    )


def now_utc():
    return datetime.now(
        timezone.utc
    ).replace(tzinfo=None)


def status_codes(raw):

    codes = [
        value.strip().upper()
        for value in raw.split(",")
    ]

    if not codes or any(
        not re.fullmatch(
            r"[A-Z0-9]{1,4}",
            value,
        )
        for value in codes
    ):
        raise CollectorError(
            "Invalid status codes; expected 1,A,B"
        )

    return tuple(
        sorted(set(codes))
    )


# ============================================================
# CONTENT FINGERPRINT
#
# Identity and relevant business values only.
#
# Does not depend on dates, timestamps, runtime states,
# request counters, or worker statuses.
# ============================================================

def fingerprint(
    row,
    columns,
):

    payload = json.dumps(
        [
            row.get(name)
            for name in columns
        ],

        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")

    return hashlib.sha256(
        payload
    ).hexdigest()


# ============================================================
# ADDRESS PARSER
# ============================================================

def parse_address(address):

    province = (
        "กรุงเทพมหานคร"
        if "กรุงเทพมหานคร" in address
        else None
    )

    if not province:

        match = re.search(
            r"(?:จังหวัด|จ\.)\s*([^\s,]+)",
            address,
        )

        province = (
            match.group(1)
            if match
            else None
        )

    district = re.search(
        r"(?:อำเภอ|เขต)\s*([^\s,]+)",
        address,
    )

    subdistrict = re.search(
        r"(?:ตำบล|แขวง)\s*([^\s,]+)",
        address,
    )

    zipcodes = re.findall(
        r"(?<!\d)\d{5}(?!\d)",
        address,
    )

    return (
        province,

        district.group(1)
        if district
        else None,

        subdistrict.group(1)
        if subdistrict
        else None,

        zipcodes[-1]
        if zipcodes
        else None,
    )


# ============================================================
# BRANCH PRINTVIEW PARSER
# ============================================================

def parse_printview(
    html,
    jp_no,
):

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    holder = soup.select_one(
        "#divbranch"
    )

    summary = soup.select_one(
        "#divbranchsummary"
    )

    if holder is None or summary is None:
        raise CollectorError(
            f"{jp_no}: invalid branch printview"
        )

    expected = None

    for row in summary.select("tr"):

        cells = row.find_all(
            "td",
            recursive=False,
        )

        if len(cells) < 2:
            continue

        if clean(
            cells[0].get_text(
                " ",
                strip=True,
            )
        ) != "รวม":
            continue

        raw = (
            clean(
                cells[1].get_text(
                    " ",
                    strip=True,
                )
            )
            or ""
        ).replace(",", "")

        if raw.isdecimal():
            expected = int(raw)

        break

    if expected is None:
        raise CollectorError(
            f"{jp_no}: missing or invalid "
            "establishment total"
        )

    cards = holder.select(
        ".card"
    )

    if len(cards) != expected:
        raise CollectorError(
            f"{jp_no}: incomplete printview "
            f"{len(cards)}/{expected}"
        )

    branches = []

    seen = set()

    headquarters = 0

    for card in cards:

        code = card.select_one(
            ".branch-code"
        )

        info = card.select_one(
            ".branch-info"
        )

        if code is None or info is None:
            raise CollectorError(
                f"{jp_no}: incomplete branch card"
            )

        label = (
            clean(
                code.get_text(
                    " ",
                    strip=True,
                )
            )
            or ""
        )

        if "สำนักงานใหญ่" in label:
            headquarters += 1
            continue

        matches = re.findall(
            r"(?<!\d)\d{5}(?!\d)",
            label,
        )

        if (
            len(matches) != 1
            or matches[0] in seen
        ):
            raise CollectorError(
                f"{jp_no}: invalid or duplicate "
                f"branch number {label!r}"
            )

        branch_no = matches[0]

        seen.add(branch_no)

        parts = info.find_all(
            "div",
            recursive=False,
        )

        if len(parts) < 2:
            raise CollectorError(
                f"{jp_no}/{branch_no}: "
                "missing detail elements"
            )

        branch_name = clean(
            parts[0].get_text(
                " ",
                strip=True,
            )
        )

        address = clean(
            parts[1].get_text(
                " ",
                strip=True,
            )
        )

        if not address:
            raise CollectorError(
                f"{jp_no}/{branch_no}: "
                "empty branch address"
            )

        branches.append(
            (
                branch_no,
                branch_name,
                address,
                *parse_address(address),
            )
        )

    if (
        headquarters > 1
        or len(branches) + headquarters != expected
    ):
        raise CollectorError(
            f"{jp_no}: inconsistent establishment count"
        )

    return branches, expected


# ============================================================
# COMPANY PAGE PARSER
# ============================================================

def parse_company_page(
    html,
    jp_no,
):

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    for node in soup.select(
        "script,style,noscript"
    ):
        node.decompose()

    visible = soup.get_text(
        " ",
        strip=True,
    )

    not_found_phrases = (
        "ไม่พบข้อมูลนิติบุคคล",
        "ไม่พบข้อมูลผู้ประกอบการ",
        "ไม่พบข้อมูลบริษัท",
        "ไม่พบข้อมูลที่ค้นหา",
    )

    if any(
        phrase in visible
        for phrase in not_found_phrases
    ):
        return "NOT_LISTED"

    identity_urls = []

    canonical = soup.find(
        "link",
        rel="canonical",
    )

    if canonical and canonical.get("href"):
        identity_urls.append(
            canonical["href"]
        )

    og = soup.find(
        "meta",
        attrs={
            "property": "og:url"
        },
    )

    if og and og.get("content"):
        identity_urls.append(
            og["content"]
        )

    verified = any(
        (
            path := urlparse(
                urljoin(SITE, url)
            ).path
        ).rstrip("/") == f"/company/{jp_no}"

        or path.startswith(
            f"/company/{jp_no}/"
        )

        for url in identity_urls
    )

    if not verified:
        return "UNAVAILABLE"

    for node in soup.select(
        "a,button,[role=button]"
    ):

        label = node.get_text(
            " ",
            strip=True,
        )

        href = (
            node.get("href")
            or ""
        )

        onclick = (
            node.get("onclick")
            or ""
        )

        path = (
            urlparse(
                urljoin(SITE, href)
            ).path
            if href
            else ""
        )

        if (
            path.startswith(
                f"/company-branch/{jp_no}/"
            )

            or "ตำแหน่งที่ตั้งของสาขา" in label

            or "ข้อมูลตำแหน่งที่ตั้งสาขา" in label

            or "ข้อมูลสาขา" in label

            or (
                "open_branch(" in onclick
                and bool(label)
            )
        ):
            return "HAS_BRANCH_LINK"

    return "NO_BRANCH_LISTED"


# ============================================================
# MYSQL
# ============================================================

class MySQL:

    def __init__(self):

        required = (
            "DB_HOST",
            "DB_PORT",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
        )

        missing = [
            name
            for name in required
            if os.getenv(name) is None
        ]

        if missing:
            raise CollectorError(
                "Missing .env: "
                + ", ".join(missing)
            )

        timeout = positive(
            "DB_TIMEOUT",
            "30",
            int,
        )

        self.config = {
            "host": os.environ["DB_HOST"],
            "port": int(
                os.environ["DB_PORT"]
            ),
            "database": os.environ["DB_NAME"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],

            "charset": "utf8mb4",

            "cursorclass": (
                pymysql.cursors.DictCursor
            ),

            "autocommit": True,

            "connect_timeout": timeout,
            "read_timeout": timeout,
            "write_timeout": timeout,
        }

        self.conn = None

        self.fields = ()

        self.connect()

    # --------------------------------------------------------
    # CONNECTION AND EXCLUSIVE LOCK
    # --------------------------------------------------------

    def connect(self):

        if self.conn is not None:

            try:
                self.conn.close()

            except Exception:
                pass

        self.conn = pymysql.connect(
            **self.config
        )

        try:

            with self.conn.cursor() as cursor:

                cursor.execute(
                    "SELECT GET_LOCK(%s,0) AS acquired",
                    (LOCK,),
                )

                result = cursor.fetchone()

                if result["acquired"] != 1:
                    raise CollectorError(
                        "Another collector already "
                        "holds the MySQL lock"
                    )

        except BaseException:

            self.conn.close()

            self.conn = None

            raise

    def close(self):

        if self.conn is not None:

            self.conn.close()

            self.conn = None

    # --------------------------------------------------------
    # QUERY WITH RECONNECTION
    # --------------------------------------------------------

    def query(
        self,
        sql,
        args=None,
        mode=None,
    ):

        for attempt in range(3):

            try:

                with self.conn.cursor() as cursor:

                    cursor.execute(
                        sql,
                        args,
                    )

                    if mode == "all":
                        return cursor.fetchall()

                    if mode == "one":
                        return cursor.fetchone()

                    return None

            except (
                pymysql.err.InterfaceError,
                pymysql.err.OperationalError,
                OSError,
            ) as exc:

                code = (
                    exc.args[0]
                    if exc.args
                    else None
                )

                retryable = (
                    isinstance(
                        exc,
                        (
                            pymysql.err.InterfaceError,
                            OSError,
                        ),
                    )

                    or code in RECONNECT
                )

                if (
                    not retryable
                    or attempt == 2
                ):
                    raise

                time.sleep(
                    0.5 * (attempt + 1)
                )

                self.connect()

    # --------------------------------------------------------
    # SCHEMA PREFLIGHT
    #
    # Read existing table structures.
    # Do not create or alter database tables.
    # --------------------------------------------------------

    def preflight(self):

        source = self.query(
            f"SHOW COLUMNS FROM `{SOURCE}`",
            mode="all",
        )

        names = {
            row["Field"]
            for row in source
        }

        if not REQUIRED_SOURCE <= names:
            raise CollectorError(
                "Source missing columns: "
                f"{sorted(REQUIRED_SOURCE - names)}"
            )

        target_exists = self.query(
            "SHOW TABLES LIKE %s",
            (TARGET,),
            "one",
        )

        if not target_exists:
            raise CollectorError(
                f"Target table {TARGET} "
                "does not exist; nothing was changed"
            )

        target = {
            row["Field"]

            for row in self.query(
                f"SHOW COLUMNS FROM `{TARGET}`",
                mode="all",
            )
        }

        if not REQUIRED_TARGET <= target:
            raise CollectorError(
                "Target missing columns: "
                f"{sorted(REQUIRED_TARGET - target)}"
            )

        expected_keys = (
            (
                SOURCE,
                [
                    "jp_type_code",
                    "jp_no",
                ],
            ),

            (
                TARGET,
                [
                    "jp_type_code",
                    "jp_no",
                    "branch_no",
                ],
            ),
        )

        for table, expected in expected_keys:

            indexes = self.query(
                f"SHOW INDEX FROM `{table}`",
                mode="all",
            )

            primary_key = [
                row["Column_name"]

                for row in sorted(
                    (
                        item
                        for item in indexes
                        if item["Key_name"] == "PRIMARY"
                    ),

                    key=lambda item: item["Seq_in_index"],
                )
            ]

            if primary_key != expected:
                raise CollectorError(
                    f"Unexpected PRIMARY in {table}: "
                    f"{primary_key}; expected {expected}"
                )

        self.fields = tuple(
            name
            for name in FINGERPRINT_FIELDS
            if name in names
        )

        if len(self.fields) < 5:
            raise CollectorError(
                "Insufficient source fields "
                "for content fingerprint"
            )

    def has_existing_branch_data(self):
        """Distinguish a fresh install from a lost Journal."""
        return bool(self.query(f"SELECT 1 AS present FROM `{TARGET}` LIMIT 1", mode="one"))

    # --------------------------------------------------------
    # KEYSET PAGINATION
    #
    # Uses legal identity, not timestamps.
    #
    # Each reconciliation pass starts from the beginning.
    # Previously verified results are skipped before HTTP.
    # --------------------------------------------------------

    def select_page(
        self,
        codes,
        after,
        size,
    ):

        placeholders = ",".join(
            "%s"
            for _ in codes
        )

        args = list(codes)

        condition = ""

        if after is not None:

            condition = """
                AND (
                    jp_type_code > %s

                    OR (
                        jp_type_code = %s
                        AND jp_no > %s
                    )
                )
            """

            args.extend(
                (
                    after[0],
                    after[0],
                    after[1],
                )
            )

        args.append(size)

        fields = ",".join(
            "`" + column + "`"
            for column in self.fields
        )

        sql = f"""
            SELECT
                {fields}

            FROM `{SOURCE}`
                FORCE INDEX (PRIMARY)

            WHERE
                jp_stat_code IN ({placeholders})

                {condition}

            ORDER BY
                jp_type_code,
                jp_no

            LIMIT %s
        """

        return self.query(
            sql,
            tuple(args),
            "all",
        )

    # --------------------------------------------------------
    # EXISTING BRANCH DATA VALIDATION
    #
    # Read existing MySQL branch rows.
    #
    # A positive historical result can be reused only
    # when its recorded branch count matches the number
    # of rows actually present for the company.
    # --------------------------------------------------------

    def existing_positive(
        self,
        companies,
    ):

        if not companies:
            return {}

        placeholders = ",".join(
            "(%s,%s)"
            for _ in companies
        )

        params = tuple(
            value

            for company in companies

            for value in key(company)
        )

        rows = self.query(
            f"""
            SELECT
                jp_type_code,
                jp_no,

                COUNT(*) AS actual,

                MIN(branch_count) AS min_count,
                MAX(branch_count) AS max_count,

                MIN(jp_name) AS jp_name,
                MIN(jp_name_e) AS jp_name_e,

                SUM(
                    CASE
                        WHEN branch_address IS NULL
                          OR TRIM(branch_address) = ''
                        THEN 1
                        ELSE 0
                    END
                ) AS bad_address

            FROM `{TARGET}`

            WHERE
                (jp_type_code, jp_no)
                IN ({placeholders})

            GROUP BY
                jp_type_code,
                jp_no
            """,

            params,
            "all",
        )

        results = {}

        for row in rows:

            identity = (
                str(row["jp_type_code"]),
                str(row["jp_no"]),
            )

            results[identity] = row

        return results

    # --------------------------------------------------------
    # EXISTING BRANCH NUMBERS
    #
    # Used to detect previously stored branches that
    # disappear from newly retrieved source data.
    # --------------------------------------------------------

    def current_branch_numbers(
        self,
        company,
    ):

        rows = self.query(
            f"""
            SELECT branch_no

            FROM `{TARGET}`

            WHERE
                jp_type_code = %s
                AND jp_no = %s
            """,

            key(company),
            "all",
        )

        return {
            str(row["branch_no"])
            for row in rows
        }

    # --------------------------------------------------------
    # TRANSACTIONAL BRANCH UPSERT
    #
    # No DELETE, REPLACE, TRUNCATE, or DROP.
    #
    # Idempotent UPSERT supports safe retry when the
    # MySQL connection is interrupted.
    # --------------------------------------------------------

    def commit_branches(
        self,
        company,
        branches,
        source_url,
    ):

        if not branches:
            return

        fetched_at = now_utc()

        rows = [
            (
                company["jp_type_code"],
                company["jp_no"],

                company["jp_name"],
                company["jp_name_e"],

                *branch,

                source_url,
                fetched_at,
                len(branches),
            )

            for branch in branches
        ]

        for attempt in range(3):

            try:

                self.conn.begin()

                with self.conn.cursor() as cursor:

                    cursor.executemany(
                        UPSERT,
                        rows,
                    )

                self.conn.commit()

                return

            except (
                pymysql.err.InterfaceError,
                pymysql.err.OperationalError,
                OSError,
            ) as exc:

                try:
                    self.conn.rollback()

                except Exception:
                    pass

                code = (
                    exc.args[0]
                    if exc.args
                    else None
                )

                retryable = (
                    isinstance(
                        exc,
                        (
                            pymysql.err.InterfaceError,
                            OSError,
                        ),
                    )

                    or code in RECONNECT
                )

                if (
                    not retryable
                    or attempt == 2
                ):
                    raise

                time.sleep(
                    0.5 * (attempt + 1)
                )

                self.connect()

            except BaseException:

                try:
                    self.conn.rollback()

                except Exception:
                    pass

                raise


# ============================================================
# DURABLE RESULT JOURNAL
#
# No additional MySQL table is created.
#
# Journal records are written only after the associated
# MySQL transaction has committed.
#
# A crash between MySQL commit and journal append can
# cause an HTTP request to be repeated, but it cannot
# duplicate the committed branch primary keys.
# ============================================================

class Journal:
    """Append-only journal; only the active phase is indexed in RAM.

    Legacy records without 'p' always belong to phase 1. Phase-start events
    form a monotonic boundary; earlier phases stay on disk rather than being
    multiplied into per-phase in-memory indexes. A writer must already hold
    the MySQL advisory lock before calling prepare_for_writes().
    """

    def __init__(self, path, read_only=False):
        self.path = Path(path)
        self.read_only = read_only
        self.exists = self.path.exists()
        self.records = {}              # "type:number" -> (fp, status, count, method)
        self.counts = Counter()        # Latest company status, active phase only
        self.phases = {}               # small: id -> metadata event
        self.active_phase = None
        self.fd = None
        self.tail_bytes = 0
        self.safe_offset = 0
        self.total_records = 0
        self.legacy_records = 0
        self._load()

    @staticmethod
    def _identity(jp_type, jp_no):
        # The colon is a separator, not an integer conversion.
        return jp_type + ":" + jp_no

    def _set_record(self, identity, value):
        old = self.records.get(identity)
        if old is not None:
            self.counts[old[1]] -= 1
            if not self.counts[old[1]]:
                del self.counts[old[1]]
        self.records[identity] = value
        self.counts[value[1]] += 1

    def _load(self):
        if not self.exists:
            return
        with self.path.open("rb") as source:
            while True:
                raw = source.readline()
                if not raw:
                    break
                if not raw.endswith(b"\n"):
                    self.tail_bytes = self.path.stat().st_size - self.safe_offset
                    break
                try:
                    obj = json.loads(raw)
                    if not isinstance(obj, dict):
                        raise ValueError("Expected object")
                    if "event" in obj:
                        self._load_event(obj)
                    else:
                        self._load_result(obj)
                except (ValueError, KeyError, TypeError, CollectorError) as exc:
                    raise CollectorError(
                        f"Malformed complete Journal record at byte "
                        f"{self.safe_offset}: {exc}"
                    ) from exc
                self.safe_offset = source.tell()

    def _load_event(self, obj):
        if set(obj) != {"event", "p", "scope", "v", "at", "origin"}:
            raise ValueError("Unexpected Phase event fields")
        if obj["event"] != "phase_start" or obj["v"] != PHASE_FORMAT_VERSION:
            raise ValueError("Unknown Phase event/version")
        phase = obj["p"]
        if type(phase) is not int or phase < 1:
            raise ValueError("Invalid phase id")
        scope = obj["scope"]
        if (not isinstance(scope, list) or not scope or
            any(not isinstance(c, str) for c in scope) or
            list(status_codes(",".join(scope))) != scope):
            raise ValueError("Invalid or unnormalized phase scope")
        if not isinstance(obj["at"], str) or not obj["at"]:
            raise ValueError("Invalid phase audit timestamp")
        if obj["origin"] not in ("initial", "legacy", "refresh"):
            raise ValueError("Invalid phase origin")
        try:
            if datetime.fromisoformat(obj["at"]).tzinfo is None:
                raise ValueError("Phase audit timestamp must include timezone")
        except (ValueError, TypeError) as exc:
            raise ValueError("Invalid Phase audit timestamp") from exc
        if phase == 1 and bool(self.legacy_records) != (obj["origin"] == "legacy"):
            raise ValueError("Legacy Phase origin is inconsistent with recorded history")
        if phase in self.phases:
            raise ValueError("Duplicate phase start event")
        if phase != len(self.phases) + 1:
            raise ValueError("Phase IDs must be contiguous")
        if phase == 1 and obj["origin"] == "refresh":
            raise ValueError("Phase 1 cannot be a refresh")
        if phase > 1 and obj["origin"] != "refresh":
            raise ValueError("Later phases must be explicit refreshes")
        if phase == 1 and self.active_phase not in (None, 1):
            raise ValueError("Invalid Phase 1 migration order")
        if phase > 1 and self.active_phase != phase - 1:
            raise ValueError("Phase event boundary mismatch")
        self.phases[phase] = obj
        self.active_phase = phase
        if phase > 1:
            self.records.clear()
            self.counts.clear()

    def _load_result(self, obj):
        required = {"t", "j", "f", "s", "n", "m"}
        if not required <= set(obj) or set(obj) - required - {"p"}:
            raise ValueError("Unexpected result fields")
        jp_type, jp_no = obj["t"], obj["j"]
        fp, status = obj["f"], obj["s"]
        count, method = obj["n"], obj["m"]
        if (not isinstance(jp_type, str) or not jp_type or 
            not isinstance(jp_no, str) or not jp_no or
            not isinstance(fp, str) or not re.fullmatch(r"[0-9a-f]{64}", fp) or
            not isinstance(status, str) or
            type(count) is not int or count < 0 or
            method not in ("http", "legacy")):
            raise ValueError("Invalid result fields")
        result_class(status)
        if (status in ("OK", "LEGACY_OK") and count < 1):
            raise ValueError("Positive result requires a positive branch count")
        if (status in ("NO_BRANCH", "NO_BRANCH_LISTED", "UNAVAILABLE", "NOT_LISTED")
            and count != 0):
            raise ValueError("Non-branch result cannot carry branch rows")
        if (method == "legacy") != (status == "LEGACY_OK"):
            raise ValueError("Legacy method/status mismatch")
        phase = obj.get("p", 1)
        if type(phase) is not int or phase < 1:
            raise ValueError("Invalid result phase")
        if "p" in obj and phase not in self.phases:
            raise ValueError("Phase-aware result missing phase_start")
        if phase == 1 and not self.phases:
            self.active_phase = 1
        if phase != self.active_phase:
            raise ValueError("Out-of-order result across phase boundary")
        if "p" not in obj:
            if phase != 1:
                raise ValueError("Legacy result outside Phase 1")
            self.legacy_records += 1
        self._set_record(self._identity(jp_type, jp_no),
                         (fp, status, count, method))
        self.total_records += 1

    def snapshot(self):
        terminal = sum(self.counts.get(s, 0) for s in PHASE_TERMINAL_STATUSES)
        deferred = sum(n for s, n in self.counts.items() if s.startswith("DEFERRED_"))
        review = sum(self.counts.get(s, 0) for s in PHASE_REVIEW_STATUSES)
        return {"terminal": terminal, "deferred": deferred, "review": review,
                "recorded": len(self.records)}

    def get(self, company):
        return self.records.get(key_text(company))

    @staticmethod
    def _backup_file(path, suffix):
        """Durable, non-destructive backup; never overwrite an existing backup."""
        target = path.with_name(path.name + suffix)
        if target.exists():
            if target.stat().st_size == 0 and path.stat().st_size:
                raise CollectorError(f"Incomplete backup needs review: {target}")
            return target
        temp = target.with_name(target.name + ".incomplete")
        if temp.exists():
            raise CollectorError(f"Incomplete backup needs manual review: {temp}")
        try:
            with path.open("rb") as src, temp.open("xb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
                dst.flush()
                os.fsync(dst.fileno())
            if target.exists():
                raise CollectorError(f"Backup unexpectedly exists: {target}")
            temp.rename(target)
        except BaseException:
            # Leave an interrupted copy as evidence; never touch source.
            raise
        return target

    def prepare_for_writes(self, legacy_migration=False):
        if self.read_only:
            raise CollectorError("Read-only Journal cannot write")
        if self.fd is not None:
            return
        if self.exists and legacy_migration:
            self._backup_file(self.path, ".before_phase_v2.bak")
        if self.tail_bytes:
            # Backup original including incomplete bytes BEFORE truncating.
            import hashlib as _hashlib
            with self.path.open("rb") as file:
                file.seek(self.safe_offset)
                digest = _hashlib.sha256(file.read()).hexdigest()[:16]
            self._backup_file(self.path,
                f".tail_{self.path.stat().st_size}_{digest}.bak")
            with self.path.open("r+b") as file:
                file.truncate(self.safe_offset)
                file.flush()
                os.fsync(file.fileno())
            self.tail_bytes = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(str(self.path), os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
        self.exists = True

    def _write(self, obj):
        if self.fd is None:
            raise CollectorError("Journal not opened for writes")
        payload = (json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
                   + "\n").encode("utf-8")
        offset = 0
        while offset < len(payload):
            count = os.write(self.fd, payload[offset:])
            if count <= 0:
                raise CollectorError("Journal append did not advance")
            offset += count
        os.fsync(self.fd)

    def begin_phase(self, phase, scope, origin):
        if type(phase) is not int or phase != len(self.phases) + 1:
            raise CollectorError("Invalid new Phase id")
        if origin == "legacy" and (phase != 1 or not self.records):
            raise CollectorError("Invalid legacy migration")
        event = {"event": "phase_start", "p": phase, "scope": list(scope),
                 "v": PHASE_FORMAT_VERSION,
                 "at": datetime.now(timezone.utc).isoformat(), "origin": origin}
        # Validate before writing so a malformed event cannot corrupt the log.
        if (list(status_codes(",".join(scope))) != list(scope) or
            (phase > 1 and origin != "refresh") or
            (phase == 1 and origin not in ("initial", "legacy"))):
            raise CollectorError("Invalid new Phase event")
        self._write(event)
        self.phases[phase] = event
        self.active_phase = phase
        if phase > 1:
            self.records.clear()
            self.counts.clear()

    def append(self, company, fp, result, count, method):
        if self.active_phase not in self.phases:
            raise CollectorError("Phase not established")
        record = {"p": self.active_phase, "t": key(company)[0],
                  "j": key(company)[1], "f": fp, "s": result,
                  "n": count, "m": method}
        # same strict checks as journal replay
        if (not isinstance(fp, str) or not re.fullmatch(r"[0-9a-f]{64}", fp)
            or type(count) is not int or count < 0 or method not in ("http", "legacy")):
            raise CollectorError("Invalid company result")
        result_class(result)
        if (result in ("OK", "LEGACY_OK") and count < 1):
            raise CollectorError("Positive result needs branch count")
        if (result in ("NO_BRANCH", "NO_BRANCH_LISTED", "UNAVAILABLE", "NOT_LISTED")
            and count != 0):
            raise CollectorError("Non-branch result needs zero branch count")
        if (method == "legacy") != (result == "LEGACY_OK"):
            raise CollectorError("Invalid legacy method/status")
        self._write(record)
        self._set_record(key_text(company), (fp, result, count, method))
        self.total_records += 1

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def establish_phase(journal, codes, explicit_continue=False, new_phase=False):
    """Must run only after exclusive lock and Journal validation."""
    scope = list(codes)
    if journal.phases:
        existing = journal.phases[journal.active_phase]["scope"]
        if not new_phase and existing != scope:
            raise CollectorError(
                f"Phase {journal.active_phase} scope mismatch: "
                f"expected {','.join(existing)}, supplied {','.join(scope)}; "
                "use the original --status-codes or explicitly --new-phase"
            )
        if new_phase:
            journal.begin_phase(journal.active_phase + 1, scope, "refresh")
            return "NEW_PHASE"
        return "RESUME"
    if journal.records:
        # Legacy results have no reliable historical scope. Record adoption.
        journal.begin_phase(1, scope, "legacy")
        if new_phase:
            journal.begin_phase(2, scope, "refresh")
            return "NEW_PHASE"
        return "RESUME (LEGACY MIGRATION)"
    if explicit_continue:
        raise CollectorError("--continue requires existing Phase/Journal state")
    if new_phase:
        raise CollectorError("--new-phase requires an existing collection Phase")
    journal.begin_phase(1, scope, "initial")
    return "INITIAL"


# ============================================================
# HISTORICAL CHECKPOINT
#
# Read-only.
#
# Preserve the existing checkpoint, counters and cursor.
# The old cursor is not used to exclude company identities.
#
# Import pending deferred identities into the selection
# policy without deleting or rewriting the checkpoint.
# ============================================================

def read_previous_checkpoint(path):

    if not path.exists():
        return set()

    try:

        with path.open(
            "r",
            encoding="utf-8",
        ) as file:

            checkpoint = json.load(file)

        if not isinstance(
            checkpoint,
            dict,
        ):
            raise ValueError(
                "Expected JSON object"
            )

        deferred = checkpoint.get(
            "deferred",
            [],
        )

        if not isinstance(
            deferred,
            list,
        ):
            raise ValueError(
                "Invalid deferred list"
            )

        pending = set()

        for row in deferred:

            if not isinstance(row, dict):
                raise ValueError(
                    "Invalid deferred entry"
                )

            pending.add(
                (
                    str(row["jp_type_code"]),
                    str(row["jp_no"]),
                )
            )

        return pending

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
    ) as exc:

        raise CollectorError(
            "Cannot safely read historical checkpoint: "
            f"{exc}"
        ) from exc


# ============================================================
# CONCURRENT HTTP CLIENT
# ============================================================

class Website:

    def __init__(self):

        self.rate = positive(
            "DATAFORTHAI_RATE_PER_SECOND",
            "2",
        )

        self.concurrency = positive(
            "DATAFORTHAI_CONCURRENCY",
            "4",
            int,
        )

        self.timeout = positive(
            "DATAFORTHAI_REQUEST_TIMEOUT",
            "30",
        )

        self.attempts = positive(
            "DATAFORTHAI_HTTP_ATTEMPTS",
            "3",
            int,
        )

        if self.concurrency > 32:
            raise CollectorError(
                "DATAFORTHAI_CONCURRENCY "
                "must be <= 32"
            )

        self.lock = threading.Lock()

        self.next_request_at = 0.0

        self.count = 0
        self.active = 0

        self.stop = threading.Event()

        self.local = threading.local()

        self.sessions = []

    # --------------------------------------------------------
    # THREAD-LOCAL HTTP SESSION
    # --------------------------------------------------------

    def session(self):

        session = getattr(
            self.local,
            "session",
            None,
        )

        if session is not None:
            return session

        session = requests.Session()

        session.headers.update(
            {
                "User-Agent": os.getenv(
                    "DATAFORTHAI_USER_AGENT",
                    "DBD-BranchCollector",
                ),

                "Accept": (
                    "text/html,"
                    "application/xhtml+xml;q=0.9,"
                    "*/*;q=0.8"
                ),

                "Accept-Language": (
                    "th-TH,th;q=0.9"
                ),
            }
        )

        self.local.session = session

        with self.lock:
            self.sessions.append(session)

        return session

    # --------------------------------------------------------
    # INTERRUPTIBLE SLEEP
    # --------------------------------------------------------

    def sleep(self, seconds):

        if self.stop.wait(
            max(0, seconds)
        ):
            raise CollectorError(
                "HTTP worker stopped"
            )

    # --------------------------------------------------------
    # GLOBAL RATE LIMITER
    # --------------------------------------------------------

    def acquire(self):

        if self.stop.is_set():
            raise CollectorError(
                "HTTP worker stopped"
            )

        with self.lock:

            due = max(
                time.monotonic(),
                self.next_request_at,
            )

            self.next_request_at = (
                due + 1 / self.rate
            )

        self.sleep(
            due - time.monotonic()
        )

        with self.lock:
            self.count += 1

    # --------------------------------------------------------
    # HTTP GET
    # --------------------------------------------------------

    def get(
        self,
        url,
        depth=0,
    ):

        if depth > 2:
            return (
                None,
                "REDIRECT_OTHER",
            )

        for attempt in range(
            self.attempts
        ):

            self.acquire()

            with self.lock:
                self.active += 1

            try:

                response = self.session().get(
                    url,

                    timeout=(
                        min(
                            15.0,
                            self.timeout,
                        ),
                        self.timeout,
                    ),

                    allow_redirects=False,
                )

            except requests.RequestException:

                if attempt + 1 == self.attempts:
                    return (
                        None,
                        "DEFERRED_NETWORK",
                    )

                self.sleep(
                    min(
                        30.0,

                        2 ** attempt
                        + random.random(),
                    )
                )

                continue

            finally:

                with self.lock:
                    self.active -= 1

            status = response.status_code

            # ------------------------------------------------
            # ACCESS RESTRICTIONS
            # ------------------------------------------------

            if status in (
                401,
                403,
                429,
            ):

                self.stop.set()

                raise AccessDenied(
                    f"HTTP {status}: "
                    "access restricted; "
                    "collection stopped safely"
                )

            # ------------------------------------------------
            # SAFE REDIRECT
            # ------------------------------------------------

            if status in (
                301,
                302,
                303,
                307,
                308,
            ):

                location = (
                    response.headers.get(
                        "Location",
                        "",
                    )
                )

                destination = (
                    urljoin(url, location)
                    if location
                    else ""
                )

                parsed = urlparse(
                    destination
                )

                if (
                    not destination

                    or parsed.scheme != "https"

                    or parsed.hostname
                    != "www.dataforthai.com"
                ):

                    self.stop.set()

                    raise AccessDenied(
                        "Unexpected redirect destination"
                    )

                match = re.search(
                    r"/(?:company|company-branch)/"
                    r"(\d{13})(?:/|$)",

                    urlparse(url).path,
                )

                allowed = bool(
                    match
                    and (
                        parsed.path.rstrip("/")
                        == f"/company/{match.group(1)}"

                        or parsed.path.startswith(
                            f"/company/{match.group(1)}/"
                        )

                        or parsed.path.startswith(
                            f"/company-branch/"
                            f"{match.group(1)}/"
                        )
                    )
                )

                if (
                    allowed
                    and destination != url
                ):
                    return self.get(
                        destination,
                        depth + 1,
                    )

                return (
                    None,

                    "REDIRECT_HOME"
                    if parsed.path in ("", "/")
                    else "REDIRECT_OTHER",
                )

            # ------------------------------------------------
            # TRANSIENT HTTP ERRORS
            # ------------------------------------------------

            if status in (
                500,
                502,
                503,
                504,
            ):

                if attempt + 1 == self.attempts:
                    return (
                        None,
                        f"DEFERRED_HTTP_{status}",
                    )

                retry_after = (
                    response.headers.get(
                        "Retry-After",
                        "",
                    )
                )

                pause = (
                    int(retry_after)
                    if retry_after.isdecimal()
                    else 2 ** attempt + 1
                )

                self.sleep(
                    min(
                        120.0,
                        max(2.0, pause),
                    )
                )

                continue

            # ------------------------------------------------
            # HTTP RESPONSE
            # ------------------------------------------------

            if status == 404:
                return (
                    None,
                    "NOT_FOUND",
                )

            if status != 200:
                return (
                    None,
                    f"DEFERRED_HTTP_{status}",
                )

            content_type = (
                response.headers.get(
                    "Content-Type",
                    "",
                ).lower()
            )

            if "html" not in content_type:
                return (
                    None,
                    "DEFERRED_CONTENT",
                )

            return (
                response.text,
                "OK",
            )

        return (
            None,
            "DEFERRED_NETWORK",
        )

    # --------------------------------------------------------
    # COLLECT ONE COMPANY
    #
    # HTTP only. No MySQL or checkpoint writes.
    # --------------------------------------------------------

    def collect(self, jp_no):

        branch_url = (
            f"{SITE}/company-branch/"
            f"{jp_no}/show/printview"
        )

        html, result = self.get(
            branch_url
        )

        # ----------------------------------------------------
        # VALID BRANCH PRINTVIEW
        # ----------------------------------------------------

        if result == "OK":

            try:

                branches, total = parse_printview(
                    html,
                    jp_no,
                )

            except CollectorError:

                return (
                    [],
                    None,
                    "DEFERRED_PARSE",
                    branch_url,
                )

            return (
                branches,
                total,

                "OK"
                if branches
                else "NO_BRANCH",

                branch_url,
            )

        # ----------------------------------------------------
        # TRANSIENT FAILURE
        # ----------------------------------------------------

        if result.startswith(
            "DEFERRED"
        ):

            return (
                [],
                None,
                result,
                branch_url,
            )

        # ----------------------------------------------------
        # FALLBACK COMPANY PAGE
        # ----------------------------------------------------

        company_url = (
            f"{SITE}/company/{jp_no}/"
        )

        fallback_html, fallback = self.get(
            company_url
        )

        if fallback == "NOT_FOUND":

            status = "NOT_LISTED"

        elif fallback == "OK":

            classification = parse_company_page(
                fallback_html,
                jp_no,
            )

            status = (
                "DEFERRED_BRANCH"
                if classification == "HAS_BRANCH_LINK"
                else classification
            )

        elif fallback.startswith(
            "DEFERRED"
        ):

            status = fallback

        else:

            status = "UNAVAILABLE"

        return (
            [],
            None,
            status,
            branch_url,
        )

    def close(self):

        for session in self.sessions:
            session.close()


# ============================================================
# TERMINAL UI
# ============================================================

class UI:
    def __init__(self, website, codes, journal, mode):
        self.console = Console()
        self.website = website
        self.codes = ",".join(codes)
        self.journal = journal
        self.mode = mode
        self.run_id = (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                       + f"-pid{os.getpid()}")
        self.started = time.monotonic()
        self.pass_no = 0
        self.stage = "READY"
        self.current = "-"
        self.examined = 0          # This pass only; not cumulative unique coverage
        self.scanned_total = 0     # Source rows scanned across all passes
        self.checked = 0           # HTTP companies committed this run
        self.branches = 0          # Branch rows returned this run
        self.legacy = 0
        self.skipped = 0           # Skip observations during first pass only
        self.unknown = 0           # First-pass identities missing current result
        self.source_changed = 0    # Fingerprint change is audit-only
        self.integrity_issues = set()
        self.outcomes = Counter()
        self.recent = deque(maxlen=6)
        self.live = None
        self.progress = Progress(SpinnerColumn(), BarColumn(bar_width=None),
                                 TextColumn("{task.completed:,.0f} source rows this pass"),
                                 TimeElapsedColumn(), expand=True)
        self.task = self.progress.add_task("scan", total=None)

    def render(self):
        elapsed = max(0.001, time.monotonic() - self.started)
        coverage = self.journal.snapshot()
        metrics = Table.grid(expand=True, padding=(0, 2))
        for _ in range(4):
            metrics.add_column(ratio=1)
        metrics.add_row(f"Phase {self.journal.active_phase}",
                        f"Run {self.run_id}", f"Pass {self.pass_no}",
                        f"Mode {self.mode}")
        metrics.add_row(f"Terminal {coverage['terminal']:,}",
                        f"Deferred {coverage['deferred']:,}",
                        f"Stale review {coverage['review']:,}",
                        f"Integrity issues {len(self.integrity_issues):,}")
        metrics.add_row(f"Source rows/pass {self.examined:,}",
                        f"Skipped (pass 1) {self.skipped:,}",
                        f"Unexamined (pass 1) {self.unknown:,}",
                        f"HTTP companies/run {self.checked:,}")
        metrics.add_row(f"Branches/run {self.branches:,}",
                        f"Legacy adopted/run {self.legacy:,}",
                        f"HTTP requests/run {self.website.count:,}",
                        f"Avg {self.checked / elapsed:.2f} company/s")
        metrics.add_row(f"Branches OK {self.journal.counts['OK']:,}",
                        f"No branches {self.journal.counts['NO_BRANCH']:,}",
                        f"Legacy OK {self.journal.counts['LEGACY_OK']:,}",
                        f"Unlisted/unavailable {sum(self.journal.counts[s] for s in ('NOT_LISTED','NO_BRANCH_LISTED','UNAVAILABLE')):,}")
        outcomes = Table.grid(expand=True)
        outcomes.add_column()
        outcomes.add_row(
            "Results this run: " + ", ".join(f"{k}={v:,}" for k,v in sorted(self.outcomes.items()))
            if self.outcomes else "Results this run: no HTTP results yet")
        recent = Table(expand=True, box=None)
        recent.add_column("Company")
        recent.add_column("Result")
        recent.add_column("Branches", justify="right")
        for jp_no, result, count in reversed(self.recent):
            recent.add_row(jp_no, result, str(count))
        footer = (f"Stage {self.stage} | Current {self.current} | Status {self.codes} "
                  f"| Source changes (no refetch) {self.source_changed:,}\n"
                  f"Workers {self.website.active}/{self.website.concurrency} "
                  f"| Global {self.website.rate:g} HTTP req/s "
                  f"| Source reads total {self.scanned_total:,} (passes may overlap)\n"
                  "Terminal = examined per Phase, not necessarily branch-data success. "
                  "CAUGHT_UP = observed source, not permanent completeness.\n"
                  "Ctrl+C = safe stop | Same command = same Phase")
        return Group(Panel(metrics, title="DBD BRANCH COLLECTOR · PHASE AWARE"),
                     self.progress, outcomes,
                     Panel(recent, title="Recent Results"),
                     Panel(footer, border_style="dim"))

    def refresh(self):
        if self.live is not None:
            self.live.update(self.render(), refresh=True)

    def result(self, company, status, count):
        self.checked += 1
        self.branches += count
        self.outcomes[status] += 1
        self.current = key_text(company)
        self.recent.append((str(company["jp_no"]), status, count))
        self.refresh()


def _validate_positive(previous, db_row):
    if not db_row or previous[2] <= 0:
        return False
    expected = previous[2]
    return (int(db_row["actual"]) == expected
            and int(db_row["min_count"]) == expected
            and int(db_row["max_count"]) == expected
            and not int(db_row["bad_address"]))


def _write_http_result(db, journal, ui, company, payload):
    """Single writer: compare, transaction commit, fsync Journal, then UI."""
    branches, total, result, url = payload
    if result in ("OK", "NO_BRANCH"):
        old_numbers = db.current_branch_numbers(company)
        incoming = {str(branch[0]) for branch in branches}
        if old_numbers - incoming:
            result = "STALE_BRANCHES"
    if branches:
        db.commit_branches(company, branches, url)
    journal.append(company, company["_fingerprint"], result, len(branches), "http")
    ui.result(company, result, len(branches))


def collect_batch(candidates, pool, website, db, journal, ui, attempted):
    """Bounded and completion-driven; all writes run in the caller's thread.

    On interruption, stop admitting tasks, preserve already finished futures,
    and re-raise after draining. No HTTP worker ever writes MySQL/Journal.
    """
    limit = max(1, website.concurrency * 2)
    work = iter(candidates)
    pending = {}
    interrupted = None

    def fill():
        while not website.stop.is_set() and len(pending) < limit:
            try:
                company = next(work)
            except StopIteration:
                break
            identity = key(company)
            if identity in attempted:
                continue
            attempted.add(identity)  # once in this Run, independent of fp
            future = pool.submit(website.collect, str(company["jp_no"]))
            pending[future] = company

    fill()
    try:
        while pending:
            done, _ = wait(tuple(pending), timeout=0.25, return_when=FIRST_COMPLETED)
            if not done:
                ui.refresh()
                continue
            for future in done:
                company = pending.pop(future)
                if future.cancelled():
                    continue
                try:
                    result = future.result()
                except AccessDenied as exc:
                    interrupted = exc
                    website.stop.set()
                    continue
                except CollectorError as exc:
                    if website.stop.is_set():
                        interrupted = interrupted or exc
                        continue
                    raise
                _write_http_result(db, journal, ui, company, result)
            if interrupted is None:
                fill()
            else:
                break
    except KeyboardInterrupt as exc:
        interrupted = exc
        website.stop.set()
    finally:
        if interrupted is not None:
            website.stop.set()
            for future in pending:
                future.cancel()
            # Preserve results already completed and usable; do not wait for
            # unfinished HTTP tasks before releasing the single writer.
            for future, company in list(pending.items()):
                if future.done() and not future.cancelled():
                    try:
                        result = future.result()
                    except (CollectorError, requests.RequestException):
                        continue
                    _write_http_result(db, journal, ui, company, result)
    if interrupted is not None:
        raise interrupted


def _check_preflight_state(journal, checkpoint_exists, has_target_data):
    if (not journal.phases and not journal.records and
        (has_target_data or checkpoint_exists)):
        raise CollectorError(
            "Journal has no recoverable company results/Phase metadata, but "
            "historical checkpoint or MySQL branch rows exist. Cannot infer "
            "zero-branch/UNAVAILABLE results; recover the original Journal "
            "from backup or use a separately authorized recovery procedure."
        )


def _check_requested_phase(journal, codes, args):
    if journal.phases and not args.new_phase:
        expected = journal.phases[journal.active_phase]["scope"]
        if expected != list(codes):
            raise CollectorError(
                f"Phase {journal.active_phase} scope mismatch; "
                f"expected {','.join(expected)}, supplied {','.join(codes)}"
            )
    if not journal.phases and not journal.records:
        if args.continue_phase:
            raise CollectorError("--continue requires a recognized previous Phase")
        if args.new_phase:
            raise CollectorError("--new-phase requires an existing Phase")


def checkpoint_conflict_count(journal, old_deferred):
    """Older checkpoint DEFERRED does not override terminal Journal truth."""
    if journal.active_phase not in (None, 1):
        return 0
    return sum(1 for t, j in old_deferred
               if (record := journal.records.get(t + ":" + j)) is not None
               and record[1] in PHASE_TERMINAL_STATUSES)


def _preflight_report(journal, codes, checkpoint, db, old_deferred):
    _check_preflight_state(journal, CHECKPOINT.exists(), db.has_existing_branch_data())
    snapshots = journal.snapshot()
    conflicts = checkpoint_conflict_count(journal, old_deferred)
    label = (f"Phase {journal.active_phase}"
             if journal.phases else
             "Legacy Phase 1 (migration eligible)" if journal.records else
             "New installation (initialize Phase 1)")
    Console().print(
        f"PREFLIGHT READ-ONLY | {label} | scope={','.join(codes)}\n"
        f"Current Journal: {journal.path} (exists={journal.exists})\n"
        f"Journal records={journal.total_records:,} "
        f"| legacy records={journal.legacy_records:,} "
        f"| latest current-Phase results={snapshots['recorded']:,}\n"
        f"Terminal={snapshots['terminal']:,} "
        f"| Deferred={snapshots['deferred']:,} "
        f"| Review={snapshots['review']:,}\n"
        f"Tail repair needed={journal.tail_bytes:,} bytes "
        f"| Historical checkpoint present={checkpoint}\n"
        f"Checkpoint deferred vs terminal Journal conflicts={conflicts:,} "
        "(Journal wins)\n"
        "Legacy scope, if any, is adopted from supplied --status-codes; "
        "not historically verified.\n"
        "Coverage counts are Journal-only; not exact live source coverage.\n"
        "No Phase metadata written. No HTTP. No Journal repair. No MySQL data writes."
    )


def run(args):
    codes = status_codes(args.status_codes)
    old_deferred = read_previous_checkpoint(CHECKPOINT)
    db = journal = website = pool = None
    try:
        # GET_LOCK is acquired before any writable state operation. Reconnects
        # reacquire this same lock or fail closed.
        db = MySQL()
        db.preflight()
        journal = Journal(JOURNAL, read_only=args.preflight)
        _check_preflight_state(journal, CHECKPOINT.exists(), db.has_existing_branch_data())
        _check_requested_phase(journal, codes, args)
        if args.preflight:
            _preflight_report(journal, codes, CHECKPOINT.exists(), db, old_deferred)
            return 0

        is_migration = bool(journal.records and not journal.phases)
        # Snapshot original Journal BEFORE any recoverable-tail truncation.
        journal.prepare_for_writes(legacy_migration=is_migration)
        mode = establish_phase(journal, codes, args.continue_phase, args.new_phase)
        Console().print(
            f"PHASE {journal.active_phase} | {mode} | "
            f"scope={','.join(codes)} | recorded={journal.snapshot()['recorded']:,}"
        )
        if args.new_phase:
            Console().print("NEW PHASE persisted: future ordinary invocations resume "
                            "this Phase; do not repeat --new-phase to resume.")
        if is_migration:
            Console().print("Legacy records adopted as Phase 1, historical scope "
                            "bound to supplied --status-codes (not independently verified).")
        if journal.active_phase == 1:
            conflicts = checkpoint_conflict_count(journal, old_deferred)
            if conflicts:
                Console().print(
                    f"CHECKPOINT WARNING: {conflicts:,} old deferred identities "
                    "have terminal Journal results; latest Journal terminal wins."
                )

        website = Website()
        ui = UI(website, codes, journal, mode)
        pool = ThreadPoolExecutor(max_workers=website.concurrency,
                                  thread_name_prefix="branch-http")
        attempted = set()          # HTTP identities (once per Run)
        validated_positive = set() # Verified without extra HTTP this Run
        review_once = set()
        quiet = 0
        limit_hit = False
        with Live(ui.render(), console=ui.console, refresh_per_second=3,
                  transient=False) as live:
            ui.live = live
            while True:
                ui.pass_no += 1
                ui.examined = 0
                ui.progress.reset(ui.task)
                position = None
                activity = 0
                while True:
                    ui.stage = "READ SOURCE IDENTITIES"
                    rows = db.select_page(codes, position, args.batch_size)
                    if not rows:
                        break
                    position = key(rows[-1])
                    ui.examined += len(rows)
                    ui.scanned_total += len(rows)
                    ui.progress.update(ui.task, advance=len(rows))
                    unknown, candidates, verify_positive = [], [], []
                    for company in rows:
                        company["_fingerprint"] = fingerprint(company, db.fields)
                        previous = journal.get(company)
                        identity = key(company)
                        if previous is None:
                            if ui.pass_no == 1:
                                ui.unknown += 1
                            # Only Phase 1 can adopt legacy MySQL data.
                            if journal.active_phase == 1:
                                unknown.append(company)
                            else:
                                candidates.append(company)
                            continue
                        kind = result_class(previous[1])
                        if previous[0] != company["_fingerprint"] and ui.pass_no == 1:
                            ui.source_changed += 1
                        if kind == "TERMINAL":
                            if identity in review_once:
                                continue
                            if previous[1] in ("OK", "LEGACY_OK") and identity not in validated_positive:
                                verify_positive.append((company, previous))
                            else:
                                if ui.pass_no == 1:
                                    ui.skipped += 1
                            continue
                        if kind == "REVIEW":
                            review_once.add(identity)
                            continue
                        # A Deferred result retries once in the next Run;
                        # its current source fingerprint cannot reset that cap.
                        if identity not in attempted:
                            candidates.append(company)

                    if verify_positive:
                        checks = db.existing_positive([c for c, _ in verify_positive])
                        for company, previous in verify_positive:
                            identity = key(company)
                            if _validate_positive(previous, checks.get(identity)):
                                validated_positive.add(identity)
                                if ui.pass_no == 1:
                                    ui.skipped += 1
                            else:
                                ui.integrity_issues.add(identity)
                                review_once.add(identity)
                                if ui.pass_no == 1:
                                    ui.skipped += 1
                    if unknown:
                        positives = db.existing_positive(unknown)
                        for company in unknown:
                            identity = key(company)
                            existing = positives.get(identity)
                            valid = bool(
                                existing and identity not in old_deferred and
                                int(existing["actual"]) > 0 and
                                int(existing["actual"]) == int(existing["min_count"])
                                == int(existing["max_count"]) and
                                not int(existing["bad_address"]) and
                                (existing["jp_name"] or "") == (company["jp_name"] or "") and
                                (existing["jp_name_e"] or "") == (company["jp_name_e"] or "")
                            )
                            if valid:
                                journal.append(company, company["_fingerprint"],
                                               "LEGACY_OK", int(existing["actual"]), "legacy")
                                validated_positive.add(identity)
                                ui.legacy += 1
                                activity += 1
                            else:
                                candidates.append(company)
                    if candidates:
                        remaining = (args.limit - len(attempted)) if args.limit else len(candidates)
                        if remaining <= 0:
                            limit_hit = True
                            break
                        if len(candidates) > remaining:
                            candidates = candidates[:remaining]
                            limit_hit = True
                        ui.stage = "COLLECT HTTP / COMMIT AS COMPLETED"
                        before = ui.checked
                        collect_batch(candidates, pool, website, db, journal, ui, attempted)
                        activity += ui.checked - before
                        if args.limit and len(attempted) >= args.limit:
                            limit_hit = True
                            break
                    ui.refresh()
                if limit_hit:
                    ui.stage = "LIMIT REACHED"
                    ui.refresh()
                    break
                quiet = 0 if activity else quiet + 1
                ui.stage = "RECONCILE" if quiet < 2 else "FINALIZE"
                ui.refresh()
                if quiet >= 2:
                    break
        coverage = journal.snapshot()
        review = coverage["review"] + len(ui.integrity_issues)
        if review:
            ui.stage = "NEEDS_REVIEW"
            exit_code = 3
        elif coverage["deferred"]:
            ui.stage = "PARTIAL - DEFERRED"
            exit_code = 2
        elif limit_hit:
            ui.stage = "LIMIT REACHED"
            exit_code = 0  # Historical --limit behavior (explicit stop)
        else:
            ui.stage = "CAUGHT_UP"
            exit_code = 0
        ui.console.print(
            f"FINISHED | phase={journal.active_phase} | run={ui.run_id} "
            f"| stage={ui.stage} | terminal={coverage['terminal']:,} "
            f"| deferred={coverage['deferred']:,} | stale={coverage['review']:,} "
            f"| integrity={len(ui.integrity_issues):,} "
            f"| HTTP companies={ui.checked:,} | HTTP requests={website.count:,} "
            f"| branches={ui.branches:,} | existing adopted={ui.legacy:,} "
            f"| outcomes={dict(ui.outcomes)}"
        )
        return exit_code
    finally:
        if website is not None:
            website.stop.set()
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)
        if website is not None:
            website.close()
        if journal is not None:
            journal.close()
        if db is not None:
            db.close()


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "DBD Branch Collector "
            "with identity-based reconciliation "
            "and one-command safe resume"
        )
    )

    parser.add_argument(
        "--status-codes",

        default=os.getenv(
            "DATAFORTHAI_STATUS_CODES",
            "1",
        ),

        help=(
            "Comma-separated company status codes. "
            "Example: 1,A,B"
        ),
    )

    parser.add_argument(
        "--batch-size",

        type=int,

        default=100,

        help=(
            "Number of source identities read "
            "per keyset page"
        ),
    )

    parser.add_argument(
        "--limit",

        type=int,

        default=0,

        help=(
            "Maximum HTTP-checked companies "
            "during this invocation; 0 = unlimited"
        ),
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--continue", dest="continue_phase", action="store_true",
                       help="Require and resume an existing Phase")
    group.add_argument("--new-phase", action="store_true",
                       help="Explicitly start a fresh Phase; never for ordinary resume")
    parser.add_argument("--preflight", action="store_true",
                        help="Read-only validation of database, checkpoint, Journal and Phase")

    args = parser.parse_args()
    if args.preflight and args.new_phase:
        parser.error("--preflight and --new-phase cannot be combined")

    if not (
        1 <= args.batch_size <= 500
    ):
        parser.error(
            "--batch-size must be between 1 and 500"
        )

    if args.limit < 0:
        parser.error(
            "--limit must be >= 0"
        )

    return run(args)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        raise SystemExit(
            main()
        )

    except KeyboardInterrupt:
        Console(stderr=True).print(
            "\n[yellow]STOPPED BY USER: current Phase preserved. "
            "Rerun the same command to resume.[/]"
        )
        raise SystemExit(130)

    except AccessDenied as exc:
        Console(stderr=True).print(f"[bold red]ACCESS RESTRICTED:[/] {exc}")
        raise SystemExit(1)

    except (
        CollectorError,
        pymysql.MySQLError,
        requests.RequestException,
        OSError,
        ValueError,
    ) as exc:

        Console(
            stderr=True
        ).print(
            f"[bold red]{'PREFLIGHT FAILED' if '--preflight' in sys.argv else 'STOPPED safely'}:[/] {exc}"
        )

        raise SystemExit(1)