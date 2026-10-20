#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DBD BRANCH COLLECTOR

Run this same command for first collection, resume, new companies, source
changes, and unresolved retries:

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
It does not identify every previously checked zero-branch or unavailable
company. Those identities may require verification again.

A company's legal identity and source-content fingerprint cannot detect
changes occurring exclusively on the DataForThai website when its source
record remains unchanged.

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
import threading
import time

from collections import Counter, deque

from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeout,
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

COMPLETE = {
    "OK",
    "NO_BRANCH",
    "LEGACY_OK",
}

# Changes in these source business fields can invalidate
# a previous branch collection result.
#
# Runtime statuses, timestamps, financial endpoints,
# and worker-control fields are intentionally excluded.

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

    def __init__(self, path):

        self.path = path

        self.records = {}

        self.fd = None

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # ----------------------------------------------------
        # LOAD EXISTING RECORDS
        # ----------------------------------------------------

        if path.exists():

            good_offset = 0

            with path.open("rb") as file:

                while True:

                    line = file.readline()

                    if not line:
                        break

                    # An incomplete trailing record may have
                    # been left by an interrupted write.

                    if not line.endswith(b"\n"):
                        break

                    try:

                        obj = json.loads(line)

                        jp_type = obj["t"]
                        jp_no = obj["j"]
                        fp = obj["f"]
                        status = obj["s"]

                        count = obj.get(
                            "n",
                            0,
                        )

                        method = obj.get(
                            "m",
                            "http",
                        )

                        valid = (
                            isinstance(jp_type, str)
                            and isinstance(jp_no, str)

                            and isinstance(fp, str)
                            and bool(
                                re.fullmatch(
                                    r"[0-9a-f]{64}",
                                    fp,
                                )
                            )

                            and isinstance(status, str)

                            and method in (
                                "http",
                                "legacy",
                            )

                            and type(count) is int
                            and count >= 0
                        )

                        if not valid:
                            raise ValueError(
                                "Malformed journal record"
                            )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                    ) as exc:

                        raise CollectorError(
                            "Invalid journal record "
                            f"at byte {good_offset}: {exc}"
                        ) from exc

                    journal_key = (
                        jp_type + ":" + jp_no
                    )

                    self.records[journal_key] = (
                        fp,
                        status,
                        count,
                        method,
                    )

                    good_offset = file.tell()

            # ------------------------------------------------
            # RECOVER AN INCOMPLETE TRAILING WRITE
            #
            # Preserve all complete, valid journal records.
            # ------------------------------------------------

            if path.stat().st_size > good_offset:

                with path.open("r+b") as file:

                    file.truncate(
                        good_offset
                    )

                    file.flush()

                    os.fsync(
                        file.fileno()
                    )

        self.fd = os.open(
            str(path),

            os.O_CREAT
            | os.O_WRONLY
            | os.O_APPEND,

            0o600,
        )

    # --------------------------------------------------------
    # LOOKUP COMPANY RESULT
    # --------------------------------------------------------

    def get(self, company):

        return self.records.get(
            key_text(company)
        )

    # --------------------------------------------------------
    # APPEND DURABLE COMPANY RESULT
    # --------------------------------------------------------

    def append(
        self,
        company,
        fp,
        result,
        count,
        method,
    ):

        if method not in (
            "http",
            "legacy",
        ):
            raise CollectorError(
                "Invalid journal method"
            )

        jp_type, jp_no = key(company)

        record = {
            "t": jp_type,
            "j": jp_no,
            "f": fp,
            "s": result,
            "n": count,
            "m": method,
        }

        payload = (
            json.dumps(
                record,

                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

        offset = 0

        while offset < len(payload):

            written = os.write(
                self.fd,
                payload[offset:],
            )

            if written <= 0:
                raise CollectorError(
                    "Unable to write result journal"
                )

            offset += written

        os.fsync(
            self.fd
        )

        self.records[
            jp_type + ":" + jp_no
        ] = (
            fp,
            result,
            count,
            method,
        )

    def close(self):

        if self.fd is not None:

            os.close(
                self.fd
            )

            self.fd = None


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

    def __init__(
        self,
        website,
        codes,
    ):

        self.console = Console()

        self.website = website

        self.codes = ",".join(codes)

        self.started = time.monotonic()

        self.pass_no = 0

        self.stage = "READY"

        self.current = "-"

        self.examined = 0

        self.checked = 0

        self.branches = 0

        self.legacy = 0

        self.skipped = 0

        self.outcomes = Counter()

        self.recent = deque(
            maxlen=6
        )

        self.live = None

        self.progress = Progress(
            SpinnerColumn(),

            BarColumn(
                bar_width=None,
            ),

            TextColumn(
                "{task.completed:,.0f} "
                "identities examined"
            ),

            TimeElapsedColumn(),

            expand=True,
        )

        self.task = self.progress.add_task(
            "scan",
            total=None,
        )

    # --------------------------------------------------------
    # DASHBOARD RENDERING
    # --------------------------------------------------------

    def render(self):

        elapsed = max(
            0.001,
            time.monotonic() - self.started,
        )

        metrics = Table.grid(
            expand=True,
            padding=(0, 2),
        )

        for _ in range(4):

            metrics.add_column(
                ratio=1
            )

        metrics.add_row(
            f"Compared {self.examined:,}",

            f"HTTP companies {self.checked:,}",

            f"Branch rows {self.branches:,}",

            f"HTTP requests {self.website.count:,}",
        )

        deferred = sum(
            amount

            for status, amount in self.outcomes.items()

            if status.startswith("DEFERRED")
        )

        metrics.add_row(
            f"Existing reused {self.legacy:,}",

            f"Skipped {self.skipped:,}",

            f"Unavailable "
            f"{self.outcomes['UNAVAILABLE']:,}",

            f"Deferred {deferred:,}",
        )

        recent = Table(
            expand=True,
            box=None,
        )

        recent.add_column(
            "Company"
        )

        recent.add_column(
            "Result"
        )

        recent.add_column(
            "Branches",
            justify="right",
        )

        for (
            jp_no,
            result,
            count,
        ) in reversed(self.recent):

            recent.add_row(
                jp_no,
                result,
                str(count),
            )

        speed = (
            self.checked / elapsed
        )

        footer = (
            f"Pass {self.pass_no} "
            f"| Stage {self.stage} "
            f"| Current {self.current}\n"

            f"{SOURCE} -> {TARGET} "
            f"| Status {self.codes}\n"

            f"Workers "
            f"{self.website.active}/"
            f"{self.website.concurrency} "

            f"| Rate cap "
            f"{self.website.rate:g} req/s "

            f"| Average "
            f"{speed:.2f} comp/s\n"

            "Ctrl+C = safe stop "
            "| Same command = resume / catch-up"
        )

        return Group(
            Panel(
                metrics,

                title=(
                    "DBD BRANCH COLLECTOR · MySQL"
                ),
            ),

            self.progress,

            Panel(
                recent,

                title="Recent Results",
            ),

            Panel(
                footer,
                border_style="dim",
            ),
        )

    def refresh(self):

        if self.live is not None:

            self.live.update(
                self.render(),
                refresh=True,
            )

    # --------------------------------------------------------
    # RECORD COMMITTED RESULT
    # --------------------------------------------------------

    def result(
        self,
        company,
        status,
        count,
    ):

        self.checked += 1

        self.branches += count

        self.outcomes[status] += 1

        self.current = key_text(
            company
        )

        self.recent.append(
            (
                str(company["jp_no"]),
                status,
                count,
            )
        )

        self.refresh()


# ============================================================
# MAIN COLLECTION
# ============================================================

def run(args):

    codes = status_codes(
        args.status_codes
    )

    # Preserve and read the previous checkpoint.
    # Historical deferred identities must not be
    # incorrectly treated as completed legacy results.

    old_deferred = read_previous_checkpoint(
        CHECKPOINT
    )

    db = None

    journal = None

    website = None

    pool = None

    try:

        # ----------------------------------------------------
        # PREFLIGHT
        #
        # Validate MySQL before creating the journal
        # or starting any HTTP request.
        # ----------------------------------------------------

        db = MySQL()

        db.preflight()

        # ----------------------------------------------------
        # DURABLE COMPANY RESULT STATE
        # ----------------------------------------------------

        journal = Journal(
            JOURNAL
        )

        # ----------------------------------------------------
        # HTTP CLIENT
        # ----------------------------------------------------

        website = Website()

        # ----------------------------------------------------
        # TERMINAL UI
        # ----------------------------------------------------

        ui = UI(
            website,
            codes,
        )

        # ----------------------------------------------------
        # HTTP WORKERS
        # ----------------------------------------------------

        pool = ThreadPoolExecutor(
            max_workers=website.concurrency,
            thread_name_prefix="branch-http",
        )

        # An unresolved identity is attempted once per
        # source fingerprint during this invocation.
        #
        # The next invocation can retry unresolved results.

        attempted = set()

        # Positive branch results are verified against
        # MySQL once per identity/fingerprint in this run.

        verified_positive = set()

        quiet = 0

        with Live(
            ui.render(),

            console=ui.console,

            refresh_per_second=3,

            transient=False,
        ) as live:

            ui.live = live

            # ------------------------------------------------
            # RECONCILIATION LOOP
            # ------------------------------------------------

            while True:

                ui.pass_no += 1

                position = None

                activity = 0

                confirmed = 0

                unresolved = 0

                # --------------------------------------------
                # COMPLETE SOURCE IDENTITY PASS
                # --------------------------------------------

                while True:

                    ui.stage = (
                        "READ SOURCE IDENTITIES"
                    )

                    rows = db.select_page(
                        codes,
                        position,
                        args.batch_size,
                    )

                    if not rows:
                        break

                    # ----------------------------------------
                    # MOVE LOCAL READING CURSOR
                    #
                    # This cursor is not a completion marker.
                    #
                    # The next reconciliation pass starts
                    # from the beginning again.
                    # ----------------------------------------

                    position = key(
                        rows[-1]
                    )

                    ui.examined += len(rows)

                    ui.progress.update(
                        ui.task,
                        advance=len(rows),
                    )

                    unknown = []

                    candidates = []

                    verify_positive = []

                    # ----------------------------------------
                    # CLASSIFY SOURCE IDENTITIES
                    # ----------------------------------------

                    for company in rows:

                        company["_fingerprint"] = fingerprint(
                            company,
                            db.fields,
                        )

                        current_fp = company[
                            "_fingerprint"
                        ]

                        previous = journal.get(
                            company
                        )

                        identity = key(
                            company
                        )

                        # ------------------------------------
                        # PREVIOUS SUCCESSFUL RESULT
                        # ------------------------------------

                        completed = (
                            previous is not None

                            and previous[0] == current_fp

                            and previous[1] in COMPLETE

                            and not (
                                identity in old_deferred

                                and previous[3] == "legacy"
                            )
                        )

                        if completed:

                            # --------------------------------
                            # POSITIVE BRANCH RESULTS
                            #
                            # Check MySQL rows before trusting
                            # an existing journal result.
                            # --------------------------------

                            if previous[1] in (
                                "OK",
                                "LEGACY_OK",
                            ):

                                verify_key = (
                                    identity,
                                    current_fp,
                                )

                                if verify_key in verified_positive:

                                    ui.skipped += 1

                                    confirmed += 1

                                else:

                                    verify_positive.append(
                                        (
                                            company,
                                            previous,
                                        )
                                    )

                            # --------------------------------
                            # VALIDATED ZERO-BRANCH RESULT
                            # --------------------------------

                            else:

                                ui.skipped += 1

                                confirmed += 1

                            continue

                        # ------------------------------------
                        # ALREADY ATTEMPTED IN THIS RUN
                        # ------------------------------------

                        attempt_key = (
                            identity,
                            current_fp,
                        )

                        if attempt_key in attempted:

                            ui.skipped += 1

                            unresolved += 1

                            continue

                        # ------------------------------------
                        # NO EXISTING RESULT
                        # ------------------------------------

                        if previous is None:

                            unknown.append(
                                company
                            )

                        # ------------------------------------
                        # CHANGED OR UNRESOLVED RESULT
                        #
                        # Do not re-import old positive rows
                        # as a fresh result when a previously
                        # recorded fingerprint has changed.
                        # ------------------------------------

                        else:

                            candidates.append(
                                company
                            )

                    # ----------------------------------------
                    # VALIDATE JOURNALED POSITIVE RESULTS
                    # ----------------------------------------

                    if verify_positive:

                        stored = db.existing_positive(
                            [
                                company

                                for company, previous
                                in verify_positive
                            ]
                        )

                        for (
                            company,
                            previous,
                        ) in verify_positive:

                            identity = key(
                                company
                            )

                            current = stored.get(
                                identity
                            )

                            expected = previous[2]

                            valid = bool(
                                current

                                and expected > 0

                                and int(
                                    current["actual"]
                                ) == expected

                                and current["min_count"]
                                == expected

                                and current["max_count"]
                                == expected

                                and not int(
                                    current["bad_address"]
                                )
                            )

                            if valid:

                                verified_positive.add(
                                    (
                                        identity,
                                        company["_fingerprint"],
                                    )
                                )

                                ui.skipped += 1

                                confirmed += 1

                            else:

                                candidates.append(
                                    company
                                )

                    # ----------------------------------------
                    # RECOVER HISTORICAL POSITIVE BRANCHES
                    #
                    # Reuse the existing table's records
                    # when their branch counts are valid.
                    #
                    # Historical deferred identities must
                    # be checked instead of automatically
                    # being marked as complete.
                    # ----------------------------------------

                    if unknown:

                        matches = db.existing_positive(
                            unknown
                        )

                        for company in unknown:

                            identity = key(
                                company
                            )

                            existing = matches.get(
                                identity
                            )

                            valid_legacy = bool(
                                existing

                                and identity not in old_deferred

                                and int(
                                    existing["actual"]
                                ) > 0

                                and int(
                                    existing["actual"]
                                )
                                == existing["min_count"]
                                == existing["max_count"]

                                and not int(
                                    existing["bad_address"]
                                )

                                and (
                                    existing["jp_name"]
                                    or ""
                                ) == (
                                    company["jp_name"]
                                    or ""
                                )

                                and (
                                    existing["jp_name_e"]
                                    or ""
                                ) == (
                                    company["jp_name_e"]
                                    or ""
                                )
                            )

                            if valid_legacy:

                                ui.stage = (
                                    "RECOVER EXISTING "
                                    "BRANCH RESULTS"
                                )

                                journal.append(
                                    company,

                                    company["_fingerprint"],

                                    "LEGACY_OK",

                                    int(
                                        existing["actual"]
                                    ),

                                    "legacy",
                                )

                                ui.legacy += 1

                                verified_positive.add(
                                    (
                                        identity,
                                        company["_fingerprint"],
                                    )
                                )

                                confirmed += 1

                                activity += 1

                            else:

                                candidates.append(
                                    company
                                )

                    ui.refresh()

                    # ----------------------------------------
                    # NO HTTP WORK FOR THIS PAGE
                    # ----------------------------------------

                    if not candidates:
                        continue

                    # ----------------------------------------
                    # CONCURRENT HTTP SUBMISSION
                    # ----------------------------------------

                    ui.stage = (
                        "COLLECT HTTP"
                    )

                    tasks = [
                        (
                            company,

                            pool.submit(
                                website.collect,
                                str(
                                    company["jp_no"]
                                ),
                            ),
                        )

                        for company in candidates
                    ]

                    # ----------------------------------------
                    # PROCESS HTTP RESULTS IN ORDER
                    #
                    # Only the main thread writes MySQL
                    # and the result journal.
                    # ----------------------------------------

                    for company, future in tasks:

                        while True:

                            try:

                                (
                                    branches,
                                    total,
                                    result,
                                    url,
                                ) = future.result(
                                    timeout=0.25
                                )

                                break

                            except FutureTimeout:

                                ui.refresh()

                        ui.stage = (
                            "COMMIT RESULTS"
                        )

                        # ------------------------------------
                        # VERIFY REMOVED / MISSING BRANCHES
                        #
                        # Preserve old rows instead of
                        # deleting data automatically.
                        # ------------------------------------

                        if result in (
                            "OK",
                            "NO_BRANCH",
                        ):

                            previous_numbers = (
                                db.current_branch_numbers(
                                    company
                                )
                            )

                            new_numbers = {
                                str(branch[0])
                                for branch in branches
                            }

                            missing_from_new = (
                                previous_numbers
                                - new_numbers
                            )

                            if missing_from_new:

                                result = (
                                    "STALE_BRANCHES"
                                )

                        # ------------------------------------
                        # MYSQL TRANSACTION
                        #
                        # No journal success is written
                        # until MySQL commit succeeds.
                        # ------------------------------------

                        if branches:

                            db.commit_branches(
                                company,
                                branches,
                                url,
                            )

                        # ------------------------------------
                        # DURABLE COMPANY RESULT
                        #
                        # NO_BRANCH is also recorded here.
                        #
                        # UNAVAILABLE, NOT_LISTED,
                        # NO_BRANCH_LISTED, DEFERRED,
                        # and STALE_BRANCHES are not
                        # considered complete.
                        # ------------------------------------

                        journal.append(
                            company,

                            company["_fingerprint"],

                            result,

                            len(branches),

                            "http",
                        )

                        # ------------------------------------
                        # MARK POSITIVE RESULT VERIFIED
                        # ------------------------------------

                        if result == "OK":

                            verified_positive.add(
                                (
                                    key(company),
                                    company["_fingerprint"],
                                )
                            )

                        # ------------------------------------
                        # PREVENT REPEATED REQUESTS WITHIN
                        # THE SAME INVOCATION
                        # ------------------------------------

                        attempted.add(
                            (
                                key(company),
                                company["_fingerprint"],
                            )
                        )

                        activity += 1

                        if result in COMPLETE:

                            confirmed += 1

                        else:

                            unresolved += 1

                        # ------------------------------------
                        # TERMINAL RESULT
                        # ------------------------------------

                        ui.result(
                            company,
                            result,
                            len(branches),
                        )

                        # ------------------------------------
                        # OPTIONAL LIMIT
                        # ------------------------------------

                        if (
                            args.limit

                            and ui.checked
                            >= args.limit
                        ):

                            ui.stage = (
                                "LIMIT REACHED"
                            )

                            ui.refresh()

                            return 0

                    ui.refresh()

                # --------------------------------------------
                # END OF IDENTITY PASS
                # --------------------------------------------

                if activity:

                    quiet = 0

                else:

                    quiet += 1

                # --------------------------------------------
                # CATCH-UP RECONCILIATION
                #
                # Continue looking for identities inserted
                # behind previous cursors or updated during
                # collection.
                #
                # Two passes without new work indicate that
                # the currently observed source is caught up.
                #
                # This is not a guarantee that the source
                # cannot change after the process finishes.
                # --------------------------------------------

                if quiet < 2:

                    ui.stage = (
                        "RECONCILE NEW / UPDATED / MISSED"
                    )

                elif unresolved == 0:

                    ui.stage = (
                        "CAUGHT UP"
                    )

                else:

                    ui.stage = (
                        "PARTIAL - UNRESOLVED"
                    )

                ui.refresh()

                if quiet >= 2:
                    break

        # ----------------------------------------------------
        # FINAL SUMMARY
        # ----------------------------------------------------

        ui.console.print(
            f"FINISHED "
            f"| stage={ui.stage} "

            f"| checked={ui.checked:,} "

            f"| branches={ui.branches:,} "

            f"| existing={ui.legacy:,} "

            f"| HTTP={website.count:,} "

            f"| unresolved={unresolved:,} "

            f"| results={dict(ui.outcomes)}"
        )

        return (
            2
            if unresolved
            else 0
        )

    finally:

        # ----------------------------------------------------
        # STOP HTTP WORKERS
        # ----------------------------------------------------

        if website is not None:

            website.stop.set()

        if pool is not None:

            pool.shutdown(
                wait=True,
                cancel_futures=True,
            )

        if website is not None:

            website.close()

        # ----------------------------------------------------
        # CLOSE JOURNAL
        # ----------------------------------------------------

        if journal is not None:

            journal.close()

        # ----------------------------------------------------
        # RELEASE MYSQL CONNECTION AND LOCK
        # ----------------------------------------------------

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

    args = parser.parse_args()

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

        Console(
            stderr=True
        ).print(
            "\n[yellow]"
            "STOPPED: Ctrl+C; "
            "rerun the same command to resume."
            "[/]"
        )

        raise SystemExit(130)

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
            f"[bold red]STOPPED safely:[/] {exc}"
        )

        raise SystemExit(1)