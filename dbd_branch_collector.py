
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DBD BRANCH COLLECTOR v5.0
=========================

One Python file. MySQL only.

SOURCE:
    dbd_company_deep

TARGET:
    dbd_company_branch

INSTALL:
    python -m pip install PyMySQL python-dotenv requests beautifulsoup4 rich

COMMANDS:
    python dbd_branch_collector.py --jp-no 0107538000533 --dry-run
    python dbd_branch_collector.py --jp-no 0107538000533
    python dbd_branch_collector.py --limit 20
    python dbd_branch_collector.py
    python dbd_branch_collector.py --refresh
    python dbd_branch_collector.py --retry-deferred
    python dbd_branch_collector.py --status-codes 1,A,B --limit 20

EXISTING .env:
    DB_HOST=127.0.0.1
    DB_PORT=3307
    DB_NAME=dbd
    DB_USER=dbd-data
    DB_PASSWORD=your_password

DATA FOR THAI .env:
    DATAFORTHAI_STATUS_CODES=1
    DATAFORTHAI_CONCURRENCY=4
    DATAFORTHAI_RATE_PER_SECOND=2
    DATAFORTHAI_REQUEST_TIMEOUT=30
    DATAFORTHAI_HTTP_ATTEMPTS=3

FEATURES:
    - Concurrent HTTP requests
    - Shared global HTTP rate limiter
    - One MySQL writer
    - Ordered checkpoint commits
    - Safe resume after Ctrl+C
    - Automatic target table creation
    - Additive schema updates
    - Idempotent UPSERT
    - Custom company-status filters
    - Handle unavailable companies
    - Handle companies without listed branches
    - Retry transient failures
    - Live Terminal UI
    - No COUNT(*) or OFFSET pagination
    - No Redis, SQL Server or extra database tables
    - No latitude/longitude

IMPORTANT:
    The configured request rate must be permitted by the source website.
    Authentication restrictions, HTTP 401/403 and HTTP 429 are not bypassed.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import threading
import time
import uuid

from collections import deque
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeout,
)
from contextlib import nullcontext
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
        BarColumn,
        Progress,
        SpinnerColumn,
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

CHECKPOINT = ROOT / ".dbd_branch_checkpoint.json"

RECONNECT_CODES = {
    2006,
    2013,
    2014,
    2055,
}


# ============================================================
# MYSQL TABLE
# ============================================================

CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS `{TARGET}` (

    jp_type_code VARCHAR(4) NOT NULL,
    jp_no VARCHAR(20) NOT NULL,

    jp_name VARCHAR(500) NULL,
    jp_name_e VARCHAR(500) NULL,

    branch_no VARCHAR(5) NOT NULL,
    branch_count INT UNSIGNED NOT NULL,
    branch_name VARCHAR(500) NULL,
    branch_address TEXT NOT NULL,

    province_name VARCHAR(191) NULL,
    district_name VARCHAR(191) NULL,
    subdistrict_name VARCHAR(191) NULL,
    postal_code VARCHAR(10) NULL,

    source_url VARCHAR(255) NOT NULL,

    first_seen_at DATETIME(6)
        NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    fetched_at DATETIME(6) NOT NULL,

    PRIMARY KEY (
        jp_type_code,
        jp_no,
        branch_no
    ),

    KEY idx_branch_jp_no (jp_no),
    KEY idx_branch_province (province_name)

)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci
"""


ADDITIVE_COLUMNS = {
    "jp_name": "VARCHAR(500) NULL",
    "jp_name_e": "VARCHAR(500) NULL",
    "branch_count": "INT UNSIGNED NULL",
    "branch_name": "VARCHAR(500) NULL",
    "branch_address": "TEXT NULL",
    "province_name": "VARCHAR(191) NULL",
    "district_name": "VARCHAR(191) NULL",
    "subdistrict_name": "VARCHAR(191) NULL",
    "postal_code": "VARCHAR(10) NULL",
    "source_url": "VARCHAR(255) NULL",
    "first_seen_at": (
        "DATETIME(6) NULL DEFAULT CURRENT_TIMESTAMP(6)"
    ),
    "fetched_at": "DATETIME(6) NULL",
}


UPSERT_SQL = f"""
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
# HELPERS
# ============================================================

class CollectorError(RuntimeError):
    pass


class AccessDenied(CollectorError):
    pass


def positive(
    name: str,
    default: str,
    kind=float,
):
    try:
        value = kind(
            os.getenv(name, default)
        )

        if value <= 0:
            raise ValueError

        return value

    except ValueError as exc:
        raise CollectorError(
            f"Invalid positive .env setting: {name}"
        ) from exc


def parse_status_codes(
    raw: str,
) -> tuple[str, ...]:

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
            "Invalid status codes; example: 1,A,B"
        )

    return tuple(
        sorted(set(codes))
    )


def clean(value):
    return (
        " ".join(str(value or "").split())
        or None
    )


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def duration(seconds):

    n = max(
        0,
        int(seconds),
    )

    if n >= 3600:
        return (
            f"{n // 3600}h "
            f"{(n % 3600) // 60}m"
        )

    return (
        f"{n // 60}m "
        f"{n % 60}s"
    )


# ============================================================
# ADDRESS PARSER
# ============================================================

def parse_address(
    address: str,
):

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

    postcodes = re.findall(
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

        postcodes[-1]
        if postcodes
        else None,
    )


# ============================================================
# BRANCH HTML PARSER
# ============================================================

def parse_printview(
    html: str,
    jp_no: str,
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

        label = clean(
            cells[0].get_text(
                " ",
                strip=True,
            )
        )

        if label != "รวม":
            continue

        value = (
            clean(
                cells[1].get_text(
                    " ",
                    strip=True,
                )
            )
            or ""
        ).replace(",", "")

        if value.isdecimal():
            expected = int(value)

        break

    if expected is None:
        raise CollectorError(
            f"{jp_no}: cannot validate branch count"
        )

    cards = holder.select(
        ".card"
    )

    if len(cards) != expected:
        raise CollectorError(
            f"{jp_no}: incomplete printview "
            f"({len(cards)}/{expected})"
        )

    branches = []

    seen = set()

    headquarters = 0

    for card in cards:

        code_element = card.select_one(
            ".branch-code"
        )

        info = card.select_one(
            ".branch-info"
        )

        if (
            code_element is None
            or info is None
        ):
            raise CollectorError(
                f"{jp_no}: unexpected branch card"
            )

        label = (
            clean(
                code_element.get_text(
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

        if len(matches) != 1:
            raise CollectorError(
                f"{jp_no}: invalid branch code {label!r}"
            )

        branch_no = matches[0]

        if branch_no in seen:
            raise CollectorError(
                f"{jp_no}: duplicate branch {branch_no}"
            )

        seen.add(branch_no)

        parts = info.find_all(
            "div",
            recursive=False,
        )

        if len(parts) < 2:
            raise CollectorError(
                f"{jp_no}: incomplete branch details"
            )

        branch_name = clean(
            parts[0].get_text(
                " ",
                strip=True,
            )
        )

        branch_address = clean(
            parts[1].get_text(
                " ",
                strip=True,
            )
        )

        if not branch_address:
            raise CollectorError(
                f"{jp_no}/{branch_no}: missing address"
            )

        (
            province,
            district,
            subdistrict,
            postal_code,
        ) = parse_address(
            branch_address
        )

        branches.append(
            (
                branch_no,
                branch_name,
                branch_address,
                province,
                district,
                subdistrict,
                postal_code,
            )
        )

    if (
        headquarters > 1
        or len(branches) + headquarters != expected
    ):
        raise CollectorError(
            f"{jp_no}: establishment count mismatch"
        )

    return branches, expected


# ============================================================
# COMPANY PAGE PARSER
# ============================================================

def parse_company_page(
    html: str,
    jp_no: str,
) -> str:

    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    for element in soup.select(
        "script, style, noscript"
    ):
        element.decompose()

    page_text = soup.get_text(
        " ",
        strip=True,
    )

    explicit_not_found = (
        "ไม่พบข้อมูลนิติบุคคล",
        "ไม่พบข้อมูลผู้ประกอบการ",
        "ไม่พบข้อมูลบริษัท",
        "ไม่พบข้อมูลที่ค้นหา",
    )

    if any(
        phrase in page_text
        for phrase in explicit_not_found
    ):
        return "NOT_LISTED"

    # Verify the requested legal ID.
    # Do not classify a generic homepage as a company
    # without branches.

    identity_urls = []

    canonical = soup.find(
        "link",
        rel="canonical",
    )

    if canonical and canonical.get("href"):
        identity_urls.append(
            canonical["href"]
        )

    og_url = soup.find(
        "meta",
        attrs={
            "property": "og:url"
        },
    )

    if og_url and og_url.get("content"):
        identity_urls.append(
            og_url["content"]
        )

    verified_identity = False

    for value in identity_urls:

        path = urlparse(
            urljoin(SITE, value)
        ).path

        if (
            path == f"/company/{jp_no}"
            or path.startswith(
                f"/company/{jp_no}/"
            )
        ):
            verified_identity = True
            break

    if not verified_identity:
        return "UNAVAILABLE"

    # Detect visible branch links/buttons.

    for element in soup.select(
        "a, button, [role=button]"
    ):

        label = element.get_text(
            " ",
            strip=True,
        )

        href = (
            element.get("href")
            or ""
        )

        onclick = (
            element.get("onclick")
            or ""
        )

        path = (
            urlparse(
                urljoin(SITE, href)
            ).path
            if href
            else ""
        )

        direct_branch_link = path.startswith(
            f"/company-branch/{jp_no}/"
        )

        visible_branch_button = (
            "ตำแหน่งที่ตั้งของสาขา" in label
            or "ข้อมูลตำแหน่งที่ตั้งสาขา" in label
            or "ข้อมูลสาขา" in label
            or (
                "open_branch(" in onclick
                and bool(label)
            )
        )

        if (
            direct_branch_link
            or visible_branch_button
        ):
            return "HAS_BRANCH_LINK"

    return "NO_BRANCH_LISTED"


# ============================================================
# CHECKPOINT
# ============================================================

def new_scan():

    return {
        "last_type": "",
        "last_no": "",

        "checked": 0,
        "branches": 0,

        "no_branch": 0,
        "not_found": 0,
        "unavailable": 0,
    }


def load_checkpoint(
    path: Path,
):

    if not path.exists():

        return {
            "version": 5,
            "scans": {},
            "deferred": [],
        }

    try:

        raw = json.loads(
            path.read_text(
                encoding="utf-8",
            )
        )

    except (
        OSError,
        ValueError,
    ) as exc:

        raise CollectorError(
            f"Cannot read checkpoint safely: {exc}"
        ) from exc

    if not isinstance(raw, dict):
        raise CollectorError(
            "Invalid checkpoint format"
        )

    # Preserve checkpoints created by older versions.
    # Never reuse an unfiltered cursor for status=1.

    if "scans" not in raw:

        required = (
            "last_type",
            "last_no",
            "checked",
            "branches",
            "no_branch",
            "not_found",
        )

        if not all(
            key in raw
            for key in required
        ):
            raise CollectorError(
                "Invalid legacy checkpoint"
            )

        result = {
            "version": 5,
            "scans": {},
            "deferred": raw.get(
                "deferred",
                [],
            ),
            "legacy_unfiltered": {
                key: raw[key]
                for key in required
            },
        }

    else:
        result = raw

    if not isinstance(
        result.get("scans"),
        dict,
    ):
        raise CollectorError(
            "Invalid checkpoint scan profiles"
        )

    if not isinstance(
        result.get("deferred"),
        list,
    ):
        raise CollectorError(
            "Invalid checkpoint deferred queue"
        )

    for key, profile in result["scans"].items():

        if (
            not isinstance(key, str)
            or not isinstance(profile, dict)
        ):
            raise CollectorError(
                "Invalid checkpoint scan profile"
            )

        if not isinstance(
            profile.get("last_type"),
            str,
        ):
            raise CollectorError(
                "Invalid checkpoint type cursor"
            )

        if not isinstance(
            profile.get("last_no"),
            str,
        ):
            raise CollectorError(
                "Invalid checkpoint company cursor"
            )

        for field in (
            "checked",
            "branches",
            "no_branch",
            "not_found",
            "unavailable",
        ):

            value = profile.get(
                field,
                0,
            )

            if (
                type(value) is not int
                or value < 0
            ):
                raise CollectorError(
                    f"Invalid checkpoint counter: {field}"
                )

    for item in result["deferred"]:

        if not isinstance(item, dict):
            raise CollectorError(
                "Invalid deferred queue entry"
            )

        for field in (
            "jp_type_code",
            "jp_no",
            "reason",
        ):

            if not isinstance(
                item.get(field),
                str,
            ):
                raise CollectorError(
                    f"Invalid deferred field: {field}"
                )

    return result


def save_checkpoint(path: Path, state: dict):

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Unique temporary filename prevents collisions.
    temporary = path.with_name(
        f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                state,
                file,
                ensure_ascii=False,
                indent=2,
            )

            file.flush()
            os.fsync(file.fileno())

        # Windows may temporarily lock the destination.
        # Retry only sharing/access errors.
        # Never overwrite the checkpoint non-atomically.

        for attempt in range(10):

            try:
                os.replace(temporary, path)
                return

            except PermissionError as exc:

                if (
                    getattr(exc, "winerror", None)
                    not in (5, 32)
                    or attempt == 9
                ):
                    raise

                time.sleep(
                    min(0.2 * (attempt + 1), 1.0)
                )

    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def mark_deferred(
    items: list,
    company: dict,
    reason: str,
    maximum: int,
):

    jp_type = str(
        company["jp_type_code"]
    )

    jp_no = str(
        company["jp_no"]
    )

    for item in items:

        if (
            item["jp_type_code"] == jp_type
            and item["jp_no"] == jp_no
        ):

            item.update(
                reason=reason,

                attempts=(
                    int(
                        item.get(
                            "attempts",
                            1,
                        )
                    )
                    + 1
                ),

                last_seen_utc=utc_now(),
            )

            return

    if len(items) >= maximum:
        raise CollectorError(
            f"Deferred queue reached {maximum} items"
        )

    items.append(
        {
            "jp_type_code": jp_type,
            "jp_no": jp_no,
            "reason": reason,
            "attempts": 1,
            "last_seen_utc": utc_now(),
        }
    )


def clear_deferred(
    items: list,
    company: dict,
):

    key = (
        str(company["jp_type_code"]),
        str(company["jp_no"]),
    )

    items[:] = [
        item
        for item in items
        if (
            item["jp_type_code"],
            item["jp_no"],
        ) != key
    ]


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
            key
            for key in required
            if os.getenv(key) is None
        ]

        if missing:
            raise CollectorError(
                "Missing MySQL .env values: "
                + ", ".join(missing)
            )

        timeout = positive(
            "DB_TIMEOUT",
            "30",
            int,
        )

        self.settings = {
            "host": os.environ["DB_HOST"],
            "port": int(
                os.environ["DB_PORT"]
            ),
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "database": os.environ["DB_NAME"],

            "charset": "utf8mb4",

            "connect_timeout": timeout,
            "read_timeout": timeout,
            "write_timeout": timeout,

            "autocommit": True,

            "cursorclass": (
                pymysql.cursors.DictCursor
            ),
        }

        self.conn = None

        self.reconnect()

    def reconnect(self):

        if self.conn is not None:

            try:
                self.conn.close()

            except Exception:
                pass

        self.conn = pymysql.connect(
            **self.settings
        )

    def close(self):

        if self.conn is not None:

            self.conn.close()
            self.conn = None

    def query(
        self,
        sql,
        params=None,
        *,
        fetch=None,
        many=False,
        transaction=False,
    ):

        for attempt in range(3):

            try:

                if transaction:
                    self.conn.begin()

                with self.conn.cursor() as cursor:

                    if many:

                        cursor.executemany(
                            sql,
                            params,
                        )

                    else:

                        cursor.execute(
                            sql,
                            params,
                        )

                    if fetch == "all":
                        result = cursor.fetchall()

                    elif fetch == "one":
                        result = cursor.fetchone()

                    else:
                        result = None

                if transaction:
                    self.conn.commit()

                return result

            except (
                pymysql.err.InterfaceError,
                pymysql.err.OperationalError,
                OSError,
            ) as exc:

                if transaction:

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
                    or code in RECONNECT_CODES
                )

                if (
                    not retryable
                    or attempt == 2
                ):
                    raise

                time.sleep(
                    0.5 * (attempt + 1)
                )

                self.reconnect()

            except BaseException:

                if transaction:

                    try:
                        self.conn.rollback()

                    except Exception:
                        pass

                raise

    # --------------------------------------------------------
    # SCHEMA PREFLIGHT
    # --------------------------------------------------------

    def preflight(
        self,
        write: bool,
    ):

        columns = {
            column["Field"]
            for column in self.query(
                f"SHOW COLUMNS FROM `{SOURCE}`",
                fetch="all",
            )
        }

        required = {
            "jp_type_code",
            "jp_no",
            "jp_name",
            "jp_name_e",
            "jp_stat_code",
        }

        missing = required - columns

        if missing:
            raise CollectorError(
                f"Source missing columns: "
                f"{sorted(missing)}"
            )

        if not write:
            return []

        changes = []

        exists = self.query(
            "SHOW TABLES LIKE %s",
            (TARGET,),
            fetch="one",
        )

        if not exists:

            self.query(
                CREATE_TABLE
            )

            changes.append(
                "TABLE CREATED"
            )

        existing = {
            column["Field"]
            for column in self.query(
                f"SHOW COLUMNS FROM `{TARGET}`",
                fetch="all",
            )
        }

        identity = (
            "jp_type_code",
            "jp_no",
            "branch_no",
        )

        if not set(identity).issubset(
            existing
        ):
            raise CollectorError(
                "Target identity columns missing; "
                "manual review required"
            )

        indexes = self.query(
            f"SHOW INDEX FROM `{TARGET}`",
            fetch="all",
        )

        primary_key = [
            item["Column_name"]
            for item in sorted(
                (
                    item
                    for item in indexes
                    if item["Key_name"] == "PRIMARY"
                ),
                key=lambda item: item["Seq_in_index"],
            )
        ]

        if primary_key != list(identity):
            raise CollectorError(
                f"Unexpected target primary key: "
                f"{primary_key}"
            )

        for name, definition in (
            ADDITIVE_COLUMNS.items()
        ):

            if name in existing:
                continue

            self.query(
                f"ALTER TABLE `{TARGET}` "
                f"ADD COLUMN `{name}` {definition}"
            )

            changes.append(
                "COLUMN ADDED: " + name
            )

        # Backfill existing branch rows without HTTP requests.
        # Fill only missing branch_count values.
        # Each company is counted separately by its composite key.

        missing_counts = self.query(
            f"SELECT 1 "
            f"FROM `{TARGET}` "
            f"WHERE branch_count IS NULL "
            f"LIMIT 1",
            fetch="one",
        )

        if missing_counts:

            self.query(
                f"""
                UPDATE `{TARGET}` AS b
                JOIN (
                    SELECT
                        jp_type_code,
                        jp_no,
                        COUNT(*) AS total_branches
                    FROM `{TARGET}`
                    GROUP BY
                        jp_type_code,
                        jp_no
                ) AS counts
                    ON b.jp_type_code = counts.jp_type_code
                   AND b.jp_no = counts.jp_no
                SET b.branch_count = counts.total_branches
                WHERE b.branch_count IS NULL
                """
            )

            changes.append(
                "BACKFILL: existing branch_count completed"
            )

        return changes

    # --------------------------------------------------------
    # KEYSET PAGINATION
    #
    # No COUNT(*)
    # No OFFSET
    # No complete-table preload
    # --------------------------------------------------------

    def select_page(
        self,
        scan: dict,
        codes: tuple[str, ...],
        limit: int,
    ):

        placeholders = ",".join(
            ["%s"] * len(codes)
        )

        sql = (
            "SELECT "
            "jp_type_code, "
            "jp_no, "
            "jp_name, "
            "jp_name_e, "
            "jp_stat_code "
            f"FROM `{SOURCE}` FORCE INDEX (PRIMARY) "
            f"WHERE jp_stat_code IN ({placeholders})"
        )

        params = list(codes)

        if scan["last_type"]:

            sql += (
                " AND ("
                "jp_type_code > %s "
                "OR ("
                "jp_type_code = %s "
                "AND jp_no > %s"
                ")"
                ")"
            )

            params.extend(
                (
                    scan["last_type"],
                    scan["last_type"],
                    scan["last_no"],
                )
            )

        sql += (
            " ORDER BY "
            "jp_type_code, jp_no "
            "LIMIT %s"
        )

        params.append(limit)

        return self.query(
            sql,
            tuple(params),
            fetch="all",
        )

    def select_single(
        self,
        jp_no: str,
    ):

        return self.query(
            "SELECT "
            "jp_type_code, "
            "jp_no, "
            "jp_name, "
            "jp_name_e, "
            "jp_stat_code "
            f"FROM `{SOURCE}` "
            "WHERE jp_no = %s "
            "ORDER BY jp_type_code, jp_no",
            (jp_no,),
            fetch="all",
        )

    def select_deferred(
        self,
        jp_type_code: str,
        jp_no: str,
    ):

        return self.query(
            "SELECT "
            "jp_type_code, "
            "jp_no, "
            "jp_name, "
            "jp_name_e, "
            "jp_stat_code "
            f"FROM `{SOURCE}` "
            "WHERE jp_type_code = %s "
            "AND jp_no = %s",
            (
                jp_type_code,
                jp_no,
            ),
            fetch="one",
        )

    # --------------------------------------------------------
    # IDEMPOTENT UPSERT
    #
    # MySQL writes always run in the main thread.
    # --------------------------------------------------------

    def upsert(
        self,
        company: dict,
        branches: list[tuple],
        url: str,
    ):

        if not branches:
            return

        stamp = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

        branch_count = len(branches)

        rows = [
            (
                company["jp_type_code"],
                company["jp_no"],
                company["jp_name"],
                company["jp_name_e"],
                *branch,
                url,
                stamp,
                branch_count,
            )
            for branch in branches
        ]

        self.query(
            UPSERT_SQL,
            rows,
            many=True,
            transaction=True,
        )


# ============================================================
# CONCURRENT HTTP CLIENT
# ============================================================

class Website:

    def __init__(self):

        self.rate = positive(
            "DATAFORTHAI_RATE_PER_SECOND",
            "2",
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

        self.concurrency = positive(
            "DATAFORTHAI_CONCURRENCY",
            "4",
            int,
        )

        if self.concurrency > 32:
            raise CollectorError(
                "DATAFORTHAI_CONCURRENCY must be <= 32"
            )

        # One rate limiter for all HTTP workers.

        self.lock = threading.Lock()

        self.next_request_at = 0.0

        self.request_count = 0

        self.active_requests = 0

        self.stop_event = threading.Event()

        # Each thread has its own requests.Session.
        # requests.Session is not shared between threads.

        self.local = threading.local()

        self.sessions = []

    # --------------------------------------------------------
    # THREAD-LOCAL SESSION
    # --------------------------------------------------------

    def session(self):

        value = getattr(
            self.local,
            "session",
            None,
        )

        if value is not None:
            return value

        value = requests.Session()

        value.headers.update(
            {
                "User-Agent": os.getenv(
                    "DATAFORTHAI_USER_AGENT",
                    "DBD-BranchCollector/5.0",
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

        self.local.session = value

        with self.lock:
            self.sessions.append(value)

        return value

    def close(self):

        for session in self.sessions:
            session.close()

    # --------------------------------------------------------
    # INTERRUPTIBLE WAIT
    # --------------------------------------------------------

    def sleep(
        self,
        seconds: float,
    ):

        if self.stop_event.wait(
            max(0.0, seconds)
        ):
            raise CollectorError(
                "HTTP workers stopped"
            )

    # --------------------------------------------------------
    # GLOBAL RATE LIMITER
    #
    # Example:
    #
    #   concurrency = 4
    #   rate = 2
    #
    # Total rate = 2 requests/second,
    # NOT 2 requests/second per worker.
    # --------------------------------------------------------

    def acquire(self):

        if self.stop_event.is_set():
            raise CollectorError(
                "HTTP workers stopped"
            )

        with self.lock:

            scheduled = max(
                time.monotonic(),
                self.next_request_at,
            )

            self.next_request_at = (
                scheduled + 1.0 / self.rate
            )

        self.sleep(
            scheduled - time.monotonic()
        )

        with self.lock:
            self.request_count += 1

    # --------------------------------------------------------
    # HTTP GET
    # --------------------------------------------------------

    def get(
        self,
        url: str,
        follow_depth: int = 0,
    ):

        if follow_depth > 2:
            return (
                None,
                "REDIRECT_OTHER",
            )

        for attempt in range(
            self.attempts
        ):

            self.acquire()

            with self.lock:
                self.active_requests += 1

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
                    self.active_requests -= 1

            status = response.status_code

            # -----------------------------------------------
            # ACCESS RESTRICTIONS
            # -----------------------------------------------

            if status in (
                401,
                403,
                429,
            ):

                self.stop_event.set()

                raise AccessDenied(
                    f"HTTP {status}: access restricted; "
                    "collection stopped safely"
                )

            # -----------------------------------------------
            # REDIRECT
            #
            # Follow only canonical destinations belonging
            # to the same company. Do not follow login or
            # security redirects.
            # -----------------------------------------------

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
                    parsed.scheme != "https"
                    or parsed.hostname
                    != "www.dataforthai.com"
                ):

                    self.stop_event.set()

                    raise AccessDenied(
                        "Unexpected redirect destination; "
                        "collection stopped safely"
                    )

                source_path = urlparse(
                    url
                ).path

                match = re.search(
                    r"/(?:company|company-branch)/"
                    r"(\d{13})(?:/|$)",
                    source_path,
                )

                allowed = bool(
                    match
                    and (
                        parsed.path.startswith(
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
                        follow_depth=follow_depth + 1,
                    )

                if parsed.path in (
                    "",
                    "/",
                ):
                    return (
                        None,
                        "REDIRECT_HOME",
                    )

                return (
                    None,
                    "REDIRECT_OTHER",
                )

            # -----------------------------------------------
            # TRANSIENT HTTP ERRORS
            # -----------------------------------------------

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

                if retry_after.isdecimal():

                    pause = int(
                        retry_after
                    )

                else:

                    pause = (
                        2 ** attempt
                        + 1
                    )

                self.sleep(
                    min(
                        120,
                        max(
                            2,
                            pause,
                        ),
                    )
                )

                continue

            # -----------------------------------------------
            # HTTP 404
            # -----------------------------------------------

            if status == 404:

                return (
                    None,
                    "NOT_FOUND",
                )

            # -----------------------------------------------
            # OTHER HTTP ERRORS
            # -----------------------------------------------

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
    # This function executes inside an HTTP worker.
    #
    # It never writes to MySQL or checkpoints.
    # --------------------------------------------------------

    def collect(
        self,
        jp_no: str,
    ):

        started = time.monotonic()

        branch_url = (
            f"{SITE}/company-branch/"
            f"{jp_no}/show/printview"
        )

        html, outcome = self.get(
            branch_url
        )

        # -----------------------------------------------
        # VALID BRANCH PAGE
        # -----------------------------------------------

        if outcome == "OK":

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
                    time.monotonic() - started,
                )

            return (
                branches,
                total,

                (
                    "OK"
                    if branches
                    else "NO_BRANCH"
                ),

                branch_url,
                time.monotonic() - started,
            )

        # -----------------------------------------------
        # TRANSIENT ERROR
        # -----------------------------------------------

        if outcome.startswith(
            "DEFERRED"
        ):

            return (
                [],
                None,
                outcome,
                branch_url,
                time.monotonic() - started,
            )

        # -----------------------------------------------
        # COMPANY PAGE FALLBACK
        #
        # Execute only when branch printview is absent
        # or redirects.
        # -----------------------------------------------

        company_url = (
            f"{SITE}/company/{jp_no}/"
        )

        company_html, fallback = self.get(
            company_url
        )

        if fallback == "NOT_FOUND":

            result = "NOT_LISTED"

        elif fallback == "OK":

            classification = parse_company_page(
                company_html,
                jp_no,
            )

            result = (
                "DEFERRED_BRANCH"
                if classification == "HAS_BRANCH_LINK"
                else classification
            )

        elif fallback.startswith(
            "DEFERRED"
        ):

            result = fallback

        else:

            result = "UNAVAILABLE"

        return (
            [],
            None,
            result,
            branch_url,
            time.monotonic() - started,
        )


# ============================================================
# LIVE TERMINAL UI
# ============================================================

class TerminalUI:

    def __init__(
        self,
        console: Console,
        scan: dict,
        mode: str,
        status: str,
        limit: int,
        website: Website,
        plain: bool,
    ):

        self.console = console

        self.scan = scan

        self.mode = mode

        self.status_codes = status

        self.limit = limit

        self.website = website

        self.plain = (
            plain
            or not console.is_terminal
        )

        self.started = time.monotonic()

        self.stage = "STARTING"

        self.current = "-"

        self.companies = 0

        self.branches = 0

        self.no_branch = 0
        self.not_listed = 0
        self.unavailable = 0
        self.deferred = 0
        self.skipped = 0

        self.retry_pending = 0

        self.recent = deque(
            maxlen=6
        )

        self.progress = Progress(
            SpinnerColumn(),

            BarColumn(
                bar_width=None,
            ),

            TextColumn(
                "{task.completed:,.0f} / {task.total:,.0f}"
                if limit
                else (
                    "{task.completed:,.0f} scanned "
                    "(total unknown)"
                )
            ),

            TimeElapsedColumn(),

            expand=True,
        )

        self.task = self.progress.add_task(
            "companies",

            total=(
                limit
                if limit
                else None
            ),
        )

        self.live = None

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    def stage_change(
        self,
        stage,
        jp_no=None,
    ):

        self.stage = stage

        if jp_no is not None:
            self.current = jp_no

        if self.plain:

            self.console.print(
                f"[cyan]{stage}[/] "
                f"{self.current}"
            )

        self.refresh()

    def refresh(self):

        if self.live is not None:

            self.live.update(
                self.render(),
                refresh=True,
            )

    # --------------------------------------------------------
    # RECORD RESULT
    # --------------------------------------------------------

    def record(
        self,
        jp_no,
        result,
        branch_count,
        page_total,
        seconds,
    ):

        self.companies += 1

        self.branches += branch_count

        self.no_branch += int(
            result in (
                "NO_BRANCH",
                "NO_BRANCH_LISTED",
            )
        )

        self.not_listed += int(
            result == "NOT_LISTED"
        )

        self.unavailable += int(
            result == "UNAVAILABLE"
        )

        self.deferred += int(
            result.startswith(
                "DEFERRED"
            )
        )

        self.skipped += int(
            result == "SKIPPED_STATUS"
        )

        self.recent.append(
            (
                self.companies,
                jp_no,
                result,
                branch_count,
                page_total,
                seconds,
            )
        )

        self.progress.update(
            self.task,
            completed=self.companies,
        )

        self.stage_change(
            result,
            jp_no,
        )

        if self.plain:

            self.console.print(
                f"[{self.companies:>5}] "
                f"{jp_no} "
                f"{result:<19} "
                f"branches={branch_count:>3} "
                f"page="
                f"{page_total if page_total is not None else '-'} "
                f"{seconds:.1f}s"
            )

    # --------------------------------------------------------
    # DASHBOARD
    # --------------------------------------------------------

    def render(self):

        elapsed = max(
            0.001,
            time.monotonic()
            - self.started,
        )

        speed = (
            self.companies / elapsed
        )

        if (
            self.limit
            and speed > 0
        ):

            remaining = max(
                0,
                self.limit
                - self.companies,
            )

            eta = duration(
                remaining / speed
            )

        else:

            eta = "—"

        # ----------------------------------------------------
        # MAIN METRICS
        # ----------------------------------------------------

        metrics = Table.grid(
            expand=True,
            padding=(0, 2),
        )

        for _ in range(4):

            metrics.add_column(
                ratio=1
            )

        metrics.add_row(
            (
                f"Companies "
                f"[cyan]{self.companies:,}[/]"
            ),

            (
                f"Branches "
                f"[green]{self.branches:,}[/]"
            ),

            (
                f"HTTP "
                f"{self.website.request_count:,}"
            ),

            (
                f"Speed "
                f"{speed:.2f} comp/s"
            ),
        )

        # ----------------------------------------------------
        # SECONDARY METRICS
        # ----------------------------------------------------

        secondary = Table.grid(
            expand=True
        )

        secondary.add_column(
            ratio=1
        )

        secondary.add_column(
            ratio=1
        )

        secondary.add_row(
            (
                f"No branches: {self.no_branch} "
                f"| Not listed: {self.not_listed} "
                f"| Unavailable: {self.unavailable} "
                f"| Deferred: {self.deferred}"
            ),

            (
                f" | Workers: "
                f"{self.website.active_requests}/"
                f"{self.website.concurrency} "
                f"| Cap: {self.website.rate:g} req/s "
                f"| ETA: {eta}"
            ),
        )

        # ----------------------------------------------------
        # RECENT COMPANIES
        # ----------------------------------------------------

        recent = Table(
            expand=True,
            show_header=True,
            box=None,
            padding=(0, 1),
        )

        recent.add_column(
            "#",
            justify="right",
            width=5,
        )

        recent.add_column(
            "Company",
            min_width=13,
        )

        recent.add_column(
            "Result",
            min_width=16,
        )

        recent.add_column(
            "Branches",
            justify="right",
        )

        recent.add_column(
            "Total",
            justify="right",
        )

        recent.add_column(
            "Time",
            justify="right",
        )

        for (
            number,
            jp_no,
            result,
            count,
            total,
            seconds,
        ) in reversed(self.recent):

            style = (
                "green"
                if result == "OK"
                else "yellow"
            )

            recent.add_row(
                str(number),
                jp_no,
                f"[{style}]{result}[/]",
                str(count),

                (
                    str(total)
                    if total is not None
                    else "—"
                ),

                f"{seconds:.1f}s",
            )

        # ----------------------------------------------------
        # FOOTER
        # ----------------------------------------------------

        footer = (
            f"Mode: {self.mode} "
            f"| Stage: {self.stage} "
            f"| Current: {self.current}\n"

            f"MySQL: {os.getenv('DB_NAME', '?')} "
            f"| {SOURCE} -> {TARGET} "
            f"| Status: {self.status_codes}\n"

            f"Cursor: "
            f"{self.scan['last_type']}:"
            f"{self.scan['last_no']} "

            f"| Pending retry: "
            f"{self.retry_pending} "

            "| Ctrl+C = safe stop"
        )

        return Group(
            Panel(
                metrics,

                title=(
                    "DBD BRANCH COLLECTOR · MySQL"
                ),

                border_style="cyan",
            ),

            secondary,

            self.progress,

            Panel(
                recent,
                title="Recent Companies",
                border_style="blue",
            ),

            Panel(
                footer,
                border_style="dim",
            ),
        )


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "DBD Concurrent Branch Collector — "
            "MySQL Terminal UI"
        )
    )

    parser.add_argument(
        "--jp-no",
        help=(
            "Single 13-digit legal ID; "
            "does not move global checkpoint"
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help=(
            "Number of eligible companies; "
            "0 means all"
        ),
    )

    parser.add_argument(
        "--status-codes",

        default=os.getenv(
            "DATAFORTHAI_STATUS_CODES",
            "1",
        ),

        help=(
            "Comma-separated jp_stat_code values; "
            "default: 1"
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "No MySQL writes and "
            "no checkpoint writes"
        ),
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
        help=(
            "Rescan selected statuses "
            "from beginning and UPSERT"
        ),
    )

    parser.add_argument(
        "--retry-deferred",
        action="store_true",
        help=(
            "Retry previously deferred companies"
        ),
    )

    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=CHECKPOINT,
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--max-deferred",
        type=int,
        default=10000,
    )

    parser.add_argument(
        "--plain",
        action="store_true",
    )

    parser.add_argument(
        "--parse-file",
        type=Path,
        help=(
            "Offline HTML parser test "
            "without MySQL or HTTP"
        ),
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # ARGUMENT VALIDATION
    # --------------------------------------------------------

    if args.limit < 0:
        parser.error(
            "--limit must be >= 0"
        )

    if args.batch_size < 1:
        parser.error(
            "--batch-size must be >= 1"
        )

    if args.max_deferred < 1:
        parser.error(
            "--max-deferred must be >= 1"
        )

    if (
        args.jp_no
        and not re.fullmatch(
            r"\d{13}",
            args.jp_no,
        )
    ):
        parser.error(
            "--jp-no must contain exactly 13 digits"
        )

    exclusive = sum(
        map(
            bool,
            (
                args.jp_no,
                args.refresh,
                args.retry_deferred,
            ),
        )
    )

    if exclusive > 1:
        parser.error(
            "--jp-no, --refresh and "
            "--retry-deferred cannot be combined"
        )

    # --------------------------------------------------------
    # STATUS FILTER
    # --------------------------------------------------------

    codes = parse_status_codes(
        args.status_codes
    )

    status_key = ",".join(
        codes
    )

    console = Console()

    # --------------------------------------------------------
    # OFFLINE PARSER TEST
    # --------------------------------------------------------

    if args.parse_file:

        html = args.parse_file.read_text(
            encoding="utf-8-sig"
        )

        branches, total = parse_printview(
            html,
            args.jp_no or "SAMPLE",
        )

        console.print(
            "[green]PARSE PASS[/] "
            f"establishments={total} "
            f"branches={len(branches)}"
        )

        return 0

    # --------------------------------------------------------
    # CHECKPOINT
    #
    # Different status filters use different cursors.
    # --------------------------------------------------------

    checkpoint = load_checkpoint(
        args.checkpoint
    )

    scan = (
        new_scan()
        if args.refresh
        else {
            **new_scan(),

            **checkpoint["scans"].get(
                status_key,
                {},
            ),
        }
    )

    # --------------------------------------------------------
    # MODE
    # --------------------------------------------------------

    if args.dry_run:
        mode = "DRY RUN"

    elif args.jp_no:
        mode = "SINGLE"

    elif args.retry_deferred:
        mode = "RETRY DEFERRED"

    elif args.refresh:
        mode = "REFRESH"

    else:
        mode = "COLLECT"

    # --------------------------------------------------------
    # HTTP
    # --------------------------------------------------------

    website = Website()

    # --------------------------------------------------------
    # TERMINAL UI
    # --------------------------------------------------------

    ui_limit = (
        1
        if args.jp_no
        else args.limit
    )

    if (
        args.retry_deferred
        and not ui_limit
    ):

        ui_limit = len(
            checkpoint["deferred"]
        )

    ui = TerminalUI(
        console=console,
        scan=scan,
        mode=mode,
        status=status_key,
        limit=ui_limit,
        website=website,
        plain=args.plain,
    )

    ui.retry_pending = len(
        checkpoint["deferred"]
    )

    db = None

    pool = None

    try:

        context = (
            Live(
                ui.render(),
                console=console,
                refresh_per_second=4,
                transient=False,
            )
            if not ui.plain
            else nullcontext()
        )

        with context as live:

            ui.live = live

            # ------------------------------------------------
            # MYSQL CONNECTION
            # ------------------------------------------------

            ui.stage_change(
                "MYSQL CONNECT"
            )

            db = MySQL()

            # ------------------------------------------------
            # SCHEMA
            # ------------------------------------------------

            ui.stage_change(
                "MYSQL SCHEMA CHECK"
            )

            changes = db.preflight(
                write=not args.dry_run
            )

            for change in changes:

                ui.stage_change(
                    "SCHEMA: " + change
                )

            # ------------------------------------------------
            # CONCURRENT HTTP WORKERS
            # ------------------------------------------------

            pool = ThreadPoolExecutor(
                max_workers=website.concurrency,
                thread_name_prefix="branch-http",
            )

            # ------------------------------------------------
            # DEFERRED QUEUE SNAPSHOT
            # ------------------------------------------------

            jobs = (
                list(
                    checkpoint["deferred"]
                )
                if args.retry_deferred
                else []
            )

            job_index = 0

            # ------------------------------------------------
            # SINGLE COMPANY
            # ------------------------------------------------

            if args.jp_no:

                ui.stage_change(
                    "MYSQL SELECT SINGLE",
                    args.jp_no,
                )

                single_records = db.select_single(
                    args.jp_no
                )

                if not single_records:
                    raise CollectorError(
                        f"{args.jp_no}: "
                        f"not found in {SOURCE}"
                    )

            else:

                single_records = None

            # ------------------------------------------------
            # MAIN COLLECTION LOOP
            # ------------------------------------------------

            while True:

                # --------------------------------------------
                # SINGLE COMPANY
                # --------------------------------------------

                if args.jp_no:

                    records = single_records

                # --------------------------------------------
                # RETRY DEFERRED
                # --------------------------------------------

                elif args.retry_deferred:

                    records = []

                    remaining = (
                        args.limit - ui.companies
                        if args.limit
                        else args.batch_size
                    )

                    if remaining <= 0:
                        break

                    while (
                        job_index < len(jobs)
                        and len(records)
                        < min(
                            args.batch_size,
                            remaining,
                        )
                    ):

                        job = jobs[job_index]

                        job_index += 1

                        company = db.select_deferred(
                            job["jp_type_code"],
                            job["jp_no"],
                        )

                        if company is None:
                            continue

                        current_status = str(
                            company["jp_stat_code"]
                            or ""
                        ).upper()

                        if current_status not in codes:
                            continue

                        records.append(
                            company
                        )

                    if not records:
                        break

                # --------------------------------------------
                # NORMAL KEYSET PAGE
                # --------------------------------------------

                else:

                    remaining = (
                        args.limit - ui.companies
                        if args.limit
                        else args.batch_size
                    )

                    if remaining <= 0:
                        break

                    page_size = min(
                        args.batch_size,
                        remaining,
                    )

                    ui.stage_change(
                        "MYSQL SELECT PAGE"
                    )

                    records = db.select_page(
                        scan,
                        codes,
                        page_size,
                    )

                if not records:
                    break

                # --------------------------------------------
                # FILTER ELIGIBLE COMPANIES
                # --------------------------------------------

                eligible = []

                for company in records:

                    current_status = str(
                        company["jp_stat_code"]
                        or ""
                    ).upper()

                    if current_status in codes:

                        eligible.append(
                            company
                        )

                    elif args.jp_no:

                        ui.record(
                            str(company["jp_no"]),
                            "SKIPPED_STATUS",
                            0,
                            None,
                            0.0,
                        )

                # --------------------------------------------
                # CONCURRENT HTTP SUBMISSION
                #
                # Only the HTTP stage runs concurrently.
                #
                # No worker accesses MySQL or checkpoints.
                # --------------------------------------------

                futures = [
                    pool.submit(
                        website.collect,
                        str(company["jp_no"]),
                    )
                    for company in eligible
                ]

                # --------------------------------------------
                # ORDERED MYSQL WRITE
                #
                # Results are consumed in source order.
                #
                # Even if company #4 finishes before #2,
                # the checkpoint cannot skip #2.
                # --------------------------------------------

                for company, future in zip(
                    eligible,
                    futures,
                ):

                    jp_no = str(
                        company["jp_no"]
                    )

                    ui.stage_change(
                        "HTTP CONCURRENT",
                        jp_no,
                    )

                    # ----------------------------------------
                    # WAIT FOR NEXT ORDERED RESULT
                    #
                    # Refresh the terminal while waiting.
                    # ----------------------------------------

                    while True:

                        try:

                            (
                                branches,
                                page_total,
                                result,
                                source_url,
                                seconds,
                            ) = future.result(
                                timeout=0.25
                            )

                            break

                        except FutureTimeout:

                            ui.refresh()

                    # ----------------------------------------
                    # MYSQL UPSERT
                    #
                    # One writer, executed on main thread.
                    # ----------------------------------------

                    if (
                        not args.dry_run
                        and branches
                    ):

                        ui.stage_change(
                            "MYSQL UPSERT",
                            jp_no,
                        )

                        db.upsert(
                            company,
                            branches,
                            source_url,
                        )

                    # ----------------------------------------
                    # CHECKPOINT
                    #
                    # Advance only after successful database
                    # commit, never on task submission.
                    # ----------------------------------------

                    if not args.jp_no:

                        next_scan = dict(
                            scan
                        )

                        next_queue = [
                            dict(item)
                            for item in checkpoint["deferred"]
                        ]

                        if result.startswith(
                            "DEFERRED"
                        ):

                            mark_deferred(
                                next_queue,
                                company,
                                result,
                                args.max_deferred,
                            )

                        else:

                            clear_deferred(
                                next_queue,
                                company,
                            )

                        # ------------------------------------
                        # NORMAL SCAN CURSOR
                        # ------------------------------------

                        if not args.retry_deferred:

                            next_scan.update(
                                {
                                    "last_type": str(
                                        company["jp_type_code"]
                                    ),

                                    "last_no": jp_no,

                                    "checked": (
                                        scan["checked"]
                                        + 1
                                    ),

                                    "branches": (
                                        scan["branches"]
                                        + len(branches)
                                    ),

                                    "no_branch": (
                                        scan["no_branch"]
                                        + int(
                                            result in (
                                                "NO_BRANCH",
                                                "NO_BRANCH_LISTED",
                                            )
                                        )
                                    ),

                                    "not_found": (
                                        scan["not_found"]
                                        + int(
                                            result == "NOT_LISTED"
                                        )
                                    ),

                                    "unavailable": (
                                        scan["unavailable"]
                                        + int(
                                            result == "UNAVAILABLE"
                                        )
                                    ),

                                    "updated_utc": utc_now(),
                                }
                            )

                        # ------------------------------------
                        # NEXT STATE
                        # ------------------------------------

                        next_state = dict(
                            checkpoint
                        )

                        next_state["scans"] = dict(
                            checkpoint["scans"]
                        )

                        next_state["scans"][
                            status_key
                        ] = next_scan

                        next_state["deferred"] = (
                            next_queue
                        )

                        # ------------------------------------
                        # ATOMIC CHECKPOINT
                        # ------------------------------------

                        if not args.dry_run:

                            ui.stage_change(
                                "CHECKPOINT SAVE",
                                jp_no,
                            )

                            save_checkpoint(
                                args.checkpoint,
                                next_state,
                            )

                        # Keep in-memory scan progress even
                        # in dry-run, without writing a file.

                        checkpoint = next_state

                        scan.update(
                            next_scan
                        )

                        ui.retry_pending = len(
                            next_queue
                        )

                    # ----------------------------------------
                    # TERMINAL RESULT
                    # ----------------------------------------

                    ui.record(
                        jp_no=jp_no,
                        result=result,
                        branch_count=len(branches),
                        page_total=page_total,
                        seconds=seconds,
                    )

                    # ----------------------------------------
                    # LIMIT
                    # ----------------------------------------

                    if (
                        args.limit
                        and ui.companies >= args.limit
                    ):
                        break

                # --------------------------------------------
                # EXIT CONDITIONS
                # --------------------------------------------

                if args.jp_no:
                    break

                if (
                    args.limit
                    and ui.companies >= args.limit
                ):
                    break

            # ------------------------------------------------
            # FINISH TERMINAL UI
            # ------------------------------------------------

            ui.stage_change(
                "FINISHED"
            )

            ui.refresh()

        # ----------------------------------------------------
        # FINAL SUMMARY
        # ----------------------------------------------------

        elapsed_total = (
            time.monotonic()
            - ui.started
        )

        console.print(
            "[green]FINISHED[/] "

            f"companies={ui.companies} "

            f"branches={ui.branches} "

            f"HTTP={website.request_count} "

            f"no_branches={ui.no_branch} "

            f"not_listed={ui.not_listed} "

            f"unavailable={ui.unavailable} "

            f"deferred={ui.deferred} "

            f"retry_pending={ui.retry_pending} "

            f"elapsed={duration(elapsed_total)}"
        )

        return (
            2
            if ui.deferred
            else 0
        )

    finally:

        # Stop accepting new HTTP work.

        website.stop_event.set()

        # Cancel queued tasks and wait for in-flight work.
        # No HTTP worker writes to MySQL.

        if pool is not None:

            pool.shutdown(
                wait=True,
                cancel_futures=True,
            )

        website.close()

        if db is not None:
            db.close()


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
            "STOPPED: Ctrl+C — "
            "resume from last committed checkpoint."
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
