
r"""
scripts/split_failed_buckets.py

DBD FAILED BUCKETS -> RECURSIVE KEYWORD SPLIT -> DISCOVERY OPEN QUEUE

Run:
    python .\scripts\split_failed_buckets.py

Behavior:
    1. Select eligible failed buckets from MySQL.
    2. Preserve every original search filter.
    3. Recursively append digits 0..9 to oversized keywords.
    4. Continue until every terminal child has <= 10,000 results.
    5. Persist complete parent/child trees transactionally.
    6. Mark internal nodes as split.
    7. Mark terminal leaves as pending.
    8. Let the existing MySQL Discovery worker process pending leaves.

No lossless-partition validation is performed.

Safety:
    - One standalone script.
    - No modifications to project source files.
    - No company data deletion.
    - No discovery page deletion.
    - No reset of existing completed buckets.
    - No Redis key modifications.
    - No database writes until an entire root tree is ready.
    - One atomic database transaction per recovered root.
    - Concurrent API counting with a shared rate limiter.
    - Active leases are never overwritten.
    - Re-running does not create duplicate child buckets.
    - Filter compatibility is checked before queue insertion.

Important:
    This script populates the MySQL discovery_buckets queue.
    A Discovery runtime consuming that queue is required.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import sys
import time
import traceback

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Lock
from typing import Any


# ============================================================
# PROJECT IMPORTS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

for directory in (
    PROJECT_ROOT,
    PROJECT_ROOT / "src",
):
    if directory.is_dir() and str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

from dbd.client.endpoints.search import (  # noqa: E402
    ITEMS_PER_PAGE,
    MAX_PAGE,
    SEARCH_PATH,
    SEARCH_REFERER,
    parse_search_response,
)

from dbd.core.settings import settings  # noqa: E402

from dbd.deep.http import (  # noqa: E402
    DeepTransientHttpError,
    FixedRateDbdClient,
)

from dbd.storage.mysql.engine import connect  # noqa: E402


# ============================================================
# CONFIGURATION
# ============================================================

TARGET_ROWS = ITEMS_PER_PAGE * MAX_PAGE

DIGITS = "0123456789"

MAX_KEYWORD_LENGTH = 13
MAX_RECURSIVE_DEPTH = 10
MAX_NODES_PER_ROOT = 3000

MAX_TOTAL_REQUESTS = 30000

ROOT_CONCURRENCY = 3
API_CONCURRENCY = 8

CONFIGURED_RATE = float(
    getattr(settings, "dbd_rate_per_second", 6.0) or 6.0
)

API_RATE_PER_SECOND = min(
    8.0,
    max(0.25, CONFIGURED_RATE),
)

HTTP_TIMEOUT = 60.0
HTTP_RETRIES = 4

EXTRA_RETRIES = 1
EXTRA_RETRY_DELAY = 5.0

LOG_EVERY_CHILD = True
LOG_EVERY_TRANSACTION = True
SHOW_TRACEBACK = True

NUMERIC_FIELDS = (
    "capAmtMin",
    "capAmtMax",
    "totalIncomeMin",
    "totalIncomeMax",
    "netProfitMin",
    "netProfitMax",
    "totalAssetMin",
    "totalAssetMax",
)

NUMERIC_SECTIONS = {
    "cap": {
        "capAmtMin",
        "capAmtMax",
    },
    "income": {
        "totalIncomeMin",
        "totalIncomeMax",
    },
    "profit": {
        "netProfitMin",
        "netProfitMax",
    },
    "asset": {
        "totalAssetMin",
        "totalAssetMax",
    },
}

NUMERIC_BOUNDS = (
    ("capAmtMin", "capAmtMax"),
    ("totalIncomeMin", "totalIncomeMax"),
    ("netProfitMin", "netProfitMax"),
    ("totalAssetMin", "totalAssetMax"),
)


# ============================================================
# ERRORS
# ============================================================

class SplitError(RuntimeError):
    pass


class PreflightError(SplitError):
    pass


class FilterError(SplitError):
    pass


class ExpansionError(SplitError):
    pass


class RequestBudgetError(SplitError):
    pass


class PersistenceError(SplitError):
    pass


class StaleBucketError(PersistenceError):
    pass


class DuplicateBucketError(PersistenceError):
    pass


# ============================================================
# TERMINAL LOGGER
# ============================================================

class Terminal:
    def __init__(self) -> None:
        self.started = time.perf_counter()
        self.lock = Lock()

        self.events = 0
        self.errors = 0
        self.warnings = 0

    def elapsed(self) -> float:
        return time.perf_counter() - self.started

    def log(
        self,
        level: str,
        event: str,
        message: str = "",
        **fields: Any,
    ) -> None:
        with self.lock:
            self.events += 1

            if level == "ERROR":
                self.errors += 1

            if level == "WARN":
                self.warnings += 1

            timestamp = datetime.now().strftime(
                "%H:%M:%S"
            )

            details = " | ".join(
                f"{key}={value}"
                for key, value in fields.items()
                if value is not None
            )

            line = (
                f"[{timestamp}]"
                f"[{self.elapsed():8.1f}s]"
                f"[{level:<5}]"
                f"[{event:<18}]"
            )

            if message:
                line += f" {message}"

            if details:
                line += f" | {details}"

            print(
                line,
                flush=True,
            )

    def section(self, title: str) -> None:
        with self.lock:
            print()
            print("=" * 110)
            print(f"  {title}")
            print("=" * 110)
            print(flush=True)

    def info(
        self,
        event: str,
        message: str = "",
        **fields: Any,
    ) -> None:
        self.log(
            "INFO",
            event,
            message,
            **fields,
        )

    def debug(
        self,
        event: str,
        message: str = "",
        **fields: Any,
    ) -> None:
        self.log(
            "DEBUG",
            event,
            message,
            **fields,
        )

    def success(
        self,
        event: str,
        message: str = "",
        **fields: Any,
    ) -> None:
        self.log(
            "OK",
            event,
            message,
            **fields,
        )

    def warning(
        self,
        event: str,
        message: str = "",
        **fields: Any,
    ) -> None:
        self.log(
            "WARN",
            event,
            message,
            **fields,
        )

    def error(
        self,
        event: str,
        message: str = "",
        **fields: Any,
    ) -> None:
        self.log(
            "ERROR",
            event,
            message,
            **fields,
        )


LOG = Terminal()


# ============================================================
# UTILITIES
# ============================================================

def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def csv_values(
    value: Any,
) -> tuple[str, ...]:
    if value is None:
        return ()

    return tuple(
        part.strip()
        for part in str(value).split(",")
        if part.strip()
    )


def decimal_value(
    value: Any,
) -> Decimal | None:
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        raise FilterError(
            "Boolean numeric filter is invalid"
        )

    try:
        number = Decimal(
            str(value).replace(",", "").strip()
        )

    except (
        InvalidOperation,
        TypeError,
        ValueError,
    ) as exc:
        raise FilterError(
            "Invalid numeric filter value"
        ) from exc

    if not number.is_finite():
        raise FilterError(
            "Non-finite numeric filter"
        )

    return number


def json_numeric(
    value: Decimal,
) -> int | float:
    if value == value.to_integral_value():
        integer = int(value)

        if abs(integer) > 2**53:
            raise FilterError(
                "Numeric integer exceeds safe JSON precision"
            )

        return integer

    converted = float(value)

    if not math.isfinite(converted):
        raise FilterError(
            "Numeric value cannot be encoded"
        )

    if Decimal(str(converted)) != value:
        raise FilterError(
            "Numeric value would lose precision"
        )

    return converted


def error_text(
    exc: BaseException,
) -> str:
    return (
        f"{type(exc).__name__}: {exc}"
    )[:500]


def numeric_from_bucket_key(
    bucket_key: str,
) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}

    for section in bucket_key.split("|"):
        label, separator, content = (
            section.partition("=")
        )

        if not separator:
            continue

        if label not in NUMERIC_SECTIONS:
            continue

        if not content:
            continue

        allowed = NUMERIC_SECTIONS[label]

        for item in content.split(","):
            name, colon, raw = item.partition(":")

            if not colon or name not in allowed:
                raise FilterError(
                    f"Malformed numeric section: {label}"
                )

            value = decimal_value(raw)

            if value is None:
                raise FilterError(
                    f"Empty numeric value: {name}"
                )

            previous = result.get(name)

            if (
                previous is not None
                and previous != value
            ):
                raise FilterError(
                    f"Conflicting numeric value: {name}"
                )

            result[name] = value

    return result


def load_numeric_filters(
    row: dict,
) -> dict[str, Decimal]:
    numeric = numeric_from_bucket_key(
        str(row["bucket_key"])
    )

    raw = row.get("numeric_filters")

    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")

    if isinstance(raw, str):
        raw = json.loads(
            raw,
            parse_float=Decimal,
        )

    if raw is None:
        raw = {}

    if not isinstance(raw, dict):
        raise FilterError(
            "numeric_filters is not a JSON object"
        )

    for name, original in raw.items():
        if name not in NUMERIC_FIELDS:
            raise FilterError(
                f"Unknown numeric filter: {name}"
            )

        value = decimal_value(original)

        if value is None:
            continue

        previous = numeric.get(name)

        if (
            previous is not None
            and previous != value
        ):
            raise FilterError(
                f"Numeric filter conflict: {name}"
            )

        numeric[name] = value

    for minimum, maximum in NUMERIC_BOUNDS:
        lower = numeric.get(minimum)
        upper = numeric.get(maximum)

        if (
            lower is not None
            and upper is not None
            and lower > upper
        ):
            raise FilterError(
                f"Invalid numeric range: {minimum}/{maximum}"
            )

    return numeric


def replace_keyword_in_key(
    original_key: str,
    original_keyword: str,
    new_keyword: str,
) -> str:
    prefix = f"k={original_keyword}"

    if not original_key.startswith(prefix):
        raise FilterError(
            "Bucket key does not begin with original keyword"
        )

    suffix = original_key[len(prefix):]

    if suffix and not suffix.startswith("|"):
        raise FilterError(
            "Malformed bucket key prefix"
        )

    return f"k={new_keyword}{suffix}"


# ============================================================
# SEARCH FILTERS
# ============================================================

@dataclass(frozen=True)
class BucketFilters:
    keyword: str
    search_type: str

    pv_codes: tuple[str, ...]
    jp_statuses: tuple[str, ...]
    jp_types: tuple[str, ...]
    business_sizes: tuple[str, ...]

    numeric: tuple[
        tuple[str, Decimal],
        ...
    ]

    original_bucket_key: str
    original_keyword: str

    def __post_init__(self) -> None:
        if not re.fullmatch(
            r"[0-9]{3,}",
            self.keyword,
        ):
            raise FilterError(
                f"Unsupported keyword: {self.keyword!r}"
            )

        if not self.keyword.startswith(
            self.original_keyword
        ):
            raise FilterError(
                "Child keyword does not extend root keyword"
            )

        replace_keyword_in_key(
            self.original_bucket_key,
            self.original_keyword,
            self.keyword,
        )

    @classmethod
    def from_row(
        cls,
        row: dict,
    ) -> "BucketFilters":
        keyword = str(row["keyword"])

        numeric = load_numeric_filters(
            row
        )

        return cls(
            keyword=keyword,
            search_type=str(
                row.get("search_type") or ""
            ),
            pv_codes=csv_values(
                row.get("pv_codes")
            ),
            jp_statuses=csv_values(
                row.get("jp_statuses")
            ),
            jp_types=csv_values(
                row.get("jp_types")
            ),
            business_sizes=csv_values(
                row.get("business_sizes")
            ),
            numeric=tuple(
                sorted(numeric.items())
            ),
            original_bucket_key=str(
                row["bucket_key"]
            ),
            original_keyword=keyword,
        )

    def with_keyword(
        self,
        keyword: str,
    ) -> "BucketFilters":
        return BucketFilters(
            keyword=keyword,
            search_type=self.search_type,
            pv_codes=self.pv_codes,
            jp_statuses=self.jp_statuses,
            jp_types=self.jp_types,
            business_sizes=self.business_sizes,
            numeric=self.numeric,
            original_bucket_key=(
                self.original_bucket_key
            ),
            original_keyword=(
                self.original_keyword
            ),
        )

    def bucket_key(self) -> str:
        return replace_keyword_in_key(
            self.original_bucket_key,
            self.original_keyword,
            self.keyword,
        )

    def payload(
        self,
        page: int = 1,
    ) -> dict:
        payload = {
            "keyword": self.keyword,
            "type": self.search_type,
            "sortBy": "jpName",
            "currentPage": page,
            "pvCodeList": list(self.pv_codes),
            "jpStatusList": list(self.jp_statuses),
            "jpTypeList": list(self.jp_types),
            "businessSizeList": list(
                self.business_sizes
            ),
        }

        for field_name in NUMERIC_FIELDS:
            payload[field_name] = None

        for field_name, value in self.numeric:
            payload[field_name] = json_numeric(
                value
            )

        return payload

    def report(self) -> dict:
        return {
            "keyword": self.keyword,
            "search_type": self.search_type,
            "pv_codes": list(self.pv_codes),
            "jp_statuses": list(
                self.jp_statuses
            ),
            "jp_types": list(self.jp_types),
            "business_sizes": list(
                self.business_sizes
            ),
            "numeric": {
                key: str(value)
                for key, value in self.numeric
            },
        }


# ============================================================
# RECURSIVE TREE MODELS
# ============================================================

@dataclass
class KeywordNode:
    keyword: str
    relative_depth: int

    parent_keyword: str | None

    count: int
    pages: int

    children: list[str] = field(
        default_factory=list
    )

    @property
    def oversized(self) -> bool:
        return self.count > TARGET_ROWS

    @property
    def is_leaf(self) -> bool:
        return not self.children


@dataclass
class SplitPlan:
    root_id: int
    root_row: dict
    filters: BucketFilters

    nodes: dict[
        str,
        KeywordNode,
    ] = field(default_factory=dict)

    created_at: str = field(
        default_factory=utc_now
    )

    def root(self) -> KeywordNode:
        return self.nodes[
            self.filters.keyword
        ]

    def internal_nodes(self) -> list[KeywordNode]:
        return [
            node
            for node in self.nodes.values()
            if node.children
        ]

    def leaves(self) -> list[KeywordNode]:
        return [
            node
            for node in self.nodes.values()
            if node.is_leaf
        ]

    def max_depth(self) -> int:
        return max(
            (
                node.relative_depth
                for node in self.nodes.values()
            ),
            default=0,
        )

    def leaf_total(self) -> int:
        return sum(
            node.count
            for node in self.leaves()
        )

    def verify_structure(self) -> None:
        if not self.nodes:
            raise ExpansionError(
                "Empty split tree"
            )

        root_keyword = self.filters.keyword

        if root_keyword not in self.nodes:
            raise ExpansionError(
                "Root node missing"
            )

        if self.root().count <= TARGET_ROWS:
            return

        for node in self.nodes.values():
            if node.children:
                expected = [
                    node.keyword + digit
                    for digit in DIGITS
                ]

                if node.children != expected:
                    raise ExpansionError(
                        f"Incomplete children: {node.keyword}"
                    )

                if len(set(node.children)) != 10:
                    raise ExpansionError(
                        f"Duplicate children: {node.keyword}"
                    )

                for child_keyword in node.children:
                    child = self.nodes.get(
                        child_keyword
                    )

                    if child is None:
                        raise ExpansionError(
                            f"Missing node: {child_keyword}"
                        )

                    if (
                        child.parent_keyword
                        != node.keyword
                    ):
                        raise ExpansionError(
                            f"Broken parent link: {child_keyword}"
                        )

                    if (
                        child.relative_depth
                        != node.relative_depth + 1
                    ):
                        raise ExpansionError(
                            f"Broken depth: {child_keyword}"
                        )

            elif node.oversized:
                raise ExpansionError(
                    f"Oversized leaf remains: "
                    f"{node.keyword} ({node.count:,})"
                )

        if len(self.nodes) > MAX_NODES_PER_ROOT:
            raise ExpansionError(
                "Root node budget exceeded"
            )

        all_keywords = [
            node.keyword
            for node in self.nodes.values()
        ]

        if len(all_keywords) != len(
            set(all_keywords)
        ):
            raise ExpansionError(
                "Duplicate keywords within tree"
            )


# ============================================================
# DATABASE PREFLIGHT
# ============================================================

def database_columns() -> dict[str, dict]:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'discovery_buckets'
                """
            )

            rows = cursor.fetchall()

    return {
        row["COLUMN_NAME"]: row
        for row in rows
    }


def validate_schema(
    columns: dict[str, dict],
) -> None:
    required = {
        "id",
        "parent_id",
        "bucket_key",
        "keyword",
        "search_type",
        "pv_codes",
        "jp_statuses",
        "jp_types",
        "business_sizes",
        "depth",
        "total_items",
        "pages_total",
        "pages_done",
        "status",
        "last_error",
        "lease_owner",
        "lease_version",
        "lease_expires_at",
        "created_at",
        "updated_at",
    }

    missing = required - set(columns)

    if missing:
        raise PreflightError(
            "discovery_buckets missing columns: "
            + ", ".join(sorted(missing))
        )

    LOG.success(
        "SCHEMA",
        "Discovery schema validated",
        columns=len(columns),
        numeric_filters=(
            "numeric_filters" in columns
        ),
        run_id=("run_id" in columns),
    )


def eligible_failed_buckets() -> list[dict]:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT b.*
                FROM discovery_buckets AS b
                WHERE b.status = 'failed'
                  AND b.total_items > %s
                  AND COALESCE(b.pages_done, 0) = 0
                  AND (
                      b.lease_owner IS NULL
                      OR b.lease_expires_at
                         < UTC_TIMESTAMP(6)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM discovery_buckets AS child
                      WHERE child.parent_id = b.id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM discovery_pages AS p
                      WHERE p.bucket_id = b.id
                  )
                ORDER BY
                    b.total_items DESC,
                    b.depth DESC,
                    b.id
                """,
                (TARGET_ROWS,),
            )

            return cursor.fetchall()


def discovery_state() -> dict:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_buckets,
                    COALESCE(
                        SUM(status='failed'), 0
                    ) AS failed_buckets,
                    COALESCE(
                        SUM(status='split'), 0
                    ) AS split_buckets,
                    COALESCE(
                        SUM(
                            status IN (
                                'pending',
                                'counted',
                                'draining'
                            )
                        ), 0
                    ) AS open_buckets,
                    COALESCE(
                        SUM(status='pending'), 0
                    ) AS pending_buckets,
                    COALESCE(
                        SUM(status='done'), 0
                    ) AS done_buckets
                FROM discovery_buckets
                """
            )

            return cursor.fetchone()


def assert_bucket_key_matches(
    row: dict,
    filters: BucketFilters,
) -> None:
    key_sections = {}

    for item in str(
        row["bucket_key"]
    ).split("|"):
        name, separator, value = (
            item.partition("=")
        )

        if separator:
            key_sections[name] = value

    expected = {
        "k": filters.keyword,
        "t": filters.search_type,
        "pv": ",".join(
            sorted(filters.pv_codes)
        ),
        "st": ",".join(
            sorted(filters.jp_statuses)
        ),
        "jt": ",".join(
            sorted(filters.jp_types)
        ),
        "sz": ",".join(
            sorted(filters.business_sizes)
        ),
    }

    for name, expected_value in expected.items():
        actual = key_sections.get(name)

        if actual is None:
            raise FilterError(
                f"Missing bucket key section: {name}"
            )

        if name in {
            "pv",
            "st",
            "jt",
            "sz",
        }:
            actual = ",".join(
                sorted(
                    csv_values(actual)
                )
            )

        if actual != expected_value:
            raise FilterError(
                f"Bucket key/filter mismatch: {name}"
            )


def validate_discovery_consumer(
    row: dict,
    filters: BucketFilters,
) -> None:
    """
    Verify that the installed MySQL Discovery consumer
    reconstructs the same filters.

    Without this check, a queued numeric bucket could
    silently lose its numeric constraints.

    This does not modify the Discovery source.
    """

    try:
        from dbd.pipelines import discovery

        load_filters = getattr(
            discovery,
            "_filters_from_row",
        )

        worker_filters = load_filters(
            dict(row)
        )

        worker_payload = (
            worker_filters.payload(page=1)
        )

    except Exception as exc:
        raise PreflightError(
            "Cannot validate installed Discovery "
            "filter reconstruction: "
            + error_text(exc)
        ) from exc

    expected_payload = filters.payload(
        page=1
    )

    mandatory_fields = (
        "keyword",
        "type",
        "pvCodeList",
        "jpStatusList",
        "jpTypeList",
        "businessSizeList",
        *NUMERIC_FIELDS,
    )

    mismatches = []

    for name in mandatory_fields:
        expected_value = (
            expected_payload.get(name)
        )

        actual_value = (
            worker_payload.get(name)
        )

        if name in NUMERIC_FIELDS:
            expected_number = decimal_value(
                expected_value
            )

            actual_number = decimal_value(
                actual_value
            )

            if expected_number != actual_number:
                mismatches.append(name)

        elif name.endswith("List"):
            if sorted(
                map(str, expected_value or [])
            ) != sorted(
                map(str, actual_value or [])
            ):
                mismatches.append(name)

        elif expected_value != actual_value:
            mismatches.append(name)

    if mismatches:
        raise PreflightError(
            "Installed MySQL Discovery consumer does "
            "not preserve required search filters: "
            + ", ".join(mismatches)
            + ". Refusing to enqueue incomplete queries."
        )

    child_keyword = filters.keyword + "0"

    child_row = dict(row)

    child_row["keyword"] = child_keyword

    child_row["bucket_key"] = (
        replace_keyword_in_key(
            str(row["bucket_key"]),
            filters.keyword,
            child_keyword,
        )
    )

    try:
        child_worker_filters = (
            load_filters(child_row)
        )

        child_worker_payload = (
            child_worker_filters.payload(page=1)
        )

    except Exception as exc:
        raise PreflightError(
            "Discovery consumer cannot reconstruct "
            "an expanded child bucket: "
            + error_text(exc)
        ) from exc

    child_expected = filters.with_keyword(
        child_keyword
    ).payload(page=1)

    for name in mandatory_fields:
        expected_value = child_expected.get(
            name
        )

        actual_value = child_worker_payload.get(
            name
        )

        if name in NUMERIC_FIELDS:
            equivalent = (
                decimal_value(expected_value)
                == decimal_value(actual_value)
            )

        elif name.endswith("List"):
            equivalent = (
                sorted(map(str, expected_value or []))
                == sorted(map(str, actual_value or []))
            )

        else:
            equivalent = (
                expected_value == actual_value
            )

        if not equivalent:
            raise PreflightError(
                "Expanded child filter would be lost "
                f"by Discovery consumer: {name}"
            )


def validate_all_candidates(
    candidates: list[dict],
    columns: dict[str, dict],
) -> list[
    tuple[dict, BucketFilters]
]:
    validated = []

    key_max_length = (
        columns["bucket_key"].get(
            "CHARACTER_MAXIMUM_LENGTH"
        )
    )

    for row in candidates:
        bucket_id = int(row["id"])

        try:
            filters = BucketFilters.from_row(
                row
            )

            assert_bucket_key_matches(
                row,
                filters,
            )

            if (
                len(filters.keyword)
                >= MAX_KEYWORD_LENGTH
            ):
                raise PreflightError(
                    "Keyword already at maximum length"
                )

            expanded_key = (
                filters.with_keyword(
                    filters.keyword + "0"
                ).bucket_key()
            )

            if (
                key_max_length is not None
                and len(expanded_key)
                > int(key_max_length)
            ):
                raise PreflightError(
                    "Expanded bucket_key exceeds "
                    "database column length"
                )

            validate_discovery_consumer(
                row,
                filters,
            )

            validated.append(
                (row, filters)
            )

            LOG.success(
                "CANDIDATE_READY",
                f"Bucket #{bucket_id}",
                keyword=filters.keyword,
                stored=f"{int(row['total_items']):,}",
                depth=row["depth"],
                numeric=len(filters.numeric),
            )

        except Exception as exc:
            LOG.error(
                "CANDIDATE_REJECT",
                f"Bucket #{bucket_id}",
                reason=error_text(exc),
            )

    return validated


# ============================================================
# LIVE API COUNTING
# ============================================================

class LiveCounter:
    def __init__(
        self,
        client: FixedRateDbdClient,
    ) -> None:
        self.client = client

        self.semaphore = asyncio.Semaphore(
            API_CONCURRENCY
        )

        self.started = time.perf_counter()

        self.successful_counts = 0
        self.failed_counts = 0

        self.extra_retries = 0

        self.request_times: deque[
            float
        ] = deque()

    def request_count(self) -> int:
        return int(
            self.client.requests_made
        )

    def effective_rate(self) -> float:
        elapsed = max(
            0.001,
            time.perf_counter() - self.started,
        )

        return self.request_count() / elapsed

    def recent_rate(self) -> float:
        now = time.perf_counter()

        while (
            self.request_times
            and now - self.request_times[0] > 60
        ):
            self.request_times.popleft()

        if not self.request_times:
            return 0.0

        duration = min(
            60.0,
            max(
                1.0,
                now - self.request_times[0],
            ),
        )

        return len(self.request_times) / duration

    async def count(
        self,
        filters: BucketFilters,
        root_id: int,
        depth: int,
    ) -> tuple[int, int]:
        for attempt in range(
            EXTRA_RETRIES + 1
        ):
            if (
                self.request_count()
                >= MAX_TOTAL_REQUESTS
            ):
                raise RequestBudgetError(
                    "Global request budget exhausted"
                )

            try:
                async with self.semaphore:
                    response = await self.client.fetch(
                        "POST",
                        SEARCH_PATH,
                        json_body=filters.payload(1),
                        referer=SEARCH_REFERER,
                        timeout=HTTP_TIMEOUT,
                    )

                page = parse_search_response(
                    response.data
                )

                if page.current_page != 1:
                    raise ExpansionError(
                        "API returned unexpected currentPage"
                    )

                if page.items_per_page != ITEMS_PER_PAGE:
                    raise ExpansionError(
                        "API returned unexpected itemsPerPage"
                    )

                if page.total_items < 0:
                    raise ExpansionError(
                        "API returned negative totalItems"
                    )

                expected_pages = math.ceil(
                    page.total_items / ITEMS_PER_PAGE
                )

                if page.total_items == 0:
                    if page.total_pages not in (0, 1):
                        raise ExpansionError(
                            "Invalid empty-result pagination"
                        )

                elif page.total_pages != expected_pages:
                    raise ExpansionError(
                        "API count/page inconsistency"
                    )

                self.successful_counts += 1

                self.request_times.append(
                    time.perf_counter()
                )

                return (
                    int(page.total_items),
                    int(page.total_pages),
                )

            except DeepTransientHttpError as exc:
                self.failed_counts += 1

                if attempt >= EXTRA_RETRIES:
                    raise

                self.extra_retries += 1

                delay = (
                    EXTRA_RETRY_DELAY
                    * (2**attempt)
                )

                LOG.warning(
                    "HTTP_RETRY",
                    f"Bucket #{root_id}",
                    keyword=filters.keyword,
                    depth=depth,
                    attempt=attempt + 1,
                    delay=f"{delay:.1f}s",
                    error=error_text(exc),
                )

                await asyncio.sleep(
                    delay
                )

        raise ExpansionError(
            "Count retry loop exhausted"
        )


# ============================================================
# RECURSIVE KEYWORD EXPANSION
# ============================================================

class RecursivePlanner:
    def __init__(
        self,
        counter: LiveCounter,
    ) -> None:
        self.counter = counter

    async def build(
        self,
        row: dict,
        filters: BucketFilters,
    ) -> SplitPlan:
        root_id = int(row["id"])

        started = time.perf_counter()

        LOG.section(
            f"ROOT #{root_id} | "
            f"KEYWORD {filters.keyword}"
        )

        LOG.info(
            "ROOT_START",
            f"Bucket #{root_id}",
            stored_count=f"{int(row['total_items']):,}",
            stored_depth=row["depth"],
            keyword=filters.keyword,
        )

        root_count, root_pages = (
            await self.counter.count(
                filters,
                root_id,
                0,
            )
        )

        plan = SplitPlan(
            root_id=root_id,
            root_row=row,
            filters=filters,
        )

        root_node = KeywordNode(
            keyword=filters.keyword,
            relative_depth=0,
            parent_keyword=None,
            count=root_count,
            pages=root_pages,
        )

        plan.nodes[root_node.keyword] = (
            root_node
        )

        LOG.info(
            "ROOT_LIVE_COUNT",
            f"Bucket #{root_id}",
            keyword=filters.keyword,
            stored=f"{int(row['total_items']):,}",
            live=f"{root_count:,}",
            delta=(
                root_count
                - int(row["total_items"])
            ),
        )

        if root_count <= TARGET_ROWS:
            LOG.success(
                "ROOT_NO_SPLIT",
                f"Bucket #{root_id}",
                live=f"{root_count:,}",
                action="REQUEUE_ORIGINAL",
            )

            return plan

        queue: deque[str] = deque(
            [filters.keyword]
        )

        expanded = 0
        child_counts = 0

        while queue:
            parent_keyword = queue.popleft()

            parent = plan.nodes[
                parent_keyword
            ]

            if parent.count <= TARGET_ROWS:
                continue

            if (
                parent.relative_depth
                >= MAX_RECURSIVE_DEPTH
            ):
                raise ExpansionError(
                    f"Maximum recursive depth reached: "
                    f"{parent.keyword} "
                    f"({parent.count:,})"
                )

            if (
                len(parent.keyword)
                >= MAX_KEYWORD_LENGTH
            ):
                raise ExpansionError(
                    f"Maximum keyword length reached: "
                    f"{parent.keyword} "
                    f"({parent.count:,})"
                )

            if (
                len(plan.nodes) + 10
                > MAX_NODES_PER_ROOT
            ):
                raise ExpansionError(
                    f"Maximum nodes per root reached: "
                    f"{root_id}"
                )

            expanded += 1

            child_keywords = [
                parent.keyword + digit
                for digit in DIGITS
            ]

            LOG.info(
                "EXPAND",
                f"Bucket #{root_id}",
                parent=parent.keyword,
                count=f"{parent.count:,}",
                depth=parent.relative_depth,
                children=10,
                queue=len(queue),
            )

            child_filters = [
                filters.with_keyword(
                    keyword
                )
                for keyword in child_keywords
            ]

            tasks = [
                self.counter.count(
                    child_filter,
                    root_id,
                    parent.relative_depth + 1,
                )
                for child_filter in child_filters
            ]

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

            failures = [
                (child_keywords[index], result)
                for index, result in enumerate(results)
                if isinstance(
                    result,
                    BaseException,
                )
            ]

            if failures:
                for keyword, exc in failures:
                    LOG.error(
                        "CHILD_ERROR",
                        f"Bucket #{root_id}",
                        keyword=keyword,
                        error=error_text(exc),
                    )

                raise ExpansionError(
                    "Failed to count all ten children "
                    f"of {parent.keyword}"
                )

            for index, keyword in enumerate(
                child_keywords
            ):
                count, pages = results[index]

                node = KeywordNode(
                    keyword=keyword,
                    relative_depth=(
                        parent.relative_depth + 1
                    ),
                    parent_keyword=(
                        parent.keyword
                    ),
                    count=count,
                    pages=pages,
                )

                plan.nodes[keyword] = node

                parent.children.append(
                    keyword
                )

                child_counts += 1

                if count > TARGET_ROWS:
                    queue.append(
                        keyword
                    )

                    decision = "RECURSE"

                elif count == 0:
                    decision = "EMPTY_LEAF"

                else:
                    decision = "LEAF"

                if LOG_EVERY_CHILD:
                    LOG.debug(
                        "CHILD",
                        f"Bucket #{root_id}",
                        keyword=keyword,
                        count=f"{count:,}",
                        pages=pages,
                        depth=node.relative_depth,
                        decision=decision,
                        queue=len(queue),
                        api_rps=(
                            f"{self.counter.effective_rate():.2f}"
                        ),
                    )

            LOG.success(
                "EXPAND_DONE",
                f"Bucket #{root_id}",
                parent=parent.keyword,
                children=10,
                queued=len(queue),
                tree_nodes=len(plan.nodes),
                elapsed=(
                    f"{time.perf_counter()-started:.1f}s"
                ),
            )

        plan.verify_structure()

        LOG.success(
            "TREE_COMPLETE",
            f"Bucket #{root_id}",
            nodes=len(plan.nodes),
            internal=len(plan.internal_nodes()),
            leaves=len(plan.leaves()),
            max_depth=plan.max_depth(),
            root_count=f"{root_count:,}",
            leaf_sum=f"{plan.leaf_total():,}",
            elapsed=(
                f"{time.perf_counter()-started:.1f}s"
            ),
        )

        return plan


# ============================================================
# TRANSACTIONAL DATABASE PERSISTENCE
# ============================================================

def check_root_for_commit(
    cursor,
    root_id: int,
) -> dict:
    cursor.execute(
        """
        SELECT *
        FROM discovery_buckets
        WHERE id = %s
        FOR UPDATE
        """,
        (root_id,),
    )

    row = cursor.fetchone()

    if row is None:
        raise StaleBucketError(
            f"Root #{root_id} disappeared"
        )

    if row["status"] != "failed":
        raise StaleBucketError(
            f"Root #{root_id} status changed "
            f"to {row['status']}"
        )

    if (
        row.get("total_items") is None
        or int(row["total_items"]) <= TARGET_ROWS
    ):
        raise StaleBucketError(
            f"Root #{root_id} is no longer eligible"
        )

    if int(
        row.get("pages_done") or 0
    ) != 0:
        raise StaleBucketError(
            f"Root #{root_id} has completed pages"
        )

    cursor.execute(
        """
        SELECT COUNT(*) AS n
        FROM discovery_buckets
        WHERE parent_id = %s
        """,
        (root_id,),
    )

    if int(cursor.fetchone()["n"]) != 0:
        raise StaleBucketError(
            f"Root #{root_id} already has children"
        )

    cursor.execute(
        """
        SELECT COUNT(*) AS n
        FROM discovery_pages
        WHERE bucket_id = %s
        """,
        (root_id,),
    )

    if int(cursor.fetchone()["n"]) != 0:
        raise StaleBucketError(
            f"Root #{root_id} already has pages"
        )

    cursor.execute(
        """
        SELECT
            (
                lease_owner IS NOT NULL
                AND lease_expires_at
                    >= UTC_TIMESTAMP(6)
            ) AS active_lease
        FROM discovery_buckets
        WHERE id = %s
        """,
        (root_id,),
    )

    if int(
        cursor.fetchone()["active_lease"]
        or 0
    ):
        raise StaleBucketError(
            f"Root #{root_id} has active lease"
        )

    return row


def verify_root_unchanged(
    original: dict,
    current: dict,
) -> None:
    protected = (
        "bucket_key",
        "keyword",
        "search_type",
        "pv_codes",
        "jp_statuses",
        "jp_types",
        "business_sizes",
        "total_items",
        "depth",
    )

    for name in protected:
        if (
            original.get(name)
            != current.get(name)
        ):
            raise StaleBucketError(
                f"Root changed during expansion: {name}"
            )

    original_numeric = load_numeric_filters(
        original
    )

    current_numeric = load_numeric_filters(
        current
    )

    if original_numeric != current_numeric:
        raise StaleBucketError(
            "Root numeric filters changed"
        )


def insert_tree_level(
    cursor,
    plan: SplitPlan,
    nodes: list[KeywordNode],
    database_ids: dict[str, int],
    columns: dict[str, dict],
) -> int:
    if not nodes:
        return 0

    fields = [
        "bucket_key",
        "parent_id",
        "depth",
        "keyword",
        "search_type",
        "pv_codes",
        "jp_statuses",
        "jp_types",
        "business_sizes",
        "total_items",
        "pages_total",
        "status",
        "created_at",
        "updated_at",
    ]

    include_numeric = (
        "numeric_filters" in columns
    )

    include_run_id = (
        "run_id" in columns
    )

    if include_numeric:
        fields.append(
            "numeric_filters"
        )

    if include_run_id:
        fields.append(
            "run_id"
        )

    placeholders = ",".join(
        ["%s"] * len(fields)
    )

    field_sql = ",".join(
        f"`{name}`"
        for name in fields
    )

    sql = (
        "INSERT INTO discovery_buckets "
        f"({field_sql}) "
        f"VALUES ({placeholders})"
    )

    root_depth = int(
        plan.root_row["depth"]
    )

    filter_data = plan.filters

    numeric_json = json.dumps(
        {
            name: str(value)
            for name, value in filter_data.numeric
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )

    now = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    values = []

    expected_keys: dict[str, tuple[str, int]] = {}

    for node in nodes:
        if node.parent_keyword is None:
            raise PersistenceError(
                "Root must not be inserted as a child"
            )

        parent_db_id = database_ids.get(
            node.parent_keyword
        )

        if parent_db_id is None:
            raise PersistenceError(
                f"Parent DB ID missing: "
                f"{node.parent_keyword}"
            )

        node_filters = (
            filter_data.with_keyword(
                node.keyword
            )
        )

        bucket_key = node_filters.bucket_key()

        key_limit = (
            columns["bucket_key"].get(
                "CHARACTER_MAXIMUM_LENGTH"
            )
        )

        if (
            key_limit is not None
            and len(bucket_key) > int(key_limit)
        ):
            raise PersistenceError(
                f"bucket_key exceeds column limit: "
                f"{node.keyword}"
            )

        node_status = (
            "split"
            if node.children
            else "pending"
        )

        # Existing Discovery workers will count positive
        # terminal leaves again when they start draining.
        #
        # This avoids using potentially stale pagination
        # totals after an extended recursive planning run.
        #
        # Internal split nodes retain observed counts.
        if node.children:
            total_items = node.count
            pages_total = node.pages

        elif node.count == 0:
            total_items = 0
            pages_total = 0

        else:
            total_items = None
            pages_total = None

        record = [
            bucket_key,
            parent_db_id,
            root_depth + node.relative_depth,
            node.keyword,
            filter_data.search_type,
            ",".join(filter_data.pv_codes),
            ",".join(filter_data.jp_statuses),
            ",".join(filter_data.jp_types),
            ",".join(filter_data.business_sizes),
            total_items,
            pages_total,
            node_status,
            now,
            now,
        ]

        if include_numeric:
            record.append(
                numeric_json
            )

        if include_run_id:
            record.append(
                plan.root_row.get("run_id")
            )

        values.append(
            tuple(record)
        )

        expected_keys[bucket_key] = (
            node.keyword,
            parent_db_id,
        )

    cursor.executemany(
        sql,
        values,
    )

    keys = list(
        expected_keys
    )

    query_placeholders = ",".join(
        ["%s"] * len(keys)
    )

    cursor.execute(
        f"""
        SELECT
            id,
            bucket_key,
            parent_id,
            status,
            keyword
        FROM discovery_buckets
        WHERE bucket_key IN (
            {query_placeholders}
        )
        FOR UPDATE
        """,
        keys,
    )

    returned = cursor.fetchall()

    if len(returned) != len(keys):
        raise PersistenceError(
            "Inserted child verification count mismatch"
        )

    for row in returned:
        bucket_key = row["bucket_key"]

        expected = expected_keys.get(
            bucket_key
        )

        if expected is None:
            raise PersistenceError(
                "Unexpected bucket key after insert"
            )

        expected_keyword, parent_id = (
            expected
        )

        if str(row["keyword"]) != expected_keyword:
            raise PersistenceError(
                "Inserted keyword verification failed"
            )

        if int(
            row["parent_id"]
        ) != int(parent_id):
            raise PersistenceError(
                "Inserted parent_id verification failed"
            )

        database_ids[
            expected_keyword
        ] = int(row["id"])

    return len(values)


def persist_plan(
    plan: SplitPlan,
    columns: dict[str, dict],
) -> dict:
    root_id = plan.root_id

    started = time.perf_counter()

    plan.verify_structure()

    result = {
        "root_id": root_id,
        "root_keyword": plan.filters.keyword,
        "inserted_nodes": 0,
        "inserted_leaves": 0,
        "inserted_internal": 0,
        "root_requeued": False,
        "root_split": False,
        "db_committed": False,
    }

    with connect() as connection:
        try:
            with connection.cursor() as cursor:
                current = check_root_for_commit(
                    cursor,
                    root_id,
                )

                verify_root_unchanged(
                    plan.root_row,
                    current,
                )

                LOG.info(
                    "DB_TX_BEGIN",
                    f"Bucket #{root_id}",
                    nodes=len(plan.nodes),
                    leaves=len(plan.leaves()),
                )

                root_node = plan.root()

                # The originally failed bucket may have
                # become small enough to drain directly.
                if root_node.count <= TARGET_ROWS:
                    cursor.execute(
                        """
                        UPDATE discovery_buckets
                        SET
                            status='pending',
                            total_items=NULL,
                            pages_total=NULL,
                            pages_done=0,
                            last_error=NULL,
                            lease_owner=NULL,
                            lease_expires_at=NULL,
                            lease_version=lease_version+1,
                            updated_at=UTC_TIMESTAMP(6)
                        WHERE id=%s
                          AND status='failed'
                        """,
                        (root_id,),
                    )

                    if cursor.rowcount != 1:
                        raise PersistenceError(
                            "Failed to requeue original root"
                        )

                    result["root_requeued"] = True

                else:
                    database_ids = {
                        plan.filters.keyword: root_id
                    }

                    depths = sorted({
                        node.relative_depth
                        for node in plan.nodes.values()
                        if node.relative_depth > 0
                    })

                    for depth in depths:
                        level_nodes = sorted(
                            (
                                node
                                for node in plan.nodes.values()
                                if node.relative_depth == depth
                            ),
                            key=lambda node: node.keyword,
                        )

                        inserted = insert_tree_level(
                            cursor,
                            plan,
                            level_nodes,
                            database_ids,
                            columns,
                        )

                        result["inserted_nodes"] += (
                            inserted
                        )

                        LOG.debug(
                            "DB_LEVEL",
                            f"Bucket #{root_id}",
                            depth=depth,
                            inserted=inserted,
                            cumulative=(
                                result["inserted_nodes"]
                            ),
                        )

                    expected_insertions = (
                        len(plan.nodes) - 1
                    )

                    if (
                        result["inserted_nodes"]
                        != expected_insertions
                    ):
                        raise PersistenceError(
                            "Inserted tree size mismatch"
                        )

                    result["inserted_leaves"] = (
                        len(plan.leaves())
                    )

                    result["inserted_internal"] = (
                        len(plan.internal_nodes()) - 1
                    )

                    cursor.execute(
                        """
                        UPDATE discovery_buckets
                        SET
                            status='split',
                            last_error=NULL,
                            lease_owner=NULL,
                            lease_expires_at=NULL,
                            lease_version=lease_version+1,
                            updated_at=UTC_TIMESTAMP(6)
                        WHERE id=%s
                          AND status='failed'
                        """,
                        (root_id,),
                    )

                    if cursor.rowcount != 1:
                        raise PersistenceError(
                            "Failed to mark root as split"
                        )

                    result["root_split"] = True

                LOG.info(
                    "DB_VERIFY",
                    f"Bucket #{root_id}",
                    expected_nodes=(
                        result["inserted_nodes"]
                    ),
                    root_split=(
                        result["root_split"]
                    ),
                    root_requeued=(
                        result["root_requeued"]
                    ),
                )

            connection.commit()

            result["db_committed"] = True

            LOG.success(
                "DB_COMMIT",
                f"Bucket #{root_id}",
                inserted_nodes=(
                    result["inserted_nodes"]
                ),
                pending_leaves=(
                    result["inserted_leaves"]
                ),
                elapsed=(
                    f"{time.perf_counter()-started:.2f}s"
                ),
            )

        except BaseException:
            connection.rollback()

            LOG.error(
                "DB_ROLLBACK",
                f"Bucket #{root_id}",
                message="Transaction rolled back",
            )

            raise

    return result


# ============================================================
# ROOT WORKFLOW
# ============================================================

@dataclass
class RecoveryStats:
    candidate_count: int = 0

    roots_attempted: int = 0
    roots_recovered: int = 0
    roots_failed: int = 0
    roots_stale: int = 0

    roots_split: int = 0
    roots_requeued: int = 0

    inserted_nodes: int = 0
    inserted_leaves: int = 0
    inserted_internal: int = 0

    expansion_nodes: int = 0
    expansion_leaves: int = 0

    failures: list[dict] = field(
        default_factory=list
    )


async def recover_failed_buckets(
    candidates: list[
        tuple[dict, BucketFilters]
    ],
    columns: dict[str, dict],
) -> tuple[RecoveryStats, LiveCounter]:
    stats = RecoveryStats(
        candidate_count=len(candidates)
    )

    db_commit_lock = asyncio.Lock()

    root_semaphore = asyncio.Semaphore(
        ROOT_CONCURRENCY
    )

    async with FixedRateDbdClient(
        concurrency=API_CONCURRENCY,
        rate_per_second=API_RATE_PER_SECOND,
        max_retries=HTTP_RETRIES,
        timeout=HTTP_TIMEOUT,
    ) as client:
        counter = LiveCounter(client)

        planner = RecursivePlanner(
            counter
        )

        async def process_root(
            row: dict,
            filters: BucketFilters,
        ) -> None:
            root_id = int(row["id"])

            async with root_semaphore:
                stats.roots_attempted += 1

                started = time.perf_counter()

                try:
                    plan = await planner.build(
                        row,
                        filters,
                    )

                    stats.expansion_nodes += (
                        len(plan.nodes)
                    )

                    stats.expansion_leaves += (
                        len(plan.leaves())
                    )

                    LOG.info(
                        "PERSIST_READY",
                        f"Bucket #{root_id}",
                        nodes=len(plan.nodes),
                        leaves=len(plan.leaves()),
                    )

                    async with db_commit_lock:
                        persisted = (
                            await asyncio.to_thread(
                                persist_plan,
                                plan,
                                columns,
                            )
                        )

                    stats.roots_recovered += 1

                    stats.inserted_nodes += (
                        persisted["inserted_nodes"]
                    )

                    stats.inserted_leaves += (
                        persisted["inserted_leaves"]
                    )

                    stats.inserted_internal += (
                        persisted["inserted_internal"]
                    )

                    if persisted["root_split"]:
                        stats.roots_split += 1

                    if persisted["root_requeued"]:
                        stats.roots_requeued += 1

                    LOG.success(
                        "ROOT_RECOVERED",
                        f"Bucket #{root_id}",
                        keyword=filters.keyword,
                        children=(
                            persisted["inserted_nodes"]
                        ),
                        pending_leaves=(
                            persisted["inserted_leaves"]
                        ),
                        progress=(
                            f"{stats.roots_recovered}/"
                            f"{stats.candidate_count}"
                        ),
                        elapsed=(
                            f"{time.perf_counter()-started:.1f}s"
                        ),
                    )

                except StaleBucketError as exc:
                    stats.roots_stale += 1

                    LOG.warning(
                        "ROOT_STALE",
                        f"Bucket #{root_id}",
                        error=error_text(exc),
                    )

                    stats.failures.append({
                        "bucket_id": root_id,
                        "keyword": filters.keyword,
                        "type": "STALE",
                        "reason": error_text(exc),
                    })

                except Exception as exc:
                    stats.roots_failed += 1

                    LOG.error(
                        "ROOT_FAILED",
                        f"Bucket #{root_id}",
                        keyword=filters.keyword,
                        error=error_text(exc),
                    )

                    if SHOW_TRACEBACK:
                        traceback.print_exc()

                    stats.failures.append({
                        "bucket_id": root_id,
                        "keyword": filters.keyword,
                        "type": type(exc).__name__,
                        "reason": str(exc)[:500],
                    })

                finally:
                    LOG.info(
                        "ROOT_PROGRESS",
                        completed=(
                            stats.roots_recovered
                            + stats.roots_failed
                            + stats.roots_stale
                        ),
                        total=stats.candidate_count,
                        recovered=stats.roots_recovered,
                        failed=stats.roots_failed,
                        stale=stats.roots_stale,
                        requests=counter.request_count(),
                        rate=(
                            f"{counter.effective_rate():.2f}/s"
                        ),
                        recent_rate=(
                            f"{counter.recent_rate():.2f}/s"
                        ),
                        pending_created=(
                            stats.inserted_leaves
                        ),
                    )

        tasks = [
            asyncio.create_task(
                process_root(
                    row,
                    filters,
                )
            )
            for row, filters in candidates
        ]

        await asyncio.gather(
            *tasks
        )

        return stats, counter


# ============================================================
# REPORTING
# ============================================================

def print_final_report(
    stats: RecoveryStats,
    counter: LiveCounter | None,
    before: dict,
    after: dict,
    elapsed: float,
    preflight_rejected: int,
) -> None:
    LOG.section(
        "DBD · FAILED BUCKET RECOVERY — FINAL"
    )

    def line(
        label: str,
        value: Any,
    ) -> None:
        print(
            f"  {label:<36} {value}",
            flush=True,
        )

    line(
        "Eligible candidates:",
        stats.candidate_count,
    )

    line(
        "Preflight rejected:",
        preflight_rejected,
    )

    line(
        "Roots attempted:",
        stats.roots_attempted,
    )

    line(
        "Roots recovered:",
        stats.roots_recovered,
    )

    line(
        "Roots split:",
        stats.roots_split,
    )

    line(
        "Original roots requeued:",
        stats.roots_requeued,
    )

    line(
        "Roots failed:",
        stats.roots_failed,
    )

    line(
        "Roots stale:",
        stats.roots_stale,
    )

    print()

    line(
        "Database child nodes inserted:",
        f"{stats.inserted_nodes:,}",
    )

    line(
        "Intermediate split nodes:",
        f"{stats.inserted_internal:,}",
    )

    line(
        "New pending terminal leaves:",
        f"{stats.inserted_leaves:,}",
    )

    print()

    line(
        "Open buckets before:",
        f"{int(before['open_buckets']):,}",
    )

    line(
        "Open buckets after:",
        f"{int(after['open_buckets']):,}",
    )

    line(
        "Failed buckets before:",
        f"{int(before['failed_buckets']):,}",
    )

    line(
        "Failed buckets after:",
        f"{int(after['failed_buckets']):,}",
    )

    line(
        "Total buckets before:",
        f"{int(before['total_buckets']):,}",
    )

    line(
        "Total buckets after:",
        f"{int(after['total_buckets']):,}",
    )

    if counter is not None:
        print()

        line(
            "API requests:",
            f"{counter.request_count():,}",
        )

        line(
            "Successful API counts:",
            f"{counter.successful_counts:,}",
        )

        line(
            "Failed count attempts:",
            f"{counter.failed_counts:,}",
        )

        line(
            "HTTP client retries:",
            counter.client.retries_made,
        )

        line(
            "Additional retries:",
            counter.extra_retries,
        )

        line(
            "HTTP 429 hits:",
            counter.client.rate_limit_hits,
        )

        line(
            "Effective API rate:",
            f"{counter.effective_rate():.2f} req/s",
        )

    print()

    line(
        "Runtime:",
        f"{elapsed:.2f} seconds",
    )

    line(
        "Finished UTC:",
        utc_now(),
    )

    print()

    if stats.failures:
        print(
            "  FAILED / SKIPPED ROOTS:",
            flush=True,
        )

        for failure in stats.failures:
            print(
                "    "
                + json.dumps(
                    failure,
                    ensure_ascii=False,
                ),
                flush=True,
            )

    else:
        print(
            "  FAILED / SKIPPED ROOTS: NONE",
            flush=True,
        )

    print()
    print("=" * 110)
    print(flush=True)


# ============================================================
# MAIN
# ============================================================

async def async_main() -> int:
    started = time.perf_counter()

    LOG.section(
        "DBD · SPLIT FAILED BUCKETS"
    )

    LOG.info(
        "START",
        "Recursive Keyword Recovery",
        database=getattr(
            settings,
            "db_name",
            "unknown",
        ),
        target_rows=TARGET_ROWS,
        branching_factor=10,
        max_depth=MAX_RECURSIVE_DEPTH,
        max_keyword_length=MAX_KEYWORD_LENGTH,
        root_concurrency=ROOT_CONCURRENCY,
        api_concurrency=API_CONCURRENCY,
        api_rate=API_RATE_PER_SECOND,
        request_budget=MAX_TOTAL_REQUESTS,
    )

    LOG.info(
        "POLICY",
        "Split/count only during expansion",
        lossless_validation="DISABLED",
        data_deletion="DISABLED",
        source_modification="DISABLED",
    )

    LOG.section(
        "STEP 1 / PREFLIGHT"
    )

    columns = await asyncio.to_thread(
        database_columns
    )

    validate_schema(
        columns
    )

    before = await asyncio.to_thread(
        discovery_state
    )

    LOG.info(
        "DB_BEFORE",
        total=before["total_buckets"],
        done=before["done_buckets"],
        split=before["split_buckets"],
        open=before["open_buckets"],
        pending=before["pending_buckets"],
        failed=before["failed_buckets"],
    )

    candidates = await asyncio.to_thread(
        eligible_failed_buckets
    )

    LOG.success(
        "SCAN_COMPLETE",
        "Eligible failed buckets loaded",
        selected=len(candidates),
    )

    if not candidates:
        LOG.success(
            "NO_WORK",
            "No eligible failed buckets",
        )

        return 0

    validated = validate_all_candidates(
        candidates,
        columns,
    )

    rejected = (
        len(candidates) - len(validated)
    )

    LOG.info(
        "PREFLIGHT_SUMMARY",
        candidates=len(candidates),
        ready=len(validated),
        rejected=rejected,
    )

    if not validated:
        LOG.error(
            "PREFLIGHT_FAILED",
            "No compatible candidate can safely "
            "enter the Discovery queue",
        )

        return 2

    LOG.warning(
        "QUEUE_TARGET",
        "Pending children will be consumed by "
        "the MySQL Discovery bucket worker",
    )

    LOG.warning(
        "RUNTIME_NOTE",
        "The v0.6 direct SQL Server discovery "
        "path does not consume this MySQL queue",
    )

    LOG.section(
        "STEP 2 / RECURSIVE KEYWORD EXPANSION"
    )

    stats, counter = (
        await recover_failed_buckets(
            validated,
            columns,
        )
    )

    LOG.section(
        "STEP 3 / DATABASE RECONCILIATION"
    )

    after = await asyncio.to_thread(
        discovery_state
    )

    expected_total = (
        int(before["total_buckets"])
        + stats.inserted_nodes
    )

    actual_total = int(
        after["total_buckets"]
    )

    if expected_total != actual_total:
        LOG.warning(
            "CONCURRENT_DB_CHANGE",
            "Total bucket count also changed "
            "outside this script or a concurrent "
            "worker modified the queue",
            expected=expected_total,
            actual=actual_total,
        )

    expected_failed = (
        int(before["failed_buckets"])
        - stats.roots_recovered
    )

    if (
        int(after["failed_buckets"])
        != expected_failed
    ):
        LOG.warning(
            "FAILED_COUNT_DRIFT",
            "Failed bucket count differs from "
            "script-local expected delta",
            expected=expected_failed,
            actual=after["failed_buckets"],
        )

    LOG.success(
        "DB_RECONCILED",
        total=after["total_buckets"],
        pending=after["pending_buckets"],
        open=after["open_buckets"],
        failed=after["failed_buckets"],
        split=after["split_buckets"],
    )

    elapsed = (
        time.perf_counter() - started
    )

    print_final_report(
        stats=stats,
        counter=counter,
        before=before,
        after=after,
        elapsed=elapsed,
        preflight_rejected=rejected,
    )

    if (
        stats.roots_failed > 0
        or stats.roots_stale > 0
        or rejected > 0
    ):
        LOG.warning(
            "PARTIAL_COMPLETION",
            "Some eligible roots were not recovered",
            recovered=stats.roots_recovered,
            failed=stats.roots_failed,
            stale=stats.roots_stale,
            preflight_rejected=rejected,
        )

        return 1

    LOG.success(
        "COMPLETE",
        "Failed buckets expanded and "
        "terminal leaves queued",
        roots=stats.roots_recovered,
        pending_leaves=stats.inserted_leaves,
    )

    return 0


def main() -> int:
    try:
        return asyncio.run(
            async_main()
        )

    except KeyboardInterrupt:
        LOG.warning(
            "INTERRUPTED",
            "Ctrl+C received",
            note=(
                "Previously committed roots remain safe; "
                "unfinished root transactions are rolled back"
            ),
        )

        return 130

    except Exception as exc:
        LOG.error(
            "FATAL",
            "Failed bucket recovery stopped",
            error=error_text(exc),
        )

        if SHOW_TRACEBACK:
            traceback.print_exc()

        return 2


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
