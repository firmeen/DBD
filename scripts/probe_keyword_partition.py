
r"""
scripts/probe_keyword_partition.py

DBD RECURSIVE KEYWORD EXPANSION PROBE
=====================================

Single file. Read-only. No source modifications. No database writes.

Strategy:
    055
      0550
      0551
      0552
      ...
      0559

    Every child exceeding 10,000 is expanded into ten more
    numeric keywords.

    Expansion does NOT stop on count mismatches.

    Count conservation, leaf totals, overlap checks and
    optional identity audits run AFTER expansion.

Examples:
    python .\scripts\probe_keyword_partition.py --bucket-id 17503

    python .\scripts\probe_keyword_partition.py --keyword 055

    python .\scripts\probe_keyword_partition.py --bucket-id 17503 --max-depth 10

    python .\scripts\probe_keyword_partition.py --bucket-id 17503 --mode sample

    python .\scripts\probe_keyword_partition.py --bucket-id 17503 --mode full --max-requests 30000

    python .\scripts\probe_keyword_partition.py --bucket-id 17503 --rate 0.5 --max-requests 1000

Notes:
    - Depth 0 is the original parent keyword.
    - Depth 1 appends one digit.
    - A child with <= 10,000 results becomes a terminal leaf.
    - A child with > 10,000 results is recursively expanded.
    - All inherited API filters remain unchanged.
    - No partition is accepted as lossless merely because
      its counts match.
    - An oversized parent cannot be fully enumerated using
      the known Search API pagination limit.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import re
import sys
import time

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


# ============================================================
# PROJECT IMPORTS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

for directory in (PROJECT_ROOT, PROJECT_ROOT / "src"):
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
# CONSTANTS
# ============================================================

DIGITS = "0123456789"

TARGET_ROWS = ITEMS_PER_PAGE * MAX_PAGE

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

NUMERIC_KEY_SECTIONS = {
    "cap": {"capAmtMin", "capAmtMax"},
    "income": {"totalIncomeMin", "totalIncomeMax"},
    "profit": {"netProfitMin", "netProfitMax"},
    "asset": {"totalAssetMin", "totalAssetMax"},
}

NUMERIC_BOUNDS = (
    ("capAmtMin", "capAmtMax"),
    ("totalIncomeMin", "totalIncomeMax"),
    ("netProfitMin", "netProfitMax"),
    ("totalAssetMin", "totalAssetMax"),
)

FINAL_STATUSES = {
    "LEAF",
    "EMPTY",
    "UNRESOLVED_DEPTH",
    "UNRESOLVED_KEYWORD_LENGTH",
    "UNRESOLVED_NODE_BUDGET",
    "COUNT_ERROR",
    "SPLIT_COMPLETE",
    "SPLIT_PARTIAL",
}

MAX_ERROR_LENGTH = 300


# ============================================================
# EXCEPTIONS
# ============================================================

class ProbeError(RuntimeError):
    pass


class ProbeContractError(ProbeError):
    pass


class RequestBudgetExceeded(ProbeError):
    pass


class NodeBudgetExceeded(ProbeError):
    pass


class ObservationDrift(ProbeError):
    pass


# ============================================================
# UTILITIES
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(event: str, **data: Any) -> None:
    record = {
        "event": event,
        "timestamp_utc": utc_now(),
        **data,
    }

    print(
        json.dumps(
            record,
            ensure_ascii=False,
            default=str,
        ),
        file=sys.stderr,
        flush=True,
    )


def digest(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")

    return hashlib.sha256(raw).hexdigest()


def csv_values(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()

    return tuple(
        part.strip()
        for part in str(value).split(",")
        if part.strip()
    )


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        raise ProbeContractError(
            "Boolean is not a valid numeric filter"
        )

    try:
        number = Decimal(
            str(value).replace(",", "").strip()
        )
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ProbeContractError(
            "Invalid numeric filter"
        ) from exc

    if not number.is_finite():
        raise ProbeContractError(
            "Non-finite numeric filter"
        )

    return number


def serialize_decimal(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        integer = int(value)

        if abs(integer) > 2**53:
            raise ProbeContractError(
                "Numeric integer exceeds safe JSON precision"
            )

        return integer

    converted = float(value)

    if not math.isfinite(converted):
        raise ProbeContractError(
            "Numeric filter cannot be encoded safely"
        )

    if Decimal(str(converted)) != value:
        raise ProbeContractError(
            "Numeric filter loses precision in JSON"
        )

    return converted


def identity(row: dict) -> tuple[str, str]:
    jp_type = row.get("jpTypeCode")
    jp_no = row.get("jpNo")

    if jp_type is None or jp_no is None:
        raise ProbeContractError(
            "Search row is missing a juristic identity"
        )

    jp_type = str(jp_type).strip()
    jp_no = str(jp_no).strip()

    if not jp_type or not jp_no:
        raise ProbeContractError(
            "Search row has an empty juristic identity"
        )

    return jp_type, jp_no


def serialize_identity(
    value: tuple[str, str],
) -> dict:
    return {
        "jp_type_code": value[0],
        "jp_no": value[1],
    }


def short_error(exc: BaseException) -> dict:
    return {
        "type": type(exc).__name__,
        "message": str(exc)[:MAX_ERROR_LENGTH],
    }


# ============================================================
# QUERY FILTERS
# ============================================================

@dataclass(frozen=True)
class QueryFilters:
    keyword: str

    search_type: str = ""

    pv_codes: tuple[str, ...] = ()
    jp_statuses: tuple[str, ...] = ()
    jp_types: tuple[str, ...] = ()
    business_sizes: tuple[str, ...] = ()

    numeric: tuple[tuple[str, Decimal], ...] = ()

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9]{3,}", self.keyword):
            raise ProbeContractError(
                "Keyword must contain at least 3 digits"
            )

        values = dict(self.numeric)

        if len(values) != len(self.numeric):
            raise ProbeContractError(
                "Duplicate numeric filter name"
            )

        unknown = set(values) - set(NUMERIC_FIELDS)

        if unknown:
            raise ProbeContractError(
                f"Unknown numeric filters: {sorted(unknown)}"
            )

        for minimum, maximum in NUMERIC_BOUNDS:
            lower = values.get(minimum)
            upper = values.get(maximum)

            if (
                lower is not None
                and upper is not None
                and lower > upper
            ):
                raise ProbeContractError(
                    f"Invalid numeric range: {minimum}/{maximum}"
                )

    def append(self, digit: str) -> "QueryFilters":
        if digit not in DIGITS:
            raise ValueError("Digit must be 0-9")

        return QueryFilters(
            keyword=self.keyword + digit,
            search_type=self.search_type,
            pv_codes=self.pv_codes,
            jp_statuses=self.jp_statuses,
            jp_types=self.jp_types,
            business_sizes=self.business_sizes,
            numeric=self.numeric,
        )

    def payload(self, page: int = 1) -> dict:
        payload = {
            "keyword": self.keyword,
            "type": self.search_type,
            "sortBy": "jpName",
            "currentPage": page,
            "pvCodeList": list(self.pv_codes),
            "jpStatusList": list(self.jp_statuses),
            "jpTypeList": list(self.jp_types),
            "businessSizeList": list(self.business_sizes),
        }

        for name in NUMERIC_FIELDS:
            payload[name] = None

        for name, value in self.numeric:
            payload[name] = serialize_decimal(value)

        return payload

    def report(self) -> dict:
        return {
            "keyword": self.keyword,
            "search_type": self.search_type,
            "pv_codes": list(self.pv_codes),
            "jp_statuses": list(self.jp_statuses),
            "jp_types": list(self.jp_types),
            "business_sizes": list(self.business_sizes),
            "numeric_filters": {
                name: str(value)
                for name, value in self.numeric
            },
        }


# ============================================================
# READ-ONLY BUCKET LOADER
# ============================================================

def parse_numeric_bucket_key(
    bucket_key: str,
) -> dict[str, Decimal]:
    numeric: dict[str, Decimal] = {}

    for section in bucket_key.split("|"):
        name, separator, content = section.partition("=")

        if not separator:
            continue

        if name not in NUMERIC_KEY_SECTIONS:
            continue

        if not content:
            continue

        allowed = NUMERIC_KEY_SECTIONS[name]

        for item in content.split(","):
            field_name, colon, raw_value = item.partition(":")

            if not colon or field_name not in allowed:
                raise ProbeContractError(
                    f"Malformed numeric bucket section: {name}"
                )

            value = parse_decimal(raw_value)

            if value is None:
                raise ProbeContractError(
                    f"Empty numeric bucket value: {field_name}"
                )

            previous = numeric.get(field_name)

            if previous is not None and previous != value:
                raise ProbeContractError(
                    f"Conflicting numeric bucket value: {field_name}"
                )

            numeric[field_name] = value

    return numeric


def load_bucket(
    bucket_id: int,
) -> tuple[QueryFilters, dict]:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM discovery_buckets
                WHERE id = %s
                """,
                (bucket_id,),
            )

            row = cursor.fetchone()

    if row is None:
        raise ProbeError(
            f"Bucket {bucket_id} does not exist"
        )

    bucket_key = str(row["bucket_key"])

    sections: dict[str, str] = {}

    for section in bucket_key.split("|"):
        key, separator, value = section.partition("=")

        if separator:
            sections[key] = value

    expected = {
        "k": str(row["keyword"]),
        "t": str(row.get("search_type") or ""),
        "pv": ",".join(
            sorted(csv_values(row.get("pv_codes")))
        ),
        "st": ",".join(
            sorted(csv_values(row.get("jp_statuses")))
        ),
        "jt": ",".join(
            sorted(csv_values(row.get("jp_types")))
        ),
        "sz": ",".join(
            sorted(csv_values(row.get("business_sizes")))
        ),
    }

    for name, value in expected.items():
        stored = sections.get(name)

        if stored is None:
            raise ProbeContractError(
                f"Missing bucket key section: {name}"
            )

        if name in {"pv", "st", "jt", "sz"}:
            stored = ",".join(
                sorted(csv_values(stored))
            )

        if stored != value:
            raise ProbeContractError(
                f"Bucket key does not match stored {name}"
            )

    numeric = parse_numeric_bucket_key(
        bucket_key
    )

    raw_numeric = row.get("numeric_filters")

    if isinstance(raw_numeric, (bytes, bytearray)):
        raw_numeric = raw_numeric.decode("utf-8")

    if isinstance(raw_numeric, str):
        raw_numeric = json.loads(
            raw_numeric,
            parse_float=Decimal,
        )

    if raw_numeric is None:
        raw_numeric = {}

    if not isinstance(raw_numeric, dict):
        raise ProbeContractError(
            "numeric_filters must be a JSON object"
        )

    for name, raw_value in raw_numeric.items():
        if name not in NUMERIC_FIELDS:
            raise ProbeContractError(
                f"Unsupported numeric filter: {name}"
            )

        value = parse_decimal(raw_value)

        if value is None:
            continue

        existing = numeric.get(name)

        if existing is not None and existing != value:
            raise ProbeContractError(
                f"Numeric filter conflict: {name}"
            )

        numeric[name] = value

    filters = QueryFilters(
        keyword=str(row["keyword"]),
        search_type=str(row.get("search_type") or ""),
        pv_codes=csv_values(row.get("pv_codes")),
        jp_statuses=csv_values(row.get("jp_statuses")),
        jp_types=csv_values(row.get("jp_types")),
        business_sizes=csv_values(row.get("business_sizes")),
        numeric=tuple(sorted(numeric.items())),
    )

    metadata = {
        "bucket_id": int(row["id"]),
        "bucket_key": bucket_key,
        "stored_count": row.get("total_items"),
        "stored_pages": row.get("pages_total"),
        "stored_status": row.get("status"),
        "stored_depth": row.get("depth"),
        "parent_id": row.get("parent_id"),
    }

    return filters, metadata


# ============================================================
# SEARCH OBSERVATION
# ============================================================

@dataclass(frozen=True)
class Observation:
    keyword: str

    count: int
    pages: int

    first_ids: tuple[tuple[str, str], ...]

    first_signature: str


@dataclass(frozen=True)
class PageResult:
    page_no: int

    observation: Observation

    ids: tuple[tuple[str, str], ...]


# ============================================================
# RECURSIVE NODE
# ============================================================

@dataclass
class Node:
    keyword: str
    depth: int

    parent_keyword: str | None = None

    count: int | None = None
    pages: int | None = None

    status: str = "PENDING"

    children: list[str] = field(
        default_factory=list
    )

    error: dict | None = None

    count_validation: dict | None = None

    def oversized(self) -> bool | None:
        if self.count is None:
            return None

        return self.count > TARGET_ROWS

    def to_dict(self) -> dict:
        return {
            "keyword": self.keyword,
            "depth": self.depth,
            "parent_keyword": self.parent_keyword,
            "count": self.count,
            "pages": self.pages,
            "oversized": self.oversized(),
            "status": self.status,
            "children": list(self.children),
            "error": self.error,
            "count_validation": self.count_validation,
        }


# ============================================================
# PROBE ENGINE
# ============================================================

class KeywordPartitionProbe:
    def __init__(
        self,
        client: FixedRateDbdClient,
        args: argparse.Namespace,
        root_filters: QueryFilters,
    ) -> None:
        self.client = client
        self.args = args

        self.root_filters = root_filters
        self.root_keyword = root_filters.keyword

        self.started = time.monotonic()

        self.nodes: dict[str, Node] = {}

        self.observations: dict[
            str,
            Observation,
        ] = {}

        self.queue: deque[str] = deque()

        self.expanded_nodes = 0
        self.counted_nodes = 0

        self.probe_retries = 0

        self.count_errors = 0
        self.depth_limited = 0
        self.length_limited = 0

        self.termination_reason: str | None = None

        self.comparisons: list[dict] = []

        self.identity_audit: dict | None = None

        self.audit_error: dict | None = None

        self.expansion_finished = False

    def elapsed(self) -> float:
        return round(
            time.monotonic() - self.started,
            2,
        )

    def requests(self) -> int:
        return int(
            self.client.requests_made
        )

    def filters_for(
        self,
        keyword: str,
    ) -> QueryFilters:
        if not keyword.startswith(self.root_keyword):
            raise ProbeContractError(
                "Child keyword does not extend root keyword"
            )

        if not keyword.isdigit():
            raise ProbeContractError(
                "Child keyword is not numeric"
            )

        return QueryFilters(
            keyword=keyword,
            search_type=self.root_filters.search_type,
            pv_codes=self.root_filters.pv_codes,
            jp_statuses=self.root_filters.jp_statuses,
            jp_types=self.root_filters.jp_types,
            business_sizes=self.root_filters.business_sizes,
            numeric=self.root_filters.numeric,
        )

    def add_node(
        self,
        keyword: str,
        depth: int,
        parent_keyword: str | None = None,
    ) -> Node:
        existing = self.nodes.get(keyword)

        if existing is not None:
            if (
                existing.depth != depth
                or existing.parent_keyword != parent_keyword
            ):
                raise ProbeContractError(
                    "Conflicting recursive keyword node"
                )

            return existing

        if len(self.nodes) >= self.args.max_nodes:
            raise NodeBudgetExceeded(
                "MAX_NODES_EXCEEDED"
            )

        node = Node(
            keyword=keyword,
            depth=depth,
            parent_keyword=parent_keyword,
        )

        self.nodes[keyword] = node

        return node

    async def fetch_page(
        self,
        filters: QueryFilters,
        page_no: int = 1,
        *,
        expected: Observation | None = None,
    ) -> PageResult:
        if not 1 <= page_no <= MAX_PAGE:
            raise ProbeContractError(
                f"Page exceeds API limit: {page_no}"
            )

        for attempt in range(
            self.args.retries + 1
        ):
            if self.requests() >= self.args.max_requests:
                raise RequestBudgetExceeded(
                    "MAX_REQUESTS_EXCEEDED"
                )

            try:
                response = await self.client.fetch(
                    "POST",
                    SEARCH_PATH,
                    json_body=filters.payload(page_no),
                    referer=SEARCH_REFERER,
                    timeout=self.args.timeout,
                )

                page = parse_search_response(
                    response.data
                )

                if page.current_page != page_no:
                    raise ProbeContractError(
                        "API returned unexpected currentPage"
                    )

                if page.items_per_page != ITEMS_PER_PAGE:
                    raise ProbeContractError(
                        "API returned unexpected itemsPerPage"
                    )

                expected_pages = math.ceil(
                    page.total_items / ITEMS_PER_PAGE
                )

                if page.total_items == 0:
                    if page.total_pages not in (0, 1):
                        raise ProbeContractError(
                            "Empty result has invalid page count"
                        )

                elif page.total_pages != expected_pages:
                    raise ProbeContractError(
                        "Pagination metadata is inconsistent"
                    )

                expected_rows = min(
                    ITEMS_PER_PAGE,
                    max(
                        0,
                        page.total_items
                        - (page_no - 1) * ITEMS_PER_PAGE,
                    ),
                )

                if len(page.rows) != expected_rows:
                    raise ProbeContractError(
                        "Search page row count mismatch"
                    )

                ids = tuple(
                    identity(row)
                    for row in page.rows
                )

                if len(ids) != len(set(ids)):
                    raise ProbeContractError(
                        "Duplicate identity within one page"
                    )

                observation = Observation(
                    keyword=filters.keyword,
                    count=page.total_items,
                    pages=page.total_pages,
                    first_ids=(
                        ids if page_no == 1 else ()
                    ),
                    first_signature=(
                        digest(ids) if page_no == 1 else ""
                    ),
                )

                if expected is not None:
                    if (
                        observation.count != expected.count
                        or observation.pages != expected.pages
                    ):
                        raise ObservationDrift(
                            f"Count/page drift for "
                            f"{filters.keyword}"
                        )

                return PageResult(
                    page_no=page_no,
                    observation=observation,
                    ids=ids,
                )

            except DeepTransientHttpError as exc:
                if attempt >= self.args.retries:
                    raise

                self.probe_retries += 1

                delay = min(
                    90.0,
                    5.0 * (2**attempt),
                )

                emit(
                    "RETRY",
                    keyword=filters.keyword,
                    page=page_no,
                    attempt=attempt + 1,
                    http_status=getattr(
                        exc,
                        "status",
                        None,
                    ),
                    delay_seconds=delay,
                )

                await asyncio.sleep(delay)

        raise ProbeError(
            "Request retry loop exhausted"
        )

    async def count_keyword(
        self,
        keyword: str,
    ) -> Observation:
        result = await self.fetch_page(
            self.filters_for(keyword),
            1,
        )

        observation = result.observation

        self.observations[keyword] = observation

        node = self.nodes[keyword]

        node.count = observation.count
        node.pages = observation.pages

        self.counted_nodes += 1

        emit(
            "KEYWORD_COUNT",
            keyword=keyword,
            depth=node.depth,
            count=node.count,
            pages=node.pages,
            oversized=node.oversized(),
            requests=self.requests(),
        )

        return observation

    async def observe_child(
        self,
        parent: Node,
        digit: str,
    ) -> None:
        keyword = parent.keyword + digit

        child = self.add_node(
            keyword=keyword,
            depth=parent.depth + 1,
            parent_keyword=parent.keyword,
        )

        parent.children.append(keyword)

        try:
            observation = await self.count_keyword(
                keyword
            )

        except (
            RequestBudgetExceeded,
            NodeBudgetExceeded,
        ):
            raise

        except Exception as exc:
            child.status = "COUNT_ERROR"
            child.error = short_error(exc)

            self.count_errors += 1

            emit(
                "CHILD_COUNT_ERROR",
                keyword=keyword,
                parent=parent.keyword,
                error_type=type(exc).__name__,
            )

            return

        if observation.count == 0:
            child.status = "EMPTY"

        elif observation.count <= TARGET_ROWS:
            child.status = "LEAF"

        elif child.depth >= self.args.max_depth:
            child.status = "UNRESOLVED_DEPTH"
            self.depth_limited += 1

        elif len(keyword) >= self.args.max_keyword_length:
            child.status = "UNRESOLVED_KEYWORD_LENGTH"
            self.length_limited += 1

        else:
            child.status = "QUEUED"
            self.queue.append(keyword)

        emit(
            "CHILD_DECISION",
            parent=parent.keyword,
            keyword=keyword,
            count=observation.count,
            depth=child.depth,
            status=child.status,
        )

    async def expand_node(
        self,
        node: Node,
    ) -> None:
        if node.count is None:
            raise ProbeContractError(
                "Cannot expand an uncounted node"
            )

        if node.count <= TARGET_ROWS:
            node.status = (
                "EMPTY" if node.count == 0 else "LEAF"
            )
            return

        if node.depth >= self.args.max_depth:
            node.status = "UNRESOLVED_DEPTH"
            self.depth_limited += 1
            return

        if (
            len(node.keyword)
            >= self.args.max_keyword_length
        ):
            node.status = "UNRESOLVED_KEYWORD_LENGTH"
            self.length_limited += 1
            return

        node.status = "SPLITTING"

        self.expanded_nodes += 1

        emit(
            "RECURSIVE_EXPANSION_START",
            keyword=node.keyword,
            depth=node.depth,
            count=node.count,
            expected_children=10,
        )

        for digit in DIGITS:
            await self.observe_child(
                node,
                digit,
            )

        if len(node.children) == 10:
            if any(
                self.nodes[key].count is None
                for key in node.children
            ):
                node.status = "SPLIT_PARTIAL"
            else:
                node.status = "SPLIT_COMPLETE"
        else:
            node.status = "SPLIT_PARTIAL"

        emit(
            "RECURSIVE_EXPANSION_FINISH",
            keyword=node.keyword,
            depth=node.depth,
            children_created=len(node.children),
            child_counts_known=sum(
                self.nodes[key].count is not None
                for key in node.children
            ),
            queued_oversized=len(self.queue),
            requests=self.requests(),
        )

    async def expand_all(self) -> None:
        """
        Phase 1:

        Recursively expand every oversized keyword until
        it reaches <= 10,000, a configured limit, or an
        operational failure.

        DO NOT evaluate parent/child count conservation here.
        """

        root = self.add_node(
            keyword=self.root_keyword,
            depth=0,
        )

        self.queue.append(
            root.keyword
        )

        emit(
            "EXPANSION_STARTED",
            root_keyword=self.root_keyword,
            max_depth=self.args.max_depth,
            max_keyword_length=(
                self.args.max_keyword_length
            ),
            max_requests=self.args.max_requests,
        )

        try:
            while self.queue:
                keyword = self.queue.popleft()

                node = self.nodes[keyword]

                if node.count is None:
                    try:
                        await self.count_keyword(
                            keyword
                        )

                    except (
                        RequestBudgetExceeded,
                        NodeBudgetExceeded,
                    ):
                        raise

                    except Exception as exc:
                        node.status = "COUNT_ERROR"
                        node.error = short_error(exc)

                        self.count_errors += 1

                        emit(
                            "PARENT_COUNT_ERROR",
                            keyword=keyword,
                            error_type=type(exc).__name__,
                        )

                        continue

                await self.expand_node(
                    node
                )

            self.expansion_finished = True

        except (
            RequestBudgetExceeded,
            NodeBudgetExceeded,
        ) as exc:
            self.termination_reason = str(exc)

            emit(
                "EXPANSION_LIMIT_REACHED",
                reason=str(exc),
                requests=self.requests(),
                nodes=len(self.nodes),
            )

        finally:
            for node in self.nodes.values():
                if node.status == "SPLITTING":
                    node.status = "SPLIT_PARTIAL"

                elif node.status in {
                    "PENDING",
                    "QUEUED",
                }:
                    node.status = "UNRESOLVED_NODE_BUDGET"

            emit(
                "EXPANSION_PHASE_FINISHED",
                finished=self.expansion_finished,
                nodes=len(self.nodes),
                expanded=self.expanded_nodes,
                counted=self.counted_nodes,
                remaining_queue=len(self.queue),
                requests=self.requests(),
                termination_reason=(
                    self.termination_reason
                ),
            )

    # ========================================================
    # PHASE 2 — COUNT COMPARISONS
    # ========================================================

    def compare_all_counts(self) -> None:
        """
        Runs only AFTER recursive expansion.

        Every expanded parent is evaluated separately.

        Root total is also compared against the sum of
        all terminal leaves when the tree is complete.
        """

        self.comparisons.clear()

        expanded = sorted(
            (
                node
                for node in self.nodes.values()
                if node.children
            ),
            key=lambda node: (
                -node.depth,
                node.keyword,
            ),
        )

        for parent in expanded:
            children = [
                self.nodes[key]
                for key in parent.children
            ]

            complete = (
                len(parent.children) == 10
                and parent.count is not None
                and all(
                    child.count is not None
                    for child in children
                )
            )

            if not complete:
                result = {
                    "keyword": parent.keyword,
                    "depth": parent.depth,
                    "parent_count": parent.count,
                    "child_count_sum": None,
                    "delta": None,
                    "status": "NOT_EVALUABLE",
                    "children_expected": 10,
                    "children_created": len(
                        parent.children
                    ),
                    "children_counted": sum(
                        child.count is not None
                        for child in children
                    ),
                }

            else:
                total = sum(
                    int(child.count)
                    for child in children
                    if child.count is not None
                )

                delta = total - int(parent.count)

                result = {
                    "keyword": parent.keyword,
                    "depth": parent.depth,
                    "parent_count": parent.count,
                    "child_count_sum": total,
                    "delta": delta,
                    "status": (
                        "MATCH"
                        if delta == 0
                        else "MISMATCH_UNCONFIRMED"
                    ),
                    "children_expected": 10,
                    "children_created": 10,
                    "children_counted": 10,
                }

            parent.count_validation = result

            self.comparisons.append(
                result
            )

            emit(
                "POST_EXPANSION_COUNT_COMPARISON",
                keyword=parent.keyword,
                depth=parent.depth,
                parent_count=result["parent_count"],
                children_sum=result["child_count_sum"],
                delta=result["delta"],
                status=result["status"],
            )

    async def confirm_mismatches(self) -> None:
        """
        Recheck mismatched nodes after all expansion
        and initial comparisons are finished.

        A changing count is recorded as drift, not a
        confirmed partition failure.
        """

        if not self.args.recheck_mismatches:
            return

        for comparison in self.comparisons:
            if comparison["status"] != (
                "MISMATCH_UNCONFIRMED"
            ):
                continue

            keyword = comparison["keyword"]

            parent = self.nodes[keyword]

            keywords = [
                keyword,
                *parent.children,
            ]

            previous = {
                key: self.observations[key]
                for key in keywords
            }

            current: dict[
                str,
                Observation,
            ] = {}

            try:
                for key in keywords:
                    result = await self.fetch_page(
                        self.filters_for(key),
                        1,
                    )

                    current[key] = result.observation

            except Exception as exc:
                comparison["status"] = (
                    "MISMATCH_RECHECK_INCOMPLETE"
                )

                comparison["recheck_error"] = (
                    short_error(exc)
                )

                if isinstance(
                    exc,
                    RequestBudgetExceeded,
                ):
                    break

                continue

            changed = [
                key
                for key in keywords
                if (
                    previous[key].count
                    != current[key].count
                    or previous[key].pages
                    != current[key].pages
                )
            ]

            if changed:
                comparison["status"] = (
                    "COUNT_DRIFT"
                )

                comparison["changed_keywords"] = (
                    changed
                )

                emit(
                    "COUNT_DRIFT",
                    keyword=keyword,
                    changed_keywords=changed,
                )

                continue

            repeated_total = sum(
                current[key].count
                for key in parent.children
            )

            repeated_delta = (
                repeated_total
                - current[keyword].count
            )

            comparison["rechecked_parent_count"] = (
                current[keyword].count
            )

            comparison["rechecked_child_sum"] = (
                repeated_total
            )

            comparison["rechecked_delta"] = (
                repeated_delta
            )

            comparison["status"] = (
                "MISMATCH_CONFIRMED"
                if repeated_delta != 0
                else "COUNT_DRIFT"
            )

            emit(
                "POST_EXPANSION_COUNT_RECHECK",
                keyword=keyword,
                delta=repeated_delta,
                status=comparison["status"],
            )

    # ========================================================
    # PHASE 3 — OPTIONAL IDENTITY VALIDATION
    # ========================================================

    @staticmethod
    def sampled_pages(
        observation: Observation,
        maximum: int,
    ) -> list[int]:
        if observation.count == 0:
            return []

        accessible = min(
            MAX_PAGE,
            observation.pages,
        )

        if maximum <= 1:
            return [1]

        candidates = [
            1,
            accessible,
            max(1, accessible // 2),
            max(1, accessible // 4),
            max(1, accessible * 3 // 4),
        ]

        pages = []
        seen = set()

        for number in candidates:
            if number in seen:
                continue

            seen.add(number)
            pages.append(number)

            if len(pages) >= maximum:
                return pages

        for number in range(1, accessible + 1):
            if number in seen:
                continue

            pages.append(number)

            if len(pages) >= maximum:
                break

        return pages

    async def collect_identity_pages(
        self,
        keyword: str,
        page_numbers: list[int],
    ) -> dict[
        tuple[str, str],
        set[int],
    ]:
        observation = self.observations[keyword]

        result: dict[
            tuple[str, str],
            set[int],
        ] = defaultdict(set)

        for page_no in page_numbers:
            if page_no == 1:
                ids = observation.first_ids
            else:
                page = await self.fetch_page(
                    self.filters_for(keyword),
                    page_no,
                    expected=observation,
                )

                ids = page.ids

            for key in ids:
                result[key].add(page_no)

        return dict(result)

    async def audit_terminal_leaves(self) -> dict:
        """
        Optional post-expansion identity audit.

        Only terminal leaves are audited.

        This does not independently prove containment
        in an oversized original parent.
        """

        leaves = sorted(
            (
                node
                for node in self.nodes.values()
                if node.status in {"LEAF", "EMPTY"}
            ),
            key=lambda node: node.keyword,
        )

        identity_locations: dict[
            tuple[str, str],
            list[tuple[str, int]],
        ] = defaultdict(list)

        pages_fetched = 0
        audited_leaves = 0
        within_leaf_duplicates = 0

        leaf_results = []

        for node in leaves:
            observation = self.observations.get(
                node.keyword
            )

            if observation is None:
                continue

            if self.args.mode == "full":
                pages = list(
                    range(
                        1,
                        observation.pages + 1,
                    )
                )
            else:
                pages = self.sampled_pages(
                    observation,
                    self.args.sample_pages,
                )

            sampled = await self.collect_identity_pages(
                node.keyword,
                pages,
            )

            pages_fetched += max(
                0,
                len(pages) - int(1 in pages),
            )

            audited_leaves += 1

            duplicate_pages = sum(
                len(locations) > 1
                for locations in sampled.values()
            )

            within_leaf_duplicates += (
                duplicate_pages
            )

            for key, numbers in sampled.items():
                for page_no in numbers:
                    identity_locations[key].append(
                        (node.keyword, page_no)
                    )

            leaf_results.append({
                "keyword": node.keyword,
                "expected_count": node.count,
                "pages_examined": len(pages),
                "unique_identities_seen": len(sampled),
                "within_leaf_duplicate_identities": (
                    duplicate_pages
                ),
                "fully_enumerated": (
                    self.args.mode == "full"
                    and len(sampled) == observation.count
                    and duplicate_pages == 0
                ),
            })

        candidates = []

        for key, locations in identity_locations.items():
            keywords = {
                keyword
                for keyword, _ in locations
            }

            if len(keywords) > 1:
                candidates.append(
                    (key, locations)
                )

        confirmed_overlaps = []

        for key, locations in candidates:
            confirmed = set()

            for keyword, page_no in locations:
                if keyword in confirmed:
                    continue

                repeated = await self.fetch_page(
                    self.filters_for(keyword),
                    page_no,
                    expected=self.observations[keyword],
                )

                if key in repeated.ids:
                    confirmed.add(keyword)

            if len(confirmed) > 1:
                confirmed_overlaps.append({
                    **serialize_identity(key),
                    "keywords": sorted(confirmed),
                })

                if (
                    len(confirmed_overlaps)
                    >= self.args.max_overlap_examples
                ):
                    break

        total_terminal_counts = sum(
            int(node.count or 0)
            for node in leaves
        )

        unique_observed = len(
            identity_locations
        )

        all_fully_enumerated = (
            self.args.mode == "full"
            and all(
                result["fully_enumerated"]
                for result in leaf_results
            )
            and len(leaf_results) == len(leaves)
        )

        report = {
            "mode": self.args.mode,
            "terminal_leaves_audited": audited_leaves,
            "pages_fetched_after_expansion": (
                pages_fetched
            ),
            "terminal_count_sum": (
                total_terminal_counts
            ),
            "unique_identities_observed": (
                unique_observed
            ),
            "within_leaf_duplicate_identities": (
                within_leaf_duplicates
            ),
            "cross_leaf_overlap_candidates": (
                len(candidates)
            ),
            "confirmed_cross_leaf_overlaps": (
                len(confirmed_overlaps)
            ),
            "confirmed_overlap_examples": (
                confirmed_overlaps
            ),
            "all_leaves_fully_enumerated": (
                all_fully_enumerated
            ),
            "leaf_results": leaf_results,
            "parent_containment_verified": False,
        }

        if all_fully_enumerated:
            report["global_unique_count"] = (
                unique_observed
            )

            report["leaf_count_sum_minus_unique"] = (
                total_terminal_counts
                - unique_observed
            )

        return report

    # ========================================================
    # FINAL STATISTICS
    # ========================================================

    def terminal_leaves(self) -> list[Node]:
        return sorted(
            (
                node
                for node in self.nodes.values()
                if node.status in {"LEAF", "EMPTY"}
            ),
            key=lambda item: item.keyword,
        )

    def unresolved_nodes(self) -> list[Node]:
        unresolved_statuses = {
            "UNRESOLVED_DEPTH",
            "UNRESOLVED_KEYWORD_LENGTH",
            "UNRESOLVED_NODE_BUDGET",
            "COUNT_ERROR",
            "SPLIT_PARTIAL",
            "PENDING",
            "QUEUED",
            "SPLITTING",
        }

        return sorted(
            (
                node
                for node in self.nodes.values()
                if node.status in unresolved_statuses
            ),
            key=lambda item: item.keyword,
        )

    def tree_is_complete(self) -> bool:
        if not self.expansion_finished:
            return False

        if self.unresolved_nodes():
            return False

        for node in self.nodes.values():
            if node.children:
                if len(node.children) != 10:
                    return False

                if node.status != "SPLIT_COMPLETE":
                    return False

        return True

    def root_count(self) -> int | None:
        root = self.nodes.get(
            self.root_keyword
        )

        return (
            root.count
            if root is not None
            else None
        )

    def build_summary(self) -> dict:
        leaves = self.terminal_leaves()
        unresolved = self.unresolved_nodes()

        leaf_total = sum(
            int(node.count or 0)
            for node in leaves
        )

        root_total = self.root_count()

        complete = self.tree_is_complete()

        comparisons = self.comparisons

        confirmed_mismatches = [
            comparison
            for comparison in comparisons
            if comparison["status"]
            == "MISMATCH_CONFIRMED"
        ]

        unconfirmed_mismatches = [
            comparison
            for comparison in comparisons
            if comparison["status"]
            in {
                "MISMATCH_UNCONFIRMED",
                "MISMATCH_RECHECK_INCOMPLETE",
            }
        ]

        count_drifts = [
            comparison
            for comparison in comparisons
            if comparison["status"] == "COUNT_DRIFT"
        ]

        matches = [
            comparison
            for comparison in comparisons
            if comparison["status"] == "MATCH"
        ]

        unresolved_by_reason: dict[
            str,
            int,
        ] = {}

        for node in unresolved:
            unresolved_by_reason[node.status] = (
                unresolved_by_reason.get(
                    node.status,
                    0,
                )
                + 1
            )

        depth_stats = []

        depths = sorted({
            node.depth
            for node in self.nodes.values()
        })

        for depth in depths:
            nodes = [
                node
                for node in self.nodes.values()
                if node.depth == depth
            ]

            depth_stats.append({
                "depth": depth,
                "keywords": len(nodes),
                "oversized": sum(
                    node.count is not None
                    and node.count > TARGET_ROWS
                    for node in nodes
                ),
                "leaves": sum(
                    node.status in {"LEAF", "EMPTY"}
                    for node in nodes
                ),
                "expanded": sum(
                    bool(node.children)
                    for node in nodes
                ),
                "count_errors": sum(
                    node.status == "COUNT_ERROR"
                    for node in nodes
                ),
            })

        leaf_delta = (
            leaf_total - root_total
            if root_total is not None
            else None
        )

        return {
            "root_keyword": self.root_keyword,
            "root_live_count": root_total,
            "total_nodes": len(self.nodes),
            "counted_nodes": self.counted_nodes,
            "expanded_nodes": self.expanded_nodes,
            "terminal_leaf_count": len(leaves),
            "terminal_leaf_total": leaf_total,
            "terminal_leaf_delta_from_root": (
                leaf_delta
            ),
            "terminal_leaf_total_is_exhaustive": (
                complete
            ),
            "unresolved_nodes": len(unresolved),
            "unresolved_by_reason": (
                unresolved_by_reason
            ),
            "tree_expansion_finished": (
                self.expansion_finished
            ),
            "tree_complete": complete,
            "partition_comparisons": (
                len(comparisons)
            ),
            "count_matches": len(matches),
            "confirmed_count_mismatches": (
                len(confirmed_mismatches)
            ),
            "unconfirmed_count_mismatches": (
                len(unconfirmed_mismatches)
            ),
            "count_drifts": len(count_drifts),
            "count_errors": self.count_errors,
            "depth_limited_nodes": (
                self.depth_limited
            ),
            "keyword_length_limited_nodes": (
                self.length_limited
            ),
            "depth_statistics": depth_stats,
        }

    def final_verdict(self) -> tuple[str, str]:
        confirmed_bad_counts = any(
            comparison["status"]
            == "MISMATCH_CONFIRMED"
            for comparison in self.comparisons
        )

        confirmed_overlap = bool(
            self.identity_audit
            and self.identity_audit.get(
                "confirmed_cross_leaf_overlaps",
                0,
            ) > 0
        )

        if confirmed_bad_counts:
            return (
                "FAIL_NOT_LOSSLESS",
                "STABLE_PARENT_CHILD_COUNT_MISMATCH",
            )

        if confirmed_overlap:
            return (
                "FAIL_NOT_LOSSLESS",
                "CONFIRMED_CROSS_LEAF_IDENTITY_OVERLAP",
            )

        if self.audit_error is not None:
            return (
                "INCOMPLETE_OR_FAILED",
                "POST_EXPANSION_IDENTITY_AUDIT_INCOMPLETE",
            )

        if not self.tree_is_complete():
            return (
                "INCOMPLETE_OR_FAILED",
                "RECURSIVE_EXPANSION_INCOMPLETE",
            )

        if any(
            comparison["status"] != "MATCH"
            for comparison in self.comparisons
        ):
            return (
                "INCONCLUSIVE",
                "UNCONFIRMED_COUNTS_OR_COUNT_DRIFT",
            )

        if (
            self.identity_audit is not None
            and self.identity_audit.get(
                "within_leaf_duplicate_identities",
                0,
            ) > 0
        ):
            return (
                "INCONCLUSIVE",
                "WITHIN_LEAF_IDENTITY_DUPLICATES_OBSERVED",
            )

        return (
            "INCONCLUSIVE",
            "RECURSIVE_EXPANSION_COMPLETE_BUT_PARENT_COVERAGE_UNPROVEN",
        )

    def build_tree(
        self,
        keyword: str,
    ) -> dict:
        node = self.nodes[keyword]

        result = {
            "keyword": node.keyword,
            "depth": node.depth,
            "count": node.count,
            "status": node.status,
            "count_validation": (
                node.count_validation
            ),
            "error": node.error,
            "children": [],
        }

        for child in node.children:
            result["children"].append(
                self.build_tree(child)
            )

        return result

    def report(self) -> dict:
        verdict, reason = self.final_verdict()

        root = self.nodes.get(
            self.root_keyword
        )

        return {
            "verdict": verdict,
            "verdict_reason": reason,
            "root": (
                self.build_tree(root.keyword)
                if root is not None
                else None
            ),
            "summary": self.build_summary(),
            "comparisons": self.comparisons,
            "identity_audit": self.identity_audit,
            "identity_audit_error": (
                self.audit_error
            ),
            "termination_reason": (
                self.termination_reason
            ),
            "execution": {
                "requests": self.requests(),
                "client_retries": (
                    self.client.retries_made
                ),
                "probe_retries": (
                    self.probe_retries
                ),
                "rate_limit_hits": (
                    self.client.rate_limit_hits
                ),
                "elapsed_seconds": self.elapsed(),
            },
        }

    # ========================================================
    # MAIN RUNNER
    # ========================================================

    async def run(self) -> dict:
        # Phase 1: Complete recursive expansion first.
        await self.expand_all()

        emit(
            "VALIDATION_PHASE_STARTED",
            phase="POST_EXPANSION_COUNTS",
        )

        # Phase 2: Compare all counts afterwards.
        self.compare_all_counts()

        # Phase 3: Optionally recheck mismatches.
        await self.confirm_mismatches()

        # Phase 4: Optional identity audit after expansion.
        if self.args.mode in {"sample", "full"}:
            emit(
                "IDENTITY_AUDIT_STARTED",
                mode=self.args.mode,
            )

            try:
                self.identity_audit = (
                    await self.audit_terminal_leaves()
                )

            except Exception as exc:
                self.audit_error = short_error(
                    exc
                )

                emit(
                    "IDENTITY_AUDIT_INCOMPLETE",
                    error_type=type(exc).__name__,
                )

        result = self.report()

        emit(
            "PROBE_FINISHED",
            verdict=result["verdict"],
            reason=result["verdict_reason"],
            nodes=result["summary"]["total_nodes"],
            expanded=result["summary"]["expanded_nodes"],
            terminal_leaves=result["summary"][
                "terminal_leaf_count"
            ],
            leaf_sum=result["summary"][
                "terminal_leaf_total"
            ],
            root_count=result["summary"][
                "root_live_count"
            ],
            leaf_delta=result["summary"][
                "terminal_leaf_delta_from_root"
            ],
            complete=result["summary"][
                "tree_complete"
            ],
            requests=self.requests(),
        )

        return result


# ============================================================
# COMMAND LINE
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only recursive DBD numeric-keyword "
            "expansion with post-expansion validation"
        )
    )

    target = parser.add_mutually_exclusive_group(
        required=True
    )

    target.add_argument(
        "--keyword",
        type=str,
        help="Numeric starting keyword, e.g. 055",
    )

    target.add_argument(
        "--bucket-id",
        type=int,
        help=(
            "Read existing bucket filters from MySQL "
            "without modifying the database"
        ),
    )

    parser.add_argument(
        "--search-type",
        type=str,
        default="",
    )

    parser.add_argument(
        "--mode",
        choices=("count", "sample", "full"),
        default="count",
        help=(
            "count: recursive counts only; "
            "sample: sample terminal identities afterwards; "
            "full: enumerate terminal leaves afterwards"
        ),
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=10,
        help=(
            "Maximum appended keyword digits "
            "(default: 10)"
        ),
    )

    parser.add_argument(
        "--max-keyword-length",
        type=int,
        default=13,
        help=(
            "Maximum keyword length before stopping "
            "a still-oversized branch"
        ),
    )

    parser.add_argument(
        "--max-nodes",
        type=int,
        default=1000,
        help="Maximum keyword nodes",
    )

    parser.add_argument(
        "--max-requests",
        type=int,
        default=500,
        help="Maximum Search API requests",
    )

    parser.add_argument(
        "--sample-pages",
        type=int,
        default=3,
        help=(
            "Pages per terminal leaf in sample mode"
        ),
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=0.5,
        help="Maximum request admission rate",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--max-overlap-examples",
        type=int,
        default=10,
    )

    parser.add_argument(
        "--recheck-mismatches",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Repeat mismatched parent/child counts "
            "after expansion"
        ),
    )

    args = parser.parse_args()

    if args.keyword is not None:
        if not re.fullmatch(
            r"[0-9]{3,}",
            args.keyword,
        ):
            parser.error(
                "--keyword must contain at least 3 digits"
            )

    if (
        args.bucket_id is not None
        and args.bucket_id <= 0
    ):
        parser.error(
            "--bucket-id must be positive"
        )

    if args.max_depth < 1:
        parser.error(
            "--max-depth must be >= 1"
        )

    if args.max_keyword_length < 4:
        parser.error(
            "--max-keyword-length must be >= 4"
        )

    if args.max_nodes < 1:
        parser.error(
            "--max-nodes must be >= 1"
        )

    if args.max_requests < 1:
        parser.error(
            "--max-requests must be >= 1"
        )

    if args.sample_pages < 1:
        parser.error(
            "--sample-pages must be >= 1"
        )

    if args.retries < 0:
        parser.error(
            "--retries must be >= 0"
        )

    if (
        not math.isfinite(args.rate)
        or args.rate <= 0
        or args.rate > 10
    ):
        parser.error(
            "--rate must be > 0 and <= 10"
        )

    if (
        not math.isfinite(args.timeout)
        or args.timeout <= 0
    ):
        parser.error(
            "--timeout must be positive"
        )

    if args.max_overlap_examples < 1:
        parser.error(
            "--max-overlap-examples must be >= 1"
        )

    return args


# ============================================================
# ENTRY POINT
# ============================================================

async def execute(
    filters: QueryFilters,
    metadata: dict,
    args: argparse.Namespace,
) -> dict:
    started = time.monotonic()

    result = {
        "program": "probe_keyword_partition",
        "version": "2.0-recursive-expansion",
        "started_at_utc": utc_now(),
        "read_only": True,
        "database_writes": False,
        "source_modified": False,
        "output_files_created": False,
        "strategy": "RECURSIVE_APPEND_DIGIT_0_TO_9",
        "expansion_policy": (
            "EXPAND_ALL_OVERSIZED_CHILDREN_BEFORE_VALIDATION"
        ),
        "target_rows": TARGET_ROWS,
        "parent_metadata": metadata,
        "filters": filters.report(),
        "configuration": {
            "mode": args.mode,
            "max_depth": args.max_depth,
            "max_keyword_length": (
                args.max_keyword_length
            ),
            "max_nodes": args.max_nodes,
            "max_requests": args.max_requests,
            "sample_pages": args.sample_pages,
            "rate": args.rate,
            "timeout": args.timeout,
            "retries": args.retries,
            "recheck_mismatches": (
                args.recheck_mismatches
            ),
        },
        "result": None,
        "status": "INCOMPLETE_OR_FAILED",
    }

    try:
        async with FixedRateDbdClient(
            concurrency=1,
            rate_per_second=args.rate,
            max_retries=0,
            timeout=args.timeout,
        ) as client:
            probe = KeywordPartitionProbe(
                client=client,
                args=args,
                root_filters=filters,
            )

            result["result"] = await probe.run()

            result["status"] = result[
                "result"
            ]["verdict"]

    except Exception as exc:
        result["status"] = "INCOMPLETE_OR_FAILED"

        result["error"] = short_error(
            exc
        )

        emit(
            "FATAL_PROBE_ERROR",
            error_type=type(exc).__name__,
        )

    finally:
        result["finished_at_utc"] = utc_now()

        result["elapsed_seconds"] = round(
            time.monotonic() - started,
            2,
        )

    return result


def main() -> int:
    args = parse_args()

    try:
        if args.bucket_id is not None:
            filters, metadata = load_bucket(
                args.bucket_id
            )

            emit(
                "BUCKET_LOADED",
                bucket_id=args.bucket_id,
                keyword=filters.keyword,
                stored_count=metadata["stored_count"],
                filters=filters.report(),
            )

        else:
            filters = QueryFilters(
                keyword=args.keyword,
                search_type=args.search_type,
            )

            metadata = {
                "source": "DIRECT_KEYWORD",
                "bucket_id": None,
                "stored_count": None,
            }

        if (
            len(filters.keyword)
            > args.max_keyword_length
        ):
            raise ProbeContractError(
                "Root keyword is longer than "
                "--max-keyword-length"
            )

        report = asyncio.run(
            execute(
                filters,
                metadata,
                args,
            )
        )

    except Exception as exc:
        report = {
            "program": "probe_keyword_partition",
            "version": "2.0-recursive-expansion",
            "status": "INCOMPLETE_OR_FAILED",
            "read_only": True,
            "database_writes": False,
            "source_modified": False,
            "error": short_error(exc),
            "finished_at_utc": utc_now(),
        }

    print(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        flush=True,
    )

    status = report.get("status")

    if status == "FAIL_NOT_LOSSLESS":
        return 1

    if status == "INCOMPLETE_OR_FAILED":
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
