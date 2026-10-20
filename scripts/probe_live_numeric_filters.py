r"""
DBD Live Numeric Filter Probe
=============================

Path:
    scripts/probe_live_numeric_filters.py

Run:
    python .\scripts\probe_live_numeric_filters.py

Optional:
    python .\scripts\probe_live_numeric_filters.py --bucket-id 15720

Purpose:
    Test sequential, recursive numeric partitioning against a real failed
    Discovery bucket selected from MySQL.

Dimension order:
    1. cap_amt
    2. total_income
    3. net_profit
    4. total_asset

Rules:
    - Use the real failed bucket; never assume a default bucket ID.
    - Split an oversized group recursively using the current dimension.
    - Move to the next dimension only when the current dimension cannot
      split a proven single-value group any further.
    - Accept a leaf only when its count is at most 10,000.
    - Preserve every inherited categorical and numeric filter.
    - Preserve NULL values as NULL; never substitute zero.
    - Require exact parent/child count conservation.
    - Validate complete leaf pagination, identity uniqueness, numeric
      filter membership, and disjointness across all final leaves.
    - Recheck every leaf page and all accepted parent counts for stability.
    - Unsupported NULL isolation and exhausted budgets remain unresolved.
    - Do not write to MySQL or change production Discovery state.
    - Do not claim global coverage certification from one failed bucket.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import sys
import time
import traceback
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

# ======================================================================
# 01. REPOSITORY BOOTSTRAP
# ======================================================================

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if SRC.is_dir() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from dbd.client.endpoints.search import (  # noqa: E402
    ITEMS_PER_PAGE,
    MAX_PAGE,
    MAX_ROWS_PER_QUERY,
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    SearchPage,
    parse_search_response,
)
from dbd.core.settings import settings  # noqa: E402
from dbd.deep.http import (  # noqa: E402
    DeepPermanentHttpError,
    DeepTransientHttpError,
    FixedRateDbdClient,
)
from dbd.pipelines.discovery import _filters_from_row  # noqa: E402
from dbd.storage.mysql.engine import connect  # noqa: E402

# ======================================================================
# 02. CONTRACTS
# ======================================================================

PRODUCTION_TARGET_ROWS = 10_000

DIMENSIONS = (
    ("cap_amt", "capAmt"),
    ("total_income", "totalIncome"),
    ("net_profit", "netProfit"),
    ("total_asset", "totalAsset"),
)

IDENTITY_FIELDS = ("jpTypeCode", "jpNo")

MAX_ATTEMPT_LOG_ENTRIES = 10_000


# ======================================================================
# 03. EXCEPTIONS
# ======================================================================


class ProbeError(RuntimeError):
    pass


class ProbeBudgetExceeded(ProbeError):
    pass


class ProbeSafetyLimitExceeded(ProbeError):
    pass


class UnstablePagination(ProbeError):
    pass


class DuplicateIdentityError(ProbeError):
    pass


class FilterMembershipError(ProbeError):
    pass


# ======================================================================
# 04. GENERAL HELPERS
# ======================================================================


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def parse_decimal(value: Any) -> Decimal | None:
    """
    Preserve NULL and empty values as missing.

    Thousands separators are removed. Invalid nonempty values are not
    converted to zero and remain distinguishable from NULL.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        raise ValueError("Boolean is not a valid numeric value")

    if isinstance(value, str):
        value = value.strip()

        if not value:
            return None

        value = value.replace(",", "")

    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("Invalid numeric value") from None

    if not result.is_finite():
        raise ValueError("Non-finite numeric value")

    return result


def identity_of(row: dict[str, Any]) -> tuple[str, str]:
    values: list[str] = []

    for field_name in IDENTITY_FIELDS:
        value = row.get(field_name)

        if value is None or str(value).strip() == "":
            raise ProbeError(
                f"Search result is missing identity field {field_name}"
            )

        values.append(str(value).strip())

    return values[0], values[1]


def identity_set(
    rows: list[dict[str, Any]],
) -> set[tuple[str, str]]:
    identities = [identity_of(row) for row in rows]

    if len(identities) != len(set(identities)):
        raise DuplicateIdentityError(
            "Duplicate company identity found in the same page"
        )

    return set(identities)


def identity_digest(
    identities: set[tuple[str, str]],
) -> str:
    canonical = json.dumps(
        sorted(identities),
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def numeric_observation_digest(
    rows: list[dict[str, Any]],
) -> str:
    observed: list[tuple[tuple[str, str], list[str | None]]] = []

    for row in rows:
        identity = identity_of(row)
        values: list[str | None] = []

        for _, api_field in DIMENSIONS:
            try:
                value = parse_decimal(row.get(api_field))
            except ValueError as exc:
                raise FilterMembershipError(f"Invalid numeric field: {api_field}") from exc
            values.append(decimal_text(value))

        observed.append((identity, values))

    canonical = json.dumps(
        sorted(observed),
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def page_signature(rows: list[dict[str, Any]]) -> str:
    identities = identity_set(rows)

    canonical = json.dumps(
        {
            "identity_sha256": identity_digest(identities),
            "numeric_values_sha256": numeric_observation_digest(rows),
            "row_count": len(rows),
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def describe_filters(filters: SearchFilters) -> dict[str, Any]:
    return {
        "keyword": filters.keyword,
        "search_type": filters.search_type,
        "categorical_filters": {
            "pv_codes": list(filters.pv_codes),
            "jp_statuses": list(filters.jp_statuses),
            "jp_types": list(filters.jp_types),
            "business_sizes": list(filters.business_sizes),
        },
        "numeric_filters": filters.numeric_range() or {},
    }


def filter_fingerprint(filters: SearchFilters) -> str:
    canonical = json.dumps(
        describe_filters(filters),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def numeric_filter_membership_violations(
    rows: list[dict[str, Any]],
    filters: SearchFilters,
) -> int:
    """
    Verify that returned rows satisfy every inherited numeric constraint.

    NULL is not zero. A NULL value returned by a bounded numeric query
    is a membership violation, not a numeric zero.
    """
    violations = 0

    for row in rows:
        for dimension, api_field in DIMENSIONS:
            lower = getattr(filters, f"{dimension}_min")
            upper = getattr(filters, f"{dimension}_max")

            if lower is None and upper is None:
                continue

            try:
                value = parse_decimal(row.get(api_field))
            except ValueError:
                violations += 1
                break

            if value is None:
                violations += 1
                break

            if lower is not None and value < lower:
                violations += 1
                break

            if upper is not None and value > upper:
                violations += 1
                break

    return violations


def get_numeric_values(
    rows: list[dict[str, Any]],
    api_field: str,
) -> tuple[list[Decimal], int, int]:
    values: list[Decimal] = []
    null_count = 0
    invalid_count = 0

    for row in rows:
        try:
            value = parse_decimal(row.get(api_field))
        except ValueError:
            invalid_count += 1
            continue

        if value is None:
            null_count += 1
        else:
            values.append(value)

    return values, null_count, invalid_count


# ======================================================================
# 05. SELECT A REAL FAILED BUCKET USING READ-ONLY SQL
# ======================================================================


def _bucket_eligibility_sql(
    *,
    has_lease_columns: bool,
) -> str:
    lease_condition = ""

    if has_lease_columns:
        lease_condition = """
          AND (
              b.lease_owner IS NULL
              OR b.lease_expires_at < UTC_TIMESTAMP(6)
          )
        """

    return f"""
        b.status = 'failed'
        AND b.total_items > %s
        AND COALESCE(b.pages_done, 0) = 0
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
        {lease_condition}
    """


def select_failed_bucket(
    bucket_id: int | None,
) -> dict[str, Any]:
    """
    Select the largest eligible oversized failed leaf by default.

    The query excludes buckets with existing page checkpoints or children.
    Selection follows the failed-leaf recovery query, including HTTP failures.
    No database records are modified.
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME AS column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'discovery_buckets'
            """
        )

        column_rows = cur.fetchall()
        columns = set()
        for row in column_rows:
            normalized = {str(key).lower(): value for key, value in row.items()}
            if "column_name" not in normalized:
                raise ProbeError("Schema query returned no COLUMN_NAME field")
            columns.add(str(normalized["column_name"]).lower())

        has_lease_columns = {
            "lease_owner",
            "lease_expires_at",
        }.issubset(columns)

        if not has_lease_columns:
            raise ProbeError("Cannot verify lease eligibility: required lease columns are missing")

        eligibility = _bucket_eligibility_sql(
            has_lease_columns=has_lease_columns,
        )

        if bucket_id is not None:
            query = f"""
                SELECT b.*
                FROM discovery_buckets AS b
                WHERE {eligibility}
                  AND b.id = %s
                LIMIT 1
            """

            cur.execute(
                query,
                (PRODUCTION_TARGET_ROWS, int(bucket_id)),
            )
        else:
            query = f"""
                SELECT b.*
                FROM discovery_buckets AS b
                WHERE {eligibility}
                ORDER BY
                    b.total_items DESC,
                    b.depth DESC,
                    b.id ASC
                LIMIT 1
            """

            cur.execute(
                query,
                (PRODUCTION_TARGET_ROWS,),
            )

        bucket = cur.fetchone()

    if bucket is None:
        if bucket_id is None:
            raise ProbeError(
                "No eligible failed oversized leaf bucket was found. "
                "The bucket must exceed 10000 rows, have no page "
                "checkpoint, have no child bucket, and have no active lease."
            )

        raise ProbeError(
            f"Bucket {bucket_id} is not eligible. Verify its status, "
            "count, existing children, page checkpoints, and lease."
        )

    if bucket.get("status") != "failed":
        raise ProbeError("Selected bucket is not in failed status")

    stored_count = bucket.get("total_items")

    if stored_count is None or int(stored_count) <= PRODUCTION_TARGET_ROWS:
        raise ProbeError(
            "Selected bucket does not exceed the 10000-row limit"
        )

    return bucket


def _numeric_bounds_from_bucket_key(
    bucket_key: str,
) -> dict[str, Decimal | None]:
    """
    Recover numeric constraints from legacy bucket keys when needed.

    Example:
        cap=capAmtMin:1000000,capAmtMax:1000000
    """
    mapping = {
        "cap": ("capAmtMin", "capAmtMax"),
        "income": ("totalIncomeMin", "totalIncomeMax"),
        "profit": ("netProfitMin", "netProfitMax"),
        "asset": ("totalAssetMin", "totalAssetMax"),
    }

    result: dict[str, Decimal | None] = {}

    for part in bucket_key.split("|"):
        if "=" not in part:
            continue

        label, content = part.split("=", 1)

        if label not in mapping:
            continue

        for item in content.split(","):
            if ":" not in item:
                raise ProbeError("Malformed numeric constraint in bucket key")

            name, raw_value = item.split(":", 1)

            if name not in mapping[label]:
                raise ProbeError(
                    "Unexpected numeric constraint in bucket key"
                )

            if not raw_value:
                result[name] = None
                continue

            try:
                value = parse_decimal(raw_value)
            except ValueError as exc:
                raise ProbeError(
                    f"Invalid numeric constraint in bucket key: {name}"
                ) from exc

            result[name] = value

    return result


def filters_from_bucket(
    bucket: dict[str, Any],
) -> SearchFilters:
    """
    Load existing filters without silently dropping numeric bounds.

    When persisted numeric_filters and bucket_key both contain a bound,
    they must agree.
    """
    filters = _filters_from_row(bucket)

    bucket_key = str(bucket.get("bucket_key") or "")
    key_bounds = _numeric_bounds_from_bucket_key(bucket_key)

    attribute_mapping = {
        "capAmtMin": "cap_amt_min",
        "capAmtMax": "cap_amt_max",
        "totalIncomeMin": "total_income_min",
        "totalIncomeMax": "total_income_max",
        "netProfitMin": "net_profit_min",
        "netProfitMax": "net_profit_max",
        "totalAssetMin": "total_asset_min",
        "totalAssetMax": "total_asset_max",
    }

    changes: dict[str, Decimal | None] = {}

    for key_name, attribute in attribute_mapping.items():
        stored_value = getattr(filters, attribute)
        key_value = key_bounds.get(key_name)

        if key_name in key_bounds:
            if stored_value is not None and key_value is None:
                raise ProbeError(
                    f"Numeric filter conflict for {key_name}: "
                    "persisted filter has a value but bucket key is empty"
                )

            if (
                stored_value is not None
                and key_value is not None
                and stored_value != key_value
            ):
                raise ProbeError(
                    f"Numeric filter conflict between bucket_key and "
                    f"numeric_filters: {key_name}"
                )

        changes[attribute] = (
            stored_value
            if stored_value is not None
            else key_value
        )

    return replace(filters, **changes)


# ======================================================================
# 06. QUERY SNAPSHOTS AND LEAF EVIDENCE
# ======================================================================


@dataclass
class QuerySnapshot:
    filters: SearchFilters
    total_items: int
    total_pages: int
    first_page: SearchPage
    first_page_identities: set[tuple[str, str]]
    first_page_signature: str


@dataclass
class LeafEvidence:
    path: str
    filters: SearchFilters
    total_items: int
    total_pages: int
    identities: set[tuple[str, str]]
    identity_sha256: str
    numeric_values_sha256: str
    first_page_signature: str

    def report(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "count": self.total_items,
            "pages": self.total_pages,
            "rows_verified": self.total_items,
            "unique_identity_count": len(self.identities),
            "identity_sha256": self.identity_sha256,
            "numeric_values_sha256": self.numeric_values_sha256,
            "first_page_signature": self.first_page_signature,
            "filters": describe_filters(self.filters),
        }


@dataclass
class PartitionResult:
    ok: bool
    tree: dict[str, Any] | None = None
    identities: set[tuple[str, str]] = field(default_factory=set)
    leaves: list[LeafEvidence] = field(default_factory=list)
    reason: str | None = None
    parents: list[QuerySnapshot] = field(default_factory=list)


# ======================================================================
# 07. LIVE SEARCH CLIENT
# ======================================================================


class LiveSearch:
    def __init__(
        self,
        client: FixedRateDbdClient,
        *,
        max_search_requests: int,
        max_retries: int,
        timeout_seconds: float,
        sample_pages: int,
    ):
        self.client = client
        self.max_search_requests = int(max_search_requests)
        self.max_retries = int(max_retries)
        self.timeout_seconds = float(timeout_seconds)
        self.sample_pages = int(sample_pages)

        self.manual_retries = 0
        self.sample_cache: dict[
            str,
            tuple[int, int, str, list[dict[str, Any]]],
        ] = {}

    async def _post_page(
        self,
        filters: SearchFilters,
        page_number: int,
    ) -> SearchPage:
        last_error: BaseException | None = None

        for attempt in range(self.max_retries + 1):
            if self.client.requests_made >= self.max_search_requests:
                raise ProbeBudgetExceeded(
                    "Maximum live search request budget exhausted"
                )

            try:
                data = await self.client.post(
                    SEARCH_PATH,
                    filters.payload(page=page_number),
                    referer=SEARCH_REFERER,
                    timeout=self.timeout_seconds,
                    rate_key="search",
                )

                page = parse_search_response(data)
                self._validate_page(page, page_number)
                return page

            except DeepTransientHttpError as exc:
                last_error = exc

                if exc.status == 429 or attempt >= self.max_retries:
                    raise

                self.manual_retries += 1

                await asyncio.sleep(
                    min(30.0, float(2 ** min(attempt, 5)))
                )

        raise ProbeError(
            f"Search retry loop exhausted: {last_error}"
        )

    @staticmethod
    def _validate_page(
        page: SearchPage,
        requested_page: int,
    ) -> None:
        if page.current_page != requested_page:
            raise UnstablePagination(
                f"Requested page {requested_page}; "
                f"received page {page.current_page}"
            )

        if page.items_per_page != ITEMS_PER_PAGE:
            raise UnstablePagination(
                f"Unexpected itemsPerPage={page.items_per_page}; "
                f"expected {ITEMS_PER_PAGE}"
            )

        if page.total_items < 0 or page.total_pages < 0:
            raise UnstablePagination("Negative pagination metadata")

        if page.total_items == 0:
            if page.total_pages not in (0, 1):
                raise UnstablePagination(
                    "Empty query returned inconsistent totalPages"
                )

            expected_rows = 0

        else:
            expected_pages = math.ceil(
                page.total_items / ITEMS_PER_PAGE
            )

            if page.total_pages != expected_pages:
                raise UnstablePagination(
                    f"totalPages mismatch: totalItems={page.total_items}, "
                    f"reported={page.total_pages}, expected={expected_pages}"
                )

            if page.total_pages < 1:
                raise UnstablePagination(
                    "Nonempty query returned no pages"
                )

            if requested_page > page.total_pages:
                raise UnstablePagination(
                    "Requested page exceeds reported totalPages"
                )

            expected_rows = min(
                ITEMS_PER_PAGE,
                page.total_items
                - ((requested_page - 1) * ITEMS_PER_PAGE),
            )

        if len(page.rows) != expected_rows:
            raise UnstablePagination(
                f"Page {requested_page} returned {len(page.rows)} rows; "
                f"expected {expected_rows}"
            )

        if len(page.rows) > ITEMS_PER_PAGE:
            raise UnstablePagination(
                "Page exceeds the API's supported page size"
            )

    async def snapshot(
        self,
        filters: SearchFilters,
    ) -> QuerySnapshot:
        page = await self._post_page(filters, 1)
        identities = identity_set(page.rows)

        return QuerySnapshot(
            filters=filters,
            total_items=page.total_items,
            total_pages=page.total_pages,
            first_page=page,
            first_page_identities=identities,
            first_page_signature=page_signature(page.rows),
        )

    async def sample_rows(
        self,
        snapshot: QuerySnapshot,
    ) -> list[dict[str, Any]]:
        """
        Read a small number of initial pages solely to propose boundaries.

        Sampling is never used as proof of coverage. Every final leaf is
        enumerated completely before the partition can pass.
        """
        cache_key = filter_fingerprint(snapshot.filters)
        cached = self.sample_cache.get(cache_key)

        if cached is not None:
            cached_count, cached_pages, cached_signature, cached_rows = cached

            if (
                cached_count != snapshot.total_items
                or cached_pages != snapshot.total_pages
                or cached_signature != snapshot.first_page_signature
            ):
                raise UnstablePagination(
                    "A repeated query changed while reusing its sample"
                )

            return list(cached_rows)

        rows = list(snapshot.first_page.rows)
        seen = set(snapshot.first_page_identities)

        page_limit = min(
            self.sample_pages,
            snapshot.total_pages,
            MAX_PAGE,
        )

        for page_number in range(2, page_limit + 1):
            page = await self._post_page(
                snapshot.filters,
                page_number,
            )

            if (
                page.total_items != snapshot.total_items
                or page.total_pages != snapshot.total_pages
            ):
                raise UnstablePagination(
                    "Count or pagination changed while sampling an "
                    "oversized query"
                )

            current_identities = identity_set(page.rows)
            overlap = seen & current_identities

            if overlap:
                raise DuplicateIdentityError(
                    "Company identities overlap across sampled pages"
                )

            seen.update(current_identities)
            rows.extend(page.rows)

        if numeric_filter_membership_violations(
            rows,
            snapshot.filters,
        ):
            raise FilterMembershipError(
                "Sampled rows violate inherited numeric filters"
            )

        self.sample_cache[cache_key] = (
            snapshot.total_items,
            snapshot.total_pages,
            snapshot.first_page_signature,
            list(rows),
        )

        return rows

    async def enumerate_leaf(
        self,
        snapshot: QuerySnapshot,
        *,
        path: str,
    ) -> LeafEvidence:
        """
        Enumerate every page of one final leaf.

        A leaf must fit within the API's actual 10000-row/1000-page limit.
        """
        if snapshot.total_items > MAX_ROWS_PER_QUERY:
            raise ProbeError(
                "Cannot enumerate an oversized query as a final leaf"
            )

        if snapshot.total_items > PRODUCTION_TARGET_ROWS:
            raise ProbeError(
                "Final leaf exceeds the production 10000-row ceiling"
            )

        if snapshot.total_pages > MAX_PAGE:
            raise ProbeError(
                "Final leaf exceeds the API's maximum page number"
            )

        identities: set[tuple[str, str]] = set()
        observations: list[tuple[tuple[str, str], list[str | None]]] = []
        first_page_signature = snapshot.first_page_signature

        for page_number in range(1, snapshot.total_pages + 1):
            if page_number == 1:
                page = snapshot.first_page
            else:
                page = await self._post_page(
                    snapshot.filters,
                    page_number,
                )

            if (
                page.total_items != snapshot.total_items
                or page.total_pages != snapshot.total_pages
            ):
                raise UnstablePagination(
                    f"Leaf {path}: count or totalPages changed "
                    f"while fetching page {page_number}"
                )

            current_identities = identity_set(page.rows)
            overlap = identities & current_identities

            if overlap:
                raise DuplicateIdentityError(
                    f"Leaf {path}: duplicate identities across pages"
                )

            violations = numeric_filter_membership_violations(
                page.rows,
                snapshot.filters,
            )

            if violations:
                raise FilterMembershipError(
                    f"Leaf {path}: {violations} rows violate inherited "
                    "numeric filters"
                )

            identities.update(current_identities)

            for row in page.rows:
                identity = identity_of(row)
                values: list[str | None] = []

                for _, api_field in DIMENSIONS:
                    try:
                        value = parse_decimal(row.get(api_field))
                    except ValueError as exc:
                        raise FilterMembershipError(f"Invalid numeric field: {api_field}") from exc
                    values.append(decimal_text(value))

                observations.append((identity, values))

        if len(identities) != snapshot.total_items:
            raise UnstablePagination(
                f"Leaf {path}: totalItems={snapshot.total_items}, "
                f"unique identities={len(identities)}"
            )

        if len(observations) != snapshot.total_items:
            raise UnstablePagination(
                f"Leaf {path}: enumerated row count differs from totalItems"
            )

        canonical_observations = json.dumps(
            sorted(observations),
            ensure_ascii=False,
            separators=(",", ":"),
        )

        return LeafEvidence(
            path=path,
            filters=snapshot.filters,
            total_items=snapshot.total_items,
            total_pages=snapshot.total_pages,
            identities=identities,
            identity_sha256=identity_digest(identities),
            numeric_values_sha256=hashlib.sha256(
                canonical_observations.encode("utf-8")
            ).hexdigest(),
            first_page_signature=first_page_signature,
        )


# ======================================================================
# 08. BOUNDARY GENERATION
# ======================================================================


def _unique_decimals(
    values: list[Decimal],
) -> list[Decimal]:
    result: list[Decimal] = []
    seen: set[Decimal] = set()

    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)

    return result


def candidate_pivots(
    filters: SearchFilters,
    dimension: str,
    rows: list[dict[str, Any]],
) -> list[Decimal]:
    """
    Propose boundaries from inherited ranges and observed values.

    No monetary quantum is assumed to be correct. Candidate boundaries
    are accepted only after live child counts and final identities validate.
    """
    lower = getattr(filters, f"{dimension}_min")
    upper = getattr(filters, f"{dimension}_max")

    if lower is not None and upper is not None and lower >= upper:
        return []

    _, api_field = next(
        pair for pair in DIMENSIONS if pair[0] == dimension
    )

    values: list[Decimal] = []

    for row in rows:
        try:
            value = parse_decimal(row.get(api_field))
        except ValueError:
            continue

        if value is None:
            continue

        if lower is not None and value < lower:
            continue

        if upper is not None and value > upper:
            continue

        values.append(value)

    distinct = sorted(set(values))
    pivots: list[Decimal] = []

    def add(value: Decimal) -> None:
        if not value.is_finite():
            return

        if lower is not None and value < lower:
            return

        if upper is not None and value >= upper:
            return

        if value not in pivots:
            pivots.append(value)

    if lower is not None and upper is not None:
        width = upper - lower

        add(lower + width / Decimal(2))
        add(lower + width / Decimal(4))
        add(lower + (width * Decimal(3) / Decimal(4)))

    if len(distinct) >= 2:
        middle = len(distinct) // 2

        pair_indexes = _unique_decimals(
            [
                Decimal(middle - 1),
                Decimal(max(0, middle - 2)),
                Decimal(min(len(distinct) - 2, middle)),
                Decimal(max(0, len(distinct) // 4)),
                Decimal(min(len(distinct) - 2, (3 * len(distinct)) // 4)),
            ]
        )

        for raw_index in pair_indexes:
            index = int(raw_index)

            if 0 <= index < len(distinct) - 1:
                add(
                    (distinct[index] + distinct[index + 1])
                    / Decimal(2)
                )

        add(distinct[middle])

        if len(distinct) >= 4:
            add(distinct[len(distinct) // 4])
            add(distinct[(3 * len(distinct)) // 4])

    elif len(distinct) == 1:
        add(distinct[0])

    if lower is not None:
        add(lower)

    return pivots


def candidate_epsilons(
    values: list[Decimal],
) -> list[Decimal]:
    """
    Produce trial offsets at multiple precisions.

    These are hypotheses, not certified numeric precision. Count
    conservation and identity validation determine whether a trial works.
    """
    positive = sorted(set(value for value in values if value.is_finite()))
    epsilons: list[Decimal] = []

    def add(value: Decimal) -> None:
        if value.is_finite() and value > 0 and value not in epsilons:
            epsilons.append(value)

    if len(positive) >= 2:
        gaps = [
            right - left
            for left, right in zip(positive, positive[1:])
            if right > left
        ]

        if gaps:
            add(min(gaps) / Decimal(1000))

    scale = 0

    for value in positive:
        scale = max(scale, max(0, -value.as_tuple().exponent))

    add(Decimal(1).scaleb(-(scale + 1)))

    for text in (
        "0.01",
        "0.1",
        "1",
        "0.001",
        "0.0001",
        "10",
        "0.00001",
        "0.000001",
        "100",
        "0.0000001",
        "0.00000001",
        "0.000000001",
        "0.0000000001",
        "0.00000000001",
        "0.000000000001",
    ):
        add(Decimal(text))

    return epsilons


def candidate_boundaries(
    filters: SearchFilters,
    dimension: str,
    rows: list[dict[str, Any]],
    *,
    limit: int,
) -> list[tuple[Decimal, Decimal]]:
    """
    Return distinct (left_max, right_min) boundary candidates.

    The right side starts strictly above the left boundary. Every candidate
    must subsequently prove count conservation and identity disjointness.
    """
    pivots = candidate_pivots(filters, dimension, rows)

    if not pivots:
        return []

    _, api_field = next(
        pair for pair in DIMENSIONS if pair[0] == dimension
    )

    sample_values: list[Decimal] = []

    for row in rows:
        try:
            value = parse_decimal(row.get(api_field))
        except ValueError:
            continue

        if value is not None:
            sample_values.append(value)

    lower = getattr(filters, f"{dimension}_min")
    upper = getattr(filters, f"{dimension}_max")

    epsilons = candidate_epsilons(sample_values)

    result: list[tuple[Decimal, Decimal]] = []
    seen: set[tuple[Decimal, Decimal]] = set()

    for epsilon in epsilons:
        for pivot in pivots:
            right_min = pivot + epsilon

            if lower is not None and pivot < lower:
                continue

            if upper is not None and pivot >= upper:
                continue

            if upper is not None and right_min > upper:
                continue

            if right_min <= pivot:
                continue

            pair = (pivot, right_min)

            if pair in seen:
                continue

            seen.add(pair)
            result.append(pair)

            if len(result) >= limit:
                return result

    return result


def make_child_filters(
    filters: SearchFilters,
    dimension: str,
    pivot: Decimal,
    right_min: Decimal,
) -> tuple[SearchFilters, SearchFilters]:
    """
    Construct two strict refinements without dropping inherited constraints.
    """
    if not pivot.is_finite() or not right_min.is_finite() or right_min <= pivot:
        raise ProbeError("Split boundaries must be finite and strictly disjoint")

    left = replace(
        filters,
        **{f"{dimension}_max": pivot},
    )

    right = replace(
        filters,
        **{f"{dimension}_min": right_min},
    )

    parent_lower = getattr(filters, f"{dimension}_min")
    parent_upper = getattr(filters, f"{dimension}_max")

    left_lower = getattr(left, f"{dimension}_min")
    left_upper = getattr(left, f"{dimension}_max")
    right_lower = getattr(right, f"{dimension}_min")
    right_upper = getattr(right, f"{dimension}_max")

    if parent_lower is not None and left_lower != parent_lower:
        raise ProbeError("Left child removed the inherited lower bound")

    if parent_upper is not None and right_upper != parent_upper:
        raise ProbeError("Right child removed the inherited upper bound")

    if left_upper != pivot or right_lower != right_min:
        raise ProbeError("Child numeric boundaries were not preserved")

    if left_lower is not None and left_upper < left_lower:
        raise ProbeError("Left child has an invalid numeric interval")

    if right_upper is not None and right_lower > right_upper:
        raise ProbeError("Right child has an invalid numeric interval")

    if (
        filter_fingerprint(left) == filter_fingerprint(filters)
        or filter_fingerprint(right) == filter_fingerprint(filters)
        or filter_fingerprint(left) == filter_fingerprint(right)
    ):
        raise ProbeError("Candidate does not produce two distinct children")

    return left, right


# ======================================================================
# 09. SEQUENTIAL RECURSIVE PARTITION ENGINE
# ======================================================================


class SequentialPartitionProbe:
    def __init__(
        self,
        search: LiveSearch,
        *,
        max_boundary_attempts: int,
        max_depth: int,
        max_nodes: int,
    ):
        self.search = search
        self.max_boundary_attempts = int(max_boundary_attempts)
        self.max_depth = int(max_depth)
        self.max_nodes = int(max_nodes)

        self.nodes_visited = 0
        self.attempt_log: list[dict[str, Any]] = []
        self.unresolved_groups: list[dict[str, Any]] = []
        self.validated_leaves: list[LeafEvidence] = []

        self.dimension_stats: dict[str, dict[str, Any]] = {
            dimension: {
                "api_field": api_field,
                "groups_considered": 0,
                "boundary_attempts": 0,
                "count_conservation_passes": 0,
                "validated_splits": 0,
                "api_filter_rejections": 0,
                "count_mismatches": 0,
                "no_progress": 0,
                "first_page_overlaps": 0,
                "subtree_unresolved": 0,
                "membership_failures": 0,
                "no_candidate_groups": 0,
            }
            for dimension, api_field in DIMENSIONS
        }

    def _record_attempt(
        self,
        record: dict[str, Any],
    ) -> None:
        if len(self.attempt_log) < MAX_ATTEMPT_LOG_ENTRIES:
            self.attempt_log.append(record)

    def _dimension_statuses(self) -> dict[str, str]:
        statuses: dict[str, str] = {}

        for dimension, stats in self.dimension_stats.items():
            if stats["validated_splits"]:
                statuses[dimension] = "USED_AND_VALIDATED"

            elif stats["groups_considered"] == 0:
                statuses[dimension] = "NOT_NEEDED"

            elif stats["boundary_attempts"] == 0:
                statuses[dimension] = "NO_CANDIDATE_BOUNDARY"

            else:
                statuses[dimension] = "NO_VALID_SPLIT_FOUND"

        return statuses

    def _unresolved(self, filters, snapshot, path, reason, dimension=None):
        group = {
            "path": path, "count": snapshot.total_items, "reason": reason,
            "dimension": dimension, "filters": describe_filters(filters),
        }
        self.unresolved_groups.append(group)
        return PartitionResult(ok=False, tree={"type": "unresolved", **group}, reason=reason)

    async def _checked_snapshot(self, filters):
        snapshot = await self.search.snapshot(filters)
        if numeric_filter_membership_violations(snapshot.first_page.rows, filters):
            raise FilterMembershipError("Probe rows violate their numeric filters")
        return snapshot

    async def _assert_stable(self, original):
        current = await self._checked_snapshot(original.filters)
        if (
            current.total_items != original.total_items
            or current.total_pages != original.total_pages
            or current.first_page_signature != original.first_page_signature
        ):
            raise UnstablePagination("Parent changed while proving a numeric boundary")

    async def walk(
        self, *, filters: SearchFilters, snapshot: QuerySnapshot,
        path: str, start_dimension_index: int, depth: int,
    ) -> PartitionResult:
        self.nodes_visited += 1
        if self.nodes_visited > self.max_nodes:
            raise ProbeSafetyLimitExceeded("Maximum partition node count exceeded")
        if depth > self.max_depth:
            return self._unresolved(filters, snapshot, path, "MAXIMUM_PARTITION_DEPTH_REACHED")
        if numeric_filter_membership_violations(snapshot.first_page.rows, filters):
            raise FilterMembershipError("Parent violates inherited numeric filters")
        if snapshot.total_items <= PRODUCTION_TARGET_ROWS:
            leaf = await self.search.enumerate_leaf(snapshot, path=path)
            self.validated_leaves.append(leaf)
            return PartitionResult(
                ok=True, tree={"type": "leaf", **leaf.report()},
                identities=set(leaf.identities), leaves=[leaf],
            )
        if start_dimension_index >= len(DIMENSIONS):
            return self._unresolved(filters, snapshot, path, "ALL_DIMENSIONS_CONSTANT")

        dimension, api_field = DIMENSIONS[start_dimension_index]
        stats = self.dimension_stats[dimension]
        stats["groups_considered"] += 1
        lower = getattr(filters, f"{dimension}_min")
        upper = getattr(filters, f"{dimension}_max")
        sample = await self.search.sample_rows(snapshot)
        values, null_count, invalid_count = get_numeric_values(sample, api_field)
        if invalid_count:
            return self._unresolved(filters, snapshot, path, "INVALID_NUMERIC_VALUE", dimension)
        if null_count:
            # SearchFilters has numeric min/max only; None means no constraint,
            # NOT IS NULL. Never invent an unsupported null-filter API field.
            return self._unresolved(
                filters, snapshot, path, "NULL_COHORT_NOT_ISOLATABLE_BY_API", dimension,
            )

        constant_filters = None
        constant_snapshot = None
        if lower is not None and lower == upper:
            constant_filters, constant_snapshot = filters, snapshot
        elif len(set(values)) == 1:
            # A uniform sample is only a proposal. Equality must cover the
            # full parent's count, and the parent must remain stable.
            value = values[0]
            exact = replace(filters, **{
                f"{dimension}_min": value, f"{dimension}_max": value,
            })
            try:
                exact_snapshot = await self._checked_snapshot(exact)
            except DeepPermanentHttpError as exc:
                if exc.status != 400:
                    raise
                stats["api_filter_rejections"] += 1
            else:
                await self._assert_stable(snapshot)
                if exact_snapshot.total_items == snapshot.total_items:
                    constant_filters, constant_snapshot = exact, exact_snapshot
                    self._record_attempt({
                        "path": path, "dimension": dimension,
                        "status": "CONSTANT_GROUP_COUNT_VERIFIED",
                        "value": decimal_text(value), "count": snapshot.total_items,
                    })

        if constant_filters is not None:
            self._record_attempt({
                "path": path, "dimension": dimension, "status": "ADVANCE_CONSTANT_GROUP",
                "next_dimension": (
                    DIMENSIONS[start_dimension_index + 1][0]
                    if start_dimension_index + 1 < len(DIMENSIONS) else None
                ),
                "filters": describe_filters(constant_filters),
            })
            result = await self.walk(
                filters=constant_filters, snapshot=constant_snapshot,
                path=path + "=", start_dimension_index=start_dimension_index + 1,
                depth=depth + 1,
            )
            if result.ok and not identity_set(sample) <= result.identities:
                raise UnstablePagination("Constant-group refinement lost observed parent identities")
            return PartitionResult(
                ok=result.ok, reason=result.reason, identities=result.identities,
                leaves=result.leaves, parents=[snapshot] + result.parents,
                tree={
                    "type": "constant_dimension", "path": path, "dimension": dimension,
                    "count": snapshot.total_items, "filters": describe_filters(filters),
                    "children": [result.tree],
                },
            )

        boundaries = candidate_boundaries(
            filters, dimension, sample, limit=self.max_boundary_attempts,
        )
        if not boundaries:
            stats["no_candidate_groups"] += 1
        for pivot, right_min in boundaries:
            stats["boundary_attempts"] += 1
            attempt = {
                "path": path, "dimension": dimension,
                "pivot_left_max": decimal_text(pivot), "right_min": decimal_text(right_min),
                "parent_count": snapshot.total_items,
            }
            try:
                left_filters, right_filters = make_child_filters(filters, dimension, pivot, right_min)
                left = await self._checked_snapshot(left_filters)
                right = await self._checked_snapshot(right_filters)
            except DeepPermanentHttpError as exc:
                if exc.status != 400:
                    raise
                stats["api_filter_rejections"] += 1
                self._record_attempt({**attempt, "status": "API_FILTER_REJECTED"})
                continue
            left_count, right_count = left.total_items, right.total_items
            attempt.update(left_count=left_count, right_count=right_count)
            if left_count + right_count != snapshot.total_items:
                stats["count_mismatches"] += 1
                self._record_attempt({
                    **attempt, "status": "COUNT_MISMATCH_NULL_GAP_OR_DRIFT_UNRESOLVED",
                })
                continue
            if not 0 < left_count < snapshot.total_items or not 0 < right_count < snapshot.total_items:
                stats["no_progress"] += 1
                self._record_attempt({**attempt, "status": "NO_PROGRESS"})
                continue

            # Both bounds are inclusive. Prove that the open gap is empty
            # instead of assuming cents (or any other monetary quantum).
            def interval(lo, hi):
                return replace(filters, **{f"{dimension}_min": lo, f"{dimension}_max": hi})

            try:
                bridge = await self._checked_snapshot(interval(pivot, right_min))
                left_endpoint = await self._checked_snapshot(interval(pivot, pivot))
                right_endpoint = await self._checked_snapshot(interval(right_min, right_min))
            except DeepPermanentHttpError as exc:
                if exc.status != 400:
                    raise
                stats["api_filter_rejections"] += 1
                self._record_attempt({**attempt, "status": "GAP_PROOF_API_REJECTED"})
                continue
            if bridge.total_items != left_endpoint.total_items + right_endpoint.total_items:
                self._record_attempt({**attempt, "status": "OPEN_GAP_NOT_PROVEN_EMPTY"})
                continue
            await self._assert_stable(snapshot)
            if left.first_page_identities & right.first_page_identities:
                raise DuplicateIdentityError("Child samples overlap")
            stats["count_conservation_passes"] += 1
            self._record_attempt({**attempt, "status": "BOUNDARY_COUNT_AND_GAP_VALIDATED"})

            # Keep this split and only advance an exhausted constant child.
            # Do not abandon an otherwise valid capital split because a
            # descendant is unresolved, and then repartition its entire parent
            # by income/profit/assets.
            left_result = await self.walk(
                filters=left_filters, snapshot=left, path=path + "L",
                start_dimension_index=start_dimension_index, depth=depth + 1,
            )
            right_result = await self.walk(
                filters=right_filters, snapshot=right, path=path + "R",
                start_dimension_index=start_dimension_index, depth=depth + 1,
            )
            if left_result.identities & right_result.identities:
                raise DuplicateIdentityError("Final leaves overlap across sibling subtrees")
            identities = left_result.identities | right_result.identities
            complete = left_result.ok and right_result.ok
            if complete and (
                len(identities) != snapshot.total_items
                or not identity_set(sample) <= identities
            ):
                raise UnstablePagination("Final leaf union lost parent count or observed identities")
            if complete:
                stats["validated_splits"] += 1
            return PartitionResult(
                ok=complete, identities=identities,
                leaves=left_result.leaves + right_result.leaves,
                parents=[snapshot] + left_result.parents + right_result.parents,
                reason=None if complete else "CHILD_GROUP_UNRESOLVED",
                tree={
                    "type": "split", "path": path, "dimension": dimension,
                    "filters": describe_filters(filters), "count": snapshot.total_items,
                    "pivot_left_max_inclusive": decimal_text(pivot),
                    "right_min_inclusive": decimal_text(right_min),
                    "open_gap_count_verified_zero": True,
                    "left_count": left_count, "right_count": right_count,
                    "count_conservation_passed": True,
                    "subtrees_validated": complete,
                    "children": [left_result.tree, right_result.tree],
                },
            )

        return self._unresolved(
            filters, snapshot, path, "BOUNDARY_SEARCH_EXHAUSTED_NOT_PROVEN_CONSTANT", dimension,
        )


# ======================================================================
# 10. STABILITY RECHECK
# ======================================================================


async def recheck_stability(
    search: LiveSearch,
    root_snapshot: QuerySnapshot,
    result: PartitionResult,
) -> dict[str, Any]:
    """Re-enumerate every leaf; counts/first pages alone cannot detect churn."""
    checks = []
    parents = {
        filter_fingerprint(item.filters): item
        for item in [root_snapshot, *result.parents]
    }

    async def check_parent(original, phase):
        current = await search.snapshot(original.filters)
        stable = (
            current.total_items == original.total_items
            and current.total_pages == original.total_pages
            and current.first_page_signature == original.first_page_signature
        )
        checks.append({
            "kind": "parent", "phase": phase,
            "filter_sha256": filter_fingerprint(original.filters),
            "status": "PASS" if stable else "FAIL",
            "original_count": original.total_items, "current_count": current.total_items,
        })

    for original in parents.values():
        await check_parent(original, "before_full_rescan")
    union = set()
    for leaf in result.leaves:
        snapshot = await search.snapshot(leaf.filters)
        current = await search.enumerate_leaf(snapshot, path=leaf.path)
        if union & current.identities:
            raise DuplicateIdentityError("Leaf identities overlap during stability rescan")
        union.update(current.identities)
        stable = (
            current.total_items == leaf.total_items
            and current.total_pages == leaf.total_pages
            and current.identities == leaf.identities
            and current.numeric_values_sha256 == leaf.numeric_values_sha256
        )
        checks.append({
            "kind": "leaf", "path": leaf.path, "status": "PASS" if stable else "FAIL",
            "all_pages_rechecked": True, "original_count": leaf.total_items,
            "current_count": current.total_items,
            "identity_set_unchanged": current.identities == leaf.identities,
            "numeric_values_unchanged": current.numeric_values_sha256 == leaf.numeric_values_sha256,
        })
    for original in parents.values():
        await check_parent(original, "after_full_rescan")
    union_stable = union == result.identities and len(union) == root_snapshot.total_items
    return {
        "status": "PASS" if union_stable and all(c["status"] == "PASS" for c in checks) else "FAIL",
        "checks": checks, "checked_root": True, "checked_every_leaf_all_pages": True,
        "leaf_union_unchanged": union_stable, "atomic_snapshot_guaranteed": False,
    }


# ======================================================================
# 11. REPORT
# ======================================================================


def make_initial_report(
    args: argparse.Namespace,
) -> dict[str, Any]:
    return {
        "program": "probe_live_numeric_filters",
        "schema_version": 6,
        "mode": "READ_ONLY_LIVE_API_SEQUENTIAL_RECURSIVE_PARTITION",
        "started_at_utc": iso_now(),
        "environment": {
            "repository_root": str(ROOT),
            "database": settings.db_name,
            "api_host": "datawarehouse.dbd.go.th",
            "items_per_page": ITEMS_PER_PAGE,
            "max_page": MAX_PAGE,
            "max_rows_per_query": MAX_ROWS_PER_QUERY,
        },
        "limits": {
            "bucket_id_requested": args.bucket_id,
            "max_search_requests": args.max_search_requests,
            "rate_per_second": args.rate_per_second,
            "max_retries": args.max_retries,
            "timeout_seconds": args.timeout_seconds,
            "sample_pages_for_boundary_proposals": args.sample_pages,
            "max_boundary_attempts_per_dimension_and_group": (
                args.max_boundary_attempts
            ),
            "max_depth": args.max_depth,
            "max_nodes": args.max_nodes,
            "production_target_rows": PRODUCTION_TARGET_ROWS,
        },
        "dimension_order": [
            dimension for dimension, _ in DIMENSIONS
        ],
        "safety": {
            "mysql_read_only": True,
            "live_api_search_only": True,
            "production_table_writes": False,
            "bucket_mutations": False,
            "checkpoint_mutations": False,
            "lease_mutations": False,
            "schema_changes": False,
            "redis_mutations": False,
            "local_report_file_writes": False,
            "raw_company_identities_in_report": False,
            "global_coverage_certified": False,
            "production_numeric_partition_enabled": False,
            "null_filter_supported": False,
            "null_policy": "Preserve NULL; fail closed if its cohort cannot be isolated",
            "atomic_upstream_snapshot_available": False,
        },
        "cohort": {},
        "partition_tree": None,
        "partition_attempts": [],
        "unresolved_groups": [],
        "dimension_stats": {},
        "dimension_statuses": {},
        "candidate_leaf_buckets": [],
        "stability_test": {
            "status": "NOT_RUN",
        },
        "summary": {
            "probe_passed": False,
            "partition_complete": False,
            "all_leaf_buckets_validated": False,
            "source_coverage_status": "NOT_PROVEN",
            "global_coverage_certified": False,
            "production_numeric_partition_enabled": False,
        },
        "execution": {
            "client_search_requests": 0,
            "manual_retries": 0,
            "client_retries": 0,
            "rate_limit_hits": 0,
            "nodes_visited": 0,
            "elapsed_seconds": None,
            "stop_reason": None,
        },
    }


def summarize_dimension_statuses(
    stats: dict[str, dict[str, Any]],
) -> dict[str, str]:
    statuses: dict[str, str] = {}

    for dimension, item in stats.items():
        if item["validated_splits"]:
            statuses[dimension] = "USED_AND_VALIDATED"

        elif item["groups_considered"] == 0:
            statuses[dimension] = "NOT_NEEDED"

        elif item["boundary_attempts"] == 0:
            statuses[dimension] = "NO_CANDIDATE_BOUNDARY"

        else:
            statuses[dimension] = "NO_VALID_SPLIT_FOUND"

    return statuses


# ======================================================================
# 12. MAIN PROBE
# ======================================================================


async def run_probe(
    args: argparse.Namespace,
) -> dict[str, Any]:
    started = time.monotonic()
    report = make_initial_report(args)

    client = FixedRateDbdClient(
        concurrency=1,
        rate_per_second=args.rate_per_second,
        max_retries=0,
        timeout=args.timeout_seconds,
    )

    search: LiveSearch | None = None
    engine: SequentialPartitionProbe | None = None

    try:
        bucket = await asyncio.to_thread(
            select_failed_bucket,
            args.bucket_id,
        )

        filters = filters_from_bucket(bucket)
        stored_count = int(bucket["total_items"])

        report["cohort"] = {
            "bucket_id": int(bucket["id"]),
            "parent_bucket_id": bucket.get("parent_id"),
            "bucket_status": bucket.get("status"),
            "stored_count": stored_count,
            "keyword": bucket.get("keyword"),
            "depth": bucket.get("depth"),
            "last_error": bucket.get("last_error"),
            "filters": describe_filters(filters),
            "stored_numeric_filters": filters.numeric_range() or {},
            "stored_count_matches_live": None,
            "root_identity_direct_comparison_possible": False,
        }

        async with client:
            search = LiveSearch(
                client,
                max_search_requests=args.max_search_requests,
                max_retries=args.max_retries,
                timeout_seconds=args.timeout_seconds,
                sample_pages=args.sample_pages,
            )

            root_snapshot = await search.snapshot(filters)

            count_matches = (
                root_snapshot.total_items == stored_count
            )

            report["cohort"].update(
                {
                    "live_count": root_snapshot.total_items,
                    "live_pages": root_snapshot.total_pages,
                    "first_page_identity_count": len(
                        root_snapshot.first_page_identities
                    ),
                    "first_page_identity_sha256": identity_digest(
                        root_snapshot.first_page_identities
                    ),
                    "first_page_signature": (
                        root_snapshot.first_page_signature
                    ),
                    "stored_count_matches_live": count_matches,
                }
            )

            if not count_matches:
                raise ProbeError(
                    "Stored failed-bucket count differs from the live API. "
                    "Refusing to partition a changing cohort."
                )

            if root_snapshot.total_items <= PRODUCTION_TARGET_ROWS:
                raise ProbeError(
                    "The selected bucket no longer exceeds 10000 rows"
                )

            minimum_requests = 2 * math.ceil(root_snapshot.total_items / ITEMS_PER_PAGE) + 2
            report["limits"]["minimum_requests_for_two_full_leaf_scans"] = minimum_requests
            report["limits"]["minimum_excludes_boundary_probes_and_retries"] = True
            if args.max_search_requests < minimum_requests:
                raise ProbeBudgetExceeded(
                    f"Budget {args.max_search_requests} cannot validate this cohort; "
                    f"at least {minimum_requests} requests are needed for two full "
                    "leaf scans and root checks, before boundary probes/retries. "
                    "Choose a smaller --bucket-id or explicitly increase --max-search-requests."
                )

            if numeric_filter_membership_violations(
                root_snapshot.first_page.rows,
                filters,
            ):
                raise FilterMembershipError(
                    "Root first page violates the bucket's inherited "
                    "numeric constraints"
                )

            engine = SequentialPartitionProbe(
                search,
                max_boundary_attempts=args.max_boundary_attempts,
                max_depth=args.max_depth,
                max_nodes=args.max_nodes,
            )

            result = await engine.walk(
                filters=filters,
                snapshot=root_snapshot,
                path="R",
                start_dimension_index=0,
                depth=0,
            )

            report["partition_tree"] = result.tree

            report["partition_attempts"] = engine.attempt_log
            report["unresolved_groups"] = engine.unresolved_groups
            report["dimension_stats"] = engine.dimension_stats
            report["dimension_statuses"] = (
                engine._dimension_statuses()
            )

            report["candidate_leaf_buckets"] = [
                leaf.report()
                for leaf in result.leaves
            ]

            partition_complete = bool(
                result.ok
                and len(result.identities) == root_snapshot.total_items
                and sum(
                    leaf.total_items for leaf in result.leaves
                ) == root_snapshot.total_items
                and all(
                    leaf.total_items <= PRODUCTION_TARGET_ROWS
                    and leaf.total_items == len(leaf.identities)
                    and leaf.total_pages <= MAX_PAGE
                    for leaf in result.leaves
                )
            )

            report["summary"].update(
                {
                    "partition_complete": partition_complete,
                    "all_leaf_buckets_validated": partition_complete,
                    "leaf_count": len(result.leaves),
                    "root_count": root_snapshot.total_items,
                    "sum_of_leaf_counts": sum(
                        leaf.total_items for leaf in result.leaves
                    ),
                    "unique_leaf_identity_count": len(
                        result.identities
                    ),
                    "all_leaf_identities_unique": (
                        len(result.identities)
                        == sum(
                            len(leaf.identities)
                            for leaf in result.leaves
                        )
                    ),
                    "all_leaves_within_production_limit": (
                        partition_complete
                    ),
                    "source_coverage_status": (
                        "PENDING_STABILITY_RECHECK"
                        if partition_complete
                        else "NOT_PROVEN"
                    ),
                    "global_coverage_certified": False,
                    "production_numeric_partition_enabled": False,
                }
            )

            if not result.ok:
                report["execution"]["stop_reason"] = (
                    result.reason or "NO_VALID_PARTITION_WITHIN_DIMENSION_ORDER"
                )

            elif not partition_complete:
                report["execution"]["stop_reason"] = (
                    "LEAF_COUNT_OR_IDENTITY_VALIDATION_FAILED"
                )

            else:
                try:
                    stability = await recheck_stability(
                        search,
                        root_snapshot,
                        result,
                    )

                    report["stability_test"] = stability

                    stability_passed = (
                        stability["status"] == "PASS"
                    )

                    probe_passed = (
                        partition_complete
                        and stability_passed
                        and len(result.identities)
                        == root_snapshot.total_items
                    )

                    report["summary"].update(
                        {
                            "probe_passed": probe_passed,
                            "partition_complete": partition_complete,
                            "all_leaf_buckets_validated": (
                                partition_complete
                            ),
                            "source_coverage_status": (
                                "COHORT_VALIDATED_BY_RECURSIVE_COUNTS_"
                                "DISJOINT_IDENTITIES_AND_FULL_RESCAN"
                                if probe_passed
                                else "NOT_PROVEN"
                            ),
                            "root_identity_direct_comparison_possible": (
                                False
                            ),
                            "root_identity_direct_comparison_reason": (
                                "The root exceeds the API's 10000-row "
                                "enumeration limit. Coverage is evaluated "
                                "through recursive count conservation, "
                                "inherited filter refinements, complete "
                                "leaf pagination, and disjoint leaf "
                                "identity sets."
                            ),
                            "global_coverage_certified": False,
                            "production_numeric_partition_enabled": False,
                        }
                    )

                    report["execution"]["stop_reason"] = (
                        "COMPLETED"
                        if probe_passed
                        else "STABILITY_OR_FINAL_VALIDATION_FAILED"
                    )

                except Exception as exc:
                    report["stability_test"] = {
                        "status": "INCOMPLETE",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }

                    if isinstance(exc, ProbeBudgetExceeded):
                        reason = "REQUEST_BUDGET_EXHAUSTED_DURING_STABILITY"
                    elif isinstance(exc, DeepTransientHttpError) and exc.status == 429:
                        reason = "HTTP_429_RATE_LIMITED"
                    else:
                        reason = "STABILITY_RECHECK_FAILED_OR_INCOMPLETE"
                    report["execution"]["stop_reason"] = reason

    except ProbeBudgetExceeded as exc:
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

        report["execution"]["stop_reason"] = (
            "REQUEST_BUDGET_EXHAUSTED"
        )

    except ProbeSafetyLimitExceeded as exc:
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

        report["execution"]["stop_reason"] = (
            "PARTITION_SAFETY_LIMIT_EXCEEDED"
        )

    except Exception as exc:
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "trace": [
                {"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name}
                for frame in traceback.extract_tb(exc.__traceback__)
            ],
        }

        if isinstance(exc, DeepTransientHttpError) and exc.status == 429:
            stop_reason = "HTTP_429_RATE_LIMITED"
        elif isinstance(exc, UnstablePagination):
            stop_reason = "UNSTABLE_PAGINATION"

        elif isinstance(exc, DuplicateIdentityError):
            stop_reason = "DUPLICATE_COMPANY_IDENTITY"

        elif isinstance(exc, FilterMembershipError):
            stop_reason = "NUMERIC_FILTER_MEMBERSHIP_FAILED"

        else:
            stop_reason = "API_DATABASE_OR_RUNTIME_ERROR"

        report["execution"]["stop_reason"] = stop_reason

    finally:
        report["finished_at_utc"] = iso_now()

        report["execution"].update(
            {
                "client_search_requests": client.requests_made,
                "manual_retries": (
                    search.manual_retries if search is not None else 0
                ),
                "client_retries": client.retries_made,
                "rate_limit_hits": client.rate_limit_hits,
                "nodes_visited": (
                    engine.nodes_visited if engine is not None else 0
                ),
                "elapsed_seconds": round(
                    time.monotonic() - started,
                    3,
                ),
            }
        )

        if engine is not None:
            report["partition_attempts"] = engine.attempt_log
            report["unresolved_groups"] = engine.unresolved_groups
            report["dimension_stats"] = engine.dimension_stats
            report["candidate_leaf_buckets"] = [leaf.report() for leaf in engine.validated_leaves]
            report["dimension_statuses"] = (
                engine._dimension_statuses()
            )

        report["summary"]["global_coverage_certified"] = False
        report["summary"]["production_numeric_partition_enabled"] = False

    return report


# ======================================================================
# 13. CLI
# ======================================================================


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only live probe of sequential recursive numeric "
            "partitioning on an existing failed DBD Discovery bucket."
        )
    )

    parser.add_argument(
        "--bucket-id",
        type=int,
        default=None,
        help=(
            "Explicit failed bucket ID. When omitted, select the largest "
            "eligible oversized failed leaf using read-only MySQL SQL."
        ),
    )

    parser.add_argument(
        "--max-search-requests",
        type=int,
        default=2500,
    )

    parser.add_argument(
        "--rate-per-second",
        type=float,
        default=0.5,
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
    )

    parser.add_argument(
        "--sample-pages",
        type=int,
        default=5,
        help=(
            "Initial pages sampled only to propose boundaries. Every "
            "final leaf is still enumerated completely."
        ),
    )

    parser.add_argument(
        "--max-boundary-attempts",
        type=int,
        default=12,
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=40,
    )

    parser.add_argument(
        "--max-nodes",
        type=int,
        default=2000,
    )

    parser.add_argument("--self-test", action="store_true", help="Run offline tests; no SQL or HTTP")
    args = parser.parse_args(argv)

    if args.bucket_id is not None and args.bucket_id < 1:
        parser.error("--bucket-id must be positive")

    if args.max_search_requests < 1:
        parser.error("--max-search-requests must be positive")

    if not math.isfinite(args.rate_per_second) or args.rate_per_second <= 0:
        parser.error("--rate-per-second must be positive")

    if args.max_retries < 0:
        parser.error("--max-retries cannot be negative")

    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")

    if args.sample_pages < 1:
        parser.error("--sample-pages must be positive")

    if args.max_boundary_attempts < 1:
        parser.error("--max-boundary-attempts must be positive")

    if args.max_depth < 1:
        parser.error("--max-depth must be positive")

    if args.max_nodes < 1:
        parser.error("--max-nodes must be positive")

    return args


def run_self_tests() -> int:
    """Offline regression tests kept here to avoid modifying any other file."""
    import io
    import unittest
    from unittest.mock import MagicMock, patch

    module = sys.modules[__name__]

    def row(number, cap="1", income="0", profit="0", asset="0"):
        return {
            "jpTypeCode": "5", "jpNo": str(number).zfill(13),
            "capAmt": cap, "totalIncome": income, "netProfit": profit, "totalAsset": asset,
        }

    class FakeClient:
        def __init__(self, rows):
            self.rows = rows
            self.requests_made = self.retries_made = self.rate_limit_hits = 0
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, path, payload, **kwargs):
            self.requests_made += 1
            self.calls.append(dict(payload))
            rows = list(self.rows)
            for _, api_field in DIMENSIONS:
                for suffix, compare in (
                    ("Min", lambda value, bound: value >= bound),
                    ("Max", lambda value, bound: value <= bound),
                ):
                    raw = payload.get(api_field + suffix)
                    if raw is not None:
                        bound = Decimal(str(raw))
                        rows = [
                            item for item in rows
                            if parse_decimal(item.get(api_field)) is not None
                            and compare(parse_decimal(item[api_field]), bound)
                        ]
            page_number = payload["currentPage"]
            begin = (page_number - 1) * ITEMS_PER_PAGE
            return {
                "meta": {
                    "totalItems": len(rows), "totalPages": math.ceil(len(rows) / ITEMS_PER_PAGE),
                    "currentPage": page_number, "itemsPerPage": ITEMS_PER_PAGE,
                },
                "contents": rows[begin:begin + ITEMS_PER_PAGE],
            }

    def search_for(rows, budget=5000):
        return LiveSearch(
            FakeClient(rows), max_search_requests=budget, max_retries=0,
            timeout_seconds=1, sample_pages=1,
        )

    def engine_for(search, attempts=12):
        return SequentialPartitionProbe(
            search, max_boundary_attempts=attempts, max_depth=40, max_nodes=2000,
        )

    async def partition(search, filters=None, engine=None):
        filters = filters or SearchFilters(keyword="010")
        engine = engine or engine_for(search)
        snapshot = await search.snapshot(filters)
        result = await engine.walk(
            filters=filters, snapshot=snapshot, path="R", start_dimension_index=0, depth=0,
        )
        return engine, snapshot, result

    class RegressionTests(unittest.IsolatedAsyncioTestCase):
        def setUp(self):
            self.target = patch.object(module, "PRODUCTION_TARGET_ROWS", 2)
            self.target.start()
            self.addCleanup(self.target.stop)
            # A self-test must fail if it accidentally reaches external services.
            self.database = patch.object(module, "connect", side_effect=AssertionError("Live SQL forbidden"))
            self.database.start()
            self.addCleanup(self.database.stop)
            self.network = patch.object(
                FixedRateDbdClient, "post", side_effect=AssertionError("Live HTTP forbidden"),
            )
            self.network.start()
            self.addCleanup(self.network.stop)

        def test_null_zero_negative_and_invalid_are_distinct(self):
            self.assertIsNone(parse_decimal(None))
            self.assertEqual(parse_decimal("0"), Decimal(0))
            self.assertEqual(parse_decimal("-1.00001"), Decimal("-1.00001"))
            for value in (True, "NaN", "Infinity", "not-a-number"):
                with self.assertRaises(ValueError):
                    parse_decimal(value)

        def test_disjoint_bounds_required(self):
            with self.assertRaises(ProbeError):
                make_child_filters(SearchFilters(keyword="010"), "cap_amt", Decimal(1), Decimal(1))

        def test_uppercase_schema_metadata_and_largest_selection(self):
            connection, cursor = MagicMock(), MagicMock()
            connection.__enter__.return_value = connection
            connection.cursor.return_value.__enter__.return_value = cursor
            cursor.fetchall.return_value = [
                {"COLUMN_NAME": "lease_owner"}, {"COLUMN_NAME": "lease_expires_at"},
            ]
            cursor.fetchone.return_value = {"id": 1, "status": "failed", "total_items": 12}
            with patch.object(module, "connect", return_value=connection):
                self.assertEqual(select_failed_bucket(None)["id"], 1)
            sql = cursor.execute.call_args_list[-1].args[0]
            self.assertIn("b.total_items DESC", sql)
            self.assertIn("NOT EXISTS", sql)
            self.assertIn("lease_expires_at", sql)

        def test_missing_lease_schema_fails_closed(self):
            connection, cursor = MagicMock(), MagicMock()
            connection.__enter__.return_value = connection
            connection.cursor.return_value.__enter__.return_value = cursor
            cursor.fetchall.return_value = [{"column_name": "id"}]
            with patch.object(module, "connect", return_value=connection):
                with self.assertRaisesRegex(ProbeError, "lease"):
                    select_failed_bucket(None)

        async def test_all_four_dimensions_and_full_rescan(self):
            search = search_for([row(i, asset=str(i)) for i in range(16)])
            engine, root, result = await partition(search)
            self.assertTrue(result.ok, result.reason)
            self.assertEqual(len(result.identities), 16)
            self.assertTrue(all(leaf.total_items <= 2 for leaf in result.leaves))
            advances = [a["dimension"] for a in engine.attempt_log if a["status"] == "ADVANCE_CONSTANT_GROUP"]
            self.assertEqual(advances[:3], ["cap_amt", "total_income", "net_profit"])
            stability = await recheck_stability(search, root, result)
            self.assertEqual(stability["status"], "PASS")
            self.assertTrue(stability["checked_every_leaf_all_pages"])

        async def test_only_constant_capital_subgroup_moves_to_income(self):
            rows = [row(0, cap="0"), row(1, cap="0")]
            rows += [row(i, income=str(i)) for i in range(2, 10)]
            engine, _, result = await partition(search_for(rows))
            self.assertTrue(result.ok, result.reason)
            self.assertEqual(result.tree["dimension"], "cap_amt")
            advances = [a for a in engine.attempt_log if a["status"] == "ADVANCE_CONSTANT_GROUP"]
            self.assertFalse(any(a["path"] == "R" for a in advances))
            self.assertTrue(any(a["dimension"] == "cap_amt" for a in advances))

        async def test_uniform_sample_does_not_prove_constant_parent(self):
            rows = [row(i, income=str(i)) for i in range(10)]
            rows += [row(10, cap="2"), row(11, cap="2")]
            engine, _, result = await partition(search_for(rows))
            self.assertTrue(result.ok, result.reason)
            self.assertEqual(result.tree["type"], "split")
            self.assertFalse(any(
                a["path"] == "R" and a["status"] == "ADVANCE_CONSTANT_GROUP"
                for a in engine.attempt_log
            ))

        async def test_failed_boundary_search_does_not_skip_dimension(self):
            with patch.object(module, "candidate_boundaries", return_value=[]):
                engine, _, result = await partition(search_for([row(i, cap=str(i)) for i in range(5)]))
            self.assertFalse(result.ok)
            self.assertIn("NOT_PROVEN_CONSTANT", result.reason)
            self.assertEqual(engine.dimension_stats["total_income"]["groups_considered"], 0)

        async def test_visible_null_is_unresolved_not_zero(self):
            engine, _, result = await partition(search_for([row(1, cap=None), row(2), row(3)]))
            self.assertFalse(result.ok)
            self.assertEqual(result.reason, "NULL_COHORT_NOT_ISOLATABLE_BY_API")
            self.assertEqual(engine.dimension_stats["total_income"]["groups_considered"], 0)

        async def test_unsampled_null_cannot_pass_count_conservation(self):
            rows = [row(i, cap=str(i)) for i in range(10)] + [row(10, cap=None)]
            _, _, result = await partition(search_for(rows))
            self.assertFalse(result.ok)

        async def test_all_values_constant_remains_unresolved(self):
            _, _, result = await partition(search_for([row(i) for i in range(5)]))
            self.assertFalse(result.ok)
            self.assertEqual(result.reason, "ALL_DIMENSIONS_CONSTANT")

        async def test_negative_and_subcent_values_are_preserved(self):
            rows = [row(i, income=str(Decimal("-0.00009") + Decimal(i) / Decimal(100000)))
                    for i in range(8)]
            _, _, result = await partition(search_for(rows))
            self.assertTrue(result.ok, result.reason)
            self.assertEqual(len(result.identities), 8)

        async def test_unresolved_sibling_keeps_validated_leaf(self):
            rows = [row(0, cap="0"), row(1, cap="0")] + [row(i) for i in range(2, 6)]
            _, _, result = await partition(search_for(rows))
            self.assertFalse(result.ok)
            self.assertEqual(result.tree["type"], "split")
            self.assertEqual(len(result.identities), 2)
            self.assertEqual(len(result.leaves), 1)

        async def test_duplicate_across_pages_rejected(self):
            rows = [row(i) for i in range(11)]
            rows[-1] = dict(rows[0])
            search = search_for(rows)
            with patch.object(module, "PRODUCTION_TARGET_ROWS", 20):
                snapshot = await search.snapshot(SearchFilters(keyword="010"))
                with self.assertRaises(DuplicateIdentityError):
                    await search.enumerate_leaf(snapshot, path="R")

        async def test_count_drift_during_pagination_rejected(self):
            search = search_for([row(i) for i in range(11)])
            snapshot = await search.snapshot(SearchFilters(keyword="010"))
            search.client.rows.append(row(12))
            with patch.object(module, "PRODUCTION_TARGET_ROWS", 20):
                with self.assertRaises(UnstablePagination):
                    await search.enumerate_leaf(snapshot, path="R")

        async def test_second_page_change_detected_even_if_first_page_is_same(self):
            search = search_for([row(i) for i in range(11)])
            with patch.object(module, "PRODUCTION_TARGET_ROWS", 20):
                root = await search.snapshot(SearchFilters(keyword="010"))
                leaf = await search.enumerate_leaf(root, path="R")
                result = PartitionResult(ok=True, identities=leaf.identities, leaves=[leaf])
                search.client.rows[-1] = row(99)
                stability = await recheck_stability(search, root, result)
            self.assertEqual(stability["status"], "FAIL")
            self.assertTrue(any(not c.get("identity_set_unchanged", True) for c in stability["checks"]))

        async def test_request_budget_stops_before_extra_call(self):
            search = search_for([row(1)], budget=1)
            await search.snapshot(SearchFilters(keyword="010"))
            with self.assertRaises(ProbeBudgetExceeded):
                await search.snapshot(SearchFilters(keyword="010"))
            self.assertEqual(search.client.requests_made, 1)

        async def test_429_is_not_retried(self):
            search = search_for([row(1)])
            search.max_retries = 3
            async def limited(*args, **kwargs):
                search.client.requests_made += 1
                raise DeepTransientHttpError("HTTP 429", status=429)
            search.client.post = limited
            with self.assertRaises(DeepTransientHttpError):
                await search.snapshot(SearchFilters(keyword="010"))
            self.assertEqual(search.client.requests_made, 1)

        def test_inherited_filters_are_preserved(self):
            filters = SearchFilters(
                keyword="010", pv_codes=["10"], jp_statuses=["1"],
                jp_types=["5"], business_sizes=["S"],
                cap_amt_min=Decimal("1000000"), cap_amt_max=Decimal("1000000"),
            )
            left, right = make_child_filters(
                filters, "total_income", Decimal("-0.001"), Decimal("0"),
            )
            for child in (left, right):
                self.assertEqual(child.keyword, filters.keyword)
                self.assertEqual(child.pv_codes, filters.pv_codes)
                self.assertEqual(child.jp_statuses, filters.jp_statuses)
                self.assertEqual(child.jp_types, filters.jp_types)
                self.assertEqual(child.business_sizes, filters.business_sizes)
                self.assertEqual(child.cap_amt_min, filters.cap_amt_min)
                self.assertEqual(child.cap_amt_max, filters.cap_amt_max)

        async def test_gap_with_real_subcent_rows_is_rejected(self):
            rows = [row(i, cap=value) for i, value in enumerate(("1", "1.005", "1.01"))]
            with patch.object(module, "candidate_boundaries",
                              return_value=[(Decimal("1"), Decimal("1.01"))]):
                engine, _, result = await partition(search_for(rows))
            self.assertFalse(result.ok)
            self.assertTrue(any("COUNT_MISMATCH" in a["status"] for a in engine.attempt_log))

        async def test_report_passes_only_for_verified_cohort(self):
            client = FakeClient([row(i, asset=str(i)) for i in range(16)])
            bucket = {"id": 1, "status": "failed", "total_items": 16}
            with (
                patch.object(module, "FixedRateDbdClient", return_value=client),
                patch.object(module, "select_failed_bucket", return_value=bucket),
                patch.object(module, "filters_from_bucket", return_value=SearchFilters(keyword="010")),
            ):
                report = await run_probe(parse_args(["--max-search-requests", "5000"]))
            self.assertTrue(report["summary"]["probe_passed"], report.get("error"))
            self.assertFalse(report["summary"]["global_coverage_certified"])
            self.assertFalse(report["summary"]["production_numeric_partition_enabled"])
            self.assertTrue(report["stability_test"]["checked_every_leaf_all_pages"])
            self.assertEqual(report["summary"]["unique_leaf_identity_count"], 16)

        async def test_report_refuses_insufficient_full_verification_budget(self):
            client = FakeClient([row(i) for i in range(16)])
            bucket = {"id": 1, "status": "failed", "total_items": 16}
            with (
                patch.object(module, "FixedRateDbdClient", return_value=client),
                patch.object(module, "select_failed_bucket", return_value=bucket),
                patch.object(module, "filters_from_bucket", return_value=SearchFilters(keyword="010")),
            ):
                report = await run_probe(parse_args(["--max-search-requests", "2"]))
            self.assertFalse(report["summary"]["probe_passed"])
            self.assertEqual(report["execution"]["stop_reason"], "REQUEST_BUDGET_EXHAUSTED")
            self.assertEqual(client.requests_made, 1)
            self.assertEqual(report["limits"]["minimum_requests_for_two_full_leaf_scans"], 6)

        async def test_ignored_numeric_filter_rejected(self):
            search = search_for([row(i, cap=str(i)) for i in range(4)])
            engine = engine_for(search)
            wrong = await search.snapshot(SearchFilters(keyword="010"))
            async def ignore(filters):
                return replace(wrong, filters=filters)
            search.snapshot = ignore
            with self.assertRaises(FilterMembershipError):
                await engine._checked_snapshot(SearchFilters(
                    keyword="010", cap_amt_min=Decimal(100),
                ))

    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTests)
    )
    print(stream.getvalue())
    return 0 if result.wasSuccessful() else 1



def main() -> int:
    args = parse_args()
    if args.self_test:
        return run_self_tests()

    try:
        report = asyncio.run(run_probe(args))

    except KeyboardInterrupt:
        report = {
            "program": "probe_live_numeric_filters",
            "schema_version": 6,
            "mode": "READ_ONLY_LIVE_API_SEQUENTIAL_RECURSIVE_PARTITION",
            "finished_at_utc": iso_now(),
            "summary": {
                "probe_passed": False,
                "partition_complete": False,
                "all_leaf_buckets_validated": False,
                "source_coverage_status": "NOT_PROVEN",
                "global_coverage_certified": False,
                "production_numeric_partition_enabled": False,
            },
            "execution": {
                "stop_reason": "INTERRUPTED_BY_USER",
            },
        }

    print(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )

    return (
        0
        if report.get("summary", {}).get("probe_passed") is True
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
