r"""Read-only DBD failed-bucket numeric partition probe.

Path:
    scripts/probe_live_numeric.py

Usage:
    python .\scripts\probe_live_numeric.py --limit 1
    python .\scripts\probe_live_numeric.py --bucket-id 17503
    python .\scripts\probe_live_numeric.py --list-candidates
    python .\scripts\probe_live_numeric.py --limit 5

Partition order:
    1. cap_amt
    2. total_income
    3. net_profit
    4. total_asset

Rules:
    - Select existing failed buckets using MySQL SELECT only.
    - Stored Count is historical information, never a blocking condition.
    - Live API Count is authoritative for the current probe.
    - Keep inherited filters when partitioning.
    - Recursively split oversized groups using the current dimension first.
    - Advance to the next dimension only when no valid split is found.
    - Never silently convert NULL to zero or discard missing records.
    - Verify count conservation, pagination, filter membership and identities.
    - Do not claim global coverage certification.

Safety:
    - No production database writes.
    - No bucket or checkpoint mutations.
    - No lease or Redis mutations.
    - No schema modifications.
    - No local report files.
    - No raw company identities in the report.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import sys
import time

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


# =============================================================================
# 01. BOOTSTRAP
# =============================================================================

ROOT = Path(__file__).resolve().parents[1]

for location in (ROOT, ROOT / "src"):
    if str(location) not in sys.path:
        sys.path.insert(0, str(location))

import pymysql
from pymysql.cursors import DictCursor

from dbd.client.endpoints.search import (
    ITEMS_PER_PAGE,
    MAX_PAGE,
    MAX_ROWS_PER_QUERY,
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    parse_search_response,
)

from dbd.core.settings import settings

from dbd.deep.http import (
    DeepPermanentHttpError,
    DeepTransientHttpError,
    FixedRateDbdClient,
)

from dbd.pipelines.discovery import _filters_from_row


# =============================================================================
# 02. CONSTANTS
# =============================================================================

PROGRAM = "probe_live_numeric_filters"

SCHEMA_VERSION = 7

TARGET = 10_000

DIMENSIONS = (
    ("cap_amt", "capAmt"),
    ("total_income", "totalIncome"),
    ("net_profit", "netProfit"),
    ("total_asset", "totalAsset"),
)

CATEGORIES = (
    ("pv_codes", "pvCode"),
    ("jp_statuses", "jpStatCode"),
    ("jp_types", "jpTypeCode"),
    ("business_sizes", "businessSize"),
)


# =============================================================================
# 03. GENERAL HELPERS
# =============================================================================


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(event: str, **values: Any) -> None:
    """
    Print structured live debug information to stderr.

    Every API request, response, partition attempt, fallback, recursive node
    and validation result can be observed while the probe is running.
    """

    print(
        json.dumps(
            {
                "at": now(),
                "event": event,
                **values,
            },
            ensure_ascii=False,
            default=str,
        ),
        file=sys.stderr,
        flush=True,
    )


def amount(value: Any) -> Decimal | None:
    """
    Preserve NULL values.

    NULL and empty strings are not converted to zero.
    Invalid or non-finite values are rejected.
    """

    if value is None:
        return None

    if isinstance(value, str) and not value.strip():
        return None

    if isinstance(value, bool):
        raise ValueError("Boolean is not a numeric value")

    try:
        number = Decimal(
            str(value).replace(",", "")
        )

    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("Invalid numeric value") from exc

    if not number.is_finite():
        raise ValueError("Non-finite numeric value")

    return number


def dec(value: Decimal | None) -> str | None:
    if value is None:
        return None

    return format(value, "f")


def sha(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest()


def identity(row: dict[str, Any]) -> tuple[str, str]:
    """
    Company identity:
        (jpTypeCode, jpNo)
    """

    jp_type = row.get("jpTypeCode")
    jp_no = row.get("jpNo")

    if (
        jp_type is None
        or jp_no is None
        or not str(jp_type).strip()
        or not str(jp_no).strip()
    ):
        raise ValueError(
            "jpTypeCode or jpNo missing from API row"
        )

    return (
        str(jp_type).strip(),
        str(jp_no).strip(),
    )


def signature(rows: list[dict[str, Any]]) -> str:
    """
    Hash identities and numeric observations without exposing raw identities.
    """

    items = [
        (
            identity(row),
            [
                (field_name, str(row.get(field_name)))
                for _, field_name in DIMENSIONS
            ],
        )
        for row in rows
    ]

    if len({item[0] for item in items}) != len(items):
        raise ValueError(
            "Duplicate identities within page"
        )

    return sha(items)


def describe(filters: SearchFilters) -> dict[str, Any]:
    return {
        "keyword": filters.keyword,
        "search_type": filters.search_type,
        "categorical": {
            name: list(getattr(filters, name))
            for name, _ in CATEGORIES
        },
        "numeric": {
            name: {
                "min": dec(
                    getattr(filters, name + "_min")
                ),
                "max": dec(
                    getattr(filters, name + "_max")
                ),
            }
            for name, _ in DIMENSIONS
            if (
                getattr(filters, name + "_min") is not None
                or getattr(filters, name + "_max") is not None
            )
        },
    }


# =============================================================================
# 04. FILTER MEMBERSHIP VALIDATION
# =============================================================================


def validate_rows(
    filters: SearchFilters,
    rows: list[dict[str, Any]],
) -> tuple[list[str], set[str]]:
    """
    Validate returned values against requested filters.

    A field present with a contradictory value is a validation failure.

    A field absent from the API response is not automatically considered
    a contradiction. It is recorded as unverifiable instead.

    Unverifiable required fields prevent full leaf validation.
    """

    violations: list[str] = []

    unavailable: set[str] = set()

    for row in rows:

        # ---------------------------------------------------------------------
        # Categorical filters
        # ---------------------------------------------------------------------

        for attribute, api_field in CATEGORIES:

            selected = getattr(filters, attribute)

            if not selected:
                continue

            if (
                api_field not in row
                or row[api_field] is None
            ):
                unavailable.add(api_field)
                continue

            allowed = {
                str(value)
                for value in selected
            }

            if str(row[api_field]) not in allowed:
                violations.append(
                    f"{api_field}:outside_requested_codes"
                )

        # ---------------------------------------------------------------------
        # Numeric filters
        # ---------------------------------------------------------------------

        for dimension, api_field in DIMENSIONS:

            lower = getattr(
                filters,
                dimension + "_min",
            )

            upper = getattr(
                filters,
                dimension + "_max",
            )

            if lower is None and upper is None:
                continue

            if api_field not in row:
                unavailable.add(api_field)
                continue

            try:
                value = amount(row[api_field])

            except ValueError:
                violations.append(
                    f"{api_field}:invalid_numeric_value"
                )
                continue

            if value is None:
                violations.append(
                    f"{api_field}:null_with_numeric_bound"
                )

            elif lower is not None and value < lower:
                violations.append(
                    f"{api_field}:below_minimum"
                )

            elif upper is not None and value > upper:
                violations.append(
                    f"{api_field}:above_maximum"
                )

    return violations[:12], unavailable


# =============================================================================
# 05. MYSQL CONNECTION
# =============================================================================


def get_setting(
    attributes: tuple[str, ...],
    envs: tuple[str, ...],
    default: Any = None,
) -> Any:

    for key in envs:

        value = os.getenv(key)

        if value not in (None, ""):
            return value

    for key in attributes:

        value = getattr(settings, key, None)

        if value not in (None, ""):
            return value

    return default


def db_connect():
    """
    Establish a database connection using existing project configuration.

    Only SELECT statements are executed by this probe.
    """

    def val(
        keys: tuple[str, ...],
        envs: tuple[str, ...],
        default: Any = None,
    ) -> Any:

        return get_setting(
            keys,
            envs,
            default,
        )

    user = val(
        (
            "mysql_user",
            "db_user",
            "mysql_username",
            "db_username",
        ),
        (
            "MYSQL_USER",
            "DB_USER",
            "DBD_MYSQL_USER",
        ),
    )

    password = val(
        (
            "mysql_password",
            "db_password",
        ),
        (
            "MYSQL_PASSWORD",
            "DB_PASSWORD",
            "DBD_MYSQL_PASSWORD",
        ),
    )

    database = val(
        (
            "db_name",
            "mysql_database",
            "database",
        ),
        (
            "DB_NAME",
            "MYSQL_DATABASE",
            "DBD_MYSQL_DATABASE",
        ),
    )

    if not user or password is None or not database:
        raise RuntimeError(
            "MySQL connection is missing credentials or database "
            "in project environment"
        )

    return pymysql.connect(
        host=str(
            val(
                ("mysql_host", "db_host", "host"),
                ("MYSQL_HOST", "DB_HOST", "DBD_MYSQL_HOST"),
                "127.0.0.1",
            )
        ),
        port=int(
            val(
                ("mysql_port", "db_port", "port"),
                ("MYSQL_PORT", "DB_PORT", "DBD_MYSQL_PORT"),
                3307,
            )
        ),
        user=str(user),
        password=str(password),
        database=str(database),
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=True,
        connect_timeout=15,
        read_timeout=120,
        write_timeout=120,
    )


# =============================================================================
# 06. FAILED BUCKET SELECTION
# =============================================================================


def select_buckets(
    limit: int,
    bucket_id: int | None,
) -> list[dict[str, Any]]:
    """
    Select existing failed oversized leaf buckets.

    Selection conditions follow the established SQL requirements.

    No persisted counts or bucket statuses are modified.
    """

    sql = """
        SELECT b.*
        FROM discovery_buckets AS b
        WHERE b.status = 'failed'
          AND b.total_items > %s
          AND COALESCE(b.pages_done, 0) = 0
          AND (
              b.lease_owner IS NULL
              OR b.lease_expires_at < UTC_TIMESTAMP(6)
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
    """

    params: list[Any] = [
        MAX_ROWS_PER_QUERY
    ]

    if bucket_id is not None:

        sql += """
            AND b.id = %s
        """

        params.append(bucket_id)

    sql += """
        ORDER BY
            b.total_items DESC,
            b.depth DESC,
            b.id ASC
        LIMIT %s
    """

    params.append(limit)

    log(
        "DB_SELECT_START",
        bucket_id=bucket_id,
        limit=limit,
    )

    with db_connect() as connection:

        with connection.cursor() as cursor:

            cursor.execute(
                "SELECT DATABASE() AS db"
            )

            selected_database = cursor.fetchone()["db"]

            cursor.execute(
                sql,
                tuple(params),
            )

            results = list(
                cursor.fetchall()
            )

    log(
        "DB_SELECT_DONE",
        database=selected_database,
        candidates=len(results),
        ids=[
            row["id"]
            for row in results
        ],
    )

    return results


def bucket_summary(
    row: dict[str, Any],
) -> dict[str, Any]:

    return {
        "bucket_id": int(row["id"]),
        "parent_id": row.get("parent_id"),
        "bucket_key": row.get("bucket_key"),
        "stored_count": int(row["total_items"]),
        "depth": int(row["depth"]),
        "status": row["status"],
        "last_error": row.get("last_error"),
    }


# =============================================================================
# 07. DATA MODELS
# =============================================================================


@dataclass
class Snapshot:

    filters: SearchFilters

    count: int

    pages: int

    first: list[dict[str, Any]]

    first_signature: str


@dataclass
class Result:

    ok: bool

    node: dict[str, Any]

    ids: set[tuple[str, str]] = field(
        default_factory=set
    )

    leaves: list[dict[str, Any]] = field(
        default_factory=list
    )

    reason: str | None = None


class StopProbe(RuntimeError):
    pass


# =============================================================================
# 08. LIVE API PROBE CONTEXT
# =============================================================================


class Probe:

    def __init__(
        self,
        client: FixedRateDbdClient,
        args: argparse.Namespace,
    ):

        self.client = client

        self.args = args

        self.requests = 0

        self.retries = 0

        self.rate_limit_hits = 0

        self.nodes = 0

        self.attempts: list[dict[str, Any]] = []

        self.missing_fields: set[str] = set()

        self.cache: dict[str, Snapshot] = {}

        self.dimension_stats = {
            dimension: {
                "attempts": 0,
                "validated": 0,
                "count_mismatch": 0,
                "ignored_or_contradictory": 0,
                "null_or_missing": 0,
            }
            for dimension, _ in DIMENSIONS
        }

    # =========================================================================
    # 09. API REQUESTS
    # =========================================================================

    async def fetch(
        self,
        filters: SearchFilters,
        page: int,
        purpose: str,
        expected: Snapshot | None = None,
    ) -> list[dict[str, Any]] | Snapshot:

        for attempt in range(
            self.args.max_retries + 1
        ):

            if self.requests >= self.args.max_requests:
                raise StopProbe(
                    "REQUEST_BUDGET_EXHAUSTED"
                )

            self.requests += 1

            log(
                "HTTP_REQUEST",
                purpose=purpose,
                page=page,
                attempt=attempt + 1,
                used=self.requests,
                budget=self.args.max_requests,
                filter_key=filters.key(),
            )

            try:

                response = await self.client.post(
                    SEARCH_PATH,
                    filters.payload(page=page),
                    referer=SEARCH_REFERER,
                    timeout=self.args.timeout_seconds,
                    rate_key="search",
                )

                parsed = parse_search_response(
                    response
                )

                count = parsed.total_items

                pages = parsed.total_pages

                rows = list(parsed.rows)

                # -------------------------------------------------------------
                # Pagination contract
                # -------------------------------------------------------------

                if (
                    parsed.current_page != page
                    or parsed.items_per_page != ITEMS_PER_PAGE
                ):
                    raise StopProbe(
                        "PAGINATION_CONTRACT_FAILED"
                    )

                if (
                    count < 0
                    or pages != math.ceil(
                        count / ITEMS_PER_PAGE
                    )
                ):

                    if not (
                        count == 0
                        and pages in (0, 1)
                    ):
                        raise StopProbe(
                            "COUNT_PAGE_CONTRACT_FAILED"
                        )

                expected_rows = max(
                    0,
                    min(
                        ITEMS_PER_PAGE,
                        count - (
                            (page - 1) * ITEMS_PER_PAGE
                        ),
                    ),
                )

                if len(rows) != expected_rows:
                    raise StopProbe(
                        "PAGE_ROW_COUNT_MISMATCH"
                    )

                signature(rows)

                # -------------------------------------------------------------
                # Stability within one filtered query
                # -------------------------------------------------------------

                if expected is not None:

                    if (
                        count != expected.count
                        or pages != expected.pages
                    ):
                        raise StopProbe(
                            "LIVE_COUNT_OR_PAGES_CHANGED_DURING_PROBE"
                        )

                log(
                    "HTTP_RESPONSE",
                    purpose=purpose,
                    page=page,
                    count=count,
                    pages=pages,
                    rows=len(rows),
                    request=self.requests,
                )

                if page != 1:
                    return rows

                return Snapshot(
                    filters=filters,
                    count=count,
                    pages=pages,
                    first=rows,
                    first_signature=signature(rows),
                )

            except (
                DeepTransientHttpError,
                DeepPermanentHttpError,
            ) as exc:

                status = getattr(
                    exc,
                    "status",
                    None,
                )

                if status == 429:
                    self.rate_limit_hits += 1

                log(
                    "HTTP_ERROR",
                    purpose=purpose,
                    status=status,
                    error=type(exc).__name__,
                )

                if status in (
                    400,
                    401,
                    403,
                    404,
                ):
                    raise StopProbe(
                        f"API_REJECTED_FILTER_HTTP_{status}"
                    ) from exc

                if attempt >= self.args.max_retries:
                    raise StopProbe(
                        "HTTP_REQUEST_FAILED:"
                        + str(
                            status
                            or type(exc).__name__
                        )
                    ) from exc

                self.retries += 1

                delay = min(
                    30,
                    4 * (2 ** attempt),
                )

                log(
                    "HTTP_RETRY",
                    purpose=purpose,
                    attempt=attempt + 1,
                    delay_seconds=delay,
                )

                await asyncio.sleep(delay)

        raise StopProbe(
            "RETRY_EXHAUSTED"
        )

    # =========================================================================
    # 10. SNAPSHOT
    # =========================================================================

    async def snapshot(
        self,
        filters: SearchFilters,
        purpose: str,
        *,
        cache: bool = False,
    ) -> Snapshot:

        key = filters.key()

        if cache and key in self.cache:

            log(
                "SNAPSHOT_CACHE_HIT",
                purpose=purpose,
                filter_key=key,
            )

            return self.cache[key]

        result = await self.fetch(
            filters,
            1,
            purpose,
        )

        assert isinstance(
            result,
            Snapshot,
        )

        if cache:
            self.cache[key] = result

        return result

    # =========================================================================
    # 11. LIVE STABILITY
    # =========================================================================

    async def stable(
        self,
        snapshot: Snapshot,
        label: str,
    ) -> None:

        newer = await self.snapshot(
            snapshot.filters,
            label,
        )

        if (
            newer.count,
            newer.pages,
            newer.first_signature,
        ) != (
            snapshot.count,
            snapshot.pages,
            snapshot.first_signature,
        ):
            raise StopProbe(
                "UNSTABLE_LIVE_ROOT_OR_SNAPSHOT"
            )

        log(
            "STABILITY_PASS",
            label=label,
            count=snapshot.count,
            pages=snapshot.pages,
        )

    # =========================================================================
    # 12. SAMPLE COLLECTION
    # =========================================================================

    async def sample(
        self,
        snapshot: Snapshot,
        path: str,
    ) -> list[dict[str, Any]]:

        positions = {1}

        accessible_pages = min(
            snapshot.pages,
            MAX_PAGE,
        )

        sample_count = min(
            self.args.sample_pages,
            accessible_pages,
        )

        if sample_count > 1:

            positions.update(
                1 + round(
                    index
                    * (accessible_pages - 1)
                    / (sample_count - 1)
                )
                for index in range(sample_count)
            )

        rows: list[dict[str, Any]] = []

        identities: set[
            tuple[str, str]
        ] = set()

        for page_number in sorted(positions):

            if page_number == 1:

                page_rows = snapshot.first

            else:

                page_rows = await self.fetch(
                    snapshot.filters,
                    page_number,
                    f"{path}:sample",
                    expected=snapshot,
                )

            assert isinstance(
                page_rows,
                list,
            )

            for row in page_rows:

                key = identity(row)

                if key in identities:
                    raise StopProbe(
                        "DUPLICATE_SAMPLED_IDENTITY"
                    )

                identities.add(key)

            rows.extend(page_rows)

        problems, unavailable = validate_rows(
            snapshot.filters,
            rows,
        )

        self.missing_fields.update(
            unavailable
        )

        if problems:
            raise StopProbe(
                "PARENT_FILTER_CONTRADICTION:"
                + ",".join(
                    sorted(set(problems))
                )
            )

        log(
            "SAMPLE_DONE",
            path=path,
            sample_pages=sorted(positions),
            sampled=len(rows),
            unavailable_fields=sorted(unavailable),
        )

        return rows

    # =========================================================================
    # 13. LEAF VERIFICATION
    # =========================================================================

    async def leaf(
        self,
        snapshot: Snapshot,
        path: str,
    ) -> Result:

        if (
            snapshot.count > TARGET
            or snapshot.pages > MAX_PAGE
        ):
            return Result(
                ok=False,
                node={
                    "path": path,
                    "status": "TOO_LARGE",
                },
                reason="TOO_LARGE",
            )

        identities: set[
            tuple[str, str]
        ] = set()

        unavailable: set[str] = set()

        log(
            "LEAF_START",
            path=path,
            count=snapshot.count,
            pages=snapshot.pages,
        )

        for page_number in range(
            1,
            snapshot.pages + 1,
        ):

            if page_number == 1:

                rows = snapshot.first

            else:

                rows = await self.fetch(
                    snapshot.filters,
                    page_number,
                    f"{path}:full_page_audit",
                    expected=snapshot,
                )

            assert isinstance(
                rows,
                list,
            )

            violations, missing = validate_rows(
                snapshot.filters,
                rows,
            )

            unavailable.update(missing)

            if violations:

                return Result(
                    ok=False,
                    node={
                        "path": path,
                        "status": "FILTER_CONTRADICTION",
                        "violations": violations,
                    },
                    reason="FILTER_CONTRADICTION",
                )

            for row in rows:

                item = identity(row)

                if item in identities:

                    return Result(
                        ok=False,
                        node={
                            "path": path,
                            "status": "DUPLICATE_IDENTITY",
                        },
                        reason="DUPLICATE_IDENTITY",
                    )

                identities.add(item)

            if (
                page_number == 1
                or page_number == snapshot.pages
                or page_number % 100 == 0
            ):

                log(
                    "LEAF_PROGRESS",
                    path=path,
                    page=page_number,
                    total_pages=snapshot.pages,
                    identities=len(identities),
                )

        if len(identities) != snapshot.count:

            return Result(
                ok=False,
                node={
                    "path": path,
                    "status": "IDENTITY_COUNT_MISMATCH",
                },
                reason="IDENTITY_COUNT_MISMATCH",
            )

        # ---------------------------------------------------------------------
        # Recheck leaf stability after reading all pages
        # ---------------------------------------------------------------------

        await self.stable(
            snapshot,
            f"{path}:leaf_stability",
        )

        record = {
            "path": path,
            "status": (
                "VERIFIED"
                if not unavailable
                else "UNVERIFIED_FIELDS"
            ),
            "count": snapshot.count,
            "pages_checked": snapshot.pages,
            "identity_sha256": sha(
                sorted(identities)
            ),
            "unverifiable_response_fields": sorted(
                unavailable
            ),
            "filters": describe(
                snapshot.filters
            ),
        }

        log(
            "LEAF_DONE",
            path=path,
            count=snapshot.count,
            status=record["status"],
        )

        return Result(
            ok=not unavailable,
            node=record,
            ids=identities,
            leaves=[record],
            reason=(
                "UNVERIFIABLE_FIELDS"
                if unavailable
                else None
            ),
        )

    # =========================================================================
    # 14. NUMERIC BOUNDARY PROPOSALS
    # =========================================================================

    def boundaries(
        self,
        snapshot: Snapshot,
        rows: list[dict[str, Any]],
        dimension: str,
        api_field: str,
    ) -> tuple[
        list[tuple[Decimal, Decimal]],
        str | None,
    ]:

        lower = getattr(
            snapshot.filters,
            dimension + "_min",
        )

        upper = getattr(
            snapshot.filters,
            dimension + "_max",
        )

        # ---------------------------------------------------------------------
        # Atomic numeric range
        # ---------------------------------------------------------------------

        if (
            lower is not None
            and upper is not None
            and lower == upper
        ):

            log(
                "DIMENSION_ATOMIC",
                dimension=dimension,
                value=dec(lower),
                reason="INHERITED_MIN_EQUALS_MAX",
            )

            return [], "ATOMIC_INHERITED_RANGE"

        values: list[Decimal] = []

        null_count = 0

        missing_count = 0

        for row in rows:

            if api_field not in row:

                missing_count += 1
                continue

            try:

                value = amount(
                    row[api_field]
                )

            except ValueError:

                return [], "INVALID_NUMERIC_SAMPLE"

            if value is None:

                null_count += 1

            else:

                values.append(value)

        unique = sorted(
            set(values)
        )

        log(
            "DIMENSION_SAMPLE",
            dimension=dimension,
            sample_numeric=len(values),
            sample_null=null_count,
            sample_field_missing=missing_count,
            distinct=len(unique),
            minimum=(
                dec(unique[0])
                if unique
                else None
            ),
            maximum=(
                dec(unique[-1])
                if unique
                else None
            ),
        )

        # ---------------------------------------------------------------------
        # Boundary candidates
        # ---------------------------------------------------------------------

        candidates: list[
            tuple[Decimal, Decimal]
        ] = []

        if len(unique) >= 2:

            middle = (
                len(unique) - 2
            ) / 2

            ordered = sorted(
                range(
                    len(unique) - 1
                ),
                key=lambda index: (
                    abs(index - middle),
                    index,
                ),
            )

            candidates.extend(
                (
                    unique[index],
                    unique[index + 1],
                )
                for index in ordered
            )

        elif unique:

            value = unique[0]

            candidates.extend(
                [
                    (
                        value,
                        value + self.args.step,
                    ),
                    (
                        value - self.args.step,
                        value,
                    ),
                ]
            )

        elif (
            lower is not None
            and upper is not None
            and lower < upper
        ):

            midpoint = (
                lower + upper
            ) / 2

            candidates.append(
                (
                    midpoint,
                    midpoint + self.args.step,
                )
            )

        # ---------------------------------------------------------------------
        # Filter candidates by inherited numeric bounds
        # ---------------------------------------------------------------------

        result: list[
            tuple[Decimal, Decimal]
        ] = []

        for left_max, right_min in candidates:

            if (
                lower is not None
                and left_max < lower
            ):
                continue

            if (
                upper is not None
                and right_min > upper
            ):
                continue

            if left_max >= right_min:
                continue

            pair = (
                left_max,
                right_min,
            )

            if pair in result:
                continue

            result.append(pair)

            # -----------------------------------------------------------------
            # NULL / missing-field diagnostics
            #
            # When a sampled parent contains NULL values, a numeric-only split
            # cannot safely represent those rows without a supported NULL
            # predicate.
            #
            # Permit one live boundary test for diagnostics, but never accept
            # a split that loses those rows.
            # -----------------------------------------------------------------

            max_attempts = (
                1
                if null_count or missing_count
                else self.args.max_boundary_attempts
            )

            if len(result) >= max_attempts:
                break

        if null_count:

            warning = "SAMPLED_NULL_NOT_EXPRESSIBLE"

        elif missing_count:

            warning = "MISSING_NUMERIC_FIELD"

        else:

            warning = None

        log(
            "BOUNDARY_PROPOSALS",
            dimension=dimension,
            candidate_count=len(result),
            warning=warning,
            candidates=[
                {
                    "left_max": dec(left),
                    "right_min": dec(right),
                }
                for left, right in result
            ],
        )

        return result, warning

    # =========================================================================
    # 15. RECURSIVE NUMERIC PARTITIONING
    # =========================================================================

    async def walk(
        self,
        snapshot: Snapshot,
        path: str,
        index: int,
        depth: int,
    ) -> Result:

        self.nodes += 1

        log(
            "NODE_START",
            path=path,
            depth=depth,
            count=snapshot.count,
            dimension=(
                DIMENSIONS[index][0]
                if index < len(DIMENSIONS)
                else None
            ),
            nodes_visited=self.nodes,
        )

        if (
            self.nodes > self.args.max_nodes
            or depth > self.args.max_depth
        ):
            raise StopProbe(
                "MAX_NODES_OR_DEPTH_EXCEEDED"
            )

        # ---------------------------------------------------------------------
        # Leaf
        # ---------------------------------------------------------------------

        if snapshot.count <= TARGET:

            return await self.leaf(
                snapshot,
                path,
            )

        # ---------------------------------------------------------------------
        # Sample the oversized parent
        # ---------------------------------------------------------------------

        samples = await self.sample(
            snapshot,
            path,
        )

        node = {
            "path": path,
            "count": snapshot.count,
            "status": "UNRESOLVED",
            "filters": describe(
                snapshot.filters
            ),
            "dimension_failures": [],
            "children": [],
        }

        # ---------------------------------------------------------------------
        # Strict sequential dimension order
        #
        # Recursion inherits the currently selected dimension index.
        # Once a group advances to the next dimension, it does not restart
        # from capital.
        # ---------------------------------------------------------------------

        for dimension_index in range(
            index,
            len(DIMENSIONS),
        ):

            dimension, api_field = DIMENSIONS[
                dimension_index
            ]

            proposed, warning = self.boundaries(
                snapshot,
                samples,
                dimension,
                api_field,
            )

            if not proposed:

                reason = (
                    warning
                    or "NO_PROPOSAL"
                )

                node[
                    "dimension_failures"
                ].append(
                    {
                        "dimension": dimension,
                        "reason": reason,
                    }
                )

                log(
                    "DIMENSION_FALLBACK",
                    path=path,
                    dimension=dimension,
                    reason=reason,
                )

                continue

            # -----------------------------------------------------------------
            # Count-validated candidate splits
            # -----------------------------------------------------------------

            good: list[
                tuple[
                    int,
                    Snapshot,
                    Snapshot,
                    dict[str, Any],
                ]
            ] = []

            for trial, (
                left_max,
                right_min,
            ) in enumerate(
                proposed,
                start=1,
            ):

                self.dimension_stats[
                    dimension
                ]["attempts"] += 1

                entry = {
                    "path": path,
                    "dimension": dimension,
                    "trial": trial,
                    "left_max": dec(
                        left_max
                    ),
                    "right_min": dec(
                        right_min
                    ),
                    "parent": snapshot.count,
                    "status": "REJECTED",
                }

                log(
                    "SPLIT_ATTEMPT",
                    **entry,
                )

                # -------------------------------------------------------------
                # Preserve all inherited filters.
                #
                # Only the currently selected numeric bound is changed.
                # -------------------------------------------------------------

                left_filters = replace(
                    snapshot.filters,
                    **{
                        dimension + "_max": left_max
                    },
                )

                right_filters = replace(
                    snapshot.filters,
                    **{
                        dimension + "_min": right_min
                    },
                )

                # -------------------------------------------------------------
                # Read live counts of both children
                # -------------------------------------------------------------

                left_snapshot = await self.snapshot(
                    left_filters,
                    path + ":L_COUNT",
                    cache=True,
                )

                right_snapshot = await self.snapshot(
                    right_filters,
                    path + ":R_COUNT",
                    cache=True,
                )

                entry.update(
                    left_count=left_snapshot.count,
                    right_count=right_snapshot.count,
                    children_sum=(
                        left_snapshot.count
                        + right_snapshot.count
                    ),
                )

                # -------------------------------------------------------------
                # Filter membership
                # -------------------------------------------------------------

                left_violations, left_unavailable = validate_rows(
                    left_filters,
                    left_snapshot.first,
                )

                right_violations, right_unavailable = validate_rows(
                    right_filters,
                    right_snapshot.first,
                )

                self.missing_fields.update(
                    left_unavailable
                    | right_unavailable
                )

                if (
                    left_violations
                    or right_violations
                ):

                    entry.update(
                        status="FILTER_CONTRADICTION",
                        sample_violations=list(
                            dict.fromkeys(
                                left_violations
                                + right_violations
                            )
                        ),
                    )

                    self.dimension_stats[
                        dimension
                    ]["ignored_or_contradictory"] += 1

                    self.attempts.append(
                        entry
                    )

                    log(
                        "SPLIT_REJECT",
                        **entry,
                    )

                    # A numeric filter returning contradictory values
                    # cannot be accepted as a valid partition.
                    #
                    # Further pivots are not assumed to repair the filter
                    # contract for this cohort.

                    break

                # -------------------------------------------------------------
                # NULL / missing-value coverage barrier
                # -------------------------------------------------------------

                if warning:

                    entry.update(
                        status="NULL_OR_MISSING_COVERAGE_BARRIER",
                        reason=warning,
                    )

                    self.dimension_stats[
                        dimension
                    ]["null_or_missing"] += 1

                    self.attempts.append(
                        entry
                    )

                    log(
                        "SPLIT_REJECT",
                        **entry,
                    )

                    break

                # -------------------------------------------------------------
                # Sample parent interval membership
                # -------------------------------------------------------------

                membership_ok = True

                for row in samples:

                    if api_field not in row:

                        membership_ok = False
                        break

                    value = amount(
                        row[api_field]
                    )

                    if (
                        value is None
                        or (
                            int(value <= left_max)
                            + int(value >= right_min)
                        ) != 1
                    ):
                        membership_ok = False
                        break

                # -------------------------------------------------------------
                # Count conservation
                # -------------------------------------------------------------

                if not membership_ok:

                    entry["status"] = (
                        "PARENT_SAMPLE_GAP"
                    )

                elif (
                    left_snapshot.count
                    + right_snapshot.count
                    != snapshot.count
                ):

                    entry["status"] = (
                        "COUNT_MISMATCH"
                    )

                    entry["count_gap"] = (
                        snapshot.count
                        - left_snapshot.count
                        - right_snapshot.count
                    )

                    self.dimension_stats[
                        dimension
                    ]["count_mismatch"] += 1

                # -------------------------------------------------------------
                # Real progress
                # -------------------------------------------------------------

                elif not (
                    0
                    < left_snapshot.count
                    < snapshot.count
                    and 0
                    < right_snapshot.count
                    < snapshot.count
                ):

                    entry["status"] = (
                        "NO_PROGRESS"
                    )

                # -------------------------------------------------------------
                # First-page identity overlap
                # -------------------------------------------------------------

                elif (
                    {
                        identity(row)
                        for row in left_snapshot.first
                    }
                    &
                    {
                        identity(row)
                        for row in right_snapshot.first
                    }
                ):

                    entry["status"] = (
                        "FIRST_PAGE_IDENTITY_OVERLAP"
                    )

                else:

                    entry["status"] = (
                        "COUNT_VALIDATED_CANDIDATE"
                    )

                    good.append(
                        (
                            max(
                                left_snapshot.count,
                                right_snapshot.count,
                            ),
                            left_snapshot,
                            right_snapshot,
                            entry,
                        )
                    )

                self.attempts.append(
                    entry
                )

                log(
                    "SPLIT_RESULT",
                    **entry,
                )

            # -----------------------------------------------------------------
            # Prefer candidates producing smaller maximum child groups.
            # -----------------------------------------------------------------

            good.sort(
                key=lambda item: item[0]
            )

            # -----------------------------------------------------------------
            # Recursive validation of candidate children
            # -----------------------------------------------------------------

            for (
                _,
                left_snapshot,
                right_snapshot,
                detail,
            ) in good:

                await self.stable(
                    snapshot,
                    path + ":parent_recheck",
                )

                log(
                    "RECURSE",
                    path=path,
                    dimension=dimension,
                    left=left_snapshot.count,
                    right=right_snapshot.count,
                )

                # -------------------------------------------------------------
                # LEFT CHILD
                # -------------------------------------------------------------

                left_outcome = await self.walk(
                    left_snapshot,
                    path + "/" + dimension + ":L",
                    dimension_index,
                    depth + 1,
                )

                if not left_outcome.ok:

                    detail["status"] = (
                        "LEFT_SUBTREE_INCOMPLETE"
                    )

                    log(
                        "SUBTREE_INCOMPLETE",
                        path=path,
                        dimension=dimension,
                        side="LEFT",
                        reason=left_outcome.reason,
                    )

                    continue

                # -------------------------------------------------------------
                # RIGHT CHILD
                # -------------------------------------------------------------

                right_outcome = await self.walk(
                    right_snapshot,
                    path + "/" + dimension + ":R",
                    dimension_index,
                    depth + 1,
                )

                if not right_outcome.ok:

                    detail["status"] = (
                        "RIGHT_SUBTREE_INCOMPLETE"
                    )

                    log(
                        "SUBTREE_INCOMPLETE",
                        path=path,
                        dimension=dimension,
                        side="RIGHT",
                        reason=right_outcome.reason,
                    )

                    continue

                # -------------------------------------------------------------
                # Cross-leaf identity verification
                # -------------------------------------------------------------

                overlap = (
                    left_outcome.ids
                    & right_outcome.ids
                )

                combined = (
                    left_outcome.ids
                    | right_outcome.ids
                )

                if (
                    overlap
                    or len(combined) != snapshot.count
                ):

                    detail["status"] = (
                        "CROSS_LEAF_IDENTITY_OR_COUNT_MISMATCH"
                    )

                    log(
                        "SPLIT_REJECT",
                        path=path,
                        dimension=dimension,
                        reason=detail["status"],
                        overlap_count=len(overlap),
                        unique_identities=len(combined),
                        parent_count=snapshot.count,
                    )

                    continue

                # -------------------------------------------------------------
                # Complete subtree
                # -------------------------------------------------------------

                detail["status"] = (
                    "FULL_SUBTREE_VALIDATED"
                )

                self.dimension_stats[
                    dimension
                ]["validated"] += 1

                log(
                    "SPLIT_PASS",
                    path=path,
                    dimension=dimension,
                    count=snapshot.count,
                    unique_identities=len(combined),
                )

                return Result(
                    ok=True,
                    node={
                        "path": path,
                        "count": snapshot.count,
                        "status": "SPLIT_VERIFIED",
                        "dimension": dimension,
                        "left_max": detail["left_max"],
                        "right_min": detail["right_min"],
                        "children": [
                            left_outcome.node,
                            right_outcome.node,
                        ],
                    },
                    ids=combined,
                    leaves=(
                        left_outcome.leaves
                        + right_outcome.leaves
                    ),
                )

            # -----------------------------------------------------------------
            # Dimension fallback
            # -----------------------------------------------------------------

            reason = (
                warning
                or "NO_COMPLETE_VALID_SPLIT_WITHIN_BUDGET"
            )

            node[
                "dimension_failures"
            ].append(
                {
                    "dimension": dimension,
                    "reason": reason,
                }
            )

            log(
                "DIMENSION_FALLBACK",
                path=path,
                dimension=dimension,
                reason=reason,
                next_dimension=(
                    DIMENSIONS[
                        dimension_index + 1
                    ][0]
                    if dimension_index + 1 < len(DIMENSIONS)
                    else None
                ),
            )

        # ---------------------------------------------------------------------
        # All available dimensions exhausted
        # ---------------------------------------------------------------------

        log(
            "UNRESOLVED",
            path=path,
            count=snapshot.count,
        )

        return Result(
            ok=False,
            node=node,
            reason="NO_VALID_COMPLETE_PARTITION",
        )


# =============================================================================
# 16. BUCKET EXECUTION
# =============================================================================


async def execute(
    args: argparse.Namespace,
    selected: list[dict[str, Any]],
    report: dict[str, Any],
) -> None:

    client = FixedRateDbdClient(
        concurrency=1,
        rate_per_second=args.rate,
        max_retries=0,
        timeout=args.timeout_seconds,
    )

    context = Probe(
        client,
        args,
    )

    try:

        async with client:

            for row in selected:

                # -------------------------------------------------------------
                # Reset candidate-specific observations
                # -------------------------------------------------------------

                context.cache.clear()

                context.missing_fields.clear()

                context.attempts.clear()

                context.dimension_stats = {
                    dimension: {
                        "attempts": 0,
                        "validated": 0,
                        "count_mismatch": 0,
                        "ignored_or_contradictory": 0,
                        "null_or_missing": 0,
                    }
                    for dimension, _ in DIMENSIONS
                }

                item = bucket_summary(
                    row
                )

                item.update(
                    {
                        "result": "INCOMPLETE",
                        "tree": None,
                        "candidate_leaves": [],
                        "dimension_stats": {},
                        "partition_attempts": [],
                        "source_coverage_status": "NOT_PROVEN",
                    }
                )

                try:

                    # ---------------------------------------------------------
                    # Restore persisted bucket filters
                    # ---------------------------------------------------------

                    filters = _filters_from_row(
                        row
                    )

                    if (
                        filters.key()
                        != row["bucket_key"]
                    ):
                        raise StopProbe(
                            "BUCKET_FILTER_RECONSTRUCTION_MISMATCH"
                        )

                    log(
                        "BUCKET_START",
                        bucket_id=row["id"],
                        stored_count=row["total_items"],
                        filters=describe(filters),
                    )

                    # ---------------------------------------------------------
                    # LIVE ROOT COUNT
                    #
                    # This is the authoritative count for the current probe.
                    #
                    # Stored Count is historical information only.
                    # A difference between Stored Count and Live Count does
                    # not block the test.
                    # ---------------------------------------------------------

                    root = await context.snapshot(
                        filters,
                        f"bucket-{row['id']}:LIVE_ROOT",
                    )

                    stored_count = int(
                        row["total_items"]
                    )

                    live_count = root.count

                    difference = (
                        live_count
                        - stored_count
                    )

                    item.update(
                        {
                            "live_count": live_count,
                            "count_difference": difference,
                            "count_source": "LIVE_API",
                            "historical_stored_count_used_as_gate": False,
                        }
                    )

                    log(
                        "ROOT_COUNT",
                        bucket_id=row["id"],
                        stored_count=stored_count,
                        live_count=live_count,
                        historical_difference=difference,
                        action="CONTINUE_WITH_LIVE_COUNT",
                    )

                    # ---------------------------------------------------------
                    # The current root count may have fallen below the limit.
                    # ---------------------------------------------------------

                    if root.count <= TARGET:

                        item["result"] = (
                            "LIVE_ROOT_ALREADY_WITHIN_LIMIT"
                        )

                        result = await context.leaf(
                            root,
                            f"bucket-{row['id']}",
                        )

                    else:

                        # -----------------------------------------------------
                        # Verify the live root is stable before partitioning.
                        # -----------------------------------------------------

                        await context.stable(
                            root,
                            f"bucket-{row['id']}:PRE_PARTITION_STABILITY",
                        )

                        # -----------------------------------------------------
                        # Recursive numeric partitioning.
                        #
                        # Always begin at capital.
                        # Existing fixed capital bounds will be detected
                        # and advanced to income automatically.
                        # -----------------------------------------------------

                        result = await context.walk(
                            root,
                            f"bucket-{row['id']}",
                            0,
                            0,
                        )

                    # ---------------------------------------------------------
                    # Store the observed partition tree in the report
                    # ---------------------------------------------------------

                    item["tree"] = result.node

                    item["candidate_leaves"] = (
                        result.leaves
                    )

                    item[
                        "unique_leaf_identities"
                    ] = len(result.ids)

                    item["leaf_count_sum"] = sum(
                        int(leaf["count"])
                        for leaf in result.leaves
                    )

                    # ---------------------------------------------------------
                    # Final verification
                    # ---------------------------------------------------------

                    if result.ok:

                        await context.stable(
                            root,
                            f"bucket-{row['id']}:FINAL_ROOT_STABILITY",
                        )

                        complete = (
                            item["leaf_count_sum"]
                            == root.count
                            == len(result.ids)
                            and all(
                                leaf["count"] <= TARGET
                                for leaf in result.leaves
                            )
                        )

                        item["result"] = (
                            "OBSERVED_PARTITION_VALIDATED"
                            if complete
                            else "COVERAGE_FAILED"
                        )

                        item["source_coverage_status"] = (
                            "OBSERVED_LEAVES_AND_COUNTS_ONLY"
                            if complete
                            else "NOT_PROVEN"
                        )

                        # -----------------------------------------------------
                        # A root exceeding 10000 cannot have every identity
                        # directly enumerated with this Search API.
                        #
                        # Leaf identity uniqueness plus count conservation
                        # is observational evidence, not global proof.
                        # -----------------------------------------------------

                        item[
                            "root_identity_set_directly_verified"
                        ] = (
                            root.count <= TARGET
                        )

                    else:

                        item["result"] = (
                            "UNRESOLVED"
                        )

                        item["reason"] = (
                            result.reason
                        )

                except Exception as exc:

                    item["result"] = (
                        "INCOMPLETE_OR_ERROR"
                    )

                    item["reason"] = (
                        f"{type(exc).__name__}: "
                        f"{str(exc)[:250]}"
                    )

                    log(
                        "BUCKET_ERROR",
                        bucket_id=row["id"],
                        reason=item["reason"],
                    )

                    if isinstance(exc, StopProbe):

                        if (
                            "REQUEST_BUDGET"
                            in str(exc)
                            or "UNSTABLE"
                            in str(exc)
                            or "LIVE_COUNT"
                            in str(exc)
                        ):

                            report["stop_reason"] = (
                                str(exc)
                            )

                # -------------------------------------------------------------
                # Attach diagnostics
                # -------------------------------------------------------------

                item["partition_attempts"] = list(
                    context.attempts
                )

                item["dimension_stats"] = dict(
                    context.dimension_stats
                )

                item[
                    "unverifiable_response_fields"
                ] = sorted(
                    context.missing_fields
                )

                report["buckets"].append(
                    item
                )

                log(
                    "BUCKET_END",
                    bucket_id=row["id"],
                    result=item["result"],
                    requests=context.requests,
                    nodes=context.nodes,
                )

                if report.get("stop_reason"):
                    break

    finally:

        report["search_requests"] = (
            context.requests
        )

        report["transport_attempts"] = getattr(
            client,
            "requests_made",
            0,
        )

        report["manual_retries"] = (
            context.retries
        )

        report["rate_limit_hits"] = (
            context.rate_limit_hits
        )

        report["nodes_examined"] = (
            context.nodes
        )


# =============================================================================
# 17. CLI ARGUMENTS
# =============================================================================


def arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--bucket-id",
        type=int,
        default=None,
        help="Test one eligible failed bucket",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Number of failed buckets to test",
    )

    parser.add_argument(
        "--list-candidates",
        action="store_true",
        help="List eligible failed buckets using SQL only",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=0.25,
        help="Maximum live API requests per second",
    )

    parser.add_argument(
        "--max-requests",
        type=int,
        default=30_000,
        help="Maximum live API request budget",
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Maximum manual retries per request",
    )

    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="Timeout per API request",
    )

    parser.add_argument(
        "--sample-pages",
        type=int,
        default=5,
        help="Number of pages sampled per oversized node",
    )

    parser.add_argument(
        "--max-boundary-attempts",
        type=int,
        default=12,
        help="Maximum boundary attempts per dimension and node",
    )

    parser.add_argument(
        "--max-nodes",
        type=int,
        default=2_000,
        help="Maximum recursive nodes",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=40,
        help="Maximum recursive partition depth",
    )

    parser.add_argument(
        "--step",
        default="0.01",
        help="Fallback decimal boundary step",
    )

    args = parser.parse_args()

    if (
        args.limit < 1
        or args.max_requests < 1
        or args.max_nodes < 1
        or args.max_depth < 1
        or args.sample_pages < 1
        or args.max_boundary_attempts < 1
        or args.max_retries < 0
        or not 0 < args.rate <= 2
        or args.timeout_seconds <= 0
        or (
            args.bucket_id is not None
            and args.bucket_id < 1
        )
    ):

        parser.error(
            "One or more numeric arguments are outside "
            "supported safety limits"
        )

    try:

        args.step = amount(
            args.step
        )

        if (
            args.step is None
            or args.step <= 0
        ):
            raise ValueError(
                "Step must be positive"
            )

    except ValueError:

        parser.error(
            "--step must be a positive finite decimal"
        )

    return args


# =============================================================================
# 18. MAIN
# =============================================================================


def main() -> int:

    args = arguments()

    began = time.monotonic()

    report: dict[str, Any] = {
        "program": PROGRAM,
        "schema_version": SCHEMA_VERSION,
        "started_at_utc": now(),
        "mode": (
            "READ_ONLY_LIVE_API_"
            "SEQUENTIAL_RECURSIVE_PARTITION"
        ),
        "dimension_order": [
            dimension
            for dimension, _ in DIMENSIONS
        ],
        "safety": {
            "mysql_select_only": True,
            "production_writes": False,
            "bucket_mutations": False,
            "checkpoint_mutations": False,
            "lease_mutations": False,
            "redis_mutations": False,
            "global_coverage_certified": False,
            "production_numeric_partition_enabled": False,
        },
        "limits": {
            "limit": args.limit,
            "bucket_id": args.bucket_id,
            "max_requests": args.max_requests,
            "rate_per_second": args.rate,
            "max_nodes": args.max_nodes,
            "max_depth": args.max_depth,
            "max_boundary_attempts": (
                args.max_boundary_attempts
            ),
        },
        "buckets": [],
        "stop_reason": None,
    }

    try:

        # ---------------------------------------------------------------------
        # MySQL candidate selection
        # ---------------------------------------------------------------------

        selected = select_buckets(
            args.limit,
            args.bucket_id,
        )

        # ---------------------------------------------------------------------
        # SQL-only mode
        # ---------------------------------------------------------------------

        if args.list_candidates:

            report["buckets"] = [
                bucket_summary(row)
                for row in selected
            ]

            report["candidate_list_only"] = True

        # ---------------------------------------------------------------------
        # Live API probe
        # ---------------------------------------------------------------------

        elif selected:

            asyncio.run(
                execute(
                    args,
                    selected,
                    report,
                )
            )

        else:

            report["stop_reason"] = (
                "NO_ELIGIBLE_FAILED_BUCKET"
            )

    except KeyboardInterrupt:

        report["stop_reason"] = (
            "USER_INTERRUPTED"
        )

    except Exception as exc:

        report["stop_reason"] = (
            "FATAL_ERROR"
        )

        report["fatal_error"] = {
            "type": type(exc).__name__,
            "message": str(exc)[:250],
        }

    # =========================================================================
    # FINAL REPORT
    # =========================================================================

    report["finished_at_utc"] = (
        now()
    )

    report["elapsed_seconds"] = round(
        time.monotonic() - began,
        2,
    )

    verified = sum(
        item.get("result")
        == "OBSERVED_PARTITION_VALIDATED"
        for item in report["buckets"]
    )

    report["summary"] = {
        "selected": len(
            report["buckets"]
        ),
        "observationally_validated": verified,
        "complete": (
            verified == len(report["buckets"])
            and bool(verified)
        ),
        "root_identities_directly_enumerated": False,
        "global_coverage_certified": False,
        "production_numeric_partition_enabled": False,
    }

    print(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )

    if (
        report["summary"]["complete"]
        or args.list_candidates
    ):
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())