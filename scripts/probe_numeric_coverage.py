r"""
scripts/probe_numeric_coverage.py

DBD NUMERIC COVERAGE PROBE — READ ONLY

Run from the repository root:
    python .\scripts\probe_numeric_coverage.py

Purpose:
    1. Select up to 3 completed small Discovery cohorts.
    2. Read every parent identity from the DBD Search API.
    3. Probe all 4 numeric dimensions:
           cap_amt
           total_asset
           total_income
           net_profit
    4. Check numeric boundaries, zero, negative, decimal and NULL behavior.
    5. Compare complete parent/child identity sets.
    6. Preview adaptive numeric pivots for up to 3 oversized failed buckets.
    7. Print observed evidence and uncertainty to the terminal.

Safety:
    - MySQL application queries are SELECT-only.
    - No bucket status changes.
    - No child bucket creation.
    - No checkpoint changes or resets.
    - No response archive or output files.
    - No modification of DBD Core.
    - No production numeric certification.
    - One HTTP client, concurrency 1.
    - Maximum 220 planned Search API requests.
    - Rate never exceeds 2 requests/second or the configured rate.
    - Existing DBD authentication, retry, cooldown and manager lock are reused.

Interpretation:
    PASS_OBSERVED:
        Complete identities passed the checks for the selected small cohort.

    FAIL:
        Observed API behavior contradicts the candidate partition.

    INSUFFICIENT:
        The available evidence cannot establish the requested property.

    DRIFT:
        The parent population or identities changed during observation.

    Passing a small cohort does not certify global DBD numeric coverage.
"""

from __future__ import annotations

import asyncio
import sys

from collections import Counter
from dataclasses import replace
from decimal import Decimal
from pathlib import Path


# =====================================================================
# 1. Project imports
# =====================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"

if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


from dbd.client.endpoints.search import (
    ITEMS_PER_PAGE,
    MAX_ROWS_PER_QUERY,
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    parse_search_response,
)

from dbd.contracts.common import money
from dbd.core.exceptions import ContractError
from dbd.core.settings import settings

from dbd.deep.http import (
    DeepPermanentHttpError,
    FixedRateDbdClient,
)

from dbd.pipelines.discovery import _filters_from_row

from dbd.pipelines.discovery_numeric import (
    CANDIDATE_STEP,
    NUMERIC_DIMENSIONS,
    NumericObservation,
    PRODUCTION_CERTIFIED_DIMENSIONS,
    numeric_partition_structure,
    plan_numeric_split,
)

from dbd.runtime.locking import (
    assert_manager_lock,
    manager_lock,
)


# =====================================================================
# 2. Configuration
# =====================================================================

SMALL_COHORT_LIMIT = 3
FAILED_BUCKET_LIMIT = 3

MIN_COHORT_ITEMS = 11
MAX_COHORT_ITEMS = 30
MAX_COHORT_PAGES = 3

MAX_PLANNED_REQUESTS = 220

MAX_PREVIEW_TRIALS = 8
MAX_DIMENSION_TRIALS = 4

RATE_CEILING = 2.0

NUMERIC_STEP = CANDIDATE_STEP

SPLIT_ERROR = (
    "no count-validated, non-overlapping split is available"
)


class RequestBudgetExhausted(RuntimeError):
    pass


class CohortTooLarge(RuntimeError):
    pass


# =====================================================================
# 3. Read-only database selection
# =====================================================================

def select_probe_targets(manager_connection):
    """
    Select:

        A. Up to 3 completed small cohorts containing 11-30 companies.
        B. Up to 3 untouched oversized SPLIT_FAILED leaf buckets.

    Only SELECT statements are executed.

    Small cohorts prefer distinct search keywords.
    No pending, draining, failed-page or checkpointed oversized
    bucket is modified.
    """

    assert_manager_lock(manager_connection)

    with manager_connection.cursor() as cur:

        # -------------------------------------------------------------
        # Verify the existing numeric schema.
        # -------------------------------------------------------------

        cur.execute(
            """
            SELECT 1 AS ok
            FROM information_schema.columns

            WHERE table_schema = DATABASE()
              AND table_name = 'discovery_buckets'
              AND column_name = 'numeric_filters'
              AND data_type = 'json'

            LIMIT 1
            """
        )

        if cur.fetchone() is None:
            raise RuntimeError(
                "MISSING_NUMERIC_FILTERS_SCHEMA: "
                "existing migration 0013 is required"
            )

        # -------------------------------------------------------------
        # Refuse to compete with active Discovery workers.
        # -------------------------------------------------------------

        cur.execute(
            """
            SELECT COUNT(*) AS active_leases
            FROM discovery_buckets

            WHERE lease_owner IS NOT NULL
              AND lease_expires_at > UTC_TIMESTAMP(6)
            """
        )

        active_leases = int(
            cur.fetchone()["active_leases"]
        )

        if active_leases:
            raise RuntimeError(
                f"DISCOVERY_WORKERS_ACTIVE: "
                f"{active_leases} bucket lease(s)"
            )

        # -------------------------------------------------------------
        # Select completed small cohorts.
        # -------------------------------------------------------------

        cur.execute(
            """
            SELECT b.*
            FROM discovery_buckets b

            WHERE b.status = 'done'

              AND b.total_items BETWEEN %s AND %s

              AND b.pages_total BETWEEN 2 AND %s

              AND b.pages_done = b.pages_total

              AND b.numeric_filters IS NULL

              AND LENGTH(b.keyword) >= 3

              AND NOT EXISTS (
                  SELECT 1
                  FROM discovery_buckets child
                  WHERE child.parent_id = b.id
              )

            ORDER BY b.id DESC

            LIMIT 600
            """,
            (
                MIN_COHORT_ITEMS,
                MAX_COHORT_ITEMS,
                MAX_COHORT_PAGES,
            ),
        )

        candidates = list(
            cur.fetchall()
        )

        small_cohorts = []
        selected_keywords = set()

        for bucket in candidates:

            keyword = bucket["keyword"]

            if keyword in selected_keywords:
                continue

            small_cohorts.append(bucket)
            selected_keywords.add(keyword)

            if len(small_cohorts) >= SMALL_COHORT_LIMIT:
                break

        # -------------------------------------------------------------
        # Select oversized split failures only.
        # -------------------------------------------------------------

        cur.execute(
            """
            SELECT b.*
            FROM discovery_buckets b

            WHERE b.status = 'failed'

              AND b.total_items > %s

              AND b.last_error LIKE %s

              AND COALESCE(b.pages_done, 0) = 0

              AND COALESCE(b.rows_seen, 0) = 0

              AND (
                  b.lease_owner IS NULL
                  OR b.lease_expires_at <= UTC_TIMESTAMP(6)
              )

              AND NOT EXISTS (
                  SELECT 1
                  FROM discovery_pages pg
                  WHERE pg.bucket_id = b.id
              )

              AND NOT EXISTS (
                  SELECT 1
                  FROM discovery_buckets child
                  WHERE child.parent_id = b.id
              )

            ORDER BY b.total_items DESC, b.id

            LIMIT %s
            """,
            (
                MAX_ROWS_PER_QUERY,
                f"%{SPLIT_ERROR}%",
                FAILED_BUCKET_LIMIT,
            ),
        )

        failed_buckets = list(
            cur.fetchall()
        )

    assert_manager_lock(manager_connection)

    return small_cohorts, failed_buckets


# =====================================================================
# 4. Controlled API requests
# =====================================================================

async def request_search_page(
    client: FixedRateDbdClient,
    manager_connection,
    filters: SearchFilters,
    page_no: int,
    budget: dict,
):
    """
    Perform one planned Search API request.

    Retry and HTTP admission control belong to FixedRateDbdClient.

    The budget counts planned calls.
    Actual HTTP attempts, including retries, are reported separately.
    """

    assert_manager_lock(manager_connection)

    if budget["planned"] >= MAX_PLANNED_REQUESTS:
        raise RequestBudgetExhausted(
            f"SEARCH_REQUEST_BUDGET_EXHAUSTED:"
            f"{MAX_PLANNED_REQUESTS}"
        )

    budget["planned"] += 1

    response = await client.post(
        SEARCH_PATH,
        filters.payload(page=page_no),
        referer=SEARCH_REFERER,
        timeout=settings.dbd_count_timeout_seconds,
        rate_key="search",
    )

    assert_manager_lock(manager_connection)

    page = parse_search_response(response)

    # -------------------------------------------------------------
    # Search pagination contract.
    # -------------------------------------------------------------

    if page.current_page != page_no:
        raise ContractError(
            f"PAGE_NUMBER_MISMATCH:"
            f"expected={page_no},"
            f"actual={page.current_page}"
        )

    if page.items_per_page != ITEMS_PER_PAGE:
        raise ContractError(
            f"PAGE_SIZE_MISMATCH:"
            f"expected={ITEMS_PER_PAGE},"
            f"actual={page.items_per_page}"
        )

    expected_pages = (
        page.total_items + ITEMS_PER_PAGE - 1
    ) // ITEMS_PER_PAGE

    if page.total_pages != expected_pages:
        raise ContractError(
            "TOTAL_PAGES_MISMATCH"
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
        raise ContractError(
            f"PAGE_ROWS_MISMATCH:"
            f"page={page_no},"
            f"expected={expected_rows},"
            f"actual={len(page.rows)}"
        )

    return page


# =====================================================================
# 5. Company identities
# =====================================================================

def company_identity(row: dict) -> tuple[str, str]:
    """
    Use the actual DBD company identity:

        jpTypeCode + jpNo

    Never use company names as unique identifiers.
    """

    jp_type = row.get("jpTypeCode")
    jp_no = row.get("jpNo")

    if (
        jp_type is None
        or jp_no is None
        or not str(jp_type).strip()
        or not str(jp_no).strip()
    ):
        raise ContractError(
            "MISSING_COMPANY_IDENTITY"
        )

    return (
        str(jp_type),
        str(jp_no),
    )


# =====================================================================
# 6. Complete small-cohort retrieval
# =====================================================================

async def read_complete_cohort(
    client: FixedRateDbdClient,
    manager_connection,
    filters: SearchFilters,
    budget: dict,
):
    """
    Retrieve every identity in a small Search cohort.

    The first response determines the current live population.

    If the query exceeds 30 items or 3 pages, stop before
    requesting the remaining pages.

    Never claim completeness from a partial query.
    """

    first = await request_search_page(
        client,
        manager_connection,
        filters,
        1,
        budget,
    )

    if (
        first.total_items > MAX_COHORT_ITEMS
        or first.total_pages > MAX_COHORT_PAGES
    ):
        raise CohortTooLarge(
            f"COHORT_EXCEEDS_READ_LIMIT:"
            f"items={first.total_items},"
            f"pages={first.total_pages}"
        )

    identities = {}

    for page_no in range(
        1,
        first.total_pages + 1,
    ):

        if page_no == 1:
            page = first

        else:
            page = await request_search_page(
                client,
                manager_connection,
                filters,
                page_no,
                budget,
            )

        if (
            page.total_items != first.total_items
            or page.total_pages != first.total_pages
        ):
            raise ContractError(
                "QUERY_COUNT_CHANGED_BETWEEN_PAGES"
            )

        for row in page.rows:

            identity = company_identity(row)

            if identity in identities:
                raise ContractError(
                    "DUPLICATE_IDENTITY_BETWEEN_PAGES"
                )

            identities[identity] = row

    if len(identities) != first.total_items:
        raise ContractError(
            "INCOMPLETE_COHORT_IDENTITY_SET"
        )

    return {
        "count": first.total_items,
        "rows": identities,
    }


# =====================================================================
# 7. Inspect numeric values
# =====================================================================

def inspect_numeric_values(
    rows: dict,
    response_key: str,
):
    """
    Distinguish:

        - Field absent from JSON
        - JSON null
        - Empty string
        - Invalid numeric value
        - Valid zero
        - Valid negative
        - Valid fractional
        - More than two decimal places

    No missing value is silently converted to zero.
    """

    values = {}
    counts = Counter()

    for identity, row in rows.items():

        if response_key not in row:
            counts["field_missing"] += 1
            values[identity] = None
            continue

        raw = row[response_key]

        if raw is None:
            counts["json_null"] += 1
            values[identity] = None
            continue

        if raw == "":
            counts["empty_string"] += 1
            values[identity] = None
            continue

        try:
            amount = money(raw)

        except (
            ValueError,
            TypeError,
            ContractError,
        ):
            counts["invalid_numeric"] += 1
            values[identity] = None
            continue

        values[identity] = amount

        if amount is None:
            continue

        counts["valid"] += 1

        if amount == 0:
            counts["zero"] += 1

        if amount < 0:
            counts["negative"] += 1

        if amount != amount.to_integral_value():
            counts["fractional"] += 1

        cents = amount * Decimal("100")

        if cents != cents.to_integral_value():
            counts["sub_cent_precision"] += 1

    return values, counts


# =====================================================================
# 8. Select observed boundary values
# =====================================================================

def choose_observed_pivot(
    values: dict,
    cohort_index: int,
):
    """
    Choose an actual numeric value observed in the complete parent.

    Rotate priorities across cohorts:

        Cohort 1: zero -> negative -> fractional
        Cohort 2: negative -> fractional -> zero
        Cohort 3: fractional -> zero -> negative

    Fall back to the observed median.

    Do not invent a test value and claim it appeared in the parent.
    """

    observed = sorted(
        amount
        for amount in values.values()
        if amount is not None
    )

    if not observed:
        return None, "NO_NUMERIC_SAMPLES"

    candidates = (
        (
            "ZERO",
            [
                value
                for value in observed
                if value == 0
            ],
        ),
        (
            "NEGATIVE",
            [
                value
                for value in observed
                if value < 0
            ],
        ),
        (
            "FRACTIONAL",
            [
                value
                for value in observed
                if value != value.to_integral_value()
            ],
        ),
    )

    for offset in range(3):

        category, options = candidates[
            (cohort_index + offset) % 3
        ]

        if options:
            return (
                options[len(options) // 2],
                category,
            )

    return (
        observed[len(observed) // 2],
        "OBSERVED_MEDIAN",
    )


# =====================================================================
# 9. Numeric membership verification
# =====================================================================

def invalid_filter_memberships(
    observation: dict,
    response_key: str,
    predicate,
    boundary: Decimal,
) -> set[tuple[str, str]]:
    """
    Find returned identities whose observed numeric value
    contradicts the submitted filter.

    Missing/NULL/unparseable numeric values are reported as
    unknown memberships, not accepted as valid numbers.
    """

    invalid = set()

    for identity, row in observation["rows"].items():

        if response_key not in row:
            invalid.add(identity)
            continue

        try:
            value = money(
                row[response_key]
            )

        except (
            ValueError,
            TypeError,
            ContractError,
        ):
            invalid.add(identity)
            continue

        if value is None or not predicate(
            value,
            boundary,
        ):
            invalid.add(identity)

    return invalid


def format_identity_sample(
    identities: set[tuple[str, str]],
) -> str:
    """
    Show at most three identities to explain a mismatch.
    """

    if not identities:
        return "-"

    return ",".join(
        f"{jp_type}/{jp_no}"
        for jp_type, jp_no in sorted(identities)[:3]
    )


# =====================================================================
# 10. One numeric dimension
# =====================================================================

async def probe_numeric_dimension(
    client: FixedRateDbdClient,
    manager_connection,
    parent_filters: SearchFilters,
    parent: dict,
    dimension: str,
    response_key: str,
    cohort_index: int,
    budget: dict,
):
    """
    Probe one numeric dimension with four complete queries.

    LEFT:
        value <= pivot

    RIGHT:
        value >= pivot + CANDIDATE_STEP

    MIN_AT_PIVOT:
        value >= pivot

    EXACT:
        value >= pivot AND value <= pivot

    These queries check:
        - Parent/child coverage
        - Overlapping identities
        - Boundary inclusivity
        - Exact-value filtering
        - Numeric membership
        - Zero/negative/fractional/NULL observations

    The result applies only to the observed small cohort.
    """

    values, kinds = inspect_numeric_values(
        parent["rows"],
        response_key,
    )

    pivot, pivot_kind = choose_observed_pivot(
        values,
        cohort_index,
    )

    result = {
        "dimension": dimension,
        "status": "INSUFFICIENT",
        "pivot": pivot,
        "pivot_kind": pivot_kind,
        "kinds": kinds,
        "request_count": 0,
    }

    if pivot is None:

        result["reason"] = (
            "NO_USABLE_NUMERIC_VALUE_IN_PARENT"
        )

        return result

    step = NUMERIC_STEP

    # -------------------------------------------------------------
    # Preserve every original parent filter.
    # -------------------------------------------------------------

    try:

        variants = {
            "LEFT": replace(
                parent_filters,
                **{
                    f"{dimension}_max": pivot,
                },
            ),
            "RIGHT": replace(
                parent_filters,
                **{
                    f"{dimension}_min": pivot + step,
                },
            ),
            "MIN_AT_PIVOT": replace(
                parent_filters,
                **{
                    f"{dimension}_min": pivot,
                },
            ),
            "EXACT": replace(
                parent_filters,
                **{
                    f"{dimension}_min": pivot,
                    f"{dimension}_max": pivot,
                },
            ),
        }

    except ValueError as exc:

        result["reason"] = (
            f"UNREPRESENTABLE_PIVOT:{exc}"
        )

        return result

    # -------------------------------------------------------------
    # Structural check.
    #
    # A temporary dimension argument is used ONLY to inspect
    # mathematical interval structure.
    #
    # It does not modify or certify the production dimension set.
    # -------------------------------------------------------------

    structure_valid = numeric_partition_structure(
        parent_filters.key(),
        [
            variants["LEFT"].key(),
            variants["RIGHT"].key(),
        ],
        certified=frozenset({dimension}),
        step=step,
    )

    if not structure_valid:

        result["reason"] = (
            "INVALID_NUMERIC_INTERVAL_STRUCTURE"
        )

        return result

    # -------------------------------------------------------------
    # Retrieve every identity of all four variants.
    # -------------------------------------------------------------

    starting_requests = budget["planned"]

    observations = {}

    try:

        for label, filters in variants.items():

            observations[label] = await read_complete_cohort(
                client,
                manager_connection,
                filters,
                budget,
            )

    except RequestBudgetExhausted:
        raise

    except (
        CohortTooLarge,
        DeepPermanentHttpError,
        ContractError,
        ValueError,
    ) as exc:

        result["request_count"] = (
            budget["planned"] - starting_requests
        )

        result["status"] = (
            "INSUFFICIENT"
            if isinstance(exc, CohortTooLarge)
            else "FAIL"
        )

        result["reason"] = (
            f"{type(exc).__name__}:{exc}"
        )

        return result

    result["request_count"] = (
        budget["planned"] - starting_requests
    )

    # -------------------------------------------------------------
    # Compare complete identity sets.
    # -------------------------------------------------------------

    parent_ids = set(
        parent["rows"]
    )

    left_ids = set(
        observations["LEFT"]["rows"]
    )

    right_ids = set(
        observations["RIGHT"]["rows"]
    )

    min_ids = set(
        observations["MIN_AT_PIVOT"]["rows"]
    )

    exact_ids = set(
        observations["EXACT"]["rows"]
    )

    combined = left_ids | right_ids

    missing = parent_ids - combined
    extra = combined - parent_ids
    overlap = left_ids & right_ids

    # -------------------------------------------------------------
    # Expected identities based on observed parent values.
    # -------------------------------------------------------------

    pivot_ids = {
        identity
        for identity, value in values.items()
        if value == pivot
    }

    expected_min_ids = {
        identity
        for identity, value in values.items()
        if (
            value is not None
            and value >= pivot
        )
    }

    observed_gap_ids = {
        identity
        for identity, value in values.items()
        if (
            value is not None
            and pivot < value < pivot + step
        )
    }

    # -------------------------------------------------------------
    # Validate every filtered result.
    # -------------------------------------------------------------

    invalid = {
        "LEFT": invalid_filter_memberships(
            observations["LEFT"],
            response_key,
            lambda value, boundary: value <= boundary,
            pivot,
        ),
        "RIGHT": invalid_filter_memberships(
            observations["RIGHT"],
            response_key,
            lambda value, boundary: value >= boundary,
            pivot + step,
        ),
        "MIN_AT_PIVOT": invalid_filter_memberships(
            observations["MIN_AT_PIVOT"],
            response_key,
            lambda value, boundary: value >= boundary,
            pivot,
        ),
        "EXACT": invalid_filter_memberships(
            observations["EXACT"],
            response_key,
            lambda value, boundary: value == boundary,
            pivot,
        ),
    }

    # -------------------------------------------------------------
    # Count parity.
    # -------------------------------------------------------------

    left_count = observations["LEFT"]["count"]
    right_count = observations["RIGHT"]["count"]

    count_parity = (
        left_count + right_count
        == parent["count"]
    )

    # -------------------------------------------------------------
    # Boundary semantics.
    # -------------------------------------------------------------

    max_inclusive = (
        bool(pivot_ids)
        and pivot_ids <= left_ids
    )

    min_inclusive = (
        bool(pivot_ids)
        and pivot_ids <= min_ids
    )

    exact_correct = (
        exact_ids == pivot_ids
    )

    min_correct = (
        min_ids == expected_min_ids
    )

    # -------------------------------------------------------------
    # Observed coverage verdict.
    # -------------------------------------------------------------

    passed = (
        count_parity
        and not missing
        and not extra
        and not overlap
        and not any(invalid.values())
        and max_inclusive
        and min_inclusive
        and exact_correct
        and min_correct
    )

    result.update(
        status=(
            "PASS_OBSERVED"
            if passed
            else "FAIL"
        ),
        reason=(
            "IDENTITY_AND_BOUNDARY_CHECKS_PASS_FOR_COHORT"
            if passed
            else "IDENTITY_OR_BOUNDARY_CONTRADICTION"
        ),
        parent_count=parent["count"],
        left_count=left_count,
        right_count=right_count,
        min_count=observations["MIN_AT_PIVOT"]["count"],
        exact_count=observations["EXACT"]["count"],
        count_parity=count_parity,
        missing=missing,
        extra=extra,
        overlap=overlap,
        invalid=invalid,
        max_inclusive=max_inclusive,
        min_inclusive=min_inclusive,
        exact_match=exact_correct,
        min_match=min_correct,
        gap_observed=len(observed_gap_ids),
    )

    return result


# =====================================================================
# 11. Terminal output for one dimension
# =====================================================================

def print_dimension_result(result: dict) -> None:

    print(
        f"  DIMENSION={result['dimension']} "
        f"STATUS={result['status']} "
        f"PIVOT={result['pivot']} "
        f"PIVOT_TYPE={result['pivot_kind']} "
        f"REQUESTS={result['request_count']}",
        flush=True,
    )

    kinds = result["kinds"]

    print(
        f"    NUMERIC_VALUES "
        f"VALID={kinds['valid']} "
        f"ZERO={kinds['zero']} "
        f"NEGATIVE={kinds['negative']} "
        f"FRACTIONAL={kinds['fractional']} "
        f"SUB_CENT={kinds['sub_cent_precision']}",
        flush=True,
    )

    print(
        f"    MISSING_VALUES "
        f"JSON_NULL={kinds['json_null']} "
        f"FIELD_MISSING={kinds['field_missing']} "
        f"EMPTY_STRING={kinds['empty_string']} "
        f"INVALID={kinds['invalid_numeric']}",
        flush=True,
    )

    if "left_count" in result:

        print(
            f"    COUNTS "
            f"PARENT={result['parent_count']} "
            f"LEFT={result['left_count']} "
            f"RIGHT={result['right_count']} "
            f"MIN={result['min_count']} "
            f"EXACT={result['exact_count']} "
            f"PARITY={result['count_parity']}",
            flush=True,
        )

        print(
            f"    IDENTITY_COVERAGE "
            f"MISSING={len(result['missing'])} "
            f"EXTRA={len(result['extra'])} "
            f"OVERLAP={len(result['overlap'])}",
            flush=True,
        )

        print(
            f"    MISSING_EXAMPLE="
            f"{format_identity_sample(result['missing'])}",
            flush=True,
        )

        print(
            f"    EXTRA_EXAMPLE="
            f"{format_identity_sample(result['extra'])}",
            flush=True,
        )

        print(
            f"    OVERLAP_EXAMPLE="
            f"{format_identity_sample(result['overlap'])}",
            flush=True,
        )

        invalid_count = sum(
            len(items)
            for items in result["invalid"].values()
        )

        print(
            f"    BOUNDARIES "
            f"MAX_INCLUSIVE={result['max_inclusive']} "
            f"MIN_INCLUSIVE={result['min_inclusive']} "
            f"EXACT_CORRECT={result['exact_match']} "
            f"MIN_CORRECT={result['min_match']} "
            f"GAP_VALUES_OBSERVED={result['gap_observed']} "
            f"INVALID_FILTERED_ROWS={invalid_count}",
            flush=True,
        )

    print(
        f"    REASON={result['reason']}",
        flush=True,
    )


# =====================================================================
# 12. Probe one complete small cohort
# =====================================================================

async def probe_small_cohort(
    client: FixedRateDbdClient,
    manager_connection,
    bucket: dict,
    cohort_index: int,
    budget: dict,
    summary: Counter,
):
    """
    Read the parent completely, probe four dimensions,
    and read the parent again.

    All four dimension results are invalidated if the parent
    identity population changes.
    """

    filters = _filters_from_row(bucket)

    print(
        f"\nSMALL_COHORT "
        f"BUCKET={bucket['id']} "
        f"KEYWORD={filters.keyword!r} "
        f"STORED={bucket['total_items']}",
        flush=True,
    )

    try:

        parent_before = await read_complete_cohort(
            client,
            manager_connection,
            filters,
            budget,
        )

    except RequestBudgetExhausted:
        raise

    except (
        CohortTooLarge,
        DeepPermanentHttpError,
        ContractError,
        ValueError,
    ) as exc:

        status = (
            "INSUFFICIENT"
            if isinstance(exc, CohortTooLarge)
            else "FAIL"
        )

        summary[status] += 1

        print(
            f"  STATUS={status} "
            f"REASON={type(exc).__name__}:{exc}",
            flush=True,
        )

        return

    # -------------------------------------------------------------
    # Verify the selected cohort is still in scope.
    # -------------------------------------------------------------

    if not (
        MIN_COHORT_ITEMS
        <= parent_before["count"]
        <= MAX_COHORT_ITEMS
    ):

        summary["DRIFT"] += 1

        print(
            f"  STATUS=DRIFT "
            f"REASON=LIVE_POPULATION_OUTSIDE_SELECTED_RANGE "
            f"LIVE={parent_before['count']}",
            flush=True,
        )

        return

    # -------------------------------------------------------------
    # Probe all four numeric dimensions.
    # -------------------------------------------------------------

    dimension_results = []

    for dimension, response_key in NUMERIC_DIMENSIONS:

        result = await probe_numeric_dimension(
            client,
            manager_connection,
            filters,
            parent_before,
            dimension,
            response_key,
            cohort_index,
            budget,
        )

        dimension_results.append(result)

    # -------------------------------------------------------------
    # Re-read the complete parent population.
    # -------------------------------------------------------------

    try:

        parent_after = await read_complete_cohort(
            client,
            manager_connection,
            filters,
            budget,
        )

    except RequestBudgetExhausted:
        raise

    except (
        CohortTooLarge,
        DeepPermanentHttpError,
        ContractError,
        ValueError,
    ) as exc:

        for result in dimension_results:

            result["status"] = "DRIFT"
            result["reason"] = (
                f"PARENT_RECHECK_FAILED:"
                f"{type(exc).__name__}:{exc}"
            )

    else:

        before_ids = set(
            parent_before["rows"]
        )

        after_ids = set(
            parent_after["rows"]
        )

        if (
            parent_before["count"] != parent_after["count"]
            or before_ids != after_ids
        ):

            for result in dimension_results:

                result["status"] = "DRIFT"
                result["reason"] = (
                    "PARENT_IDENTITY_SET_CHANGED"
                )

    # -------------------------------------------------------------
    # Report final observed results.
    # -------------------------------------------------------------

    for result in dimension_results:

        summary[result["status"]] += 1

        print_dimension_result(result)


# =====================================================================
# 13. Oversized failed-bucket preview
# =====================================================================

async def preview_oversized_bucket(
    client: FixedRateDbdClient,
    manager_connection,
    bucket: dict,
    budget: dict,
    summary: Counter,
):
    """
    Run the existing DBD numeric planner without persistence.

    Only page 1 is requested for each count observation.

    The preview:
        - Checks stored/live population.
        - Uses existing four-dimension planning.
        - Prints actual pivot/count attempts.
        - Checks candidate interval structure.
        - Rechecks parent count and first-page identities.

    This cannot prove full oversized-parent identity coverage.
    """

    filters = _filters_from_row(bucket)

    print(
        f"\nOVERSIZED_PREVIEW "
        f"BUCKET={bucket['id']} "
        f"STORED={bucket['total_items']}",
        flush=True,
    )

    async def observe(
        candidate: SearchFilters,
    ) -> NumericObservation:

        page = await request_search_page(
            client,
            manager_connection,
            candidate,
            1,
            budget,
        )

        return NumericObservation(
            page.total_items,
            page.rows,
        )

    try:

        # ---------------------------------------------------------
        # Parent before.
        # ---------------------------------------------------------

        parent_before = await observe(
            filters
        )

        before_ids = tuple(
            company_identity(row)
            for row in parent_before.rows
        )

        stored_count = int(
            bucket["total_items"]
        )

        if parent_before.count != stored_count:

            summary["DRIFT"] += 1

            print(
                f"  STATUS=DRIFT "
                f"STORED={stored_count} "
                f"LIVE={parent_before.count} "
                f"REASON=STORED_LIVE_COUNT_MISMATCH "
                f"NO_WRITES",
                flush=True,
            )

            return

        # ---------------------------------------------------------
        # Existing count-aware numeric planner.
        # ---------------------------------------------------------

        plan = await plan_numeric_split(
            filters,
            parent_before.count,
            observe,
            parent_before.rows,
            max_total_trials=MAX_PREVIEW_TRIALS,
            max_dimension_trials=MAX_DIMENSION_TRIALS,
            step=NUMERIC_STEP,
        )

        # ---------------------------------------------------------
        # Parent after.
        # ---------------------------------------------------------

        parent_after = await observe(
            filters
        )

        after_ids = tuple(
            company_identity(row)
            for row in parent_after.rows
        )

        if (
            parent_before.count != parent_after.count
            or before_ids != after_ids
        ):

            summary["DRIFT"] += 1

            print(
                "  STATUS=DRIFT "
                "REASON=PARENT_COUNT_OR_FIRST_PAGE_IDENTITIES_CHANGED",
                flush=True,
            )

            return

        # ---------------------------------------------------------
        # Display every numeric planner attempt.
        # ---------------------------------------------------------

        for index, attempt in enumerate(
            plan.attempts,
            start=1,
        ):

            print(
                f"  TRIAL={index} "
                f"DIMENSION={attempt['dimension']} "
                f"PIVOT={attempt['pivot']} "
                f"LEFT={attempt['left_count']} "
                f"RIGHT={attempt['right_count']} "
                f"REASON={attempt['reason']}",
                flush=True,
            )

        # ---------------------------------------------------------
        # Structural candidate check.
        # ---------------------------------------------------------

        if plan.candidate:

            structure_valid = numeric_partition_structure(
                filters.key(),
                [
                    child.key()
                    for child in plan.children
                ],
                certified=frozenset({
                    plan.dimension
                }),
                step=NUMERIC_STEP,
            )

            production_certified = (
                plan.dimension
                in PRODUCTION_CERTIFIED_DIMENSIONS
            )

            print(
                f"  CANDIDATE_DIMENSION={plan.dimension}",
                flush=True,
            )

            print(
                f"  CANDIDATE_CHILD_COUNTS="
                f"{plan.child_counts}",
                flush=True,
            )

            print(
                f"  CANDIDATE_RANGES="
                f"{[child.numeric_range() for child in plan.children]}",
                flush=True,
            )

            print(
                f"  STRUCTURAL_CHECK={structure_valid} "
                f"PRODUCTION_CERTIFIED={production_certified}",
                flush=True,
            )

        # ---------------------------------------------------------
        # Never promote count-only evidence to lossless coverage.
        # ---------------------------------------------------------

        summary["INSUFFICIENT"] += 1

        print(
            f"  STATUS=INSUFFICIENT "
            f"PLANNER_REASON={plan.reason} "
            f"REASON=COUNT_CANDIDATE_ONLY_"
            f"NOT_GLOBAL_IDENTITY_PROOF",
            flush=True,
        )

    except RequestBudgetExhausted:
        raise

    except DeepPermanentHttpError as exc:

        summary["FAIL"] += 1

        print(
            f"  STATUS=FAIL "
            f"REASON=HTTP_FILTER_REJECTED:{exc}",
            flush=True,
        )

    except (
        ContractError,
        ValueError,
    ) as exc:

        summary["FAIL"] += 1

        print(
            f"  STATUS=FAIL "
            f"REASON=API_CONTRACT:{exc}",
            flush=True,
        )


# =====================================================================
# 14. Main read-only execution
# =====================================================================

async def run_probe(
    manager_connection,
) -> int:

    # -------------------------------------------------------------
    # Read-only preflight and work selection.
    # -------------------------------------------------------------

    small_cohorts, failed_buckets = (
        select_probe_targets(
            manager_connection
        )
    )

    budget = {
        "planned": 0,
    }

    summary = Counter()

    effective_rate = min(
        RATE_CEILING,
        settings.dbd_rate_per_second,
    )

    print(
        "\n"
        "============================================================\n"
        "DBD NUMERIC COVERAGE PROBE\n"
        "MODE: READ ONLY\n"
        "============================================================",
        flush=True,
    )

    print(
        f"DATABASE={settings.db_name}\n"
        f"SMALL_COHORTS={len(small_cohorts)}\n"
        f"OVERSIZED_FAILED_BUCKETS={len(failed_buckets)}\n"
        f"MAX_PLANNED_REQUESTS={MAX_PLANNED_REQUESTS}\n"
        f"CONCURRENCY=1\n"
        f"RATE={effective_rate}/s\n"
        f"NUMERIC_STEP={NUMERIC_STEP}\n"
        f"PRODUCTION_CERTIFIED_DIMENSIONS="
        f"{sorted(PRODUCTION_CERTIFIED_DIMENSIONS)}",
        flush=True,
    )

    # -------------------------------------------------------------
    # One existing DBD HTTP client.
    # -------------------------------------------------------------

    async with FixedRateDbdClient(
        concurrency=1,
        rate_per_second=effective_rate,
        max_retries=settings.dbd_http_retries,
        timeout=settings.request_timeout,
    ) as client:

        try:

            # -----------------------------------------------------
            # Phase A: complete small-cohort identity coverage.
            # -----------------------------------------------------

            print(
                "\n"
                "============================================================\n"
                "PHASE A: COMPLETE SMALL-COHORT COVERAGE\n"
                "============================================================",
                flush=True,
            )

            if not small_cohorts:

                summary["INSUFFICIENT"] += 1

                print(
                    "STATUS=INSUFFICIENT "
                    "REASON=NO_ELIGIBLE_COMPLETED_SMALL_COHORTS",
                    flush=True,
                )

            for index, bucket in enumerate(
                small_cohorts
            ):

                await probe_small_cohort(
                    client,
                    manager_connection,
                    bucket,
                    index,
                    budget,
                    summary,
                )

            # -----------------------------------------------------
            # Phase B: oversized failed-bucket numeric previews.
            # -----------------------------------------------------

            print(
                "\n"
                "============================================================\n"
                "PHASE B: OVERSIZED FAILED-BUCKET PIVOT PREVIEW\n"
                "============================================================",
                flush=True,
            )

            if not failed_buckets:

                summary["INSUFFICIENT"] += 1

                print(
                    "STATUS=INSUFFICIENT "
                    "REASON=NO_ELIGIBLE_OVERSIZED_SPLIT_FAILURES",
                    flush=True,
                )

            for bucket in failed_buckets:

                await preview_oversized_bucket(
                    client,
                    manager_connection,
                    bucket,
                    budget,
                    summary,
                )

        except RequestBudgetExhausted as exc:

            summary["INSUFFICIENT"] += 1

            print(
                f"\nREQUEST_BUDGET_STOP "
                f"STATUS=INSUFFICIENT "
                f"REASON={exc}",
                flush=True,
            )

        finally:

            # -----------------------------------------------------
            # Final report.
            # -----------------------------------------------------

            print(
                "\n"
                "============================================================\n"
                "DBD NUMERIC COVERAGE PROBE — FINAL REPORT\n"
                "============================================================",
                flush=True,
            )

            print(
                f"PASS_OBSERVED={summary['PASS_OBSERVED']}\n"
                f"FAIL={summary['FAIL']}\n"
                f"INSUFFICIENT={summary['INSUFFICIENT']}\n"
                f"DRIFT={summary['DRIFT']}",
                flush=True,
            )

            print(
                "\nREQUEST STATISTICS",
                flush=True,
            )

            print(
                f"PLANNED_SEARCH_REQUESTS="
                f"{budget['planned']}",
                flush=True,
            )

            print(
                f"ACTUAL_HTTP_ATTEMPTS="
                f"{client.requests_made}",
                flush=True,
            )

            print(
                f"HTTP_RETRIES="
                f"{client.retries_made}",
                flush=True,
            )

            print(
                f"RATE_LIMIT_HITS="
                f"{client.rate_limit_hits}",
                flush=True,
            )

            print(
                "\nDECISION=OBSERVATION_ONLY\n"
                "MYSQL_APPLICATION_WRITES=0\n"
                "BUCKET_STATUS_CHANGES=0\n"
                "CHILD_BUCKETS_CREATED=0\n"
                "CHECKPOINT_RESETS=0\n"
                "NUMERIC_CERTIFICATION_CHANGES=0\n"
                "GLOBAL_LOSSLESS_COVERAGE_CLAIM=NO",
                flush=True,
            )

    return 0


# =====================================================================
# 15. One-command entry point
# =====================================================================

def main() -> int:

    try:

        # Existing database-scoped API manager lock.
        #
        # The lock is held throughout API observation.
        # No independent worker, scheduler or rate controller is created.

        with manager_lock() as manager_connection:

            return asyncio.run(
                run_probe(
                    manager_connection
                )
            )

    except KeyboardInterrupt:

        print(
            "\nSTOPPED: "
            "NO_DISCOVERY_APPLICATION_DATA_MODIFIED",
            flush=True,
        )

        return 130

    except Exception as exc:

        print(
            "\nSTOPPED: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())