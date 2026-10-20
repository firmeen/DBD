"""
DBD Discovery — 8-Dimension Virtual Split Strategy Test

Location:
    scripts/test_discovery_split_strategy.py

Run:
    python .\\scripts\\test_discovery_split_strategy.py

Purpose:
    Experiment with all eight Discovery filter dimensions to find
    recursive partition strategies where every virtual leaf contains
    strictly fewer than 5,000 companies.

Safety:
    - Production MySQL: SELECT only
    - DBD Search API: read-only requests
    - No bucket inserts, updates, or deletes
    - No checkpoint modifications
    - No production split commits
    - No modifications to production certification
    - No new workers, migrations, or infrastructure

Important:
    Matching child counts does not establish global identity coverage.
    Virtual success is not permission to commit a numeric split.

    If an API limitation, missing identity, NULL value, unstable count,
    or unavailable partition method prevents verification, the result
    must remain unresolved rather than report false success.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import subprocess
import sys
import time

from collections import Counter, deque
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


# ================================================================
# 01 — REPOSITORY BOOTSTRAP
# ================================================================

ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


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
from dbd.deep.http import FixedRateDbdClient

from dbd.runtime.locking import (
    assert_manager_lock,
    manager_lock,
)

from dbd.storage.mysql.engine import connect

from dbd.pipelines.discovery import (
    _filters_from_row,
    JP_TYPES,
    BUSINESS_SIZES,
)


# ================================================================
# 02 — CONFIGURATION
# ================================================================

TARGET = 5_000

MAX_PLANNED_SEARCH_REQUESTS = 2_400
MAX_ROOTS = 64
MAX_VIRTUAL_NODES = 1_200
MAX_RELATIVE_DEPTH = 30

MAX_PIVOT_TRIALS_PER_DIMENSION = 9
MAX_CATEGORICAL_OPTIONS = 100

API_RATE = min(
    2.0,
    float(settings.dbd_rate_per_second),
)

NUMERIC_DIMENSIONS = (
    (
        "cap_amt",
        "capAmt",
        Decimal("1000000"),
        Decimal("250000"),
    ),
    (
        "total_asset",
        "totalAsset",
        Decimal("10000000"),
        Decimal("2500000"),
    ),
    (
        "total_income",
        "totalIncome",
        Decimal("10000000"),
        Decimal("2500000"),
    ),
    (
        "net_profit",
        "netProfit",
        Decimal("0"),
        Decimal("1000000"),
    ),
)

CATEGORICAL_DIMENSIONS = (
    ("jp_types", "jpTypeCode"),
    ("jp_statuses", "jpStatCode"),
    ("business_sizes", "businessSizeCode"),
    ("pv_codes", "pvCode"),
)

# Experimental boundary hypotheses.
# None of these values represents certified backend precision.

QUANTUM_CANDIDATES = (
    Decimal("0.01"),
    Decimal("0.0001"),
    Decimal("0.000001"),
    Decimal("0.00000001"),
    Decimal("0.0000000000000001"),
)


# ================================================================
# 03 — EXCEPTIONS
# ================================================================

class BudgetExhausted(Exception):
    pass


class LiveDrift(Exception):
    pass


class UnsafeEnvironment(Exception):
    pass


class ResponseViolation(Exception):
    pass


# ================================================================
# 04 — DATA MODELS
# ================================================================

@dataclass(frozen=True)
class Observation:
    count: int
    pages: int
    rows: tuple[dict[str, Any], ...]
    identities: frozenset[tuple[str, str]]


@dataclass(frozen=True)
class Candidate:
    dimension: str
    method: str
    children: tuple[SearchFilters, ...]
    observations: tuple[Observation, ...]
    parent_count: int
    evidence: str
    score: tuple[int, int, int, str]

    @property
    def child_counts(self) -> tuple[int, ...]:
        return tuple(
            observation.count
            for observation in self.observations
        )


@dataclass
class Node:
    filters: SearchFilters
    count: int | None
    depth: int
    label: str

    verified: bool = False
    reason: str = ""


@dataclass
class RootOutcome:
    bucket_id: int
    stored_count: int

    live_count: int | None = None

    leaves: list[Node] = field(default_factory=list)

    splits: int = 0

    trials: Counter = field(default_factory=Counter)

    rejection_reasons: Counter = field(
        default_factory=Counter
    )

    verdict: str = "UNFINISHED"
    explanation: str = ""

    planned_requests: int = 0

    virtual_splits: list[dict[str, Any]] = field(
        default_factory=list
    )


@dataclass
class RuntimeState:
    planned_requests: int = 0

    actual_http_attempts: int = 0
    http_retries: int = 0
    rate_limit_hits: int = 0

    cache: dict[str, Observation] = field(
        default_factory=dict
    )

    seen_pivots: set[tuple[str, str, str]] = field(
        default_factory=set
    )

    started_at: float = field(
        default_factory=time.monotonic
    )


# ================================================================
# 05 — DECIMAL AND IDENTITY HELPERS
# ================================================================

def as_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, str) and not value.strip():
        return None

    try:
        result = Decimal(
            str(value).replace(",", "").strip()
        )
    except (InvalidOperation, ValueError):
        return None

    if not result.is_finite():
        return None

    return result


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def company_identity(
    row: dict[str, Any],
) -> tuple[str, str]:

    jp_type = row.get("jpTypeCode")
    jp_no = row.get("jpNo")

    if (
        jp_type is None
        or jp_no is None
        or not str(jp_type).strip()
        or not str(jp_no).strip()
    ):
        raise ResponseViolation(
            "Search row is missing company identity"
        )

    return str(jp_type), str(jp_no)


# ================================================================
# 06 — SOURCE INSPECTION
# ================================================================

def git_value(*args: str) -> str:
    try:
        process = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=12,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "UNAVAILABLE"

    if process.returncode != 0:
        return "UNAVAILABLE"

    return process.stdout.strip()


def verify_source_contract() -> None:
    if not (ROOT / "src" / "dbd").is_dir():
        raise UnsafeEnvironment(
            "Expected the DBD repository root"
        )

    if MAX_ROWS_PER_QUERY != ITEMS_PER_PAGE * MAX_PAGE:
        raise UnsafeEnvironment(
            "Unexpected Search pagination limits"
        )

    if (
        API_RATE <= 0
        or not math.isfinite(API_RATE)
    ):
        raise UnsafeEnvironment(
            "Invalid configured request rate"
        )

    signature = inspect.signature(
        FixedRateDbdClient.post
    )

    if "json_body" not in signature.parameters:
        raise UnsafeEnvironment(
            "FixedRateDbdClient.post signature changed"
        )

    print(
        "DBD DISCOVERY — 8-DIMENSION SPLIT STRATEGY"
    )
    print("=" * 72)

    print(
        "TARGET:",
        "EVERY NEW VIRTUAL LEAF < 5000",
    )

    print(
        "MODE:",
        "READ-ONLY MYSQL + LIVE API",
    )

    print(
        "BRANCH:",
        git_value("branch", "--show-current"),
    )

    print(
        "HEAD:",
        git_value("rev-parse", "HEAD"),
    )

    status = git_value("status", "--porcelain")

    print(
        "WORKTREE:",
        "CLEAN" if status == "" else "DIRTY OR UNAVAILABLE",
    )

    print(
        "API_RATE:",
        API_RATE,
    )

    print(
        "REQUEST_BUDGET:",
        MAX_PLANNED_SEARCH_REQUESTS,
    )

    print(
        "CERTIFICATION:",
        "NOT MODIFIED",
    )


# ================================================================
# 07 — READ-ONLY MYSQL PREFLIGHT
# ================================================================

def read_discovery_roots() -> tuple[
    list[dict[str, Any]],
    Counter,
    list[str],
]:

    required_tables = {
        "discovery_buckets",
        "discovery_pages",
        "response_archive",
        "company_master",
    }

    with connect() as conn, conn.cursor() as cur:

        cur.execute(
            "SELECT DATABASE() AS db"
        )

        database = cur.fetchone()["db"]

        if (
            not database
            or database != settings.db_name
        ):
            raise UnsafeEnvironment(
                "MySQL database identity mismatch"
            )

        print(
            "DATABASE:",
            database,
        )

        # Explicit column alias fixes the MySQL DictCursor
        # TABLE_NAME/table_name case mismatch.

        cur.execute(
            """
            SELECT TABLE_NAME AS table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            """
        )

        available_tables = {
            str(row["table_name"])
            for row in cur.fetchall()
        }

        missing = required_tables - available_tables

        if missing:
            raise UnsafeEnvironment(
                f"Required tables missing: {sorted(missing)}"
            )

        # Existing failed leaves only.
        #
        # Do not select checkpointed buckets,
        # existing split parents, or live worker leases.

        cur.execute(
            """
            SELECT
                b.*
            FROM discovery_buckets b
            WHERE b.status = 'failed'
              AND b.total_items >= %s
              AND COALESCE(b.pages_done, 0) = 0
              AND COALESCE(b.rows_seen, 0) = 0

              AND NOT EXISTS (
                  SELECT 1
                  FROM discovery_pages p
                  WHERE p.bucket_id = b.id
              )

              AND NOT EXISTS (
                  SELECT 1
                  FROM discovery_buckets child
                  WHERE child.parent_id = b.id
              )

              AND (
                  b.lease_owner IS NULL
                  OR b.lease_expires_at IS NULL
                  OR b.lease_expires_at <= UTC_TIMESTAMP(6)
              )

            ORDER BY
                b.total_items DESC,
                b.id

            LIMIT %s
            """,
            (
                TARGET,
                MAX_ROOTS,
            ),
        )

        roots = [
            dict(row)
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT
                status,
                COUNT(*) AS n
            FROM discovery_buckets
            WHERE total_items >= %s
            GROUP BY status
            """,
            (TARGET,),
        )

        overview = Counter({
            str(row["status"]): int(row["n"])
            for row in cur.fetchall()
        })

        cur.execute(
            """
            SELECT DISTINCT
                observed_jp_stat_code AS code
            FROM company_master
            WHERE observed_jp_stat_code IS NOT NULL
              AND observed_jp_stat_code <> ''
            ORDER BY observed_jp_stat_code
            """
        )

        statuses = [
            str(row["code"])
            for row in cur.fetchall()
        ]

    print(
        "ELIGIBLE_ROOTS:",
        len(roots),
    )

    print(
        "BUCKET_STATUS_OVERVIEW:",
        dict(overview),
    )

    return roots, overview, statuses


# ================================================================
# 08 — LIVE SEARCH CLIENT
# ================================================================

class LiveSearch:

    def __init__(
        self,
        client: FixedRateDbdClient,
        lock_connection: Any,
        state: RuntimeState,
    ) -> None:

        self.client = client
        self.lock_connection = lock_connection
        self.state = state

    async def query(
        self,
        filters: SearchFilters,
        *,
        fresh: bool = False,
    ) -> Observation:

        key = filters.key()

        if (
            not fresh
            and key in self.state.cache
        ):
            return self.state.cache[key]

        if (
            self.state.planned_requests
            >= MAX_PLANNED_SEARCH_REQUESTS
        ):
            raise BudgetExhausted(
                "Global Search request budget exhausted"
            )

        assert_manager_lock(
            self.lock_connection
        )

        self.state.planned_requests += 1

        response = await self.client.post(
            SEARCH_PATH,
            filters.payload(page=1),
            referer=SEARCH_REFERER,
            rate_key="search",
        )

        assert_manager_lock(
            self.lock_connection
        )

        page = parse_search_response(
            response
        )

        if page.current_page != 1:
            raise ResponseViolation(
                "Search API returned unexpected currentPage"
            )

        if page.total_items < 0:
            raise ResponseViolation(
                "Negative Search totalItems"
            )

        if (
            page.total_items > 0
            and not page.rows
        ):
            raise ResponseViolation(
                "Nonempty Search returned no first-page rows"
            )

        expected_pages = math.ceil(
            page.total_items / ITEMS_PER_PAGE
        )

        if page.total_pages != expected_pages:
            raise ResponseViolation(
                "Search pagination metadata inconsistent"
            )

        if len(page.rows) > ITEMS_PER_PAGE:
            raise ResponseViolation(
                "Unexpected Search page length"
            )

        identities = [
            company_identity(row)
            for row in page.rows
        ]

        if len(identities) != len(set(identities)):
            raise ResponseViolation(
                "Duplicate identity on Search page"
            )

        observation = Observation(
            count=page.total_items,
            pages=page.total_pages,
            rows=tuple(page.rows),
            identities=frozenset(identities),
        )

        if not fresh:
            self.state.cache[key] = observation

        return observation

    async def verify_stability(
        self,
        filters: SearchFilters,
        original: Observation,
    ) -> None:

        current = await self.query(
            filters,
            fresh=True,
        )

        if (
            current.count != original.count
            or current.pages != original.pages
        ):
            raise LiveDrift(
                "Search population changed: "
                f"{original.count}/{original.pages} "
                f"-> {current.count}/{current.pages}"
            )

        if (
            current.identities
            != original.identities
        ):
            raise LiveDrift(
                "First-page identities changed during observation"
            )


# ================================================================
# 09 — FILTER MEMBERSHIP VERIFICATION
# ================================================================

def row_matches_filters(
    filters: SearchFilters,
    raw: dict[str, Any],
) -> tuple[bool, str]:

    for field_name, raw_key in CATEGORICAL_DIMENSIONS:

        selected = getattr(
            filters,
            field_name,
        )

        if not selected:
            continue

        actual = raw.get(raw_key)

        if actual is None:
            return (
                False,
                f"missing_categorical_field:{raw_key}",
            )

        if str(actual) not in selected:
            return (
                False,
                f"categorical_mismatch:{raw_key}",
            )

    for dimension, raw_key, _, _ in NUMERIC_DIMENSIONS:

        minimum = getattr(
            filters,
            dimension + "_min",
        )

        maximum = getattr(
            filters,
            dimension + "_max",
        )

        if (
            minimum is None
            and maximum is None
        ):
            continue

        amount = as_decimal(
            raw.get(raw_key)
        )

        if amount is None:
            return (
                False,
                f"numeric_null_or_invalid:{raw_key}",
            )

        if (
            minimum is not None
            and amount < minimum
        ):
            return (
                False,
                f"below_numeric_min:{raw_key}",
            )

        if (
            maximum is not None
            and amount > maximum
        ):
            return (
                False,
                f"above_numeric_max:{raw_key}",
            )

    return True, ""


# ================================================================
# 10 — CANDIDATE VALIDATION
# ================================================================

def validate_candidate(
    parent: SearchFilters,
    parent_observation: Observation,
    children: tuple[SearchFilters, ...],
    observations: tuple[Observation, ...],
    *,
    numeric_dimension: str | None = None,
) -> tuple[bool, str, str]:

    if (
        not children
        or len(children) != len(observations)
    ):
        return (
            False,
            "invalid_child_shape",
            "CONTRADICTION",
        )

    parent_key = parent.key()

    child_keys = [
        child.key()
        for child in children
    ]

    if (
        parent_key in child_keys
        or len(child_keys) != len(set(child_keys))
    ):
        return (
            False,
            "duplicate_or_unchanged_child_filter",
            "CONTRADICTION",
        )

    counts = [
        observation.count
        for observation in observations
    ]

    child_total = sum(counts)

    if child_total != parent_observation.count:
        return (
            False,
            (
                f"count_mismatch:"
                f"parent={parent_observation.count},"
                f"children={child_total}"
            ),
            "CONTRADICTION",
        )

    if (
        max(counts) >= parent_observation.count
        or sum(count > 0 for count in counts) < 2
    ):
        return (
            False,
            "non_progressing_split",
            "CONTRADICTION",
        )

    observed_child_identities: set[
        tuple[str, str]
    ] = set()

    for child, observation in zip(
        children,
        observations,
    ):

        if (
            child.keyword != parent.keyword
            or child.search_type != parent.search_type
        ):
            return (
                False,
                "keyword_or_search_type_changed",
                "CONTRADICTION",
            )

        # Existing categorical restrictions
        # must never become broader.

        for field_name, _ in CATEGORICAL_DIMENSIONS:

            inherited = set(
                getattr(parent, field_name)
            )

            selected = set(
                getattr(child, field_name)
            )

            if inherited and (
                not selected
                or not selected.issubset(inherited)
            ):
                return (
                    False,
                    f"categorical_constraint_broadened:{field_name}",
                    "CONTRADICTION",
                )

        # Existing numeric ranges
        # must never become broader.

        for dimension, _, _, _ in NUMERIC_DIMENSIONS:

            old_min = getattr(
                parent,
                dimension + "_min",
            )

            old_max = getattr(
                parent,
                dimension + "_max",
            )

            new_min = getattr(
                child,
                dimension + "_min",
            )

            new_max = getattr(
                child,
                dimension + "_max",
            )

            if old_min is not None and (
                new_min is None
                or new_min < old_min
            ):
                return (
                    False,
                    f"numeric_lower_bound_broadened:{dimension}",
                    "CONTRADICTION",
                )

            if old_max is not None and (
                new_max is None
                or new_max > old_max
            ):
                return (
                    False,
                    f"numeric_upper_bound_broadened:{dimension}",
                    "CONTRADICTION",
                )

        # Verify actual first-page rows.

        for raw in observation.rows:

            matches, reason = row_matches_filters(
                child,
                raw,
            )

            if not matches:
                return (
                    False,
                    reason,
                    "CONTRADICTION",
                )

        # Detect directly observed sibling overlap.

        overlapping = (
            observation.identities
            & observed_child_identities
        )

        if overlapping:
            return (
                False,
                "observed_child_identity_overlap",
                "CONTRADICTION",
            )

        observed_child_identities.update(
            observation.identities
        )

    # For numeric partitions, every sampled parent value
    # must belong to exactly one proposed child interval.

    if numeric_dimension is not None:

        raw_key = next(
            key
            for dimension, key, _, _ in NUMERIC_DIMENSIONS
            if dimension == numeric_dimension
        )

        for raw in parent_observation.rows:

            amount = as_decimal(
                raw.get(raw_key)
            )

            if amount is None:
                return (
                    False,
                    f"parent_sample_numeric_null:{numeric_dimension}",
                    "CONTRADICTION",
                )

            membership_count = 0

            for child in children:

                minimum = getattr(
                    child,
                    numeric_dimension + "_min",
                )

                maximum = getattr(
                    child,
                    numeric_dimension + "_max",
                )

                belongs = (
                    (
                        minimum is None
                        or amount >= minimum
                    )
                    and (
                        maximum is None
                        or amount <= maximum
                    )
                )

                if belongs:
                    membership_count += 1

            if membership_count != 1:
                return (
                    False,
                    (
                        "parent_sample_numeric_boundary_"
                        f"gap_or_overlap:{numeric_dimension}"
                    ),
                    "CONTRADICTION",
                )

    return (
        True,
        "stable_count_and_first_page_samples",
        "GLOBAL_COVERAGE_UNPROVEN",
    )


# ================================================================
# 11 — CANDIDATE SCORING
# ================================================================

def score_candidate(
    observations: tuple[Observation, ...],
    dimension: str,
) -> tuple[int, int, int, str]:

    counts = [
        observation.count
        for observation in observations
    ]

    oversized_children = sum(
        count >= TARGET
        for count in counts
    )

    largest_child = max(counts)

    return (
        oversized_children,
        largest_child,
        len(counts),
        dimension,
    )


# ================================================================
# 12 — CATEGORICAL SPLIT STRATEGY
# ================================================================

async def try_categorical_split(
    api: LiveSearch,
    parent: SearchFilters,
    parent_observation: Observation,
    field_name: str,
    values: list[str],
    outcome: RootOutcome,
) -> Candidate | None:

    if getattr(parent, field_name):
        return None

    if not values:
        return None

    values = list(
        dict.fromkeys(
            str(value)
            for value in values
            if value
        )
    )

    if len(values) < 2:
        return None

    if len(values) > MAX_CATEGORICAL_OPTIONS:
        outcome.rejection_reasons[
            f"{field_name}:too_many_options"
        ] += 1
        return None

    children = tuple(
        replace(
            parent,
            **{
                field_name: [value],
            },
        )
        for value in values
    )

    if any(
        child.key() == parent.key()
        for child in children
    ):
        return None

    observations: list[Observation] = []

    for child in children:

        observation = await api.query(
            child
        )

        observations.append(
            observation
        )

    outcome.trials[field_name] += len(children)

    observations_tuple = tuple(
        observations
    )

    valid, reason, evidence = validate_candidate(
        parent,
        parent_observation,
        children,
        observations_tuple,
    )

    if not valid:
        outcome.rejection_reasons[
            f"{field_name}:{reason}"
        ] += 1
        return None

    await api.verify_stability(
        parent,
        parent_observation,
    )

    return Candidate(
        dimension=field_name,
        method="categorical_enumeration",
        children=children,
        observations=observations_tuple,
        parent_count=parent_observation.count,
        evidence=evidence,
        score=score_candidate(
            observations_tuple,
            field_name,
        ),
    )


# ================================================================
# 13 — NUMERIC PIVOT SAFETY
# ================================================================

def safe_numeric_pivot(
    pivot: Decimal,
    lower: Decimal | None,
    upper: Decimal | None,
    quantum: Decimal,
) -> bool:

    if (
        not pivot.is_finite()
        or not quantum.is_finite()
        or quantum <= 0
    ):
        return False

    if (
        lower is not None
        and pivot < lower
    ):
        return False

    if (
        upper is not None
        and pivot + quantum > upper
    ):
        return False

    return True


# ================================================================
# 14 — NUMERIC PIVOT GENERATION
# ================================================================

def generate_numeric_pivots(
    filters: SearchFilters,
    parent_observation: Observation,
    dimension: str,
    raw_key: str,
    initial_seed: Decimal,
    movement_step: Decimal,
) -> list[Decimal]:

    lower = getattr(
        filters,
        dimension + "_min",
    )

    upper = getattr(
        filters,
        dimension + "_max",
    )

    if (
        lower is not None
        and upper is not None
        and lower >= upper
    ):
        return []

    observed_values = sorted({
        value
        for raw in parent_observation.rows
        if (
            value := as_decimal(
                raw.get(raw_key)
            )
        ) is not None
    })

    candidates: list[Decimal] = []

    # Bounded ranges: test central and quartile pivots.

    if (
        lower is not None
        and upper is not None
    ):
        width = upper - lower

        candidates.extend([
            lower + width / 2,
            lower + width / 4,
            lower + width * Decimal(3) / 4,
        ])

    # Initial dimension-specific seeds.

    candidates.extend([
        initial_seed,
        initial_seed - movement_step,
        initial_seed + movement_step,
        initial_seed - movement_step * 4,
        initial_seed + movement_step * 4,
    ])

    # First-page values are only pivot suggestions.
    # They do not represent the full parent distribution.

    if observed_values:
        candidates.extend([
            observed_values[len(observed_values) // 2],
            observed_values[0],
            observed_values[-1],
        ])

    if (
        lower is not None
        and upper is None
    ):
        candidates.extend([
            lower + movement_step,
            lower + movement_step * 10,
        ])

    if (
        lower is None
        and upper is not None
    ):
        candidates.extend([
            upper - movement_step,
            upper - movement_step * 10,
        ])

    unique: set[Decimal] = set()

    output: list[Decimal] = []

    for pivot in candidates:

        if pivot in unique:
            continue

        if not safe_numeric_pivot(
            pivot,
            lower,
            upper,
            QUANTUM_CANDIDATES[-1],
        ):
            continue

        unique.add(pivot)
        output.append(pivot)

    return output


# ================================================================
# 15 — ADAPTIVE NUMERIC SPLIT
# ================================================================

async def try_numeric_split(
    api: LiveSearch,
    parent: SearchFilters,
    parent_observation: Observation,
    dimension: str,
    raw_key: str,
    initial_seed: Decimal,
    movement_step: Decimal,
    outcome: RootOutcome,
) -> Candidate | None:

    lower = getattr(
        parent,
        dimension + "_min",
    )

    upper = getattr(
        parent,
        dimension + "_max",
    )

    # A fully fixed numeric interval cannot be subdivided
    # further using that same numeric dimension.

    if (
        lower is not None
        and upper is not None
        and lower == upper
    ):
        outcome.rejection_reasons[
            f"{dimension}:fixed_numeric_value"
        ] += 1

        return None

    # If a parent sample contains NULL, a two-child
    # numeric range partition cannot safely cover it
    # without independently verified NULL handling.

    if any(
        as_decimal(raw.get(raw_key)) is None
        for raw in parent_observation.rows
    ):
        outcome.rejection_reasons[
            f"{dimension}:parent_sample_null"
        ] += 1

        return None

    pivots = generate_numeric_pivots(
        parent,
        parent_observation,
        dimension,
        raw_key,
        initial_seed,
        movement_step,
    )

    best_candidate: Candidate | None = None

    tried_pivots: set[Decimal] = set()

    previous_observation: tuple[
        Decimal,
        int,
        int,
    ] | None = None

    while (
        pivots
        and len(tried_pivots)
        < MAX_PIVOT_TRIALS_PER_DIMENSION
    ):

        pivot = pivots.pop(0)

        if pivot in tried_pivots:
            continue

        tried_pivots.add(
            pivot
        )

        last_counts: tuple[int, int] | None = None

        for quantum in QUANTUM_CANDIDATES:

            if not safe_numeric_pivot(
                pivot,
                lower,
                upper,
                quantum,
            ):
                continue

            pivot_key = (
                parent.key(),
                dimension,
                (
                    f"{decimal_text(pivot)}:"
                    f"{decimal_text(quantum)}"
                ),
            )

            if pivot_key in api.state.seen_pivots:
                continue

            api.state.seen_pivots.add(
                pivot_key
            )

            try:

                left = replace(
                    parent,
                    **{
                        dimension + "_max": pivot,
                    },
                )

                right = replace(
                    parent,
                    **{
                        dimension + "_min":
                            pivot + quantum,
                    },
                )

            except ValueError:
                continue

            if (
                left.key() == parent.key()
                or right.key() == parent.key()
                or left.key() == right.key()
            ):
                continue

            left_observation = await api.query(
                left
            )

            right_observation = await api.query(
                right
            )

            last_counts = (
                left_observation.count,
                right_observation.count,
            )

            outcome.trials[dimension] += 1

            children = (
                left,
                right,
            )

            observations = (
                left_observation,
                right_observation,
            )

            valid, reason, evidence = validate_candidate(
                parent,
                parent_observation,
                children,
                observations,
                numeric_dimension=dimension,
            )

            if valid:

                await api.verify_stability(
                    parent,
                    parent_observation,
                )

                candidate = Candidate(
                    dimension=dimension,
                    method=(
                        "adaptive_numeric_range:"
                        f"quantum={decimal_text(quantum)}"
                    ),
                    children=children,
                    observations=observations,
                    parent_count=parent_observation.count,
                    evidence=evidence,
                    score=score_candidate(
                        observations,
                        dimension,
                    ),
                )

                if (
                    best_candidate is None
                    or candidate.score < best_candidate.score
                ):
                    best_candidate = candidate

                # A sufficiently balanced partition is
                # preferable to spending the entire budget
                # on additional pivots at the same node.

                largest_child = max(
                    last_counts
                )

                useful_threshold = max(
                    TARGET - 1,
                    math.ceil(
                        parent_observation.count * 0.60
                    ),
                )

                if largest_child <= useful_threshold:
                    return best_candidate

                break

            outcome.rejection_reasons[
                f"{dimension}:{reason}"
            ] += 1

            # A count mismatch may be caused by the
            # current precision hypothesis.
            #
            # Test another quantum without assuming
            # that finer precision is automatically safe.

            if reason.startswith("count_mismatch:"):
                continue

            # For ignored filters, invalid returned rows,
            # or non-progressing children, changing
            # only the quantum is not sufficient evidence.

            break

        # Midpoint refinement when opposite balance
        # directions bracket the current pivot.

        if (
            previous_observation is not None
            and last_counts is not None
        ):

            (
                previous_pivot,
                previous_left,
                previous_right,
            ) = previous_observation

            previous_balance = (
                previous_left - previous_right
            )

            current_balance = (
                last_counts[0] - last_counts[1]
            )

            if (
                previous_balance * current_balance < 0
                and previous_pivot != pivot
            ):

                midpoint = (
                    previous_pivot + pivot
                ) / 2

                if (
                    midpoint not in tried_pivots
                    and safe_numeric_pivot(
                        midpoint,
                        lower,
                        upper,
                        QUANTUM_CANDIDATES[-1],
                    )
                ):
                    pivots.insert(
                        0,
                        midpoint,
                    )

        # Count-directed movement.

        if last_counts is not None:

            previous_observation = (
                pivot,
                last_counts[0],
                last_counts[1],
            )

            if last_counts[0] > last_counts[1]:

                if lower is not None:
                    next_pivot = (
                        lower + pivot
                    ) / 2
                else:
                    next_pivot = pivot - max(
                        movement_step,
                        abs(pivot) / 2,
                    )

            else:

                if upper is not None:
                    next_pivot = (
                        pivot + upper
                    ) / 2
                else:
                    next_pivot = pivot + max(
                        movement_step,
                        abs(pivot) / 2,
                    )

            if (
                next_pivot not in tried_pivots
                and safe_numeric_pivot(
                    next_pivot,
                    lower,
                    upper,
                    QUANTUM_CANDIDATES[-1],
                )
            ):
                pivots.insert(
                    0,
                    next_pivot,
                )

    return best_candidate


# ================================================================
# 16 — EIGHT-DIMENSION STRATEGY SELECTION
# ================================================================

async def explore_strategies(
    api: LiveSearch,
    filters: SearchFilters,
    parent_observation: Observation,
    categorical_values: dict[str, list[str]],
    outcome: RootOutcome,
) -> Candidate | None:

    best: Candidate | None = None

    # Start with finite categorical dimensions.
    #
    # Province enumeration is deferred because it
    # may require many more Search requests.

    for field_name, _ in CATEGORICAL_DIMENSIONS[:3]:

        candidate = await try_categorical_split(
            api,
            filters,
            parent_observation,
            field_name,
            categorical_values.get(
                field_name,
                [],
            ),
            outcome,
        )

        if candidate is not None:

            if (
                best is None
                or candidate.score < best.score
            ):
                best = candidate

        if (
            best is not None
            and max(best.child_counts) < TARGET
        ):
            return best

    # Four numeric dimensions.

    for (
        dimension,
        raw_key,
        initial_seed,
        movement_step,
    ) in NUMERIC_DIMENSIONS:

        candidate = await try_numeric_split(
            api,
            filters,
            parent_observation,
            dimension,
            raw_key,
            initial_seed,
            movement_step,
            outcome,
        )

        if candidate is not None:

            if (
                best is None
                or candidate.score < best.score
            ):
                best = candidate

        if (
            best is not None
            and max(best.child_counts) < TARGET
        ):
            return best

    # Province fallback.
    #
    # A valid province partition can be useful even
    # if a numeric split was also found.

    province_candidate = await try_categorical_split(
        api,
        filters,
        parent_observation,
        "pv_codes",
        categorical_values.get(
            "pv_codes",
            [],
        ),
        outcome,
    )

    if province_candidate is not None:

        if (
            best is None
            or province_candidate.score < best.score
        ):
            best = province_candidate

    return best


# ================================================================
# 17 — DATABASE ROOT SAFETY RECHECK
# ================================================================

def verify_root_still_safe(
    bucket_id: int,
) -> None:

    with connect() as conn, conn.cursor() as cur:

        cur.execute(
            """
            SELECT
                status,
                lease_owner,
                lease_expires_at,
                pages_done,
                rows_seen
            FROM discovery_buckets
            WHERE id = %s
            """,
            (bucket_id,),
        )

        row = cur.fetchone()

        if (
            row is None
            or row["status"] != "failed"
        ):
            raise UnsafeEnvironment(
                f"Bucket {bucket_id} is no longer failed"
            )

        if (
            int(row["pages_done"] or 0) > 0
            or int(row["rows_seen"] or 0) > 0
        ):
            raise UnsafeEnvironment(
                f"Bucket {bucket_id} has existing progress"
            )

        if (
            row["lease_owner"]
            and row["lease_expires_at"]
        ):

            cur.execute(
                "SELECT UTC_TIMESTAMP(6) AS utc"
            )

            current_utc = cur.fetchone()["utc"]

            if row["lease_expires_at"] > current_utc:
                raise UnsafeEnvironment(
                    f"Bucket {bucket_id} has an active lease"
                )

        cur.execute(
            """
            SELECT 1
            FROM discovery_pages
            WHERE bucket_id = %s
            LIMIT 1
            """,
            (bucket_id,),
        )

        if cur.fetchone() is not None:
            raise UnsafeEnvironment(
                f"Bucket {bucket_id} has page checkpoints"
            )

        cur.execute(
            """
            SELECT 1
            FROM discovery_buckets
            WHERE parent_id = %s
            LIMIT 1
            """,
            (bucket_id,),
        )

        if cur.fetchone() is not None:
            raise UnsafeEnvironment(
                f"Bucket {bucket_id} already has children"
            )


# ================================================================
# 18 — VIRTUAL SPLIT REPORT
# ================================================================

def print_virtual_split(
    root_id: int,
    node: Node,
    candidate: Candidate,
) -> None:

    print(
        f"\nSPLIT root={root_id}"
        f" node={node.label}"
        f" depth={node.depth}"
        f" parent={candidate.parent_count:,}"
    )

    print(
        "DIMENSION:",
        candidate.dimension,
    )

    print(
        "METHOD:",
        candidate.method,
    )

    print(
        "CHILD_COUNTS:",
        list(candidate.child_counts),
    )

    for index, (
        child,
        observation,
    ) in enumerate(
        zip(
            candidate.children,
            candidate.observations,
        )
    ):

        print(
            f"  CHILD[{index}]"
            f" count={observation.count:,}"
            f" key={child.key()}"
        )

    print(
        "EVIDENCE:",
        candidate.evidence,
    )

    print(
        "COVERAGE:",
        "NOT GLOBALLY CERTIFIED",
    )


# ================================================================
# 19 — RECURSIVE VIRTUAL SPLIT EXPLORER
# ================================================================

async def explore_root(
    api: LiveSearch,
    row: dict[str, Any],
    categorical_values: dict[str, list[str]],
) -> RootOutcome:

    bucket_id = int(
        row["id"]
    )

    stored_count = int(
        row["total_items"]
    )

    outcome = RootOutcome(
        bucket_id=bucket_id,
        stored_count=stored_count,
    )

    request_start = (
        api.state.planned_requests
    )

    print("\n" + "=" * 72)

    print(
        f"ROOT #{bucket_id}"
        f" keyword={row['keyword']!r}"
        f" stored={stored_count:,}"
        f" depth={row['depth']}"
    )

    pending: deque[Node] = deque()
    active_node: Node | None = None

    try:

        verify_root_still_safe(
            bucket_id
        )

        filters = _filters_from_row(
            row
        )

        original_observation = await api.query(
            filters,
            fresh=True,
        )

        api.state.cache[filters.key()] = original_observation

        outcome.live_count = (
            original_observation.count
        )

        if original_observation.count != stored_count:

            print(
                "HISTORICAL_COUNT_DIFFERENCE:",
                f"stored={stored_count:,}",
                f"live={original_observation.count:,}",
            )

            print(
                "LIVE_DRIFT:",
                "NOT ESTABLISHED BY HISTORICAL DIFFERENCE",
            )

        pending.append(
            Node(
                filters=filters,
                count=original_observation.count,
                depth=0,
                label="root",
            )
        )

        encountered_keys: set[str] = set()

        while pending:

            active_node = pending.popleft()
            node = active_node

            if (
                node.count is not None
                and node.count < TARGET
            ):

                node.reason = (
                    "UNDER_TARGET_COUNT_ONLY_"
                    "GLOBAL_COVERAGE_UNPROVEN"
                )

                outcome.leaves.append(
                    node
                )

                active_node = None
                continue

            if node.depth >= MAX_RELATIVE_DEPTH:

                node.reason = (
                    "DEPTH_BUDGET_EXHAUSTED"
                )

                outcome.leaves.append(
                    node
                )

                active_node = None
                continue

            if (
                len(encountered_keys)
                >= MAX_VIRTUAL_NODES
            ):

                node.reason = (
                    "VIRTUAL_NODE_BUDGET_EXHAUSTED"
                )

                outcome.leaves.append(
                    node
                )

                active_node = None

                outcome.leaves.extend(
                    pending
                )

                pending.clear()
                break

            key = node.filters.key()

            if key in encountered_keys:

                node.reason = (
                    "REPEATED_BUCKET_KEY"
                )

                outcome.leaves.append(
                    node
                )

                active_node = None
                continue

            encountered_keys.add(
                key
            )

            observation = await api.query(
                node.filters
            )

            if (
                node.count is not None
                and node.count != observation.count
            ):
                raise LiveDrift(
                    "Virtual child count changed: "
                    f"{node.count} -> {observation.count}"
                )

            if observation.count < TARGET:

                node.count = observation.count
                node.reason = (
                    "UNDER_TARGET_COUNT_ONLY_"
                    "GLOBAL_COVERAGE_UNPROVEN"
                )

                outcome.leaves.append(
                    node
                )

                active_node = None
                continue

            candidate = await explore_strategies(
                api,
                node.filters,
                observation,
                categorical_values,
                outcome,
            )

            if candidate is None:

                node.reason = (
                    "NO_VALIDATED_SPLIT_FOUND_"
                    "WITHIN_SEARCH_LIMITS"
                )

                outcome.leaves.append(
                    node
                )

                print(
                    f"BLOCKED node={node.label}"
                    f" count={observation.count:,}"
                    f" reason={node.reason}"
                )

                active_node = None
                continue

            verify_root_still_safe(
                bucket_id
            )

            print_virtual_split(
                bucket_id,
                node,
                candidate,
            )

            outcome.virtual_splits.append({
                "node": node.label,
                "parent_count": observation.count,
                "dimension": candidate.dimension,
                "method": candidate.method,
                "evidence": candidate.evidence,
                "children": [
                    {
                        "count": child_observation.count,
                        "bucket_key": child.key(),
                        "numeric_filters": child.numeric_range(),
                    }
                    for child, child_observation in zip(
                        candidate.children,
                        candidate.observations,
                    )
                ],
            })

            outcome.splits += 1

            for index, (
                child_filters,
                child_observation,
            ) in enumerate(
                zip(
                    candidate.children,
                    candidate.observations,
                )
            ):

                pending.append(
                    Node(
                        filters=child_filters,
                        count=child_observation.count,
                        depth=node.depth + 1,
                        label=f"{node.label}/{index}",
                    )
                )

            active_node = None

        await api.verify_stability(
            filters,
            original_observation,
        )

        unresolved = [
            node
            for node in outcome.leaves
            if (
                node.count is None
                or node.count >= TARGET
            )
        ]

        if unresolved:

            outcome.verdict = "UNRESOLVED"

            outcome.explanation = (
                f"{len(unresolved)} virtual leaves "
                "remain at or above 5000, "
                "or have unknown counts"
            )

        else:

            outcome.verdict = (
                "TARGET_COUNTS_REACHED_COVERAGE_UNPROVEN"
            )

            outcome.explanation = (
                "All virtual leaves are below 5000. "
                "Global identity coverage remains unproven."
            )

    except BudgetExhausted as exc:

        outcome.verdict = (
            "BUDGET_EXHAUSTED"
        )

        outcome.explanation = str(exc)

    except LiveDrift as exc:

        outcome.verdict = (
            "LIVE_DRIFT"
        )

        outcome.explanation = str(exc)

    except UnsafeEnvironment as exc:

        outcome.verdict = (
            "SAFETY_STOP"
        )

        outcome.explanation = str(exc)

    except (
        ResponseViolation,
        ValueError,
        TypeError,
    ) as exc:

        outcome.verdict = (
            "CONTRACT_BLOCKED"
        )

        outcome.explanation = (
            f"{type(exc).__name__}: {exc}"
        )

    except Exception as exc:

        outcome.verdict = (
            "API_OR_RUNTIME_UNAVAILABLE"
        )

        outcome.explanation = (
            f"{type(exc).__name__}: {str(exc)[:180]}"
        )

    finally:

        if active_node is not None:

            active_node.reason = (
                outcome.explanation
                or "ACTIVE_NODE_INTERRUPTED"
            )

            outcome.leaves.append(
                active_node
            )

        for node in pending:

            node.reason = (
                outcome.explanation
                or "NOT_YET_EXAMINED"
            )

            outcome.leaves.append(
                node
            )

        pending.clear()

    outcome.planned_requests = (
        api.state.planned_requests - request_start
    )

    largest_leaf = max(
        (
            node.count
            for node in outcome.leaves
            if node.count is not None
        ),
        default=None,
    )

    unresolved_count = sum(
        (
            node.count is None
            or node.count >= TARGET
        )
        for node in outcome.leaves
    )

    print("\nROOT RESULT")

    print(
        "BUCKET:",
        bucket_id,
    )

    print(
        "STATUS:",
        outcome.verdict,
    )

    print(
        "LIVE_COUNT:",
        outcome.live_count,
    )

    print(
        "SPLITS:",
        outcome.splits,
    )

    print(
        "VIRTUAL_LEAVES:",
        len(outcome.leaves),
    )

    print(
        "LARGEST_LEAF:",
        largest_leaf,
    )

    print(
        "UNRESOLVED_LEAVES:",
        unresolved_count,
    )

    print(
        "REQUESTS:",
        outcome.planned_requests,
    )

    print(
        "REASON:",
        outcome.explanation,
    )

    if outcome.rejection_reasons:

        print(
            "REJECTED_STRATEGIES:",
            dict(
                outcome.rejection_reasons.most_common(
                    15
                )
            ),
        )

    return outcome


# ================================================================
# 20 — LIVE PROVINCE ENUMERATION
# ================================================================

async def read_provinces(
    api: LiveSearch,
) -> list[str]:

    assert_manager_lock(
        api.lock_connection
    )

    try:

        response = await api.client.get(
            "/api/v1/commons/provinces"
        )

    except Exception as exc:

        print(
            "PROVINCE_ENUMERATION_UNAVAILABLE:",
            type(exc).__name__,
            str(exc)[:180],
        )

        return []

    assert_manager_lock(
        api.lock_connection
    )

    if not isinstance(response, list):
        print(
            "PROVINCE_ENUMERATION_UNAVAILABLE:",
            "Response is not a list",
        )

        return []

    provinces = sorted({
        str(row["pvCode"])
        for row in response
        if (
            isinstance(row, dict)
            and row.get("pvCode")
        )
    })

    return provinces


# ================================================================
# 21 — FINAL REPORT
# ================================================================

def print_final_report(
    outcomes: list[RootOutcome],
    state: RuntimeState,
    eligible_roots: int,
    *,
    aborted_reason: str = "",
) -> int:

    statuses = Counter(
        outcome.verdict
        for outcome in outcomes
    )

    all_leaves = [
        leaf
        for outcome in outcomes
        for leaf in outcome.leaves
    ]

    unresolved_leaves = [
        leaf
        for leaf in all_leaves
        if (
            leaf.count is None
            or leaf.count >= TARGET
        )
    ]

    not_examined = max(
        0,
        eligible_roots - len(outcomes),
    )

    print("\n" + "=" * 72)
    print(
        "FINAL — 8-DIMENSION SPLIT STRATEGY EXPERIMENT"
    )
    print("=" * 72)

    print(
        "TARGET:",
        "<5000",
    )

    print(
        "ELIGIBLE_ROOTS:",
        eligible_roots,
    )

    print(
        "ROOTS_EXAMINED:",
        len(outcomes),
    )

    print(
        "ROOTS_NOT_EXAMINED:",
        not_examined,
    )

    print(
        "ROOT_VERDICTS:",
        dict(statuses),
    )

    print(
        "TOTAL_VIRTUAL_SPLITS:",
        sum(
            outcome.splits
            for outcome in outcomes
        ),
    )

    print(
        "TOTAL_VIRTUAL_LEAVES:",
        len(all_leaves),
    )

    print(
        "LEAVES_BELOW_5000:",
        sum(
            leaf.count is not None
            and leaf.count < TARGET
            for leaf in all_leaves
        ),
    )

    print(
        "LEAVES_GE_5000_OR_UNKNOWN:",
        len(unresolved_leaves),
    )

    largest = max(
        (
            leaf.count
            for leaf in all_leaves
            if leaf.count is not None
        ),
        default=None,
    )

    print(
        "LARGEST_VIRTUAL_LEAF:",
        largest,
    )

    print(
        "PLANNED_SEARCH_REQUESTS:",
        state.planned_requests,
    )

    print(
        "ACTUAL_HTTP_ATTEMPTS:",
        state.actual_http_attempts,
    )

    print(
        "HTTP_RETRIES:",
        state.http_retries,
    )

    print(
        "RATE_LIMIT_HITS:",
        state.rate_limit_hits,
    )

    print(
        "ELAPSED_SECONDS:",
        round(
            time.monotonic() - state.started_at,
            2,
        ),
    )

    dimension_counts = Counter()

    for outcome in outcomes:
        for split in outcome.virtual_splits:
            dimension_counts[
                split["dimension"]
            ] += 1

    print(
        "DIMENSIONS_USED:",
        dict(dimension_counts),
    )

    print(
        "PRODUCTION_APPLICATION_WRITES:",
        0,
    )

    print(
        "PRODUCTION_CHILDREN_CREATED:",
        0,
    )

    print(
        "PRODUCTION_CHECKPOINTS_MODIFIED:",
        0,
    )

    print(
        "PRODUCTION_CERTIFICATION_CHANGED:",
        False,
    )

    print(
        "GLOBAL_LOSSLESS_COVERAGE_CERTIFIED:",
        False,
    )

    if unresolved_leaves:

        print("\nUNRESOLVED VIRTUAL LEAVES")

        for leaf in unresolved_leaves[:25]:

            print(
                f"  {leaf.label}"
                f" count={leaf.count}"
                f" reason={leaf.reason or 'UNEXAMINED'}"
            )

    if aborted_reason:

        print(
            "ABORTED_REASON:",
            aborted_reason,
        )

    all_count_targets_reached = (
        len(outcomes) == eligible_roots
        and eligible_roots > 0
        and not unresolved_leaves
        and not aborted_reason
        and all(
            outcome.verdict
            == "TARGET_COUNTS_REACHED_COVERAGE_UNPROVEN"
            for outcome in outcomes
        )
    )

    if all_count_targets_reached:

        print(
            "FINAL_VERDICT:",
            "COUNT_TARGET_REACHED__GLOBAL_COVERAGE_UNPROVEN",
        )

    else:

        print(
            "FINAL_VERDICT:",
            "NOT_ALL_VIRTUAL_LEAVES_BELOW_5000",
        )

    print(
        "READY_FOR_PRODUCTION_NUMERIC_COMMIT:",
        False,
    )

    return 2


# ================================================================
# 22 — MAIN EXECUTION
# ================================================================

async def execute() -> int:

    verify_source_contract()

    roots, overview, statuses = (
        read_discovery_roots()
    )

    if not roots:

        print(
            "NO_ELIGIBLE_CHECKPOINT_FREE_FAILED_ROOTS"
        )

        return 2

    state = RuntimeState()

    outcomes: list[RootOutcome] = []

    aborted_reason = ""

    # Existing manager lock:
    # connection scoped, same ownership protocol
    # used by the Discovery runtime.
    #
    # Do not call discovery._count().
    # It writes response archives.
    #
    # Do not call split_bucket().
    # The experiment must never commit children.

    with manager_lock() as lock_connection:

        assert_manager_lock(
            lock_connection
        )

        async with FixedRateDbdClient(
            concurrency=1,
            rate_per_second=API_RATE,
            max_retries=settings.dbd_http_retries,
            timeout=settings.request_timeout,
        ) as client:

            api = LiveSearch(
                client,
                lock_connection,
                state,
            )

            try:

                provinces = await read_provinces(
                    api
                )

                categorical_values = {
                    "jp_types": list(JP_TYPES),
                    "jp_statuses": statuses,
                    "business_sizes": list(BUSINESS_SIZES),
                    "pv_codes": provinces,
                }

                print(
                    "CATEGORY_CARDINALITIES:",
                    {
                        dimension: len(values)
                        for dimension, values
                        in categorical_values.items()
                    },
                )

                for row in roots:

                    if (
                        state.planned_requests
                        >= MAX_PLANNED_SEARCH_REQUESTS
                    ):

                        aborted_reason = (
                            "Global request budget exhausted"
                        )

                        break

                    outcome = await explore_root(
                        api,
                        row,
                        categorical_values,
                    )

                    outcomes.append(
                        outcome
                    )

            finally:

                state.actual_http_attempts = (
                    client.requests_made
                )

                state.http_retries = (
                    client.retries_made
                )

                state.rate_limit_hits = (
                    client.rate_limit_hits
                )

    return print_final_report(
        outcomes,
        state,
        len(roots),
        aborted_reason=aborted_reason,
    )


# ================================================================
# 23 — PROCESS EXIT HANDLING
# ================================================================

def main() -> int:

    try:

        return asyncio.run(
            execute()
        )

    except KeyboardInterrupt:

        print(
            "\nINTERRUPTED_BY_USER"
        )

        print(
            "PRODUCTION_WRITES_ISSUED_BY_SCRIPT=0"
        )

        return 130

    except (
        UnsafeEnvironment,
        RuntimeError,
    ) as exc:

        print(
            "FATAL_SAFETY_OR_CONFIGURATION_ERROR:",
            type(exc).__name__,
            str(exc),
        )

        return 3

    except Exception as exc:

        print(
            "FATAL_TEST_HARNESS_ERROR:",
            type(exc).__name__,
            str(exc),
        )

        return 3


if __name__ == "__main__":
    sys.exit(
        main()
    )