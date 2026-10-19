#!/usr/bin/env python
"""Read-only live probe for numeric Discovery partitions from existing failed buckets.

This script performs SELECT queries and read-only DBD Search requests only.
It never creates/updates buckets, archives responses, or writes page checkpoints.
A passing count/page probe is evidence for this bucket, not production-wide API
certification. Numeric NULL and boundary semantics still require independent proof.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from dbd.client.async_client import AsyncDbdClient
from dbd.client.endpoints.search import (
    ITEMS_PER_PAGE,
    MAX_PAGE,
    MAX_ROWS_PER_QUERY,
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    parse_search_response,
)
from dbd.contracts.common import money
from dbd.core.exceptions import ContractError
from dbd.core.settings import settings
from dbd.pipelines.discovery import _filters_from_row
from dbd.pipelines.discovery_numeric import plan_numeric_split
from dbd.runtime.locking import assert_manager_lock, manager_lock
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.discovery_repository import numeric_schema_ready

DIMENSION_ORDER = (
    ("cap_amt", "capAmt"),
    ("total_income", "totalIncome"),
    ("net_profit", "netProfit"),
    ("total_asset", "totalAsset"),
)
CATEGORICAL_FIELDS = (
    ("jp_types", "jpTypeCode"),
    ("pv_codes", "pvCode"),
    ("jp_statuses", "jpStatCode"),
    ("business_sizes", "businessSizeCode"),
)
NUMERIC_FIELDS = (
    ("cap_amt", "capAmt"),
    ("total_income", "totalIncome"),
    ("net_profit", "netProfit"),
    ("total_asset", "totalAsset"),
)
DEFAULT_STEP = Decimal("0.01")

CANDIDATE_SQL = """
SELECT b.*
FROM discovery_buckets AS b
WHERE b.status = 'failed'
  AND b.total_items > %s
  AND COALESCE(b.pages_done, 0) = 0
  AND (
      b.lease_owner IS NULL
      OR b.lease_expires_at IS NULL
      OR b.lease_expires_at <= UTC_TIMESTAMP(6)
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
  AND (
      COALESCE(b.last_error, '') LIKE %s
      OR COALESCE(b.last_error, '') LIKE %s
  )
  {bucket_clause}
ORDER BY b.total_items DESC, b.depth DESC, b.id
LIMIT %s
"""


class ProbeBudgetExceeded(RuntimeError):
    pass


class ProbeRequestFailed(RuntimeError):
    pass


@dataclass
class LiveObservation:
    count: int
    rows: list[dict]
    total_pages: int
    current_page: int
    items_per_page: int


@dataclass
class ProbeContext:
    client: AsyncDbdClient
    manager_connection: Any
    max_requests: int
    max_nodes: int
    max_depth: int
    step: Decimal
    verify_pages: bool
    requests: int = 0
    nodes: int = 0
    stop_reason: str | None = None
    fatal_error: dict | None = None
    leaf_identity_owner: dict[tuple[str, str], str] = field(default_factory=dict)
    leaf_counts: list[int] = field(default_factory=list)
    elapsed_started: float = field(default_factory=time.monotonic)

    def event(self, name: str, **fields: Any) -> None:
        record = {
            "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "event": name,
            **fields,
        }
        print(json.dumps(record, ensure_ascii=False, default=str), file=sys.stderr, flush=True)

    async def fetch(
        self,
        filters: SearchFilters,
        *,
        page_no: int,
        label: str,
    ) -> LiveObservation:
        if self.stop_reason:
            raise ProbeRequestFailed(self.stop_reason)
        if self.requests >= self.max_requests:
            self.stop_reason = "REQUEST_BUDGET_EXHAUSTED"
            raise ProbeBudgetExceeded(self.stop_reason)

        try:
            await asyncio.to_thread(assert_manager_lock, self.manager_connection)
        except Exception as exc:
            self.stop_reason = "MANAGER_LOCK_LOST"
            self.fatal_error = {"type": type(exc).__name__, "message": str(exc)[:240]}
            self.event("MANAGER_LOCK_LOST", label=label, error=self.fatal_error)
            raise ProbeRequestFailed(self.stop_reason) from exc

        self.requests += 1
        started = time.monotonic()
        try:
            data = await self.client.post(
                SEARCH_PATH,
                filters.payload(page=page_no),
                referer=SEARCH_REFERER,
                timeout=settings.dbd_count_timeout_seconds,
                rate_key="search",
            )
            page = parse_search_response(data)
            if page.current_page != page_no:
                raise ContractError(
                    f"Expected page {page_no}, received page {page.current_page}"
                )
            if page.items_per_page != ITEMS_PER_PAGE:
                raise ContractError(
                    f"Expected {ITEMS_PER_PAGE} rows/page, received {page.items_per_page}"
                )
            if len(page.rows) > ITEMS_PER_PAGE:
                raise ContractError("Search returned more rows than the backend page limit")
            if page.total_items > 0 and not page.rows:
                raise ContractError("Non-empty Search count returned an empty page")
            if page.total_items == 0 and page.total_pages not in (0, 1):
                raise ContractError("Empty Search count returned inconsistent pagination")
            observation = LiveObservation(
                count=page.total_items,
                rows=page.rows,
                total_pages=page.total_pages,
                current_page=page.current_page,
                items_per_page=page.items_per_page,
            )
            self.event(
                "REQUEST_OK",
                label=label,
                page=page_no,
                total_items=page.total_items,
                total_pages=page.total_pages,
                rows=len(page.rows),
                elapsed_ms=round((time.monotonic() - started) * 1000, 1),
            )
            return observation
        except Exception as exc:
            self.stop_reason = "UPSTREAM_OR_RESPONSE_CONTRACT_ERROR"
            self.fatal_error = {"type": type(exc).__name__, "message": str(exc)[:240]}
            self.event(
                "REQUEST_FAILED",
                label=label,
                page=page_no,
                error=self.fatal_error,
                elapsed_ms=round((time.monotonic() - started) * 1000, 1),
            )
            raise ProbeRequestFailed(self.stop_reason) from exc


def _identity(row: dict) -> tuple[str, str] | None:
    jp_type = row.get("jpTypeCode")
    jp_no = row.get("jpNo")
    if not isinstance(jp_type, str) or not jp_type:
        return None
    if not isinstance(jp_no, str) or not jp_no:
        return None
    return jp_type, jp_no


def _identity_sequence(rows: list[dict]) -> list[tuple[str, str]] | None:
    identities = [_identity(row) for row in rows]
    if any(identity is None for identity in identities):
        return None
    typed = [identity for identity in identities if identity is not None]
    return typed if len(set(typed)) == len(typed) else None


def _sample_mismatch(filters: SearchFilters, rows: list[dict]) -> str | None:
    for row in rows:
        for attribute, raw_key in CATEGORICAL_FIELDS:
            selected = getattr(filters, attribute)
            if selected and (
                row.get(raw_key) is None or str(row[raw_key]) not in selected
            ):
                return f"sample_filter_mismatch:{attribute}"

        for dimension, raw_key in NUMERIC_FIELDS:
            lower = getattr(filters, dimension + "_min")
            upper = getattr(filters, dimension + "_max")
            if lower is None and upper is None:
                continue
            try:
                amount = money(row.get(raw_key))
            except ContractError:
                return f"sample_invalid_numeric_value:{dimension}"
            if amount is None:
                return f"sample_null_in_filtered_dimension:{dimension}"
            if lower is not None and amount < lower:
                return f"sample_below_min:{dimension}"
            if upper is not None and amount > upper:
                return f"sample_above_max:{dimension}"

    return None


def _candidate_mismatch(
    parent: SearchFilters,
    parent_observation: LiveObservation,
    dimension: str,
    plan: Any,
    step: Decimal,
) -> str | None:
    if not plan.candidate:
        return plan.reason
    if len(plan.children) != 2 or len(plan.child_observations) != 2:
        return "candidate_child_count_mismatch"

    children = plan.children
    observations = plan.child_observations
    counts = [int(item.count) for item in observations]
    if sum(counts) != parent_observation.count:
        return "child_count_sum_mismatch"
    if any(count <= 0 or count >= parent_observation.count for count in counts):
        return "no_real_progress"

    if any(child.keyword != parent.keyword or child.search_type != parent.search_type for child in children):
        return "parent_search_identity_changed"
    for attribute, _ in CATEGORICAL_FIELDS:
        if any(getattr(child, attribute) != getattr(parent, attribute) for child in children):
            return f"parent_filter_changed:{attribute}"

    # Exactly one dimension may change, and its two child intervals must
    # retain the parent bounds while separating by the tested decimal step.
    for other_dimension, _ in NUMERIC_FIELDS:
        for bound in ("min", "max"):
            original = getattr(parent, other_dimension + "_" + bound)
            for child in children:
                value = getattr(child, other_dimension + "_" + bound)
                if other_dimension != dimension and value != original:
                    return f"unrelated_numeric_constraint_changed:{other_dimension}"
    lower = getattr(parent, dimension + "_min")
    upper = getattr(parent, dimension + "_max")
    left, right = children
    left_min = getattr(left, dimension + "_min")
    left_max = getattr(left, dimension + "_max")
    right_min = getattr(right, dimension + "_min")
    right_max = getattr(right, dimension + "_max")
    if left_min != lower or right_max != upper:
        return "parent_numeric_bounds_not_preserved"
    if left_max is None or right_min is None or right_min != left_max + step:
        return "candidate_intervals_not_adjacent"

    parent_sample_error = _sample_mismatch(parent, parent_observation.rows)
    if parent_sample_error:
        return f"parent_{parent_sample_error}"
    parent_ids = _identity_sequence(parent_observation.rows)
    if parent_ids is None:
        return "parent_sample_identity_missing_or_duplicate"

    raw_key = dict(DIMENSION_ORDER)[dimension]
    for row in parent_observation.rows:
        try:
            amount = money(row.get(raw_key))
        except ContractError:
            return f"parent_sample_invalid_numeric_value:{dimension}"
        if amount is None:
            return f"parent_sample_contains_null:{dimension}"
        matches = 0
        for child in children:
            child_min = getattr(child, dimension + "_min")
            child_max = getattr(child, dimension + "_max")
            matches += int(
                (child_min is None or amount >= child_min)
                and (child_max is None or amount <= child_max)
            )
        if matches != 1:
            return f"parent_sample_not_covered_exactly_once:{dimension}"

    sample_sets: list[set[tuple[str, str]]] = []
    for child, observation in zip(children, observations, strict=True):
        mismatch = _sample_mismatch(child, observation.rows)
        if mismatch:
            return mismatch
        identities = _identity_sequence(observation.rows)
        if identities is None:
            return "child_sample_identity_missing_or_duplicate"
        sample_sets.append(set(identities))
    if sample_sets[0] & sample_sets[1]:
        return "child_first_page_identity_overlap"

    return None


def _new_node(path: str, depth: int, filters: SearchFilters, expected_count: int | None) -> dict:
    return {
        "path": path,
        "depth": depth,
        "filter_key": filters.key(),
        "expected_count": expected_count,
        "live_count": None,
        "status": "PROBING",
        "reason": None,
        "split_dimension": None,
        "attempts": [],
        "dimension_failures": [],
        "children": [],
    }


async def _verify_leaf_pages(
    context: ProbeContext,
    filters: SearchFilters,
    first_page: LiveObservation,
    *,
    path: str,
) -> dict:
    total = first_page.count
    pages = first_page.total_pages
    if total > MAX_ROWS_PER_QUERY:
        return {"status": "FAILED", "reason": "leaf_exceeds_query_limit"}
    if total == 0:
        if pages not in (0, 1):
            return {"status": "FAILED", "reason": "empty_leaf_pagination_mismatch"}
        context.leaf_counts.append(0)
        return {"status": "PASSED", "pages_checked": 0, "unique_identities": 0}
    if pages < 1 or pages > MAX_PAGE:
        return {"status": "FAILED", "reason": "leaf_page_count_out_of_range"}

    seen: set[tuple[str, str]] = set()
    for page_no in range(1, pages + 1):
        if page_no == 1:
            observation = first_page
        else:
            observation = await context.fetch(
                filters, page_no=page_no, label=f"{path}:page-audit",
            )
            if (
                observation.count != total
                or observation.total_pages != pages
                or observation.items_per_page != ITEMS_PER_PAGE
            ):
                context.stop_reason = "PAGE_COUNT_DRIFT"
                return {"status": "FAILED", "reason": "page_metadata_changed"}
        if observation.current_page != page_no:
            context.stop_reason = "PAGE_NUMBER_MISMATCH"
            return {"status": "FAILED", "reason": "wrong_page_returned"}
        if page_no < pages and not observation.rows:
            context.stop_reason = "EMPTY_INTERMEDIATE_PAGE"
            return {"status": "FAILED", "reason": "empty_intermediate_page"}
        mismatch = _sample_mismatch(filters, observation.rows)
        if mismatch:
            return {"status": "FAILED", "reason": mismatch}
        identities = _identity_sequence(observation.rows)
        if identities is None:
            context.stop_reason = "DUPLICATE_OR_MISSING_IDENTITY"
            return {"status": "FAILED", "reason": "page_identity_missing_or_duplicate"}
        for identity in identities:
            if identity in seen:
                context.stop_reason = "DUPLICATE_IDENTITY_WITHIN_LEAF"
                return {"status": "FAILED", "reason": "duplicate_identity_within_leaf"}
            if identity in context.leaf_identity_owner:
                context.stop_reason = "DUPLICATE_IDENTITY_ACROSS_LEAVES"
                return {
                    "status": "FAILED",
                    "reason": "duplicate_identity_across_leaves",
                    "other_leaf": context.leaf_identity_owner[identity],
                }
            seen.add(identity)
            context.leaf_identity_owner[identity] = path

    if len(seen) != total:
        return {
            "status": "FAILED",
            "reason": "unique_identity_count_mismatch",
            "expected": total,
            "actual": len(seen),
        }
    context.leaf_counts.append(total)
    return {"status": "PASSED", "pages_checked": pages, "unique_identities": len(seen)}


async def _explore_node(
    context: ProbeContext,
    filters: SearchFilters,
    *,
    path: str,
    depth: int,
    expected_count: int | None,
    expected_observation: LiveObservation | None = None,
) -> dict:
    node = _new_node(path, depth, filters, expected_count)
    if context.stop_reason:
        node.update(status="NOT_ATTEMPTED", reason=context.stop_reason)
        return node
    if context.nodes >= context.max_nodes:
        node.update(status="INCOMPLETE_NODE_LIMIT", reason="max_nodes_reached")
        return node
    context.nodes += 1

    try:
        current = await context.fetch(filters, page_no=1, label=f"{path}:count")
    except (ProbeBudgetExceeded, ProbeRequestFailed) as exc:
        node.update(status="INCOMPLETE", reason=context.stop_reason or str(exc))
        return node

    node["live_count"] = current.count
    if expected_count is not None and current.count != expected_count:
        context.stop_reason = "CHILD_COUNT_DRIFT"
        node.update(
            status="BLOCKED_COUNT_DRIFT",
            reason=f"expected={expected_count},live={current.count}",
        )
        return node
    if expected_observation is not None:
        old_ids = _identity_sequence(expected_observation.rows)
        new_ids = _identity_sequence(current.rows)
        if old_ids is None or new_ids is None or old_ids != new_ids:
            context.stop_reason = "CHILD_FIRST_PAGE_IDENTITY_DRIFT"
            node.update(status="BLOCKED_IDENTITY_DRIFT", reason=context.stop_reason)
            return node

    if current.count <= MAX_ROWS_PER_QUERY:
        if context.verify_pages:
            try:
                audit = await _verify_leaf_pages(context, filters, current, path=path)
            except (ProbeBudgetExceeded, ProbeRequestFailed) as exc:
                audit = {"status": "INCOMPLETE", "reason": context.stop_reason or str(exc)}
            node["page_audit"] = audit
            if audit["status"] == "PASSED":
                node.update(status="LEAF_PAGES_VERIFIED", reason="all_pages_and_identities_checked")
            else:
                node.update(status="LEAF_PAGE_AUDIT_FAILED", reason=audit.get("reason"))
        else:
            node.update(
                status="LEAF_COUNTED_ONLY",
                reason="within_10000_limit; pagination_not_enumerated",
            )
        return node

    if depth >= context.max_depth:
        node.update(status="INCOMPLETE_DEPTH_LIMIT", reason="max_depth_reached")
        return node

    for dimension, raw_key in DIMENSION_ORDER:
        if context.stop_reason:
            node.update(status="INCOMPLETE", reason=context.stop_reason)
            return node
        sample_problem = None
        for sample in current.rows:
            try:
                sample_amount = money(sample.get(raw_key))
            except ContractError:
                sample_problem = "parent_first_page_has_invalid_numeric_value"
                break
            if sample_amount is None:
                sample_problem = "parent_first_page_has_null_or_missing_numeric_value"
                break
        if sample_problem:
            node["dimension_failures"].append({"dimension": dimension, "reason": sample_problem})
            context.event("DIMENSION_SKIPPED", path=path, dimension=dimension, reason=sample_problem)
            continue

        trial_index = 0

        async def read_count(candidate: SearchFilters) -> LiveObservation:
            nonlocal trial_index
            trial_index += 1
            side = "left" if trial_index % 2 else "right"
            trial = (trial_index + 1) // 2
            return await context.fetch(
                candidate,
                page_no=1,
                label=f"{path}:{dimension}:trial-{trial}:{side}",
            )

        context.event(
            "DIMENSION_START",
            path=path,
            dimension=dimension,
            parent_count=current.count,
            step=format(context.step, "f"),
        )
        try:
            plan = await plan_numeric_split(
                filters,
                current.count,
                read_count,
                current.rows,
                dimensions=(dimension,),
                max_total_trials=4,
                max_dimension_trials=4,
                step=context.step,
            )
        except (ProbeBudgetExceeded, ProbeRequestFailed) as exc:
            node.update(status="INCOMPLETE", reason=context.stop_reason or str(exc))
            return node
        except ContractError as exc:
            reason = f"numeric_sample_contract_error:{str(exc)[:120]}"
            node["dimension_failures"].append({"dimension": dimension, "reason": reason})
            context.event("DIMENSION_REJECTED", path=path, dimension=dimension, reason=reason)
            continue

        node["attempts"].extend(
            {"dimension": dimension, **attempt} for attempt in plan.attempts
        )
        if not plan.candidate:
            reason = plan.reason
            node["dimension_failures"].append({"dimension": dimension, "reason": reason})
            context.event("DIMENSION_REJECTED", path=path, dimension=dimension, reason=reason)
            continue

        mismatch = _candidate_mismatch(filters, current, dimension, plan, context.step)
        if mismatch:
            node["dimension_failures"].append({"dimension": dimension, "reason": mismatch})
            context.event("CANDIDATE_REJECTED", path=path, dimension=dimension, reason=mismatch)
            continue

        try:
            confirmed = await context.fetch(
                filters, page_no=1, label=f"{path}:{dimension}:parent-confirm",
            )
        except (ProbeBudgetExceeded, ProbeRequestFailed) as exc:
            node.update(status="INCOMPLETE", reason=context.stop_reason or str(exc))
            return node
        if (
            confirmed.count != current.count
            or _identity_sequence(confirmed.rows) != _identity_sequence(current.rows)
        ):
            context.stop_reason = "PARENT_COUNT_OR_IDENTITY_DRIFT"
            node.update(status="BLOCKED_PARENT_DRIFT", reason=context.stop_reason)
            context.event(
                "PARENT_DRIFT",
                path=path,
                before_count=current.count,
                after_count=confirmed.count,
            )
            return node

        node["status"] = "SPLIT_CANDIDATE"
        node["split_dimension"] = dimension
        node["reason"] = "count_and_sample_checks_passed_observation_only"
        context.event(
            "SPLIT_CANDIDATE_ACCEPTED",
            path=path,
            dimension=dimension,
            parent_count=current.count,
            child_counts=plan.child_counts,
            next_step="recurse_without_persisting",
        )
        for index, child in enumerate(plan.children):
            child_path = f"{path}/{dimension}[{index + 1}]"
            child_node = await _explore_node(
                context,
                child,
                path=child_path,
                depth=depth + 1,
                expected_count=plan.child_counts[index],
                expected_observation=plan.child_observations[index],
            )
            node["children"].append(child_node)
        if all(
            child["status"] in ("LEAF_COUNTED_ONLY", "LEAF_PAGES_VERIFIED")
            or child["status"] == "SPLIT_CANDIDATE"
            and _tree_complete(child)
            for child in node["children"]
        ):
            node["reason"] = "all_descendants_within_limit_observation_only"
        else:
            node["reason"] = "one_or_more_descendants_unresolved"
        return node

    node.update(
        status="UNSPLITTABLE",
        reason="all_dimensions_exhausted_without_a_valid_count_candidate",
    )
    context.event(
        "NODE_UNSPLITTABLE",
        path=path,
        count=current.count,
        dimensions=[dimension for dimension, _ in DIMENSION_ORDER],
    )
    return node


def _tree_complete(node: dict) -> bool:
    if node["status"] in ("LEAF_COUNTED_ONLY", "LEAF_PAGES_VERIFIED"):
        return True
    if node["status"] != "SPLIT_CANDIDATE" or len(node.get("children", [])) != 2:
        return False
    return all(_tree_complete(child) for child in node["children"])


def _tree_stats(node: dict) -> dict:
    result = {
        "nodes": 1,
        "split_candidates": int(node["status"] == "SPLIT_CANDIDATE"),
        "leaves_within_limit": int(node["status"] in ("LEAF_COUNTED_ONLY", "LEAF_PAGES_VERIFIED")),
        "pages_verified_leaves": int(node["status"] == "LEAF_PAGES_VERIFIED"),
        "unresolved_nodes": int(
            node["status"] not in ("SPLIT_CANDIDATE", "LEAF_COUNTED_ONLY", "LEAF_PAGES_VERIFIED")
        ),
    }
    for child in node.get("children", []):
        child_stats = _tree_stats(child)
        for key in result:
            result[key] += child_stats[key]
    return result


def _select_candidates(connection: Any, *, limit: int, bucket_id: int | None) -> list[dict]:
    bucket_clause = "AND b.id = %s" if bucket_id is not None else ""
    sql = CANDIDATE_SQL.format(bucket_clause=bucket_clause)
    params: list[Any] = [
        MAX_ROWS_PER_QUERY,
        "%no count-validated, non-overlapping split is available%",
        "%numeric:%",
    ]
    if bucket_id is not None:
        params.append(bucket_id)
    params.append(limit)
    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        return list(cursor.fetchall())


def _candidate_summary(row: dict) -> dict:
    return {
        "bucket_id": int(row["id"]),
        "parent_id": row.get("parent_id"),
        "bucket_key": row.get("bucket_key"),
        "keyword": row.get("keyword"),
        "jp_types": row.get("jp_types"),
        "total_items": int(row["total_items"]),
        "pages_total": row.get("pages_total"),
        "pages_done": row.get("pages_done"),
        "depth": int(row["depth"]),
        "status": row["status"],
        "last_error": (row.get("last_error") or "")[:300],
    }


def _print_report(report: dict, *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return
    print("DBD LIVE NUMERIC FILTER PROBE")
    print(f"Mode: {report['mode']} | Read-only: {report['read_only']}")
    print(f"Database: {report['database']} | Selected buckets: {len(report['buckets'])}")
    for item in report["buckets"]:
        print(
            f"Bucket {item['bucket_id']}: stored={item.get('stored_count')} "
            f"result={item.get('result')} requests={item.get('search_requests')}"
        )
        if item.get("tree"):
            stats = _tree_stats(item["tree"])
            print(
                f"  nodes={stats['nodes']} splits={stats['split_candidates']} "
                f"leaves<={MAX_ROWS_PER_QUERY}={stats['leaves_within_limit']} "
                f"unresolved={stats['unresolved_nodes']}"
            )
        if item.get("reason"):
            print(f"  reason={item['reason']}")
        if item.get("page_audit"):
            print(f"  page_audit={item['page_audit']}")
    print(f"Search requests: {report['search_requests']} / {report['max_requests']}")
    print(f"Transport retries: {report['transport_retries']} | HTTP 429s: {report['rate_limit_hits']}")
    print(f"Conclusion: {report['conclusion']}")


async def _run_live_probe(
    candidates: list[dict],
    *,
    manager_connection: Any,
    rate: float,
    max_requests: int,
    max_nodes: int,
    max_depth: int,
    step: Decimal,
    verify_pages: bool,
) -> tuple[list[dict], ProbeContext]:
    reports: list[dict] = []
    async with AsyncDbdClient(
        concurrency=1,
        rate_per_second=rate,
        max_retries=0,
        endpoint_rates={"search": rate},
    ) as client:
        context = ProbeContext(
            client=client,
            manager_connection=manager_connection,
            max_requests=max_requests,
            max_nodes=max_nodes,
            max_depth=max_depth,
            step=step,
            verify_pages=verify_pages,
        )
        for row in candidates:
            summary = _candidate_summary(row)
            item_report = {
                **summary,
                "stored_count": int(row["total_items"]),
                "result": None,
                "reason": None,
                "tree": None,
                "page_audit": None,
            }
            if context.stop_reason:
                item_report.update(result="NOT_ATTEMPTED", reason=context.stop_reason)
                reports.append(item_report)
                continue
            context.leaf_identity_owner.clear()
            context.leaf_counts.clear()
            try:
                filters = _filters_from_row(row)
                if filters.key() != row.get("bucket_key"):
                    item_report.update(
                        result="BLOCKED_FILTER_RECONSTRUCTION_MISMATCH",
                        reason=(
                            "Persisted numeric_filters do not reconstruct the exact bucket_key; "
                            "refusing to send a different query to DBD."
                        ),
                    )
                    reports.append(item_report)
                    continue
                tree = await _explore_node(
                    context,
                    filters,
                    path=f"bucket-{int(row['id'])}",
                    depth=0,
                    expected_count=int(row["total_items"]),
                )
                item_report["tree"] = tree
                stats = _tree_stats(tree)
                if not _tree_complete(tree):
                    item_report["result"] = "INCOMPLETE_OR_UNSPLITTABLE"
                    item_report["reason"] = tree.get("reason") or tree.get("status")
                elif verify_pages:
                    item_report["result"] = "PAGE_AUDIT_PASSED_OBSERVATION_ONLY"
                    item_report["page_audit"] = {
                        "leaf_count_sum": sum(context.leaf_counts),
                        "unique_identities_across_leaves": len(context.leaf_identity_owner),
                        "root_live_count": tree.get("live_count"),
                        "leaf_identity_total_matches_root": (
                            sum(context.leaf_counts) == tree.get("live_count")
                            and len(context.leaf_identity_owner) == tree.get("live_count")
                        ),
                        "note": "Observed bucket only; this is not global API certification.",
                    }
                    if not item_report["page_audit"]["leaf_identity_total_matches_root"]:
                        item_report["result"] = "PAGE_AUDIT_COUNT_MISMATCH"
                        item_report["reason"] = "leaf_identity_total_does_not_match_root_count"
                else:
                    item_report["result"] = "COUNT_TREE_COMPLETE_OBSERVATION_ONLY"
                    item_report["reason"] = (
                        "Every observed leaf is <= 10000, but leaf pages and complete identities "
                        "were not enumerated. Not a coverage certificate."
                    )
                item_report["tree_stats"] = stats
            except (ProbeBudgetExceeded, ProbeRequestFailed) as exc:
                item_report["result"] = "INCOMPLETE"
                item_report["reason"] = context.stop_reason or str(exc)
            except Exception as exc:
                item_report["result"] = "ERROR"
                item_report["reason"] = f"{type(exc).__name__}: {str(exc)[:240]}"
                context.stop_reason = context.stop_reason or "UNEXPECTED_ERROR"
                context.fatal_error = {"type": type(exc).__name__, "message": str(exc)[:240]}
            reports.append(item_report)

        context.event(
            "PROBE_FINISHED",
            selected_buckets=len(candidates),
            search_requests=context.requests,
            transport_requests=client.requests_made,
            retries=client.retries_made,
            rate_limit_hits=client.rate_limit_hits,
            stop_reason=context.stop_reason,
        )
        transport = {
            "search_requests": context.requests,
            "transport_requests": client.requests_made,
            "transport_retries": client.retries_made,
            "rate_limit_hits": client.rate_limit_hits,
            "nodes_examined": context.nodes,
            "elapsed_seconds": round(time.monotonic() - context.elapsed_started, 2),
            "stop_reason": context.stop_reason,
            "fatal_error": context.fatal_error,
        }
    return reports, context


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Probe live DBD numeric filters against existing failed Discovery buckets. "
            "Read-only: no bucket/page/archive/checkpoint writes."
        )
    )
    parser.add_argument("--bucket-id", type=int, help="Probe one exact failed bucket ID")
    parser.add_argument("--limit", type=int, default=1, help="Candidate buckets to select (default: 1)")
    parser.add_argument("--list-candidates", action="store_true", help="Only list eligible SQL candidates; no API calls")
    parser.add_argument("--rate", type=float, default=0.25, help="Search requests per second (default: 0.25)")
    parser.add_argument("--max-requests", type=int, default=120, help="Global Search request budget (default: 120)")
    parser.add_argument("--max-nodes", type=int, default=100, help="Maximum recursive nodes (default: 100)")
    parser.add_argument("--max-depth", type=int, default=24, help="Maximum split depth (default: 24)")
    parser.add_argument("--step", default="0.01", help="Candidate decimal step; hypothesis only (default: 0.01)")
    parser.add_argument(
        "--verify-pages",
        action="store_true",
        help="Enumerate every leaf page and audit unique identities; may require many requests",
    )
    parser.add_argument("--json", action="store_true", help="Print final report as JSON")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.limit < 1:
        raise SystemExit("--limit must be positive")
    if args.bucket_id is not None and args.bucket_id < 1:
        raise SystemExit("--bucket-id must be positive")
    if not 0 < args.rate <= 10:
        raise SystemExit("--rate must be greater than 0 and no more than 10")
    if args.max_requests < 1 or args.max_nodes < 1 or args.max_depth < 1:
        raise SystemExit("--max-requests, --max-nodes and --max-depth must be positive")
    try:
        step = Decimal(str(args.step))
    except (InvalidOperation, ValueError):
        raise SystemExit("--step must be a finite positive decimal") from None
    if not step.is_finite() or step <= 0:
        raise SystemExit("--step must be a finite positive decimal")

    if args.list_candidates:
        with connect() as connection:
            candidates = _select_candidates(
                connection, limit=args.limit, bucket_id=args.bucket_id,
            )
        output = {
            "mode": "candidate_list",
            "read_only": True,
            "database": settings.db_name,
            "candidate_count": len(candidates),
            "buckets": [_candidate_summary(row) for row in candidates],
            "note": "Transient HTTP/auth failures are excluded from numeric partition candidates.",
        }
        _print_report(output, json_output=args.json)
        return 0

    started = time.monotonic()
    with manager_lock() as connection:
        if not numeric_schema_ready():
            raise SystemExit(
                "BLOCKED: migration 0013 numeric_filters JSON column is missing; "
                "cannot reconstruct persisted numeric bounds safely."
            )
        candidates = _select_candidates(
            connection, limit=args.limit, bucket_id=args.bucket_id,
        )
        if not candidates:
            output = {
                "mode": "live_numeric_filter_probe",
                "read_only": True,
                "database": settings.db_name,
                "buckets": [],
                "search_requests": 0,
                "max_requests": args.max_requests,
                "conclusion": (
                    "NO_ELIGIBLE_CANDIDATES: no oversized failed leaf with a partition-related "
                    "error, no saved pages, no children and no active lease was found."
                ),
            }
            _print_report(output, json_output=args.json)
            return 0

        reports, context = asyncio.run(
            _run_live_probe(
                candidates,
                manager_connection=connection,
                rate=args.rate,
                max_requests=args.max_requests,
                max_nodes=args.max_nodes,
                max_depth=args.max_depth,
                step=step,
                verify_pages=args.verify_pages,
            )
        )

    all_complete = bool(reports) and all(
        item.get("tree") is not None and _tree_complete(item["tree"]) for item in reports
    )
    any_tree = any(item.get("tree") for item in reports)
    if all_complete and any_tree:
        if args.verify_pages and all(
            item.get("result") == "PAGE_AUDIT_PASSED_OBSERVATION_ONLY" for item in reports
        ):
            conclusion = "OBSERVED_BUCKET_PAGE_AUDITS_PASSED_NOT_PRODUCTION_CERTIFIED"
        elif not args.verify_pages:
            conclusion = "COUNT_PARTITIONS_LOOK_FEASIBLE_PAGE_COVERAGE_NOT_VERIFIED"
        else:
            conclusion = "INCOMPLETE_PAGE_AUDIT"
    else:
        conclusion = "NUMERIC_PARTITION_PROBE_INCOMPLETE_OR_UNSPLITTABLE"

    output = {
        "mode": "live_numeric_filter_probe",
        "read_only": True,
        "database": settings.db_name,
        "dimension_order": [dimension for dimension, _ in DIMENSION_ORDER],
        "candidate_step": format(step, "f"),
        "candidate_step_status": "UNVERIFIED_API_BOUNDARY_HYPOTHESIS",
        "selected_buckets": len(candidates),
        "buckets": reports,
        **{
            "search_requests": context.requests,
            "transport_requests": context.client.requests_made if context.client else context.requests,
            "transport_retries": context.client.retries_made if context.client else 0,
            "rate_limit_hits": context.client.rate_limit_hits if context.client else 0,
            "nodes_examined": context.nodes,
            "max_requests": args.max_requests,
            "elapsed_seconds": round(time.monotonic() - started, 2),
            "stop_reason": context.stop_reason,
            "fatal_error": context.fatal_error,
            "conclusion": conclusion,
        },
        "safety": {
            "database_mutations": False,
            "response_archive_writes": False,
            "bucket_or_page_checkpoint_writes": False,
            "production_numeric_certification": False,
            "manager_advisory_lock": "held during live API probe",
        },
    }
    _print_report(output, json_output=args.json)
    successful_observations = {
        "COUNT_PARTITIONS_LOOK_FEASIBLE_PAGE_COVERAGE_NOT_VERIFIED",
        "OBSERVED_BUCKET_PAGE_AUDITS_PASSED_NOT_PRODUCTION_CERTIFIED",
    }
    return 0 if conclusion in successful_observations else 2


if __name__ == "__main__":
    raise SystemExit(main())
