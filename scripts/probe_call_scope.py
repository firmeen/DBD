"""Probe DBD request scope and call-elimination opportunities without persisting responses.

The probe performs live upstream HTTP requests and read-only sampling from `company_master`.
It never archives, projects, mutates queue state, or changes company data. Its purpose is to
separate request reductions that are already proven by endpoint dependencies from candidates
that still need stronger evidence before production may suppress an HTTP call.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dbd.client.async_client import AsyncDbdClient  # noqa: E402
from dbd.client.endpoints.search import (  # noqa: E402
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    parse_search_response,
)
from dbd.contracts.company import normalize  # noqa: E402
from dbd.runtime.resources import FINANCIAL, PARTS, normalize_resource, request_path  # noqa: E402
from dbd.storage.mysql.engine import connect  # noqa: E402

DEFAULT_ENDPOINTS = tuple(
    endpoint for part in ("profile", "details", "financials") for endpoint in PARTS[part]
)
DETAIL_ENDPOINTS = tuple(PARTS["details"])
BENCHMARK = "industry_benchmark"


@dataclass(frozen=True)
class Target:
    kind: str
    number: str
    status: str | None
    province: str | None
    business_size: str | None

    @property
    def stratum(self) -> tuple[str, str, str, str]:
        return (
            self.status or "?",
            self.kind or "?",
            self.province or "?",
            self.business_size or "?",
        )


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def positive_rate(value):
    number = float(value)
    if number <= 0 or not math.isfinite(number):
        raise argparse.ArgumentTypeError("must be a finite positive rate")
    return number


def canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def payload_hash(value) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def flatten_fields(value, prefix="") -> dict[str, object]:
    """Flatten object keys for field-coverage comparison; lists remain atomic values."""
    if not isinstance(value, dict):
        return {}
    result = {}
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            result.update(flatten_fields(item, path))
        else:
            result[path] = item
    return result


def is_nonempty(normalized) -> bool:
    if normalized is None:
        return False
    if isinstance(normalized, (list, tuple, dict, set)):
        return bool(normalized)
    return True


def _error_signature(exc: BaseException) -> str:
    message = str(exc)
    suffix = ""
    if "429" in message:
        suffix = "[429]"
    elif "503" in message:
        suffix = "[503]"
    code = getattr(exc, "code", None) or getattr(exc, "curl_code", None)
    if code is not None:
        suffix += f"[curl={code}]"
    return f"{type(exc).__name__}{suffix}"


def load_stratified_targets(limit: int) -> list[Target]:
    """Choose a stable, broad sample instead of the first N company IDs.

    A deterministic CRC ordering spreads candidates across the master table. Python then
    round-robins observed status/type/province/size strata so one large stratum cannot consume
    the whole sample. This directly avoids the old capacity probe's first-N sampling bias.
    """
    pool_size = min(max(limit * 20, 500), 20_000)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.jp_type_code,m.jp_no,m.observed_jp_stat_code,
                   c.pv_code,c.business_size_code
            FROM company_master m
            LEFT JOIN companies c ON c.jp_no=m.jp_no
            WHERE m.identity_status<>'conflict'
            ORDER BY CRC32(CONCAT(m.jp_type_code,':',m.jp_no))
            LIMIT %s
            """,
            (pool_size,),
        )
        rows = cur.fetchall()

    groups: dict[tuple[str, str, str, str], deque[Target]] = defaultdict(deque)
    for row in rows:
        target = Target(
            kind=str(row["jp_type_code"]),
            number=str(row["jp_no"]),
            status=None if row["observed_jp_stat_code"] is None else str(row["observed_jp_stat_code"]),
            province=None if row["pv_code"] is None else str(row["pv_code"]),
            business_size=None if row["business_size_code"] is None else str(row["business_size_code"]),
        )
        groups[target.stratum].append(target)

    selected = []
    ordered = deque(sorted(groups))
    while ordered and len(selected) < limit:
        key = ordered.popleft()
        bucket = groups[key]
        if bucket:
            selected.append(bucket.popleft())
        if bucket:
            ordered.append(key)
    return selected


async def fetch_search_match(client: AsyncDbdClient, target: Target):
    response = await client.fetch(
        "POST",
        SEARCH_PATH,
        json=SearchFilters(keyword=target.number).payload(page=1),
        referer=SEARCH_REFERER,
        rate_key="search",
    )
    page = parse_search_response(response.data)
    for row in page.rows:
        if str(row.get("jpNo") or "") == target.number and str(row.get("jpTypeCode") or "") == target.kind:
            return row
    return None


async def fetch_endpoint(client: AsyncDbdClient, target: Target, endpoint: str, fiscal_year: str | None):
    params = {"fiscalYear": fiscal_year} if endpoint in FINANCIAL else None
    return await client.fetch(
        "GET",
        request_path(endpoint, target.kind, target.number),
        params=params,
        rate_key=endpoint,
    )


def benchmark_scope(normalized) -> tuple[str, str, str, str, str] | None:
    if normalized is None:
        return None
    return (
        str(normalized.submit_obj_code),
        str(normalized.fiscal_year),
        str(normalized.business_size_code),
        str(normalized.location_type),
        str(normalized.location_no),
    )


async def inspect_target(client: AsyncDbdClient, target: Target, verbose: bool) -> dict:
    result = {
        "target": asdict(target),
        "search": {},
        "profile": {},
        "endpoints": {},
        "errors": [],
        "benchmark": None,
    }

    search_row = None
    try:
        search_row = await fetch_search_match(client, target)
        result["search"] = {
            "matched": search_row is not None,
            "fields": sorted(flatten_fields(search_row).keys()) if search_row else [],
        }
    except Exception as exc:
        result["errors"].append({"endpoint": "search", "error": _error_signature(exc)})

    profile_payload = None
    fiscal_year = None
    try:
        response = await fetch_endpoint(client, target, "profile", None)
        profile_payload = response.data
        profile = normalize(profile_payload)
        if (profile.jp_type_code, profile.jp_no) != (target.kind, target.number):
            raise ValueError("Profile response identity mismatch")
        years = [item for item in str(profile.submit_fin_year or "").split() if item]
        fiscal_year = max(years) if years else None
        search_fields = flatten_fields(search_row)
        profile_fields = flatten_fields(profile_payload)
        overlap = sorted(set(search_fields) & set(profile_fields))
        profile_only = sorted(set(profile_fields) - set(search_fields))
        search_only = sorted(set(search_fields) - set(profile_fields))
        result["profile"] = {
            "fiscal_year": fiscal_year,
            "search_field_count": len(search_fields),
            "profile_field_count": len(profile_fields),
            "overlap_fields": overlap,
            "profile_only_fields": profile_only,
            "search_only_fields": search_only,
            "submit_fin_year_visible_in_search": any(
                field.lower().endswith("submitfinyear") for field in search_fields
            ),
        }
        result["endpoints"]["profile"] = {"ok": True, "nonempty": True}
    except Exception as exc:
        result["errors"].append({"endpoint": "profile", "error": _error_signature(exc)})
        result["endpoints"]["profile"] = {"ok": False, "nonempty": False}
        if verbose:
            print(f"  {target.number} profile FAILED {_error_signature(exc)}")
        return result

    for endpoint in DETAIL_ENDPOINTS:
        try:
            response = await fetch_endpoint(client, target, endpoint, fiscal_year)
            normalized = normalize_resource(endpoint, response.data)
            result["endpoints"][endpoint] = {
                "ok": True,
                "nonempty": is_nonempty(normalized),
                "rows": len(normalized) if isinstance(normalized, list) else None,
                "payload_sha256": payload_hash(response.data),
            }
        except Exception as exc:
            result["errors"].append({"endpoint": endpoint, "error": _error_signature(exc)})
            result["endpoints"][endpoint] = {"ok": False, "nonempty": False}

    if fiscal_year:
        for endpoint in PARTS["financials"]:
            try:
                response = await fetch_endpoint(client, target, endpoint, fiscal_year)
                normalized = normalize_resource(endpoint, response.data)
                endpoint_result = {
                    "ok": True,
                    "nonempty": is_nonempty(normalized),
                    "rows": len(normalized) if isinstance(normalized, list) else None,
                    "payload_sha256": payload_hash(response.data),
                }
                result["endpoints"][endpoint] = endpoint_result
                if endpoint == BENCHMARK:
                    scope = benchmark_scope(normalized)
                    result["benchmark"] = {
                        "scope": list(scope) if scope else None,
                        "payload_sha256": endpoint_result["payload_sha256"],
                    }
            except Exception as exc:
                result["errors"].append({"endpoint": endpoint, "error": _error_signature(exc)})
                result["endpoints"][endpoint] = {"ok": False, "nonempty": False}
    else:
        for endpoint in PARTS["financials"]:
            result["endpoints"][endpoint] = {
                "ok": None,
                "nonempty": False,
                "skipped": "no_submitted_fiscal_year",
            }

    if verbose:
        details = ", ".join(
            f"{endpoint}={'data' if value.get('nonempty') else ('skip' if value.get('skipped') else 'empty')}"
            for endpoint, value in result["endpoints"].items()
        )
        print(f"  {target.number} year={fiscal_year or '-'} | {details}")
    return result


def analyse(results: list[dict]) -> dict:
    sample = len(results)
    fiscal = sum(bool(row.get("profile", {}).get("fiscal_year")) for row in results)
    no_fiscal = sample - fiscal
    errors = Counter(
        f"{item['endpoint']}:{item['error']}"
        for row in results
        for item in row.get("errors", [])
    )

    endpoint_stats = {}
    for endpoint in DEFAULT_ENDPOINTS:
        rows = [row.get("endpoints", {}).get(endpoint, {}) for row in results]
        attempted = sum(item.get("ok") is not None for item in rows)
        successes = sum(item.get("ok") is True for item in rows)
        nonempty = sum(item.get("nonempty") is True for item in rows)
        skipped = sum(bool(item.get("skipped")) for item in rows)
        endpoint_stats[endpoint] = {
            "attempted": attempted,
            "success": successes,
            "nonempty": nonempty,
            "empty_or_null": max(0, successes - nonempty),
            "precondition_skipped": skipped,
        }

    overlap_rows = [row["profile"] for row in results if row.get("profile", {}).get("profile_field_count")]
    profile_overlap = {
        "companies_compared": len(overlap_rows),
        "search_has_submit_fin_year": sum(
            bool(row.get("submit_fin_year_visible_in_search")) for row in overlap_rows
        ),
        "avg_search_fields": (
            sum(row["search_field_count"] for row in overlap_rows) / len(overlap_rows)
            if overlap_rows
            else 0.0
        ),
        "avg_profile_fields": (
            sum(row["profile_field_count"] for row in overlap_rows) / len(overlap_rows)
            if overlap_rows
            else 0.0
        ),
        "avg_overlap_fields": (
            sum(len(row["overlap_fields"]) for row in overlap_rows) / len(overlap_rows)
            if overlap_rows
            else 0.0
        ),
    }

    benchmark_rows = [row["benchmark"] for row in results if row.get("benchmark", {}).get("scope")]
    scope_hashes: dict[tuple[str, ...], set[str]] = defaultdict(set)
    scope_counts = Counter()
    for item in benchmark_rows:
        scope = tuple(item["scope"])
        scope_hashes[scope].add(item["payload_sha256"])
        scope_counts[scope] += 1
    stable_repeated = {
        scope: count
        for scope, count in scope_counts.items()
        if count > 1 and len(scope_hashes[scope]) == 1
    }
    conflicting = {
        scope: len(hashes) for scope, hashes in scope_hashes.items() if len(hashes) > 1
    }
    observed_benchmark_savings = sum(count - 1 for count in stable_repeated.values())

    # Current first-load logical model: profile + 3 details for every company;
    # three financial calls only when Profile exposes a submitted fiscal year.
    current_theoretical_depth = sample * 7
    observed_required_depth = sample * 4 + fiscal * 3
    proven_no_year_savings = no_fiscal * 3
    candidate_benchmark_depth = observed_required_depth - observed_benchmark_savings

    transformations = endpoint_stats.get("transformations", {})
    transformation_success = transformations.get("success", 0)
    transformation_empty = transformations.get("empty_or_null", 0)

    return {
        "sample": sample,
        "fiscal_year": {"found": fiscal, "missing": no_fiscal},
        "endpoint_coverage": endpoint_stats,
        "field_overlap": profile_overlap,
        "benchmark_scope": {
            "eligible_responses": len(benchmark_rows),
            "unique_scopes": len(scope_hashes),
            "repeated_exact_scope_groups": len(stable_repeated),
            "conflicting_scope_groups": len(conflicting),
            "observed_duplicate_calls": observed_benchmark_savings,
            "classification": "CANDIDATE",
            "reason": (
                "Repeated normalized scope with identical payload is strong sample evidence, "
                "but production pre-HTTP suppression still requires endpoint-scope validation."
            ),
        },
        "transformations": {
            "success": transformation_success,
            "empty": transformation_empty,
            "empty_rate": (
                transformation_empty / transformation_success if transformation_success else 0.0
            ),
            "classification": "UNKNOWN",
            "reason": "An empty response alone does not prove absence can be known before HTTP.",
        },
        "elimination": {
            "no_fiscal_year_financial_calls": {
                "calls": proven_no_year_savings,
                "classification": "PROVEN",
                "reason": "Financial requests require a resolved fiscal year and are not sent without one.",
            },
            "profile_from_search": {
                "classification": "CANDIDATE",
                "reason": "Only safe if Search supplies every authoritative dependency field required by Profile.",
            },
            "benchmark_shared_scope": {
                "calls_observed_duplicate": observed_benchmark_savings,
                "classification": "CANDIDATE",
            },
        },
        "projected_calls_per_company": {
            "current_model": 7.0 if sample else 0.0,
            "proven_required": observed_required_depth / sample if sample else 0.0,
            "with_observed_benchmark_coalescing": candidate_benchmark_depth / sample if sample else 0.0,
            "target_4_or_less_proven": bool(sample and observed_required_depth / sample <= 4.0),
            "target_3_5_or_less_proven": bool(sample and observed_required_depth / sample <= 3.5),
        },
        "request_model": {
            "current_theoretical_depth_calls": current_theoretical_depth,
            "proven_required_depth_calls": observed_required_depth,
            "candidate_after_benchmark_coalescing": candidate_benchmark_depth,
            "note": "Probe-only exact Search lookups are excluded from the production depth-call model.",
        },
        "errors": dict(errors),
    }


def print_summary(summary: dict) -> None:
    calls = summary["projected_calls_per_company"]
    fiscal = summary["fiscal_year"]
    benchmark = summary["benchmark_scope"]
    transformations = summary["transformations"]
    print("\n" + "=" * 88)
    print("DBD CALL / SCOPE PROBE — FINAL SUMMARY")
    print("=" * 88)
    print(f"Companies sampled                         {summary['sample']:>8,}")
    print(f"Fiscal year found                         {fiscal['found']:>8,}")
    print(f"No fiscal year                            {fiscal['missing']:>8,}")
    print("\nCALL MODEL (depth HTTP only)")
    print(f"Current theoretical                       {calls['current_model']:>8.2f} calls/company")
    print(f"PROVEN required from sample               {calls['proven_required']:>8.2f} calls/company")
    print(
        f"CANDIDATE with observed benchmark reuse   "
        f"{calls['with_observed_benchmark_coalescing']:>8.2f} calls/company"
    )
    print(f"Target <=4.0 proven                       {'YES' if calls['target_4_or_less_proven'] else 'NO'}")
    print(f"Target <=3.5 proven                       {'YES' if calls['target_3_5_or_less_proven'] else 'NO'}")
    print("\nREQUEST ELIMINATION")
    no_year = summary["elimination"]["no_fiscal_year_financial_calls"]
    print(f"PROVEN no-year financial skips            {no_year['calls']:>8,} calls")
    print(f"CANDIDATE benchmark duplicate calls       {benchmark['observed_duplicate_calls']:>8,} calls")
    print("CANDIDATE profile elimination             requires Search/Profile dependency proof")
    print("\nBENCHMARK SCOPE")
    print(f"Eligible benchmark responses              {benchmark['eligible_responses']:>8,}")
    print(f"Unique normalized scopes                  {benchmark['unique_scopes']:>8,}")
    print(f"Repeated exact-scope groups               {benchmark['repeated_exact_scope_groups']:>8,}")
    print(f"Conflicting scope groups                  {benchmark['conflicting_scope_groups']:>8,}")
    print("\nTRANSFORMATIONS")
    print(f"Successful responses                      {transformations['success']:>8,}")
    print(f"Empty responses                           {transformations['empty']:>8,}")
    print(f"Empty rate                                {transformations['empty_rate']:>8.1%}")
    print("Can absence be known before HTTP?         UNKNOWN")
    print("\nERRORS")
    if summary["errors"]:
        for error, count in sorted(summary["errors"].items()):
            print(f"  {error}: {count}")
    else:
        print("  none")
    print("=" * 88)


async def run_probe(args) -> tuple[list[dict], int]:
    targets = load_stratified_targets(args.companies)
    if not targets:
        raise ValueError("No eligible company identities in company_master")
    print("DBD CALL / SCOPE PROBE")
    print(
        f"  stratified sample={len(targets)} concurrency={args.concurrency} rate={args.rate}/s "
        f"(no archive / no projection / no writes)"
    )
    queue: asyncio.Queue[Target] = asyncio.Queue()
    for target in targets:
        queue.put_nowait(target)
    results = []
    result_lock = asyncio.Lock()

    async with AsyncDbdClient(
        concurrency=args.concurrency,
        rate_per_second=args.rate,
        endpoint_rates={endpoint: args.rate for endpoint in (*DEFAULT_ENDPOINTS, "search")},
        max_retries=args.max_retries,
    ) as client:

        async def worker():
            while True:
                try:
                    target = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    inspected = await inspect_target(client, target, args.verbose)
                    async with result_lock:
                        results.append(inspected)
                finally:
                    queue.task_done()

        await asyncio.gather(*(worker() for _ in range(min(args.concurrency, len(targets)))))
        return results, client.requests_made


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "--collector-stopped",
        action="store_true",
        help="Required acknowledgement that production collectors are stopped during the live probe",
    )
    parser.add_argument("--companies", type=positive, default=100)
    parser.add_argument("--concurrency", type=positive, default=12)
    parser.add_argument("--rate", type=positive_rate, default=8)
    parser.add_argument("--max-retries", type=int, choices=range(0, 4), default=1)
    parser.add_argument("--json-out")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if not args.collector_stopped:
        parser.error("Pass --collector-stopped after stopping other DBD collectors")

    results, requests = asyncio.run(run_probe(args))
    summary = analyse(results)
    summary["probe_http_requests"] = requests
    print_summary(summary)

    if args.json_out:
        path = Path(args.json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"summary": summary, "companies": results}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"JSON written: {path}")


if __name__ == "__main__":
    main()
