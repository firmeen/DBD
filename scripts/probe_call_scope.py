from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import json
from pathlib import Path

from dbd.client.async_client import AsyncDbdClient
from dbd.contracts.identity import parse_code
from dbd.runtime.collection_fabric import derive_fiscal_year
from dbd.runtime.resources import FINANCIAL, endpoints, normalize_resource, request_path
from dbd.storage.mysql.engine import connect

DEFAULT_ENDPOINTS = endpoints(["profile", "details", "financials"])


@dataclass(frozen=True)
class Target:
    jp_type_code: str
    jp_no: str


@dataclass
class CompanyResult:
    code: str
    fiscal_year: str | None
    calls: int
    skipped_financial: int
    endpoint_rows: dict[str, int]
    benchmark_scope: str | None
    errors: list[str]


def spread_master(limit: int, all_statuses: bool) -> list[Target]:
    """Read-only deterministic spread across the current company_id range."""
    status = "" if all_statuses else " AND observed_jp_stat_code='1'"
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT MIN(company_id) lo,MAX(company_id) hi,COUNT(*) n FROM company_master "
            "WHERE identity_status<>'conflict'" + status
        )
        summary = cur.fetchone()
        if not summary or not summary["n"]:
            return []
        lo, hi, count = int(summary["lo"]), int(summary["hi"]), int(summary["n"])
        wanted = min(limit, count)
        anchors = [lo] if wanted == 1 else [lo + (hi - lo) * i // (wanted - 1) for i in range(wanted)]
        seen = set()
        targets = []
        for anchor in anchors:
            cur.execute(
                "SELECT company_id,jp_type_code,jp_no FROM company_master "
                "WHERE company_id>=%s AND identity_status<>'conflict'"
                + status
                + " ORDER BY company_id LIMIT 1",
                (anchor,),
            )
            row = cur.fetchone()
            if row is None:
                continue
            key = (row["jp_type_code"], row["jp_no"])
            if key not in seen:
                seen.add(key)
                targets.append(Target(*key))

        if len(targets) < wanted:
            cur.execute(
                "SELECT jp_type_code,jp_no FROM company_master WHERE identity_status<>'conflict'"
                + status
                + " ORDER BY company_id LIMIT %s",
                (wanted * 2,),
            )
            for row in cur.fetchall():
                key = (row["jp_type_code"], row["jp_no"])
                if key in seen:
                    continue
                seen.add(key)
                targets.append(Target(*key))
                if len(targets) >= wanted:
                    break
    return targets


def file_targets(path: str, limit: int) -> list[Target]:
    targets = []
    seen = set()
    with open(path, encoding="utf-8-sig") as handle:
        for raw in handle:
            value = raw.strip()
            if not value or value.startswith("#"):
                continue
            identity = parse_code(value)
            key = (identity.jp_type_code, identity.jp_no)
            if key in seen:
                continue
            seen.add(key)
            targets.append(Target(*key))
            if len(targets) >= limit:
                break
    return targets


def row_count(normalized) -> int:
    if normalized is None:
        return 0
    if isinstance(normalized, list):
        return len(normalized)
    return 1


def benchmark_scope(normalized) -> str | None:
    if normalized is None:
        return None
    values = (
        getattr(normalized, "submit_obj_code", None),
        getattr(normalized, "fiscal_year", None),
        getattr(normalized, "business_size_code", None),
        getattr(normalized, "location_type", None),
        getattr(normalized, "location_no", None),
    )
    if any(value in (None, "") for value in values):
        return None
    return "|".join(str(value) for value in values)


async def probe_company(client: AsyncDbdClient, target: Target) -> CompanyResult:
    endpoint_rows = {}
    errors = []
    calls = 0
    skipped_financial = 0
    fiscal_year = None
    scope = None

    profile_path = request_path("profile", target.jp_type_code, target.jp_no)
    try:
        response = await client.fetch("GET", profile_path, rate_key="profile")
        calls += 1
        profile = normalize_resource("profile", response.data)
        endpoint_rows["profile"] = row_count(profile)
        fiscal_year = derive_fiscal_year(getattr(profile, "submit_fin_year", None))
    except Exception as exc:
        errors.append(f"profile:{type(exc).__name__}")
        return CompanyResult(
            f"{target.jp_type_code}{target.jp_no}",
            None,
            calls,
            len(FINANCIAL),
            endpoint_rows,
            None,
            errors,
        )

    for endpoint in DEFAULT_ENDPOINTS:
        if endpoint == "profile":
            continue
        if endpoint in FINANCIAL and not fiscal_year:
            skipped_financial += 1
            continue
        path = request_path(endpoint, target.jp_type_code, target.jp_no)
        params = {"fiscalYear": fiscal_year} if endpoint in FINANCIAL else None
        try:
            response = await client.fetch("GET", path, params=params, rate_key=endpoint)
            calls += 1
            normalized = normalize_resource(endpoint, response.data)
            endpoint_rows[endpoint] = row_count(normalized)
            if endpoint == "industry_benchmark":
                scope = benchmark_scope(normalized)
        except Exception as exc:
            calls += 1
            errors.append(f"{endpoint}:{type(exc).__name__}")

    return CompanyResult(
        f"{target.jp_type_code}{target.jp_no}",
        fiscal_year,
        calls,
        skipped_financial,
        endpoint_rows,
        scope,
        errors,
    )


def print_report(results: list[CompanyResult], client: AsyncDbdClient) -> dict:
    companies = len(results)
    total_calls = sum(result.calls for result in results)
    skipped_financial = sum(result.skipped_financial for result in results)
    with_year = sum(result.fiscal_year is not None for result in results)
    endpoint_calls = Counter()
    endpoint_nonempty = Counter()
    errors = Counter()
    scopes = Counter()

    for result in results:
        for endpoint, count in result.endpoint_rows.items():
            endpoint_calls[endpoint] += 1
            if count:
                endpoint_nonempty[endpoint] += 1
        errors.update(result.errors)
        if result.benchmark_scope:
            scopes[result.benchmark_scope] += 1

    benchmark_calls = endpoint_calls["industry_benchmark"]
    unique_scopes = len(scopes)
    candidate_saved = max(0, benchmark_calls - unique_scopes)
    current_model = companies * len(DEFAULT_ENDPOINTS)
    proven_saved = skipped_financial
    measured_per_company = total_calls / companies if companies else 0.0
    proven_floor = max(0, current_model - proven_saved) / companies if companies else 0.0
    candidate_floor = max(0, current_model - proven_saved - candidate_saved) / companies if companies else 0.0

    print("=" * 88)
    print("DBD CALL / SCOPE PROBE — READ ONLY")
    print("=" * 88)
    print(f"Companies sampled             {companies:>10,}")
    print(f"Companies with fiscal year    {with_year:>10,}")
    print(f"Actual upstream calls         {total_calls:>10,}")
    print(f"Measured calls/company        {measured_per_company:>10.2f}")
    print(f"Client requests incl retries  {client.requests_made:>10,}")
    print()
    print("ENDPOINT COVERAGE")
    print("-" * 88)
    for endpoint in DEFAULT_ENDPOINTS:
        attempted = endpoint_calls[endpoint]
        nonempty = endpoint_nonempty[endpoint]
        pct = (100 * nonempty / attempted) if attempted else 0.0
        print(f"{endpoint:28} attempted={attempted:6,} nonempty={nonempty:6,} ({pct:6.2f}%)")
    print()
    print("REQUEST ELIMINATION")
    print("-" * 88)
    print(f"Baseline 7-depth-call model   {current_model:>10,}")
    print(f"PROVEN no-year skips          {proven_saved:>10,}")
    print(f"PROVEN floor/company          {proven_floor:>10.2f}")
    print(f"Benchmark requests            {benchmark_calls:>10,}")
    print(f"Unique observed scopes        {unique_scopes:>10,}")
    print(f"CANDIDATE scope-coalesce save {candidate_saved:>10,}")
    print(f"CANDIDATE floor/company       {candidate_floor:>10.2f}")
    print("  Candidate is not proven removable until the exact scope can be derived before HTTP.")
    print()
    print("ERRORS")
    print("-" * 88)
    if errors:
        for key, value in errors.most_common():
            print(f"{key:45} {value:>8,}")
    else:
        print("none")
    print("=" * 88)

    return {
        "companies": companies,
        "companies_with_fiscal_year": with_year,
        "actual_calls": total_calls,
        "measured_calls_per_company": measured_per_company,
        "client_requests_including_retries": client.requests_made,
        "baseline_depth_calls": current_model,
        "proven_no_year_skips": proven_saved,
        "proven_floor_calls_per_company": proven_floor,
        "benchmark_calls": benchmark_calls,
        "benchmark_unique_observed_scopes": unique_scopes,
        "candidate_scope_coalesce_saved": candidate_saved,
        "candidate_floor_calls_per_company": candidate_floor,
        "endpoint_attempted": dict(endpoint_calls),
        "endpoint_nonempty": dict(endpoint_nonempty),
        "errors": dict(errors),
        "results": [asdict(result) for result in results],
    }


async def run(args):
    targets = (
        file_targets(args.file, args.companies)
        if args.file
        else await asyncio.to_thread(spread_master, args.companies, args.all_statuses)
    )
    if not targets:
        raise SystemExit("No companies available for the probe")
    print(
        f"Sampling {len(targets)} companies; concurrency={args.concurrency}; rate={args.rate:g}/s; "
        "database writes=NONE",
        flush=True,
    )

    async with AsyncDbdClient(
        concurrency=args.concurrency,
        rate_per_second=args.rate,
    ) as client:
        semaphore = asyncio.Semaphore(args.concurrency)

        async def one(target):
            async with semaphore:
                return await probe_company(client, target)

        results = await asyncio.gather(*(one(target) for target in targets))
        report = print_report(results, client)

    if args.json_out:
        path = Path(args.json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"JSON report: {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Read-only DBD call/scope probe; never archives or projects API responses"
    )
    parser.add_argument("--companies", type=int, default=100)
    parser.add_argument("--file", help="Optional company-code file; default samples company_master read-only")
    parser.add_argument("--all-statuses", action="store_true")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--rate", type=float, default=10.0)
    parser.add_argument("--json-out")
    args = parser.parse_args()
    if args.companies < 1 or args.concurrency < 1 or args.rate <= 0:
        parser.error("companies/concurrency/rate must be positive")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
