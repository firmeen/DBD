from __future__ import annotations

import argparse

from ..client import DbdClient
from ..pipelines import bulk, company_details, company_profile, financials
from ..storage.mysql.engine import connect
from ..storage.mysql.repositories.run_repository import (
    create_run,
    failed_tasks,
    latest_unfinished_run,
    list_runs,
    requeue_failed,
    run_progress,
)

PARTS = ("profile", "details", "financials")


def _parse_parts(only: str | None) -> list[str]:
    if not only:
        return list(PARTS)
    parts = [p.strip() for p in only.split(",") if p.strip()]
    unknown = [p for p in parts if p not in PARTS]
    if unknown:
        raise SystemExit(f"Unknown --only value(s): {unknown}. Valid: {', '.join(PARTS)}")
    return parts


def command_check_db() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS db, CURRENT_USER() AS user, VERSION() AS version")
            row = cur.fetchone()
    print(f"Database: {row['db']}")
    print(f"User: {row['user']}")
    print(f"MySQL: {row['version']}")


def command_fetch_company(company_code: str, parts: list[str], fiscal_year: str | None) -> None:
    if not company_code.isdigit() or len(company_code) != 14:
        raise SystemExit("company_code must be the 14-digit code used in /company/profile/<code> URLs")

    client = DbdClient()

    if "profile" in parts:
        result = company_profile.run(client, company_code)
        print(f"profile          raw_response_id={result.raw_response_id}")
        print(f"                 jp_no={result.jp_no} jp_type_code={result.jp_type_code}")
        print(f"                 jp_name={result.jp_name}")

    if "details" in parts:
        counts = company_details.run(client, company_code)
        for resource, rows in counts.items():
            print(f"{resource:17}{rows} row(s)")

    if "financials" in parts:
        fin = financials.run(client, company_code, fiscal_year)
        print(f"financials       fiscal_year={fin.fiscal_year}")
        print(f"                 {fin.submissions} submission(s), {fin.statements} statement(s), {fin.items} item(s)")
        print(f"industry_bench   {fin.benchmark_items} item(s)")


def command_bulk(file: str | None, run_id: int | None, parts: list[str], limit: int | None) -> None:
    if file:
        codes = bulk.read_codes(file)
        if not codes:
            raise SystemExit(f"No company codes found in {file}")
        run_id, enqueued = create_run(source_label=file, parts=parts, company_codes=codes)
        print(f"run {run_id}: enqueued {enqueued} company code(s) from {file}")
    elif run_id is None:
        run = latest_unfinished_run()
        if run is None:
            raise SystemExit("No unfinished run to resume. Start one with --file <codes.txt>")
        run_id = int(run["id"])
        parts = run["parts"].split(",")
        print(f"resuming run {run_id} ({run['source_label']})")

    progress = run_progress(run_id)
    print(f"run {run_id}: {progress['done']} done, {progress['failed']} failed, {progress['pending']} pending")

    result = bulk.process(run_id, parts, limit)

    print(
        f"\nrun {result.run_id}: processed {result.processed}, "
        f"succeeded {result.succeeded}, failed {result.failed}, remaining {result.remaining}"
    )
    if result.remaining:
        print(f"resume with: dbd-data bulk --run-id {result.run_id}")


def command_bulk_status(run_id: int | None) -> None:
    if run_id is None:
        rows = list_runs()
        if not rows:
            print("No ingestion runs yet.")
            return
        print(f"{'run':>4}  {'done':>5} {'fail':>5} {'pend':>5}  {'created':19}  source")
        for r in rows:
            print(
                f"{r['id']:>4}  {int(r['done'] or 0):>5} {int(r['failed'] or 0):>5} "
                f"{int(r['pending'] or 0):>5}  {str(r['created_at'])[:19]:19}  {r['source_label']}"
            )
        return

    progress = run_progress(run_id)
    print(f"run {run_id}: {progress}")
    rows = failed_tasks(run_id)
    if rows:
        print(f"\n{len(rows)} failed task(s):")
        for r in rows:
            print(f"  {r['company_code']}  attempts={r['attempts']}  {str(r['last_error'])[:120]}")
        print(f"\nretry them with: dbd-data bulk-retry {run_id}")


def command_bulk_retry(run_id: int) -> None:
    count = requeue_failed(run_id)
    print(f"re-queued {count} failed task(s) in run {run_id}")
    if count:
        print(f"now run: dbd-data bulk --run-id {run_id}")


def command_discover(
    seeds: list[str], search_type: str, concurrency: int | None, rate: float | None, max_buckets: int | None
) -> None:
    import asyncio

    from ..pipelines import discovery

    result = asyncio.run(
        discovery.run(
            seeds,
            search_type=search_type,
            concurrency=concurrency,
            rate=rate,
            max_buckets=max_buckets,
        )
    )

    print(
        f"\nbuckets {result.buckets_processed}, pages {result.pages_fetched}, "
        f"companies written {result.companies_written}, requests {result.requests_made}"
    )
    command_discover_status()


def command_discover_status() -> None:
    from ..pipelines import discovery

    s = discovery.summary()
    print("\ndiscovery so far:")
    print(f"  unique companies : {int(s['unique_companies']):,}")
    print(f"  rows fetched     : {int(s['rows_fetched']):,}")
    print(
        f"  buckets          : {s['buckets']} total "
        f"({s['buckets_done']} done, {s['buckets_split']} split, "
        f"{s['buckets_open']} open, {s['buckets_failed']} failed)"
    )
    if int(s["partitions_lossy"] or 0):
        print(
            f"  NOTE: {s['partitions_lossy']} partition(s) where SUM(children) != parent."
            " Check the delta before drawing conclusions: a row or two is usually"
            " live data changing between counts, a large gap means the split is"
            " losing rows. See section 15 of sql/reports/data_inventory.sql."
        )


def main() -> None:
    parser = argparse.ArgumentParser(prog="dbd-data")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check-db", help="Test MySQL connectivity")

    fetch = sub.add_parser("fetch-company", help="Fetch a company and store it in MySQL")
    fetch.add_argument("company_code", help="14-digit code from /company/profile/<code>, e.g. 70107556000051")
    fetch.add_argument(
        "--only",
        help=f"Comma-separated subset of: {', '.join(PARTS)} (default: all)",
    )
    fetch.add_argument(
        "--fiscal-year",
        help="Fiscal year for the financial request (default: the company's latest submitted year)",
    )

    bulk_cmd = sub.add_parser("bulk", help="Ingest many companies, resumable")
    bulk_cmd.add_argument("--file", help="Text file with one 14-digit company code per line")
    bulk_cmd.add_argument("--run-id", type=int, help="Resume a specific run")
    bulk_cmd.add_argument("--only", help=f"Comma-separated subset of: {', '.join(PARTS)}")
    bulk_cmd.add_argument("--limit", type=int, help="Stop after N companies (default: until queue empty)")

    status_cmd = sub.add_parser("bulk-status", help="Show ingestion runs, or one run's failures")
    status_cmd.add_argument("run_id", type=int, nargs="?")

    retry_cmd = sub.add_parser("bulk-retry", help="Re-queue a run's failed companies")
    retry_cmd.add_argument("run_id", type=int)

    disc = sub.add_parser("discover", help="Enumerate companies via search (concurrent, resumable)")
    disc.add_argument("seeds", nargs="*", help="Seed keywords, min 3 chars each (e.g. TSIC codes)")
    disc.add_argument("--seed-file", help="File with one seed keyword per line")
    disc.add_argument("--type", default="", help='Search type: "" (normal) or "biz" (TSIC)')
    disc.add_argument("--concurrency", type=int, help="Requests in flight (default from .env)")
    disc.add_argument("--rate", type=float, help="Requests per second cap (default from .env)")
    disc.add_argument("--max-buckets", type=int, help="Stop after N buckets (useful for a trial run)")

    sub.add_parser("discover-status", help="Show discovery coverage so far")

    args = parser.parse_args()

    if args.command == "check-db":
        command_check_db()
        return

    if args.command == "bulk-status":
        command_bulk_status(args.run_id)
        return

    if args.command == "bulk-retry":
        command_bulk_retry(args.run_id)
        return

    if args.command == "discover-status":
        command_discover_status()
        return

    if args.command == "discover":
        seeds = list(args.seeds)
        if args.seed_file:
            with open(args.seed_file, encoding="utf-8-sig") as handle:
                seeds += [
                    line.strip()
                    for line in handle
                    if line.strip() and not line.strip().startswith("#")
                ]
        # Resuming an existing run needs no seeds: pending buckets drive it.
        short = [s for s in seeds if len(s) < 3]
        if short:
            raise SystemExit(f"Seed keywords must be at least 3 characters: {short}")
        command_discover(seeds, args.type, args.concurrency, args.rate, args.max_buckets)
        return

    if args.command == "bulk":
        parts = _parse_parts(args.only)
        command_bulk(args.file, args.run_id, parts, args.limit)
        return

    if args.command == "fetch-company":
        command_fetch_company(args.company_code, _parse_parts(args.only), args.fiscal_year)


if __name__ == "__main__":
    main()
