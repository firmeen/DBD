from __future__ import annotations

import argparse

from ..client import DbdClient
from ..pipelines import bulk, company_details, company_profile, financials
from ..presentation import TerminalPresenter
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


def check_db_info() -> dict:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS db, CURRENT_USER() AS user, VERSION() AS version")
            return dict(cur.fetchone())


def command_check_db() -> None:
    from ..presentation import TerminalPresenter

    TerminalPresenter().check_db(check_db_info())


def command_fetch_company(
    company_code: str,
    parts: list[str],
    fiscal_year: str | None,
    *,
    presenter: TerminalPresenter | None = None,
) -> None:
    presenter = presenter or TerminalPresenter()
    if not company_code.isdigit() or len(company_code) != 14:
        raise SystemExit("company_code must be the 14-digit code used in /company/profile/<code> URLs")

    client = DbdClient()

    payload = {"company_code": company_code}
    if "profile" in parts:
        result = company_profile.run(client, company_code)
        payload.update(
            {
                "raw_response_id": result.raw_response_id,
                "jp_no": result.jp_no,
                "jp_type_code": result.jp_type_code,
                "jp_name": result.jp_name,
            }
        )

    if "details" in parts:
        counts = company_details.run(client, company_code)
        payload.update({f"{resource}_rows": rows for resource, rows in counts.items()})

    if "financials" in parts:
        fin = financials.run(client, company_code, fiscal_year)
        payload.update(
            {
                "fiscal_year": fin.fiscal_year,
                "financial_submissions": fin.submissions,
                "financial_statements": fin.statements,
                "financial_items": fin.items,
                "industry_benchmark_items": fin.benchmark_items,
            }
        )

    presenter.command_result("DBD · COMPANY FETCH", payload)


def command_bulk(
    file: str | None,
    run_id: int | None,
    parts: list[str],
    limit: int | None,
    *,
    presenter: TerminalPresenter | None = None,
) -> None:
    presenter = presenter or TerminalPresenter()
    if file:
        codes = bulk.read_codes(file)
        if not codes:
            raise SystemExit(f"No company codes found in {file}")
        run_id, enqueued = create_run(source_label=file, parts=parts, company_codes=codes)
        presenter.command_result(
            "DBD · LEGACY RUN",
            {"run_id": run_id, "enqueued": enqueued, "source": file},
        )
    elif run_id is None:
        run = latest_unfinished_run()
        if run is None:
            raise SystemExit("No unfinished run to resume. Start one with --file <codes.txt>")
        run_id = int(run["id"])
        parts = run["parts"].split(",")
        presenter.message(
            f"Resuming legacy run {run_id} ({run['source_label']})",
            kind="info",
        )

    from ..storage.mysql.repositories.run_repository import get_run

    stored = get_run(run_id)
    if stored is None:
        raise ValueError("Legacy run not found")
    parts = stored["parts"].split(",")
    progress = run_progress(run_id)
    presenter.command_result(
        "DBD · LEGACY RUN STATUS",
        {"run_id": run_id, **progress},
    )

    result = bulk.process(run_id, parts, limit)

    payload = {
        "run_id": result.run_id,
        "processed": result.processed,
        "succeeded": result.succeeded,
        "failed": result.failed,
        "remaining": result.remaining,
    }
    if result.remaining:
        payload["next_command"] = f"dbd-data bulk --run-id {result.run_id}"
    presenter.command_result("DBD · LEGACY RUN RESULT", payload)


def command_bulk_status(
    run_id: int | None,
    *,
    presenter: TerminalPresenter | None = None,
) -> None:
    presenter = presenter or TerminalPresenter()
    if run_id is None:
        rows = list_runs()
        if not rows:
            presenter.message("No ingestion runs yet.", kind="info")
            return
        presenter.table(
            "DBD · LEGACY RUNS",
            ("RUN", "DONE", "FAILED", "PENDING", "CREATED", "SOURCE"),
            (
                (
                    r["id"],
                    int(r["done"] or 0),
                    int(r["failed"] or 0),
                    int(r["pending"] or 0),
                    str(r["created_at"])[:19],
                    r["source_label"],
                )
                for r in rows
            ),
        )
        return

    progress = run_progress(run_id)
    presenter.command_result("DBD · LEGACY RUN STATUS", {"run_id": run_id, **progress})
    rows = failed_tasks(run_id)
    if rows:
        presenter.table(
            "FAILED TASKS",
            ("COMPANY", "ATTEMPTS", "LAST ERROR"),
            (
                (
                    r["company_code"],
                    r["attempts"],
                    str(r["last_error"])[:120],
                )
                for r in rows
            ),
            caption=f"Retry with: dbd-data bulk-retry {run_id}",
        )


def command_bulk_retry(
    run_id: int,
    *,
    presenter: TerminalPresenter | None = None,
) -> None:
    presenter = presenter or TerminalPresenter()
    count = requeue_failed(run_id)
    payload = {"run_id": run_id, "requeued": count}
    if count:
        payload["next_command"] = f"dbd-data bulk --run-id {run_id}"
    presenter.command_result("DBD · LEGACY RETRY", payload)


def command_discover(
    seeds: list[str],
    search_type: str,
    concurrency: int | None,
    rate: float | None,
    max_buckets: int | None,
    health=None,
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
            health=health,
        )
    )

    print(
        f"\nbuckets {result.buckets_processed}, pages {result.pages_fetched}, "
        f"companies written {result.companies_written}, requests {result.requests_made}"
    )
    command_discover_status()


def command_discover_status(
    *,
    presenter: TerminalPresenter | None = None,
) -> None:
    from ..pipelines import discovery

    presenter = presenter or TerminalPresenter()
    s = discovery.summary()
    presenter.discovery_status(s)

    if int(s.get("partitions_lossy") or 0):
        presenter.message(
            f"{int(s['partitions_lossy']):,} lossy split partition(s) block completeness.",
            kind="error",
        )

    # Observed prefix gaps are a useful negative check, not a proof that every
    # company was discoverable through the chosen seeds.
    gaps = discovery.coverage_gaps()
    if gaps:
        presenter.command_result(
            "DBD · DISCOVERY COVERAGE GAP",
            {
                "gap_count": len(gaps),
                "prefixes": ", ".join(gaps[:20]) + (" ..." if len(gaps) > 20 else ""),
                "next_command": "dbd-data discover " + " ".join(gaps[:5]),
            },
        )
    else:
        presenter.message(
            "Observed prefixes have matching seeds; full completeness still requires partition and population reconciliation.",
            kind="info",
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
                    line.strip() for line in handle if line.strip() and not line.strip().startswith("#")
                ]
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
