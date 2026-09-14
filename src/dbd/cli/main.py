from __future__ import annotations

import argparse

from ..client import DbdClient
from ..pipelines import company_details, company_profile, financials
from ..storage.mysql.engine import connect

PARTS = ("profile", "details", "financials")


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

    args = parser.parse_args()

    if args.command == "check-db":
        command_check_db()
        return

    if args.command == "fetch-company":
        parts = list(PARTS)
        if args.only:
            parts = [p.strip() for p in args.only.split(",") if p.strip()]
            unknown = [p for p in parts if p not in PARTS]
            if unknown:
                raise SystemExit(f"Unknown --only value(s): {unknown}. Valid: {', '.join(PARTS)}")
        command_fetch_company(args.company_code, parts, args.fiscal_year)


if __name__ == "__main__":
    main()
