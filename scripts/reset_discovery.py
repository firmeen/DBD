"""Clear discovery progress so a full pass can start from the province seeds.

Why this exists
---------------
Buckets created before durable per-page checkpointing cannot be resumed
correctly: their progress was derived from a count of successful concurrent
requests rather than from recorded page numbers, so "pages_done = 40" does not
identify *which* forty pages succeeded. Such a bucket can only be redone.

What it removes
---------------
Discovery bookkeeping only:

    discovery_buckets      which searches were planned/counted/drained
    discovery_pages        per-page checkpoints
    discovery_sightings    which bucket first produced each company

What it keeps
-------------
Every piece of company data:

    company_master, companies, company_facts, all projection tables,
    raw_responses, response_archive, worker_runs, endpoint_tasks

Re-running discovery re-finds those companies and upserts over them, so nothing
is lost by clearing the bookkeeping - only the record of how they were found.

Usage
-----
    python scripts\\reset_discovery.py            # dry run, shows counts only
    python scripts\\reset_discovery.py --apply    # actually delete
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dbd.core.settings import settings  # noqa: E402
from dbd.storage.mysql.engine import connect  # noqa: E402

# Children before parents: discovery_pages references discovery_buckets.
TABLES = ("discovery_pages", "discovery_sightings", "discovery_buckets")

PRESERVED = (
    "company_master",
    "companies",
    "company_facts",
    "raw_responses",
    "response_archive",
    "endpoint_tasks",
    "worker_runs",
)


def counts(cur, tables) -> dict[str, int]:
    result = {}
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
            result[table] = int(cur.fetchone()["n"])
        except Exception:
            result[table] = -1  # table absent in this schema version
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="Delete instead of only reporting")
    args = parser.parse_args()

    # A reset is only ever meant for a working database, never a scratch one by
    # accident, so say plainly which database is about to be touched.
    print(f"database: {settings.db_name} on {settings.db_host}:{settings.db_port}\n")

    with connect() as conn:
        with conn.cursor() as cur:
            to_clear = counts(cur, TABLES)
            keep = counts(cur, PRESERVED)

        print("will be CLEARED:")
        for table, n in to_clear.items():
            print(f"  {table:24} {n if n >= 0 else '(absent)'}")

        print("\nwill be KEPT:")
        for table, n in keep.items():
            print(f"  {table:24} {n if n >= 0 else '(absent)'}")

        if not args.apply:
            print("\nDry run. Nothing was changed. Re-run with --apply to delete.")
            return

        if not any(n > 0 for n in to_clear.values()):
            print("\nNothing to clear.")
            return

        with conn.cursor() as cur:
            for table in TABLES:
                if to_clear.get(table, 0) > 0:
                    cur.execute(f"DELETE FROM {table}")
                    print(f"cleared {table}: {cur.rowcount} row(s)")
        conn.commit()

    print("\nDiscovery reset. Start a full pass with: dbd-data discover")


if __name__ == "__main__":
    main()
