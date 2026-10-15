"""Run the existing Discovery CLI with optional isolated test-DB selection.

No page/bucket limit, new scheduler, schema creation, or DB write logic here.
The same Discovery command performs the full workload in either database.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

RETRY_DELAY_SECONDS = 50
TEST_DB_PATTERN = re.compile(r"dbd_test_[A-Za-z0-9_]+\Z")
EXPECTED_REVISION = "0013_discovery_capital_ranges"

# Executes in the selected child environment. All statements are SELECTs.
PREFLIGHT_CODE = """
import json
import sys
from dbd.storage.mysql.engine import connect
from dbd.core.settings import settings

required = {
    "discovery_buckets", "discovery_pages", "discovery_sightings",
    "discovery_rejections", "response_archive", "company_master",
}
with connect() as conn, conn.cursor() as cur:
    cur.execute("SELECT DATABASE() AS db")
    actual = cur.fetchone()["db"]
    if actual != settings.db_name or not actual:
        raise RuntimeError("Selected database does not match connection")
    cur.execute("SELECT version_num FROM alembic_version")
    revision = cur.fetchone()["version_num"]
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE()"
    )
    tables = {row["table_name"] for row in cur.fetchall()}
    absent = sorted(required - tables)
    cur.execute(
        "SELECT COUNT(*) AS n FROM information_schema.columns "
        "WHERE table_schema=DATABASE() AND table_name='discovery_buckets' "
        "AND column_name='numeric_filters'"
    )
    numeric_column = int(cur.fetchone()["n"]) == 1
    cur.execute(
        "SELECT IS_USED_LOCK(CONCAT(DATABASE(), ':dbd-api-manager')) AS holder"
    )
    target_lock = cur.fetchone()["holder"]
    cur.execute("SELECT IS_USED_LOCK('dbd:dbd-api-manager') AS holder")
    production_lock = cur.fetchone()["holder"]
print(json.dumps({
    "database": actual, "revision": revision,
    "missing_tables": absent, "numeric_column": numeric_column,
    "target_manager_active": target_lock is not None,
    "production_manager_active": production_lock is not None,
}))
"""


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Resume full Discovery with a 50s process retry delay")
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--rate", type=float, default=None)
    parser.add_argument("--test-db", default=None, metavar="dbd_test_NAME")
    args = parser.parse_args(argv)
    if args.concurrency is not None and args.concurrency < 1:
        parser.error("--concurrency must be positive")
    if args.rate is not None:
        import math
        if not math.isfinite(args.rate) or args.rate <= 0:
            parser.error("--rate must be finite and positive")
    if args.test_db is not None and not TEST_DB_PATTERN.fullmatch(args.test_db):
        parser.error("--test-db must be named dbd_test_* (never production)")
    return args


def resolve_executable():
    interpreter = Path(sys.executable)
    for candidate in (interpreter.with_name("dbd-data.exe"), interpreter.with_name("dbd-data")):
        if candidate.is_file():
            return str(candidate)
    found = shutil.which("dbd-data")
    if found:
        return found
    raise RuntimeError("dbd-data was not found in the current Python environment")


def build_environment(args):
    env = os.environ.copy()
    if args.test_db is not None:
        env["DB_NAME"] = args.test_db
    return env


def preflight(env, *, test_db=None):
    """Inspect selected DB without running migrations or changing any rows."""
    process = subprocess.run(
        [sys.executable, "-c", PREFLIGHT_CODE],
        env=env, capture_output=True, text=True, check=False,
    )
    if process.returncode:
        raise RuntimeError("Database preflight failed: " + (process.stderr or process.stdout).strip()[-500:])
    try:
        details = json.loads(process.stdout)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Database preflight did not return valid JSON") from exc
    actual = details.get("database")
    if not actual or (test_db is not None and actual != test_db):
        raise RuntimeError("Database mismatch; refusing Discovery")
    if test_db is not None and not TEST_DB_PATTERN.fullmatch(actual):
        raise RuntimeError("Test mode resolved to a non-test database")
    if details.get("revision") != EXPECTED_REVISION:
        raise RuntimeError("Database migration is not at " + EXPECTED_REVISION)
    if details.get("missing_tables") or not details.get("numeric_column"):
        raise RuntimeError("Discovery database schema is incomplete")
    if details.get("target_manager_active"):
        raise RuntimeError("Another Discovery/API manager is active on the selected database")
    if test_db is not None and details.get("production_manager_active"):
        raise RuntimeError("Production API manager is active; do not overlap full test and production traffic")
    return details


def discovery_command(executable, args):
    command = [executable, "discover"]
    if args.concurrency is not None:
        command.extend(("--concurrency", str(args.concurrency)))
    if args.rate is not None:
        command.extend(("--rate", str(args.rate)))
    return command


def run_discovery(command, env):
    """One child at a time; console output and Ctrl+C remain interactive."""
    child = subprocess.Popen(command, env=env, shell=False)
    try:
        return child.wait()
    except KeyboardInterrupt:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        raise


def read_status(executable, env):
    process = subprocess.run(
        [executable, "discover-status", "--json"],
        env=env, capture_output=True, text=True, check=False,
    )
    if process.returncode:
        raise RuntimeError("Cannot read Discovery status: " + process.stderr.strip()[-500:])
    try:
        status = json.loads(process.stdout)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Discovery status is not JSON") from exc
    if not isinstance(status, dict):
        raise RuntimeError("Discovery status is not a JSON object")
    for key in ("buckets_open", "buckets_failed", "partitions_lossy",
                "pages_failed", "pages_partial", "unresolved_rejections",
                "pages_done", "buckets_done", "buckets_split", "unique_companies"):
        if key not in status:
            raise RuntimeError(f"Discovery status missing {key}")
    return status


def evaluate_progress(before, after, returncode):
    """Return a single decision: complete, resume, incomplete, blocked, retry."""
    if returncode == 130:
        return "interrupted"
    if returncode != 0:
        return "retry"
    blockers = ("buckets_open", "buckets_failed", "partitions_lossy",
                "pages_failed", "pages_partial", "unresolved_rejections")
    if not any(int(after[k] or 0) for k in blockers):
        return "complete"
    if int(after["buckets_open"] or 0) == 0:
        return "incomplete"
    progress_keys = ("pages_done", "buckets_done", "buckets_split", "unique_companies")
    progressed = any(int(after[k] or 0) > int(before[k] or 0) for k in progress_keys)
    return "resume" if progressed else "blocked"


def main(argv=None):
    args = parse_args(argv)
    executable = resolve_executable()
    env = build_environment(args)
    try:
        while True:
            details = preflight(env, test_db=args.test_db)
            print(f"DISCOVERY AUTO | database={details['database']} | 50s restart delay", flush=True)
            before = read_status(executable, env)
            rc = run_discovery(discovery_command(executable, args), env)
            after = read_status(executable, env)
            decision = evaluate_progress(before, after, rc)
            print(f"DISCOVERY AUTO | exit={rc} | decision={decision} | "
                  f"pages={after['pages_done']} | open={after['buckets_open']} | "
                  f"failed={after['buckets_failed']}", flush=True)
            if decision in ("complete", "incomplete", "blocked", "interrupted"):
                return 0 if decision == "complete" else (130 if decision == "interrupted" else 2)
            print(f"DISCOVERY AUTO | next attempt in {RETRY_DELAY_SECONDS}s", flush=True)
            time.sleep(RETRY_DELAY_SECONDS)
    except KeyboardInterrupt:
        print("DISCOVERY AUTO | stopped by user; no restart", flush=True)
        return 130
    except RuntimeError as exc:
        print(f"DISCOVERY AUTO | stopped: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
