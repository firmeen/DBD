"""Run normal Discovery with bounded restart and optional isolated test-DB selection.

The runner does not create schemas, mutate checkpoints, or launch concurrent children.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

from dotenv import dotenv_values

from dbd.core.settings import ENV_FILE

RETRY_DELAY_SECONDS = 50
MAX_CONSECUTIVE_STALLED_FAILURES = 3
TEST_DB_PATTERN = re.compile(r"dbd_test_[A-Za-z0-9_]+\Z")
EXPECTED_REVISION = "0013_discovery_capital_ranges"

# Runs inside the selected child environment. The SQL statements are read-only.
PREFLIGHT_CODE = """
import json
import os
import sys

from dbd.core.settings import settings
from dbd.storage.mysql.engine import connect

required = {
    "discovery_buckets", "discovery_pages", "discovery_sightings",
    "discovery_rejections", "response_archive", "company_master",
}
try:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS db")
        actual = cur.fetchone()["db"]
        if not actual or actual != settings.db_name:
            raise RuntimeError("Selected database does not match connection")
        cur.execute("SELECT version_num FROM alembic_version")
        row = cur.fetchone()
        revision = row["version_num"] if row else None
        cur.execute(
            "SELECT TABLE_NAME AS table_name FROM information_schema.tables WHERE table_schema=DATABASE()"
        )
        tables = {row["table_name"] for row in cur.fetchall()}
        absent = sorted(required - tables)
        cur.execute(
            "SELECT COUNT(*) AS n FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='discovery_buckets' "
            "AND column_name='numeric_filters'"
        )
        numeric_column = int(cur.fetchone()["n"]) == 1
        production_db = os.environ.get("DBD_AUTO_PRODUCTION_DB")
        lock_names = {
            "target_manager_active": actual + ":dbd-api-manager",
            "target_deep_active": actual + ":dbd-deep-collector",
        }
        if production_db:
            lock_names["production_manager_active"] = production_db + ":dbd-api-manager"
            lock_names["production_deep_active"] = production_db + ":dbd-deep-collector"
        locks = {}
        for key, name in lock_names.items():
            cur.execute("SELECT IS_USED_LOCK(%s) AS holder", (name,))
            locks[key] = cur.fetchone()["holder"] is not None
    print(json.dumps({
        "database": actual, "revision": revision,
        "missing_tables": absent, "numeric_column": numeric_column,
        "production_database": production_db,
        **locks,
    }))
except Exception as exc:
    origin = getattr(exc, "orig", exc)
    error_args = getattr(origin, "args", ())
    mysql_code = error_args[0] if error_args and type(error_args[0]) is int else None
    temporary = {1040, 1042, 1158, 1159, 1160, 1161, 1205, 2002, 2003, 2006, 2013, 2055}
    if mysql_code in temporary:
        print("Temporary MySQL connection failure: " + str(mysql_code), file=sys.stderr)
        sys.exit(75)
    print(type(exc).__name__ + ": " + str(exc)[-400:], file=sys.stderr)
    sys.exit(1)
"""

STATUS_FIELDS = (
    "buckets", "buckets_open", "buckets_uncounted", "buckets_failed",
    "partitions_lossy", "pages_failed", "pages_partial",
    "unresolved_rejections", "remaining_counted_pages", "pages_done",
    "buckets_done", "buckets_split", "unique_companies",
)
BLOCKING_FIELDS = (
    "buckets_open", "buckets_uncounted", "buckets_failed",
    "partitions_lossy", "pages_failed", "pages_partial",
    "unresolved_rejections", "remaining_counted_pages",
)
PROGRESS_FIELDS = (
    "buckets", "pages_done", "buckets_done", "buckets_split", "unique_companies",
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Resume full Discovery with a 50s process retry delay"
    )
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
    raise RuntimeError(
        "dbd-data is not installed next to this Python interpreter; "
        "activate the correct environment and run pip install -e ."
    )


def build_environment(args):
    env = os.environ.copy()
    if args.test_db is not None:
        # Production DB is read from the configured .env, never a guessed name.
        production_db = dotenv_values(ENV_FILE).get("DB_NAME")
        if not isinstance(production_db, str) or not production_db.strip():
            raise RuntimeError("Test mode requires an explicit DB_NAME in the configured .env")
        production_db = production_db.strip()
        if production_db == args.test_db or TEST_DB_PATTERN.fullmatch(production_db):
            raise RuntimeError("Test mode requires a distinct production DB_NAME in .env")
        env["DBD_AUTO_PRODUCTION_DB"] = production_db
        env["DB_NAME"] = args.test_db
    else:
        # Discard a stale test-only guard from the invoking environment.
        env.pop("DBD_AUTO_PRODUCTION_DB", None)
    return env


def preflight(env, *, test_db=None):
    """Fail closed on schema, selection and active managers before any API request."""
    process = subprocess.run(
        [sys.executable, "-c", PREFLIGHT_CODE],
        env=env, capture_output=True, text=True, check=False,
    )
    if process.returncode == 75:
        raise ConnectionError("Database temporarily unavailable during preflight")
    if process.returncode:
        raise RuntimeError(
            "Database preflight failed: " + (process.stderr or process.stdout).strip()[-500:]
        )
    try:
        details = json.loads(process.stdout)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Database preflight did not return valid JSON") from exc
    if not isinstance(details, dict):
        raise RuntimeError("Database preflight did not return an object")
    actual = details.get("database")
    if not actual or (test_db is not None and actual != test_db):
        raise RuntimeError("Database mismatch; refusing Discovery")
    if test_db is not None:
        production_db = env.get("DBD_AUTO_PRODUCTION_DB")
        if (
            not TEST_DB_PATTERN.fullmatch(actual)
            or not production_db
            or production_db == actual
            or details.get("production_database") != production_db
        ):
            raise RuntimeError("Test database or production guard mismatch")
    if details.get("revision") != EXPECTED_REVISION:
        raise RuntimeError("Database migration is not at " + EXPECTED_REVISION)
    if (not isinstance(details.get("missing_tables"), list)
            or details["missing_tables"] or details.get("numeric_column") is not True):
        raise RuntimeError("Discovery database schema is incomplete")
    required_locks = ["target_manager_active", "target_deep_active"]
    if test_db is not None:
        required_locks.extend(("production_manager_active", "production_deep_active"))
    for field in required_locks:
        if type(details.get(field)) is not bool:
            raise RuntimeError("Database preflight missing manager lock state: " + field)
        if details[field]:
            raise RuntimeError("Another collector is active: " + field)
    return details


def discovery_command(executable, args):
    command = [executable, "discover"]
    if args.concurrency is not None:
        command.extend(("--concurrency", str(args.concurrency)))
    if args.rate is not None:
        command.extend(("--rate", str(args.rate)))
    return command


def run_discovery(command, env):
    """Keep one child at a time and stop it before returning from Ctrl+C."""
    kwargs = {"env": env, "shell": False}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    child = subprocess.Popen(command, **kwargs)
    try:
        return child.wait()
    except KeyboardInterrupt:
        if child.poll() is None:
            if os.name == "nt":
                try:
                    child.send_signal(signal.CTRL_BREAK_EVENT)
                except OSError:
                    child.terminate()
            else:
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
        raise ConnectionError(
            "Cannot read Discovery status (exit "
            + str(process.returncode) + "): " + process.stderr.strip()[-400:]
        )
    try:
        status = json.loads(process.stdout)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Discovery status is not JSON") from exc
    if not isinstance(status, dict):
        raise RuntimeError("Discovery status is not a JSON object")
    result = dict(status)
    for key in STATUS_FIELDS:
        if key not in status:
            raise RuntimeError("Discovery status missing " + key)
        value = status[key]
        if type(value) is int and value >= 0:
            result[key] = value
        elif isinstance(value, str) and value.isascii() and value.isdecimal():
            result[key] = int(value)
        else:
            raise RuntimeError("Invalid non-negative integer status: " + key)
    return result


def evaluate_progress(before, after, returncode):
    """Pure function: evaluate recorded checkpoints and process result."""
    if returncode == 130:
        return "interrupted"
    progressed = any(after[key] > before[key] for key in PROGRESS_FIELDS)
    if returncode != 0:
        return "retry" if progressed else "retry_stalled"
    if after["buckets"] > 0 and not any(after[key] for key in BLOCKING_FIELDS):
        return "complete"
    if after["buckets"] > 0 and after["buckets_open"] == 0:
        return "incomplete"
    return "resume" if progressed else "blocked"


def main(argv=None):
    args = parse_args(argv)
    try:
        executable = resolve_executable()
        env = build_environment(args)
        stalled = 0

        def check_with_retries(action, label):
            for attempt in range(1, MAX_CONSECUTIVE_STALLED_FAILURES + 1):
                try:
                    return action()
                except ConnectionError as exc:
                    if attempt >= MAX_CONSECUTIVE_STALLED_FAILURES:
                        raise RuntimeError(
                            f"{label} failed after {attempt} checks: {exc}"
                        ) from exc
                    print(
                        f"DISCOVERY AUTO | {label} temporarily unavailable "
                        f"| retry in {RETRY_DELAY_SECONDS}s",
                        flush=True,
                    )
                    time.sleep(RETRY_DELAY_SECONDS)

        while True:
            details = check_with_retries(
                lambda: preflight(env, test_db=args.test_db), "preflight"
            )
            before = check_with_retries(
                lambda: read_status(executable, env), "status-before"
            )
            if evaluate_progress(before, before, 0) == "complete":
                print("DISCOVERY AUTO | already complete; no child started", flush=True)
                return 0

            print(
                f"DISCOVERY AUTO | database={details['database']} "
                f"| 50s restart delay",
                flush=True,
            )
            rc = run_discovery(discovery_command(executable, args), env)
            if rc == 130:
                print("DISCOVERY AUTO | collector interrupted; no restart", flush=True)
                return 130
            after = check_with_retries(
                lambda: read_status(executable, env), "status-after"
            )
            decision = evaluate_progress(before, after, rc)
            print(
                f"DISCOVERY AUTO | exit={rc} | decision={decision} "
                f"| pages={after['pages_done']} | open={after['buckets_open']} "
                f"| failed={after['buckets_failed']}",
                flush=True,
            )
            if decision == "complete":
                return 0
            if decision in ("incomplete", "blocked", "interrupted"):
                return 130 if decision == "interrupted" else 2
            if decision == "retry_stalled":
                stalled += 1
                if stalled >= MAX_CONSECUTIVE_STALLED_FAILURES:
                    print("DISCOVERY AUTO | stopped: repeated stalled failures", flush=True)
                    return 2
            else:
                stalled = 0
            print(
                f"DISCOVERY AUTO | next attempt in {RETRY_DELAY_SECONDS}s",
                flush=True,
            )
            time.sleep(RETRY_DELAY_SECONDS)
    except KeyboardInterrupt:
        print("DISCOVERY AUTO | stopped by user; no restart", flush=True)
        return 130
    except (RuntimeError, OSError) as exc:
        print(f"DISCOVERY AUTO | stopped: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
