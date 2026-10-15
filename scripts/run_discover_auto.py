"""Run normal Discovery with bounded restart and optional isolated test-DB selection.

The runner does not create schemas, mutate checkpoints, or launch concurrent children.
"""
from __future__ import annotations

import argparse
import codecs
import ctypes
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

from dotenv import dotenv_values

from dbd.core.settings import ENV_FILE, settings

RETRY_DELAY_SECONDS = 50
MAX_CONSECUTIVE_STALLED_FAILURES = 3
TEST_DB_PATTERN = re.compile(r"dbd_test_[A-Za-z0-9_]+\Z")
EXPECTED_REVISION = "0013_discovery_capital_ranges"


# Render in the main thread; background readers never control child lifecycle.
MONITOR_INTERVAL_SECONDS = 30
MONITOR_STATUS_TIMEOUT_SECONDS = 20
GRACEFUL_STOP_SECONDS = 30


def _attach_windows_job(child):
    """Bind the child to a kill-on-close Job Object; fail closed if this fails.

    This is a process-containment fallback for terminal/window closure. Normal
    Ctrl+C uses graceful CTRL_BREAK before the job is closed.
    """
    if os.name != "nt":
        return None

    process_handle = getattr(child, "_handle", None)
    if process_handle is None:
        # The existing unit test supplies a fake child with neither PID nor
        # native handle. Real spawned Windows processes must have a handle.
        if getattr(child, "pid", None) is None:
            return None
        raise OSError("Windows child process has no native handle")

    from ctypes import wintypes

    class BasicLimit(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [(name, ctypes.c_ulonglong) for name in (
            "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
        )]

    class ExtendedLimit(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimit),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    kernel.CreateJobObjectW.restype = wintypes.HANDLE
    kernel.SetInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
    ]
    kernel.SetInformationJobObject.restype = wintypes.BOOL
    kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL

    handle = kernel.CreateJobObjectW(None, None)
    if not handle:
        raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
    try:
        limits = ExtendedLimit()
        limits.BasicLimitInformation.LimitFlags = 0x00002000  # KILL_ON_JOB_CLOSE
        if not kernel.SetInformationJobObject(handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            raise OSError(ctypes.get_last_error(), "SetInformationJobObject failed")
        if not kernel.AssignProcessToJobObject(handle, wintypes.HANDLE(int(process_handle))):
            raise OSError(ctypes.get_last_error(), "AssignProcessToJobObject failed")
        return handle
    except BaseException:
        kernel.CloseHandle(handle)
        raise


def _close_windows_job(handle):
    if handle is not None:
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel.CloseHandle.restype = ctypes.c_int
        kernel.CloseHandle(handle)


def _stop_child(child):
    if child.poll() is not None:
        return "ALREADY EXITED"
    try:
        if os.name == "nt":
            child.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            child.terminate()
        child.wait(timeout=GRACEFUL_STOP_SECONDS)
        return "GRACEFUL"
    except (OSError, subprocess.TimeoutExpired):
        if child.poll() is None:
            child.kill()
            child.wait()
        return "FORCED"


_PROGRESS_RE = re.compile(
    r"DISCOVERY\s+\[[^\]\r\n]*\]\s*([\d,]+)/([\d,]+)\s+[\d.]+%"
    r"\s*\|\s*bucket\s+(\d+)\s*\|\s*rows\s+([\d,]+)"
    r"\s*\|\s*partial\s+(\d+)\s*\|\s*batch\s+(\d+)\s*\|\s*q\s+(\d+)"
)
_SPLIT_RE = re.compile(r"bucket\s+\d+:\s+[\d,]+\s+rows\s+->\s+split", re.I)


def _record_output(text, state, lock, progress_only=False):
    # Logging can append a timestamp immediately after a carriage-return frame.
    for part in re.split(r"(?=20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)", text):
        part = part.strip()
        if not part:
            continue
        found = _PROGRESS_RE.search(part)
        if found:
            cur, total, bucket, rows, partial, batch, queue = (
                int(v.replace(",", "")) for v in found.groups()
            )
            with lock:
                state["progress"] = (cur, total, bucket, rows, partial, batch, queue)
                state["progress_at"] = time.monotonic()
            if progress_only:
                continue
            part = part[found.end():].strip()
            if not part:
                continue
        elif progress_only:
            continue
        if _SPLIT_RE.search(part):
            with lock:
                state["event"] = part[-160:]
        elif "rate_penalty" in part or "status=429" in part:
            rate = re.search(r"new_rate=([\d.]+)", part)
            cooldown = re.search(r"cooldown_seconds=([\d.]+)", part)
            detail = "HTTP 429 · Search rate penalty"
            if rate:
                detail += f" · {rate.group(1)} req/s"
            if cooldown:
                detail += f" · cooldown {float(cooldown.group(1)):g}s"
            with lock:
                state["warning"] = detail
        elif " [warning" in part or part.startswith(("WARN ", "ERROR ")):
            with lock:
                state["warning"] = part[-160:]
        elif "rate_recovered" in part or part.startswith("bucket "):
            with lock:
                state["event"] = part[-160:]


def _drain_output(pipe, state, lock):
    """Drain without line buffering: ProgressLine sends CR but no trailing LF."""
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    pending = ""
    try:
        with pipe:
            while True:
                chunk = pipe.read(4096)
                if not chunk:
                    break
                pending += decoder.decode(chunk) if isinstance(chunk, bytes) else chunk
                parts = re.split(r"[\r\n]", pending)
                pending = parts.pop()
                for part in parts:
                    _record_output(part, state, lock)
                if pending and len(pending) < 4096:
                    _record_output(pending, state, lock, progress_only=True)
                if len(pending) > 8192:
                    _record_output(pending, state, lock)
                    pending = ""
            pending += decoder.decode(b"", final=True)
            if pending:
                _record_output(pending, state, lock)
    except (OSError, UnicodeError, ValueError):
        pass


def _pages_per_minute(samples, seconds):
    if len(samples) < 2:
        return "COLLECTING"
    latest_at, latest_pages = samples[-1]
    if seconds == 0:
        before_at, before_pages = samples[-2]
        if latest_at - before_at > MONITOR_INTERVAL_SECONDS * 3:
            return "--"
    else:
        valid = [sample for sample in samples if sample[0] <= latest_at - seconds]
        if not valid:
            return "COLLECTING"
        before_at, before_pages = valid[-1]
    duration = latest_at - before_at
    if duration <= 0 or latest_pages < before_pages:
        return "--"
    return f"{(latest_pages - before_pages) * 60 / duration:,.1f}"


def _view(state, lock):
    with lock:
        return {key: list(value) if key == "samples" else value for key, value in state.items()}


def _discovery_panel(database, state):
    from rich.console import Group
    from rich.panel import Panel
    from rich.progress_bar import ProgressBar
    from rich.table import Table
    from rich.text import Text

    content = [Text(f"DATABASE  {database}     PROCESS  RUNNING", style="bold")]
    progress = state["progress"]
    if progress is None:
        stage = "SPLITTING" if state["event"] and "-> split" in state["event"] else "WAITING FOR PROGRESS"
        content.append(Text(f"\nCURRENT BUCKET   {stage}", style="dim"))
    else:
        cur, total, bucket, rows, partial, batch, queue = progress
        pct = cur * 100 / total if total else 0
        content.append(Text(
            f"\nCURRENT BUCKET #{bucket}     {pct:.1f}%     {cur:,}/{total:,} pages",
            style="bold",
        ))
        content.append(ProgressBar(total=max(total, 1), completed=cur, width=44))
        content.append(Text(
            f"ROWS {rows:,}    PARTIAL {partial}    BATCH {batch}    QUEUE {queue}"
        ))
        age = time.monotonic() - state["progress_at"]
        if age > 10:
            content.append(Text(f"Last bucket update {int(age)}s ago", style="dim"))
    if state["event"]:
        content.append(Text(f"EVENT  {state['event']}", style="dim"))

    samples = state["samples"]
    flow = Table.grid(expand=True, padding=(0, 2))
    flow.add_column(ratio=1)
    flow.add_column(ratio=1)
    flow.add_row(
        f"NOW   {_pages_per_minute(samples, 0)} pages/min",
        f"1 MIN   {_pages_per_minute(samples, 60)} pages/min",
    )
    flow.add_row(
        f"5 MIN   {_pages_per_minute(samples, 300)} pages/min",
        f"1 HOUR   {_pages_per_minute(samples, 3600)} pages/min",
    )
    session = state["session_start"]
    if samples and session is not None:
        elapsed = samples[-1][0] - session[0]
        pages = max(0, samples[-1][1] - session[1])
        average = f"{pages * 60 / elapsed:,.1f}" if elapsed > 0 else "--"
        flow.add_row(f"SESSION   +{pages:,} pages", f"AVG   {average} pages/min")
    content.extend([Text("\nCOLLECTION FLOW", style="bold"), flow])

    total = Table.grid(expand=True, padding=(0, 2))
    total.add_column(ratio=1)
    total.add_column(ratio=1)
    snapshot = state["snapshot"]
    if snapshot:
        for left, right in (
            (("UNIQUE COMPANIES", "unique_companies"), ("PAGES DONE", "pages_done")),
            (("ROWS FETCHED", "rows_fetched"), ("BUCKETS OPEN", "buckets_open")),
            (("FAILED BUCKETS", "buckets_failed"), ("FAILED PAGES", "pages_failed")),
            (("PARTIAL PAGES", "pages_partial"), ("LOSSY PARTITIONS", "partitions_lossy")),
            (("BUCKETS SPLIT", "buckets_split"), ("KNOWN PAGES LEFT", "remaining_counted_pages")),
        ):
            total.add_row(
                f"{left[0]}   {int(snapshot.get(left[1], 0)):,}",
                f"{right[0]}   {int(snapshot.get(right[1], 0)):,}",
            )
    else:
        total.add_row("Waiting for MySQL snapshot", "")
    content.extend([Text("\nTOTAL DISCOVERY · MYSQL", style="bold"), total])
    if state["status_error"]:
        age = (
            f" · last good snapshot {int(time.monotonic() - state['snapshot_at'])}s ago"
            if state["snapshot_at"] is not None else ""
        )
        content.append(Text(f"MONITOR STALE   {state['status_error'][:90]}{age}", style="yellow"))
    if state["warning"]:
        content.extend([
            Text("\nLATEST WARNING", style="bold yellow"),
            Text(state["warning"], style="yellow"),
        ])
    content.append(Text("\nCtrl+C → Stop Collector and Runner · Keep committed checkpoints", style="dim"))
    return Panel(Group(*content), title="DBD · DISCOVERY", border_style="cyan")


def _show_dashboard(executable, env, state, lock, stopped):
    """Sample existing discover-status only; rendering belongs to main thread."""
    while not stopped.is_set():
        try:
            snapshot = read_status(executable, env)
            now = time.monotonic()
            with lock:
                pages = snapshot["pages_done"]
                if state["samples"] and pages < state["samples"][-1][1]:
                    state["samples"].clear()
                    state["session_start"] = None
                state["samples"].append((now, pages))
                if state["session_start"] is None:
                    state["session_start"] = (now, pages)
                state["snapshot"] = snapshot
                state["snapshot_at"] = now
                state["status_error"] = None
        except Exception as exc:
            with lock:
                state["status_error"] = f"{type(exc).__name__}: {str(exc)[-100:]}"
        stopped.wait(MONITOR_INTERVAL_SECONDS)


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
    """One collector and one terminal, with Ctrl+C taking priority over retry."""
    kwargs = {
        "env": env, "shell": False, "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT, "bufsize": 0,
    }
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    child_env = dict(env)
    child_env.setdefault("PYTHONIOENCODING", "utf-8")
    # Do not append --json: ProgressLine's bucket frames are part of the UI.
    child = subprocess.Popen(command, **{**kwargs, "env": child_env})
    job = None
    stopped = threading.Event()
    lock = threading.Lock()
    state = {
        "progress": None, "progress_at": None, "event": None, "warning": None,
        "snapshot": None, "snapshot_at": None, "status_error": None,
        "samples": deque(maxlen=160), "session_start": None,
    }
    reader = None
    sampler = None
    child_reaped = False
    try:
        try:
            job = _attach_windows_job(child)
        except BaseException:
            if child.poll() is None:
                child.kill()
                child.wait()
            child_reaped = True
            raise

        if getattr(child, "stdout", None) is not None:
            reader = threading.Thread(
                target=_drain_output, args=(child.stdout, state, lock), daemon=True,
            )
            reader.start()
            sampler = threading.Thread(
                target=_show_dashboard,
                args=(command[0], env, state, lock, stopped), daemon=True,
            )
            sampler.start()
        try:
            if reader is not None:
                try:
                    from rich.live import Live

                    database = env.get("DB_NAME") or settings.db_name
                    with Live(
                        _discovery_panel(database, _view(state, lock)),
                        auto_refresh=False, transient=False,
                        redirect_stdout=False, redirect_stderr=False,
                    ) as live:
                        while child.poll() is None:
                            live.update(
                                _discovery_panel(database, _view(state, lock)),
                                refresh=True,
                            )
                            stopped.wait(0.8)
                except Exception:
                    # A failing terminal renderer must not stop collection.
                    pass
            code = child.wait()
            child_reaped = True
            return code
        except KeyboardInterrupt:
            # The Rich context has exited, so shutdown messages cannot clash
            # with the live dashboard.
            stopped.set()
            print("\nDISCOVERY | STOP REQUESTED", flush=True)
            outcome = _stop_child(child)
            child_reaped = True
            print(f"DISCOVERY | CHILD STOPPED ({outcome})", flush=True)
            raise
    finally:
        stopped.set()
        if sampler is not None:
            sampler.join(timeout=0.2)
        if reader is not None:
            reader.join(timeout=0.2)
        _close_windows_job(job)
        if not child_reaped and child.poll() is None:
            child.kill()
            child.wait()

def read_status(executable, env):
    try:
        process = subprocess.run(
            [executable, "discover-status", "--json"],
            env=env, capture_output=True, text=True, check=False,
            timeout=MONITOR_STATUS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise ConnectionError("Discovery status query timed out") from exc
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
    if returncode in (130, -2, 0xC000013A, -1073741510):
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
    previous_break = None
    if os.name == "nt" and hasattr(signal, "SIGBREAK"):
        previous_break = signal.signal(signal.SIGBREAK, signal.default_int_handler)
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
            if evaluate_progress(before, before, rc) == "interrupted":
                print("DISCOVERY | STOPPED | NO RESTART | COMMITTED CHECKPOINTS PRESERVED", flush=True)
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
        print("DISCOVERY | NO RESTART | COMMITTED CHECKPOINTS PRESERVED", flush=True)
        return 130
    except (RuntimeError, OSError) as exc:
        print(f"DISCOVERY AUTO | stopped: {exc}", file=sys.stderr, flush=True)
        return 1

    finally:
        if previous_break is not None:
            signal.signal(signal.SIGBREAK, previous_break)


if __name__ == "__main__":
    raise SystemExit(main())
