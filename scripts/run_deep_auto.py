from __future__ import annotations

import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

RETRY_DELAY_SECONDS = 50
SERVING_BATCH_SIZE = 32
SERVING_INTERVAL_SECONDS = 10.0
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVING_LOG_PATH = PROJECT_ROOT / "logs" / "deep-serving.log"

CANARY_COMMAND = [
    "collect",
    "--limit", "20",
    "--concurrency", "4",
    "--rate", "5",
    "--http-retries", "3",
]

LONG_RUN_COMMAND = [
    "collect",
    "--continuous",
    "--concurrency", "8",
    "--rate", "15",
    "--http-retries", "3",
    "--buffer-size", "32",
    "--idle-seconds", "5",
]


# ============================================================
# HELPERS
# ============================================================

def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print("=" * 78)
    print(f"[{now}] {message}")
    print("=" * 78)
    print(flush=True)


def find_dbd_deep() -> str:
    """
    Prefer dbd-deep.exe from the same virtual environment
    as the Python interpreter currently running this script.
    """

    python_path = Path(sys.executable)

    candidates = [
        python_path.with_name("dbd-deep.exe"),
        python_path.with_name("dbd-deep"),
    ]

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    found = shutil.which("dbd-deep")

    if found:
        return found

    raise SystemExit(
        "ERROR: dbd-deep executable not found.\n"
        "Activate .venv and run: python -m pip install -e ."
    )


DBD_DEEP = find_dbd_deep()


def start_serving_sidecar() -> tuple[subprocess.Popen | None, object | None]:
    log_file = None
    try:
        SERVING_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        log_file = SERVING_LOG_PATH.open("a", encoding="utf-8")
        command = [
            sys.executable,
            "-m",
            "dbd.deep.serving",
            "--continuous",
            "--batch-size",
            str(SERVING_BATCH_SIZE),
            "--interval-seconds",
            str(SERVING_INTERVAL_SECONDS),
        ]
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            if sys.platform == "win32"
            else 0
        )
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            shell=False,
            creationflags=creationflags,
        )
        return process, log_file
    except Exception as exc:
        if log_file is not None:
            log_file.close()
        print(
            f"WARNING: serving sidecar did not start: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return None, None


def stop_serving_sidecar(
    process: subprocess.Popen | None,
    log_file: object | None,
) -> None:
    try:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    finally:
        close = getattr(log_file, "close", None)
        if close is not None:
            close()


def run_dbd_deep(arguments: list[str]) -> int:
    """
    Run dbd-deep directly in the current console.

    stdout/stderr are intentionally NOT captured so Rich terminal UI
    continues to render normally.
    """

    command = [
        DBD_DEEP,
        *arguments,
    ]

    print(
        "COMMAND:",
        subprocess.list2cmdline(command),
        flush=True,
    )
    print()

    try:
        completed = subprocess.run(
            command,
            stdin=None,
            stdout=None,
            stderr=None,
            shell=False,
            check=False,
        )

        return int(completed.returncode)

    except KeyboardInterrupt:
        raise


# ============================================================
# CANARY RECOVERY LOOP
# ============================================================

def wait_until_canary_passes() -> None:
    attempt = 0

    while True:
        attempt += 1

        log(
            f"RECOVERY CANARY ATTEMPT #{attempt} "
            f"• limit=20 • concurrency=4 • rate=5"
        )

        return_code = run_dbd_deep(
            CANARY_COMMAND
        )

        if return_code == 0:
            log(
                "CANARY PASSED "
                "• Deep runtime/API path is working again"
            )
            return

        log(
            f"CANARY FAILED • exit code {return_code} "
            f"• retry in {RETRY_DELAY_SECONDS} seconds"
        )

        time.sleep(
            RETRY_DELAY_SECONDS
        )


# ============================================================
# SUPERVISOR
# ============================================================

def main() -> int:
    restart_count = 0

    log(
        "DBD DEEP AUTO-RECOVERY SUPERVISOR STARTED"
    )

    print(f"Python   : {sys.executable}")
    print(f"dbd-deep : {DBD_DEEP}")
    print()
    print(
        "Policy   : "
        "LONG RUN failure -> CANARY -> "
        f"{RETRY_DELAY_SECONDS}s retry until PASS -> LONG RUN"
    )
    print(
        "Serving  : isolated sidecar process; "
        "failure never stops Deep collection"
    )
    print(
        "Ctrl+C   : stop everything; "
        "no automatic restart"
    )

    while True:
        restart_count += 1

        log(
            f"START LONG RUN #{restart_count} "
            f"• concurrency=8 • rate=15 • buffer=32"
        )

        serving_process, serving_log = start_serving_sidecar()
        if serving_process is not None:
            print(
                f"Serving  : PID {serving_process.pid} "
                f"• batch={SERVING_BATCH_SIZE} "
                f"• interval={SERVING_INTERVAL_SECONDS:g}s"
            )
            print(f"Serving log: {SERVING_LOG_PATH}")
            print()

        try:
            return_code = run_dbd_deep(
                LONG_RUN_COMMAND
            )
        finally:
            stop_serving_sidecar(serving_process, serving_log)

        # ----------------------------------------------------
        # LONG RUN EXITED
        # ----------------------------------------------------

        if return_code == 0:
            # Continuous mode normally should not terminate by itself.
            # Treat an unexpected clean exit as a reason to validate
            # before starting another long-running collector.
            log(
                "LONG RUN EXITED CLEANLY "
                "• running recovery canary before restart"
            )

        else:
            log(
                f"LONG RUN FAILED • exit code {return_code} "
                "• entering recovery mode"
            )

        # ----------------------------------------------------
        # RECOVERY
        # ----------------------------------------------------

        wait_until_canary_passes()

        log(
            "RECOVERY CONFIRMED "
            "• restarting continuous collector"
        )


if __name__ == "__main__":
    try:
        raise SystemExit(
            main()
        )

    except KeyboardInterrupt:
        print()
        print("=" * 78)
        print("STOPPED BY USER")
        print("=" * 78)
        print(
            "No automatic restart will be performed."
        )
        raise SystemExit(130)
