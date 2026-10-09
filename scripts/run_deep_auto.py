from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

RETRY_DELAY_SECONDS = 50

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


def run_dbd_deep(arguments: list[str], *, backend: str) -> int:
    """
    Run dbd-deep directly in the current console.

    stdout/stderr are intentionally NOT captured so Rich terminal UI
    continues to render normally.
    """

    command = [
        sys.executable, "-m", "dbd.deep.cli",
        *arguments, "--backend", backend,
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

def wait_until_canary_passes(*, backend: str) -> None:
    attempt = 0

    while True:
        attempt += 1

        log(
            f"RECOVERY CANARY ATTEMPT #{attempt} "
            f"• limit=20 • concurrency=4 • rate=5"
        )

        return_code = run_dbd_deep(
            CANARY_COMMAND, backend=backend
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
    from dbd.core.settings import settings

    parser = argparse.ArgumentParser(description="Backend-aware v0.6 Deep recovery")
    parser.add_argument("--backend", choices=("sqlserver", "mysql"),
                        default=settings.dbd_production_backend)
    args = parser.parse_args()
    backend = args.backend
    restart_count = 0

    log(
        "DBD DEEP AUTO-RECOVERY SUPERVISOR STARTED"
    )

    print(f"Python   : {sys.executable}")
    print(f"dbd-deep : {sys.executable} -m dbd.deep.cli")
    print(f"Backend  : {backend}")
    print()
    print(
        "Policy   : "
        "LONG RUN failure -> CANARY -> "
        f"{RETRY_DELAY_SECONDS}s retry until PASS -> LONG RUN"
    )
    print(
        "Ctrl+C   : stop everything; "
        "no automatic restart"
    )

    while True:
        restart_count += 1

        log(
            f"START LONG RUN #{restart_count} "
            f"• concurrency=8 • rate=15 • direct Deep claim"
        )

        return_code = run_dbd_deep(
            LONG_RUN_COMMAND, backend=backend
        )

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

        wait_until_canary_passes(backend=backend)

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
