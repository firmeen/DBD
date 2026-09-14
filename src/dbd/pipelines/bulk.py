from __future__ import annotations

import re
from dataclasses import dataclass

from ..client import DbdClient
from ..storage.mysql.repositories.run_repository import (
    claim_next_task,
    close_run_if_complete,
    finish_task,
    reset_stale_tasks,
    run_progress,
)
from . import company_details, company_profile, financials

_CODE = re.compile(r"^\d{14}$")


@dataclass
class BulkResult:
    run_id: int
    processed: int
    succeeded: int
    failed: int
    remaining: int


def read_codes(path: str) -> list[str]:
    """One 14-digit company code per line. Blank lines, comments and any
    surrounding whitespace or full profile URLs are tolerated.
    """
    codes: list[str] = []
    seen: set[str] = set()

    with open(path, encoding="utf-8-sig") as handle:
        for lineno, raw in enumerate(handle, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            # Accept a pasted URL as well as a bare code.
            match = re.search(r"(\d{14})", line)
            if not match:
                raise ValueError(f"{path}:{lineno}: no 14-digit company code in {line!r}")

            code = match.group(1)
            if code not in seen:
                seen.add(code)
                codes.append(code)

    return codes


def run_one(client: DbdClient, company_code: str, parts: list[str]) -> None:
    if not _CODE.match(company_code):
        raise ValueError(f"Invalid company code: {company_code!r}")

    if "profile" in parts:
        company_profile.run(client, company_code)
    if "details" in parts:
        company_details.run(client, company_code)
    if "financials" in parts:
        financials.run(client, company_code)


def process(run_id: int, parts: list[str], limit: int | None = None, *, verbose: bool = True) -> BulkResult:
    """Work a run's pending queue until it is empty or `limit` is reached.

    One client (and therefore one auth token) is reused for the whole batch.
    A company that fails is recorded and skipped rather than aborting the run,
    since one bad code should not cost the whole batch.
    """
    requeued = reset_stale_tasks(run_id)
    if requeued and verbose:
        print(f"re-queued {requeued} task(s) left running by a previous process")

    client = DbdClient()
    processed = succeeded = failed = 0

    while limit is None or processed < limit:
        task = claim_next_task(run_id)
        if task is None:
            break

        processed += 1
        code = task["company_code"]

        try:
            run_one(client, code, parts)
            finish_task(task["id"])
            succeeded += 1
            if verbose:
                print(f"  [{processed}] {code} ok")
        except KeyboardInterrupt:
            # Leave it pending so the next run retries it cleanly.
            finish_task(task["id"], error="interrupted", interrupted=True)
            raise
        except Exception as exc:
            finish_task(task["id"], error=f"{type(exc).__name__}: {exc}")
            failed += 1
            if verbose:
                print(f"  [{processed}] {code} FAILED  {type(exc).__name__}: {str(exc)[:120]}")

    close_run_if_complete(run_id)
    progress = run_progress(run_id)

    return BulkResult(
        run_id=run_id,
        processed=processed,
        succeeded=succeeded,
        failed=failed,
        remaining=int(progress["pending"] or 0),
    )
