#!/usr/bin/env python3
"""Collect branches once per phase, reusing the original HTTP/MySQL helpers.

Default/--continue resumes the current phase. --new-phase starts a fresh HTTP
check of every eligible identity, regardless of old fingerprints or branch rows.
The first default invocation imports the old result journal read-only.
No old Python, checkpoint, journal, or MySQL schema is changed.

Run: python dbd_branch_collector_phased.py --status-codes 1,A,B
See docs/BRANCH_PHASES.md for state, stop, and retry semantics.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import threading
import uuid
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

# Import helpers without writing a cache for the original file.
sys.dont_write_bytecode = True
import dbd_branch_collector as base  # noqa: E402

STATE = base.ROOT / ".dbd_branch_phases.jsonl"
CHECKED = frozenset({
    "OK", "NO_BRANCH", "LEGACY_OK", "NO_BRANCH_LISTED",
    "UNAVAILABLE", "NOT_LISTED", "STALE_BRANCHES",
})
UNCONFIRMED = frozenset({"NO_BRANCH_LISTED", "UNAVAILABLE", "NOT_LISTED"})
REVIEW = frozenset({"STALE_BRANCHES"})


def validate_result(record):
    if (
        not isinstance(record, dict)
        or not all(isinstance(record.get(k), str) and record[k] for k in ("t", "j", "s"))
        or not isinstance(record.get("f"), str)
        or re.fullmatch(r"[0-9a-f]{64}", record["f"]) is None
        or type(record.get("n")) is not int
        or record["n"] < 0
        or record.get("m") not in ("http", "legacy")
    ):
        raise base.CollectorError("Invalid company result in journal")
    return record


def read_old_results(path):
    """Read old history without opening it for writing, even for a torn tail."""
    latest = {}
    if not path.exists():
        return latest
    with path.open("rb") as stream:
        for line in stream:
            if not line.endswith(b"\n"):
                break
            try:
                record = validate_result(json.loads(line))
            except (ValueError, TypeError) as exc:
                raise base.CollectorError("Invalid original result journal") from exc
            latest[(record["t"], record["j"])] = record
    return latest


class PhaseJournal:
    """One append-only file makes phase creation and result ownership explicit.

    Caller must hold the original MySQL exclusive lock throughout the lifetime
    of this object. A result is persisted only after its MySQL commit.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.phase = None
        self.records = {}
        self.seed_done = False
        self.fd = None
        good_offset = 0
        if self.path.exists():
            with self.path.open("rb") as stream:
                for line in stream:
                    if not line.endswith(b"\n"):
                        break
                    try:
                        event = json.loads(line)
                        self._apply(event)
                    except (ValueError, TypeError, KeyError) as exc:
                        raise base.CollectorError(
                            f"Invalid phase journal at byte {good_offset}"
                        ) from exc
                    good_offset = stream.tell()
            # Only this NEW journal is repaired; old files remain read-only.
            if self.path.stat().st_size > good_offset:
                with self.path.open("r+b") as stream:
                    stream.truncate(good_offset)
                    stream.flush()
                    os.fsync(stream.fileno())
        self.fd = os.open(str(self.path), os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)

    def _apply(self, event):
        if not isinstance(event, dict) or event.get("v") != 1:
            raise base.CollectorError("Unsupported phase journal event")
        kind = event.get("kind")
        if kind == "phase":
            codes = event.get("codes")
            if (
                not isinstance(event.get("phase"), str)
                or re.fullmatch(r"[0-9a-f]{32}", event["phase"]) is None
                or not isinstance(codes, list)
                or not codes
                or not all(isinstance(code, str) for code in codes)
                or tuple(codes) != base.status_codes(",".join(codes))
                or type(event.get("seed_legacy")) is not bool
            ):
                raise base.CollectorError("Invalid phase header")
            self.phase = event
            self.records = {}
            self.seed_done = not event["seed_legacy"]
        elif self.phase is None or event.get("phase") != self.phase["phase"]:
            raise base.CollectorError("Result does not belong to the current phase")
        elif kind == "result":
            record = validate_result(event)
            self.records[(record["t"], record["j"])] = record
        elif kind == "seed_done":
            self.seed_done = True
        else:
            raise base.CollectorError("Unknown phase journal event")

    def append_event(self, event):
        payload = (json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        offset = 0
        while offset < len(payload):
            written = os.write(self.fd, payload[offset:])
            if written <= 0:
                raise base.CollectorError("Unable to persist phase journal")
            offset += written
        os.fsync(self.fd)
        self._apply(event)

    def start(self, codes, *, seed_legacy):
        self.append_event({
            "v": 1, "kind": "phase", "phase": uuid.uuid4().hex,
            "codes": list(codes), "seed_legacy": seed_legacy,
            "created_at": base.now_utc().isoformat() + "Z",
        })

    def select(self, codes, *, new_phase=False, continue_only=False, old_path=base.JOURNAL):
        if new_phase:
            self.start(codes, seed_legacy=False)
        elif self.phase is None:
            if continue_only:
                raise base.CollectorError("No phase exists; omit --continue to create the first phase")
            self.start(codes, seed_legacy=True)
        elif tuple(self.phase["codes"]) != codes:
            raise base.CollectorError(
                "Status codes differ from the active phase; resume with its codes "
                "or use --new-phase"
            )
        # Resume an interrupted import before any HTTP work. Existing seeded
        # entries are never overwritten, and the original journal is not edited.
        if not self.seed_done:
            for identity, record in read_old_results(old_path).items():
                if identity not in self.records:
                    self.append_event({
                        **record, "v": 1, "kind": "result",
                        "phase": self.phase["phase"], "origin": "original_journal",
                    })
            self.append_event({
                "v": 1, "kind": "seed_done", "phase": self.phase["phase"],
            })

    def get(self, company):
        return self.records.get(base.key(company))

    def checked(self, company):
        record = self.get(company)
        # Intentionally independent of the current source fingerprint.
        return record is not None and record["s"] in CHECKED

    def record(self, company, result, count):
        self.append_event({
            "v": 1, "kind": "result", "phase": self.phase["phase"],
            "t": str(company["jp_type_code"]), "j": str(company["jp_no"]),
            "f": company["_fingerprint"], "s": result, "n": count, "m": "http",
        })

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class StopController:
    """First Ctrl+C requests drain; no asynchronous exception during a commit."""

    def __init__(self, website):
        self.website = website
        self.requested = False
        self.previous = None

    def request(self, *_):
        self.requested = True
        # Prevent new HTTP starts/retries/fallbacks. Requests already in progress
        # may finish and their valid results will still be committed.
        self.website.stop.set()

    def install(self):
        if threading.current_thread() is threading.main_thread():
            self.previous = signal.signal(signal.SIGINT, self.request)

    def restore(self):
        if self.previous is not None:
            signal.signal(signal.SIGINT, self.previous)


def persist_result(db, journal, company, response):
    branches, _total, outcome, url = response
    if outcome in ("OK", "NO_BRANCH"):
        previous = db.current_branch_numbers(company)
        current = {str(branch[0]) for branch in branches}
        if previous - current:
            outcome = "STALE_BRANCHES"
    if branches:
        db.commit_branches(company, branches, url)
    journal.record(company, outcome, len(branches))
    return outcome, len(branches)


def collect_candidates(candidates, pool, website, stop, consume, refresh=lambda: None):
    """Bound outstanding jobs and persist completed results before refilling.

    On a requested stop, cancel jobs not started and drain completed/in-flight
    jobs. A fatal HTTP error also drains other successful results, then raises.
    """
    iterator = iter(candidates)
    pending = {}
    fatal = None
    exhausted = False
    while pending or not exhausted:
        while (
            not stop.requested and fatal is None and not website.stop.is_set()
            and not exhausted and len(pending) < website.concurrency
        ):
            try:
                company = next(iterator)
            except StopIteration:
                exhausted = True
                break
            future = pool.submit(website.collect, str(company["jp_no"]))
            pending[future] = company

        if stop.requested or fatal is not None or website.stop.is_set():
            exhausted = True
            for future in pending:
                future.cancel()

        if not pending:
            break

        ready, _ = wait(tuple(pending), timeout=0.25, return_when=FIRST_COMPLETED)
        for future in ready:
            company = pending.pop(future)
            if future.cancelled():
                continue
            try:
                response = future.result()
            except Exception as exc:
                # Access-denied is still an error even if Ctrl+C was pressed.
                if not stop.requested or isinstance(exc, base.AccessDenied):
                    fatal = fatal or exc
                    website.stop.set()
                continue
            # Do not suppress DB/journal errors: recording a partial success
            # would be misleading. Finally shuts down the remaining workers.
            consume(company, response)
        refresh()
    if fatal is not None:
        raise fatal


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--continue", dest="continue_only", action="store_true",
                       help="Resume an existing phase; fail if none exists")
    modes.add_argument("--new-phase", action="store_true",
                       help="Start fresh HTTP checks; do not reuse old results")
    parser.add_argument("--status-codes", default=None,
                        help="Status codes, e.g. 1,A,B; resume defaults to the active phase")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--limit", type=int, default=0,
                        help="Maximum companies scheduled this invocation; 0 = unlimited")
    return parser


def parse_args(argv=None):
    parser = make_parser()
    args = parser.parse_args(argv)
    if not 1 <= args.batch_size <= 500:
        parser.error("--batch-size must be between 1 and 500")
    if args.limit < 0:
        parser.error("--limit must be >= 0")
    if args.status_codes is not None:
        try:
            base.status_codes(args.status_codes)
        except base.CollectorError as exc:
            parser.error(str(exc))
    return args


def resolve_codes(args, journal):
    if args.status_codes is not None:
        return base.status_codes(args.status_codes)
    if journal.phase is not None and not args.new_phase:
        return tuple(journal.phase["codes"])
    return base.status_codes(os.getenv("DATAFORTHAI_STATUS_CODES", "1"))


def run(args):
    db = journal = website = pool = stop = None
    try:
        # Same lock as the old collector prevents old/new programs overlapping.
        db = base.MySQL()
        db.preflight()
        website = base.Website()  # Validate HTTP settings before creating a phase.
        journal = PhaseJournal(STATE)
        codes = resolve_codes(args, journal)
        journal.select(codes, new_phase=args.new_phase, continue_only=args.continue_only)
        ui = base.UI(website, codes)
        ui.console.print(
            f"Phase {journal.phase['phase']} | status={','.join(codes)} "
            f"| saved results={len(journal.records):,} | journal={STATE.name}"
        )
        pool = ThreadPoolExecutor(max_workers=website.concurrency, thread_name_prefix="branch-phase")
        stop = StopController(website)
        stop.install()
        attempted = set()
        quiet = 0
        scheduled = 0
        counts = Counter()

        def consume(company, response):
            outcome, branch_count = persist_result(db, journal, company, response)
            attempted.add(base.key(company))
            counts[outcome] += 1
            ui.result(company, outcome, branch_count)

        with base.Live(ui.render(), console=ui.console, refresh_per_second=3, transient=False) as live:
            ui.live = live
            while not stop.requested:
                ui.pass_no += 1
                position = None
                activity = 0
                counts = Counter()
                while not stop.requested:
                    ui.stage = "READ PHASE IDENTITIES"
                    rows = db.select_page(codes, position, args.batch_size)
                    if not rows:
                        break
                    position = base.key(rows[-1])
                    ui.examined += len(rows)
                    ui.progress.update(ui.task, advance=len(rows))
                    candidates = []
                    for company in rows:
                        company["_fingerprint"] = base.fingerprint(company, db.fields)
                        identity = base.key(company)
                        previous = journal.get(company)
                        if journal.checked(company) or identity in attempted:
                            counts[previous["s"] if previous else "RETRY_PENDING"] += 1
                            ui.skipped += 1
                        else:
                            candidates.append(company)
                    if args.limit:
                        candidates = candidates[:max(0, args.limit - scheduled)]
                    if candidates and not stop.requested:
                        ui.stage = "COLLECT / COMMIT PHASE RESULTS"
                        scheduled += len(candidates)
                        activity += len(candidates)
                        collect_candidates(candidates, pool, website, stop, consume, ui.refresh)
                    ui.refresh()
                    if args.limit and scheduled >= args.limit:
                        break
                if stop.requested or (args.limit and scheduled >= args.limit):
                    break
                quiet = 0 if activity else quiet + 1
                ui.stage = "RECONCILE PHASE"
                ui.refresh()
                if quiet >= 2:
                    break

            retry_pending = sum(n for status, n in counts.items() if status not in CHECKED)
            limited = bool(args.limit and scheduled >= args.limit)
            if stop.requested:
                ui.stage = "STOPPED - RESULTS DRAINED"
            elif limited:
                ui.stage = "LIMIT REACHED - RESUME SAME PHASE"
            elif retry_pending:
                ui.stage = "PARTIAL - RETRY PENDING"
            else:
                ui.stage = "PHASE CHECKED - SOURCE CAUGHT UP"
            ui.refresh()

        ui.console.print(
            f"FINISHED | phase={journal.phase['phase']} | stage={ui.stage} "
            f"| HTTP companies committed={ui.checked:,} | HTTP requests={website.count:,} "
            f"| observed results={dict(counts)} "
            f"| unconfirmed={sum(counts[s] for s in UNCONFIRMED):,} "
            f"| review={sum(counts[s] for s in REVIEW):,} | retry pending={retry_pending:,}"
        )
        if stop.requested or limited:
            ui.console.print("Observed counts cover only the scanned part of this phase.")
        return 130 if stop.requested else (2 if retry_pending else 0)
    finally:
        if website is not None:
            website.stop.set()
        try:
            if pool is not None:
                pool.shutdown(wait=True, cancel_futures=True)
        finally:
            if stop is not None:
                stop.restore()
            if website is not None:
                website.close()
            if journal is not None:
                journal.close()
            if db is not None:
                db.close()


def main(argv=None):
    try:
        return run(parse_args(argv))
    except KeyboardInterrupt:
        base.Console(stderr=True).print("Stopped before graceful drain; resume the same phase.")
        return 130
    except (base.CollectorError, base.pymysql.MySQLError, base.requests.RequestException,
            OSError, ValueError) as exc:
        base.Console(stderr=True).print(f"STOPPED: {exc}", markup=False)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
