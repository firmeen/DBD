# Branch collector with phases

Use `dbd_branch_collector_phased.py` alongside the unchanged
`dbd_branch_collector.py`. The new entry point imports the original MySQL,
HTTP, parsing, and dashboard helpers, so keep both files in the same directory.
It uses the same dependencies and `.env`. It does not create MySQL tables.

## Commands

First use, importing the old journal's latest company results:

```powershell
python .\dbd_branch_collector_phased.py --status-codes 1,A,B
```

Continue the current phase (the original command also resumes):

```powershell
python .\dbd_branch_collector_phased.py --continue
```

Start a new phase and request fresh website results:

```powershell
python .\dbd_branch_collector_phased.py --status-codes 1,A,B --new-phase
```

Do not leave `--new-phase` in a restart script: every invocation with it starts
another fresh phase. `--continue` fails when no phase exists. The two options
are mutually exclusive.

On resume, omitted status codes come from the saved phase, even if the
environment changes. Explicit different codes are rejected; use a new phase
to change scope. On first use/new phase, omitted codes come from
`DATAFORTHAI_STATUS_CODES` (fallback: `1`).

`--batch-size` defaults to 100 (1-500). `--limit N` bounds the companies
scheduled for HTTP during that invocation, not the number of individual
requests. Omit it or use zero for no limit. A limited run can resume normally.

## What is skipped

Within one phase, results OK, NO_BRANCH, LEGACY_OK, NO_BRANCH_LISTED,
UNAVAILABLE, NOT_LISTED, and STALE_BRANCHES are not requested again.
This remains true if source business fields or branch rows change.
STALE_BRANCHES is shown separately as needing review; its older branch rows
are preserved. NO_BRANCH_LISTED/UNAVAILABLE/NOT_LISTED mean the check has
ended for this phase, not that absence of branches was confirmed.

DEFERRED_* and other nonterminal results are attempted again on the next
invocation, once per company per invocation. HTTP retries within one attempt
still follow DATAFORTHAI_HTTP_ATTEMPTS.

A new phase does not trust old fingerprints, old results, or existing positive
MySQL rows as a reason to skip HTTP. LEGACY_OK can only be inherited during
the initial import; a fresh phase does not manufacture LEGACY_OK from MySQL.

Each source pass begins at the start of the selected 1/A/B (or other chosen)
identities. Two passes without new work end the run. Newly inserted identities
can be discovered even behind the previous cursor. This is not an immutable
snapshot and does not promise coverage of changes after the last scan.

## State and first migration

The new append-only file is `.dbd_branch_phases.jsonl`, next to the script.
It records phase headers, phase-owned company results, and import completion.
It keeps old phase history and fsyncs each event.

The first default invocation reads `.dbd_branch_results.jsonl` without
modifying it, using the latest result per company. An incomplete final line is
ignored in that old file; an invalid complete line stops import. Interrupted
imports resume before HTTP starts. Use `--new-phase` on first use to skip
import and recheck everything instead.

The original checkpoint and original journal are never written by this entry
point. Positive rows with no original journal record are checked over HTTP;
the new program does not infer that they were checked in this phase.

Back up the new journal together with MySQL. Terminal results are trusted for
the rest of their phase, including after DB rows are changed externally.
Only an incomplete tail of the NEW journal is truncated during recovery;
invalid complete events cause a stop.

The original and new collectors share the original MySQL exclusive lock.
Do not switch back to the old collector expecting it to read the new phase
journal; the old program is unchanged and cannot consume this new state.

## Ctrl+C and failure behavior

The first Ctrl+C requests a graceful stop. No further work is scheduled,
queued tasks are canceled, and running HTTP requests may finish. Completed
responses are persisted as they finish, not in submission order. The main
thread remains the only MySQL/journal writer. Retry sleeps and fallback
requests stop, while successful in-flight responses can still be committed.

Wait for the program to return before restarting. Repeated Ctrl+C requests
remain graceful. A forced process kill, power loss, or DB/journal failure can
still cause repeated HTTP after restart: there is no exactly-once guarantee
across a remote HTTP request, a MySQL commit, and a local file append.

MySQL commits precede result events. Existing branch keys are UPSERTed;
no rows are deleted. HTTP 401/403/429 stop collection and drain other usable
results; they do not bypass restrictions. Rate/concurrency behavior remains
the same as the original (concurrency maximum: 32).

Exit codes: 0 for a checked phase or a requested limit; 2 for remaining retry
results; 130 for Ctrl+C; 1 for a handled runtime error. A checked phase can
still contain unconfirmed or review results. Stopped/limited summary counts
cover only the scanned part, not the whole source.

## Offline verification

```powershell
.\.venv\Scripts\python.exe -B -m pytest --noconftest tests/test_branch_collector_phased.py -q -p no:cacheprovider
```

Tests replace MySQL and HTTP and reject attempts to contact either service.
They exercise resume, phase changes, migration, torn writes, scope changes,
limits, new identities behind a cursor, and shutdown/result draining.
