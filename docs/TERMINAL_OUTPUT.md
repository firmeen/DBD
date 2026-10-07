# Terminal output and diagnostics

`dbd-data` is human-first on interactive CMD/PowerShell terminals.

Structured runtime/repository data remains the source of truth, but command handlers
must pass that data through the terminal presentation layer instead of printing raw
Python dictionaries or pretty-printed JSON by default.

## Output modes

### Human view — default

Use commands normally:

```powershell
dbd-data campaign status 8
dbd-data bulk-status 8
dbd-data master-status
dbd-data discover-status
```

The output is organized into operator-facing sections with explicit units and data
source semantics.

### Machine-readable JSON

Status/result commands that expose output options support:

```powershell
dbd-data campaign status 8 --json
dbd-data bulk-status 8 --json
dbd-data master-status --json
dbd-data discover-status --json
```

JSON mode is intended for PowerShell parsing, jq, CI, automation and external tools.
It preserves the structured payload rather than re-parsing human terminal text.

The SQL Server v0.6 production commands use the unified human terminal only and do
not expose `--json`. This applies to `dbd-data check-db`, `migrate`, `discover`,
`fetch-company`, the `cycle` commands, and all four `dbd-deep` commands.
Legacy/general JSON surfaces listed above remain unchanged.

### Extended diagnostics

```powershell
dbd-data campaign status 8 --verbose
dbd-data campaign status 8 --debug
```

`--debug` implies the extended terminal view. It does not print secrets.

The extended campaign view includes worker-level heartbeat/counter rows and durable
failure diagnostics.

## Package intelligence

Use the package/environment health view instead of parsing `pip show` manually:

```powershell
dbd-data about
```

The default human view summarizes:

- source release version vs installed distribution metadata
- package profile metadata
- Python implementation/version and active executable
- virtual environment path
- editable vs standard installation mode
- project/package/site-packages/dist-info locations
- dependency and reverse-dependency counts
- invalid `~*` distribution remnants such as interrupted pip metadata
- recommended remediation when an inconsistency is detected

A source/metadata disagreement is explicitly labeled `VERSION MISMATCH`. Invalid
distribution remnants without a version mismatch are labeled `CLEANUP NEEDED`.
A clean environment is labeled `HEALTHY`.

For the expanded direct dependency inventory:

```powershell
dbd-data about --verbose
dbd-data about --debug
```

For automation:

```powershell
dbd-data about --json
```

The command is read-only. It does not run pip, delete site-packages entries, mutate
the virtual environment, connect to MySQL, or contact Redis/DBD services.

## Campaign status semantics

`campaign status` prefers the live Redis supervisor/worker heartbeat snapshot while
a supervisor is active. The terminal labels this explicitly:

```text
LIVE · Redis worker heartbeat
```

This is intentionally a low-cost operational estimate and is not a full scan of the
multi-million-resource durable scheduler.

If live state is unavailable, the command falls back to MySQL reconciliation and
labels it:

```text
EXACT · MySQL reconciliation
```

## Bulk status semantics

`bulk-status <run>` is the durable scheduler view. It reads MySQL and labels itself:

```text
EXACT · MySQL durable scheduler state
```

For `fabric_v2`, two units are deliberately shown separately:

- **failed resources** — terminal endpoint bits in `failed_mask`
- **company states** — rows in `company_collection_state`

A company state can contain more than one failed resource. The counts are therefore
not expected to match.

## Failure diagnostics

Fabric failure state is compacted into endpoint bitmasks. Diagnostics expand those
bits to produce an exact **failed resources by endpoint** table.

The existing `last_error` column is state-level latest context, not an error stored
per endpoint bit. The terminal therefore presents it separately as:

```text
LATEST STATE ERROR CONTEXT · NOT PER-ENDPOINT
```

The UI must never imply that one state-level `last_error` caused every failed
endpoint bit.

Legacy endpoint tasks are one row per endpoint, so their task error/endpoint relation
is exact.

## Logging

Interactive structured logs use a human-readable Structlog console renderer.

Headless workers write redirected logs as JSON by default, preserving machine-friendly
forensics.

Override when needed:

```powershell
$env:DBD_LOG_FORMAT = "console"
$env:DBD_LOG_FORMAT = "json"
```

Default is `auto`.

## Safety boundary

The presentation layer must not own MySQL, Redis or HTTP clients and must not mutate
scheduler state.

Read-only diagnostic aggregation is allowed for explicit status commands.

Terminal rendering must not change:

- scheduler transitions
- leases/version fencing
- HTTP retry policy
- adaptive rate control
- archive-before-projection ordering
- projection freshness
- Redis coordination semantics


## SQL Server v0.6 unified terminal

The production v0.6 commands share one presentation boundary:
`src/dbd/presentation/v06.py`. The renderer consumes existing workflow results and
events only. It does not query SQL Server, call DBD HTTP endpoints, use Redis, schedule
work or persist terminal state.

The redesigned command surface is:

```text
dbd-data check-db
dbd-data migrate
dbd-data discover
dbd-data fetch-company
dbd-data cycle run
dbd-data cycle loop
dbd-data cycle status

dbd-deep prepare
dbd-deep collect
dbd-deep retry
dbd-deep status
```

Every command prints its local, timezone-aware start time outside the blocks. Active
commands use monotonic elapsed time; after completion the same field becomes Duration.

Interactive TTY output uses inline Rich `Live` rendering rather than an alternate
screen. The final static receipt remains in normal terminal scrollback. Redirected or
captured output is plain newline text: no ANSI repaint, carriage-return progress,
spinner frames or animation-frame logging.

### Live scene and motion

The interactive scene contains an outside header, one or more information blocks when
they represent distinct real domains, and an optional outside footer. Cycle commands
keep a stable CYCLE block plus one active-work block that morphs through the existing
workflow phases:

```text
PRECHECK -> DISCOVER -> PREPARE -> COLLECT -> VERIFY -> FINALIZE
```

Motion is presentation-only and is selected by runtime state:

- Orbit: opaque active work such as PRECHECK, database checks and retry.
- Sweep: discovery traversal with no claimed denominator.
- Stream: concurrent collection activity.
- Stitch: PREPARE.
- Radar: VERIFY.
- Seal: FINALIZE/schema application.
- Phase handoff: short completed-to-next-phase transition.
- Value flash: a real counter changed in an event; numbers are never interpolated.
- Endpoint baton: the real active endpoint for `fetch-company`.
- Split/retry/warning cues: transient emphasis only when the matching real event occurs.
- Wait clock and determinate rail: `cycle loop` only, because its interval is known.
- Completion/failure/stop settle: motion stops when work is no longer active.

The interactive renderer is intentionally capped at four refreshes per second, below
the eight-FPS ceiling, using only process-local state and `time.monotonic()`. This
keeps WAITING scenes within the low-motion cadence without adding a scheduler or
polling loop. Rendering frames never trigger SQL, HTTP or Redis work.

### Truth-only values

The UI displays only values already supplied by command results/events or locally
truthful command timing. In particular it does not invent ETA, percent completion,
remaining companies, actual throughput, DB/network latency, worker utilization or
company names absent from the current result contract.

Collector rate is labelled `Configured rate`; it is not presented as observed
throughput. Final HTTP request/retry/rate-limit counters appear only when the collector
final result supplies them.

### Semantic color contract

| Color | Meaning |
| --- | --- |
| cyan | RUNNING / ACTIVE / DISCOVERING / PREPARING / COLLECTING / VERIFYING / FINALIZING |
| green | READY / COMPLETE only |
| yellow | WAITING / RETRY / DEGRADED / STOPPING / COMPLETE_WITH_FAILURES |
| red | FAILED / ERROR / schema failure |
| dim | PENDING / N/A / IDLE / SKIPPED / non-active context |

Status is always written as text as well as color. `COMPLETE_WITH_FAILURES` is never
rendered as green.

### Responsive layout

The renderer recomposes from the current terminal width on each render. Wide terminals
may use paired metric columns; standard layouts stack blocks with compact paired
metrics; compact layouts stack exact values without converting numbers to abbreviated
forms such as `14.8k`.

### Command-specific boundaries

`dbd-data cycle status` is a durable snapshot with GENERATION and WORK blocks only;
it does not add a coverage option. `dbd-deep status --coverage` may add a COVERAGE
block using the existing companies/relations/events/financials/industries summary.

`dbd-deep prepare` and `retry` render directly from their typed results. They do
not issue an additional status read solely for presentation. `dbd-deep collect`
uses existing collector events and its final result; the collector event contract is
not expanded by this UI.

### Ctrl+C and failures

The presentation layer does not alter cancellation or durability semantics.
Ctrl+C remains exit code 130. The live view may show STOPPING before it freezes as
STOPPED, and it reminds the operator that committed checkpoints remain durable and
unfinished work is resumable where applicable. Fatal command errors remain exit code
1. Presentation failures are best-effort and must not become collection failures.

Version identities remain source-controlled and are not changed by this terminal work.

### Bootstrap, serving and SQL Server terminal views

The current-model maintenance/output commands use the separate human-only `MasterDeepTerminal` presentation boundary:

```powershell
python -m dbd.deep.bootstrap --batch-size 5000
python -m dbd.deep.serving --ensure --backfill --batch-size 100 --limit 100
python -m dbd.pipelines.sqlserver_sync --batch-size 100 --limit 100
```

The same semantic colors apply:

- bootstrap overlap is red only when non-zero;
- serving INSERT is green, UPDATE is magenta, unchanged is dim;
- missing identity is red only when non-zero;
- normalization errors and target-only counts are warnings when non-zero;
- phase/state values follow RUNNING/ACTIVE cyan, COMPLETE/READY green, DEGRADED/waiting yellow and ERROR/FAILED red.

These terminal classes remain presentation-only. They do not change source selection, database transactions, collector scheduling or SQL Server synchronization semantics.
