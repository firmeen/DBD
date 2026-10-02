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


## Standalone dbd-deep

`dbd-deep` has its own presentation boundary and does not import Campaign, Fabric,
Guardian, Supervisor or Redis fleet state.

### Semantic color contract

สีใน interactive Deep Terminal UI เป็น state/priority signal ไม่ใช่ decoration และใช้กับ **value/state** เป็นหลัก:

| Color | Meaning | Examples |
| --- | --- | --- |
| bold red | critical/failure/operator attention | FAILED, ERROR, overlap > 0, missing identity > 0 |
| yellow / bold yellow | warning/degraded/retry | DEGRADED, RETRY_WAIT, 429 > 0, normalization error > 0 |
| green / bold green | healthy/completed/success | READY, COMPLETE, finished, successful inserts |
| cyan | active execution/throughput/configured work | RUNNING, ACTIVE, requests, rate, concurrency |
| blue | queued/scheduled/promoted work | claimed, pending, Master promoted, recovered runtime |
| magenta | refresh/change semantics | ACTIVE refresh, updated serving rows |
| bold white | primary totals/important values | total/scanned/limits |
| dim | secondary/context/no-change | IDLE, unchanged, elapsed, table/context labels |

Alarm-like counters are conditional: zero failures/429/overlap/missing-identity/normalization-errors are not rendered as alarms; color appears only when the value is non-zero.

This styling exists only in the interactive Rich render path. Redirected/non-TTY output remains plain newline text without ANSI repaint sequences, and JSON output remains unstyled structured JSON.

Normal one-shot collection:

```powershell
dbd-deep collect --concurrency 12 --rate 20 --buffer-size 64
```

Continuous collection keeps one standalone manager process alive and checks MySQL
again after the idle interval when work drains:

```powershell
dbd-deep collect --continuous --concurrency 12 --rate 20 --buffer-size 64 --idle-seconds 5
```

`--limit N` remains a whole-command company budget, including continuous mode.
It is not reset on every internal refill cycle.

Interactive terminals use a compact Rich live view. Redirected output is newline
based and does not use cursor-return repainting. Explicit `--json` remains machine
readable and does not mix Rich output into stdout.

Static/status commands use the same Deep presentation layer:

```powershell
dbd-deep prepare
dbd-deep status
dbd-deep status --coverage
dbd-deep retry --limit 500
```

Version identities are intentionally separate:

```text
dbd-data --version  -> dbd-data 0.5.0
dbd-deep --version  -> dbd-deep 0.1.0
```

Both console scripts are still shipped by the single `dbd-data` Python
distribution.


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
