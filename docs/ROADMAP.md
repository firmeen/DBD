# Roadmap

Current runtime already has durable discovery, endpoint-level queueing, replay,
leases, Alembic migrations, strict contracts, natural-key freshness and MySQL
integration tests. This roadmap therefore starts **after** PR #1 hardening; it is
not a description of missing basic architecture.

## Principle

Do not add infrastructure because it looks “enterprise”. Add the next component
only when measured workload, correctness or operating evidence requires it.

Priority order:

1. prove population completeness;
2. prove source and local throughput;
3. execute and measure the accepted campaign;
4. then optimize the bottleneck that measurements actually identify.

## R1 — Population reconciliation

Goal: establish an independent denominator for “all companies”.

Deliverables:

- authoritative DBD population/count source;
- deterministic reconciliation report against `company_master`;
- explicit categories for conflict, quarantine and unexplained missing identities;
- preserved campaign timestamp/source version.

Exit criterion: no unexplained population gap.

## R2 — Clean hardened discovery campaign

Run migration 0004 and a fresh/resumed full discovery using current contracts and
quarantine behavior.

Required evidence:

- failed buckets = 0;
- partial pages = 0;
- unresolved rejections = 0;
- known lossy partitions = 0;
- observed prefix gaps reconciled;
- R1 denominator matched.

Do not reset checkpoints merely to hide failures.

## R3 — Representative capacity model

### Source-side

Re-run `probe_capacity.py` with current transport diagnostics after stopping all
collectors. Determine a conservative starting rate from successful throughput,
latency and transport errors, not from configured target rate.

### Local-side

Build/extend a disposable-DB benchmark that exercises representative mixed endpoint
projection, not only claim/finish queue operations. Use archived/synthetic contract-
valid payloads without contacting DBD.

Measure:

- projection rows/s;
- DB connection/pool saturation;
- queue claim/finish overhead;
- archive/raw bytes written;
- per-endpoint cost distribution.

Exit criterion: know whether current bottleneck is source, network, Python,
projection SQL or storage.

## R4 — Full all-status campaign

Create one immutable run:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

Resume only that run ID with measured rate/concurrency.

Record:

- start timestamp;
- master denominator;
- endpoint-task total;
- requests/retries/cooldowns;
- page/new-identity rates;
- endpoint outcomes;
- archive/projection growth;
- wall-clock duration.

Exit criterion: accepted seven-endpoint data complete with no unexplained failed
work and total campaign <=48 hours if that business target is still required.

## R5 — Storage lifecycle

Only after a representative campaign, measure storage by source and endpoint.

Then decide:

- raw/archive retention period;
- partitioning/archival tier;
- compression/export strategy;
- whether legacy `raw_responses` FK dependencies should be migrated so duplicate
  payload storage can be reduced safely.

No automatic deletion before a backup/restore and replay-retention policy exists.

## R6 — Operational resilience

Add evidence and automation around:

- backup/restore drill;
- campaign manifest/report artifact;
- alert thresholds for stalled queue, lost lock, quarantine growth and rate
  penalties;
- documented conflict-resolution workflow;
- schema/DB capacity alarms.

## R7 — Safe request reduction

Optimize only scopes that can be proven equivalent before HTTP.

Candidates:

- discovery seed-overlap reduction after set-equivalence measurement;
- industry benchmark reservation only if full natural key, including location
  scope, can be derived before request;
- endpoint reuse based on explicit source version/freshness evidence.

Rejected shortcut: dedupe from incomplete TSIC/year/size scope.

## R8 — Scale-out only when one manager is insufficient

Current MySQL named-lock architecture intentionally permits one API manager per
database. Do not add Redis/Kafka/Kubernetes solely for architectural fashion.

If measured workload proves one manager cannot meet the required campaign SLA,
first design:

- distributed global admission limiter;
- globally safe work ownership;
- source-friendly partition strategy;
- idempotent natural-key projection guarantees across managers.

Only then select external coordination infrastructure.

## Explicit non-goals until evidence changes

- bypassing DBD limits with multiple IPs/managers;
- guessing missing identity digits;
- treating contract failure as no-data;
- deleting raw evidence to save space before measuring retention needs;
- claiming complete population from search seeds alone;
- claiming 48-hour capability from a short profile probe.

`docs/EXECUTION_PLAN_TH.md` is the acceptance sequence. `docs/VALIDATION.md` records
what has actually been measured. `docs/ARCHITECTURE.md` describes current runtime
invariants.