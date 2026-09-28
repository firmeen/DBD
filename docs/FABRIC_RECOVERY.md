# Fabric V2 upstream recovery

Issue: #61  
Parent production-runtime acceptance: #8

## Why this exists

A long-running campaign can outlive one network path, public IP, browser-cookie state,
or anonymous DBD token. Those are infrastructure conditions; they are not properties
of a juristic person.

The Fabric scheduler therefore distinguishes:

- **DONE** — the endpoint was observed and projected successfully.
- **N/A** — the endpoint was observed successfully but has no applicable data.
- **deferred infrastructure work** — auth/access/network/upstream availability is
  temporarily unhealthy. The work stays retryable and does not consume the durable
  task-attempt budget.
- **FAILED** — a resource reached a genuine terminal correctness/data boundary such
  as a contract violation, identity conflict, or exhausted non-infrastructure work.

A profile failure is a dependency boundary for financial resources, but it must never
overwrite financial resources already observed as DONE or N/A.

## Operator workflow

Always migrate before using the recovery command from a new checkout:

```powershell
dbd-data migrate
```

Pause the contaminated/source run first. The recovery operation refuses to materialize
while the source has an active supervisor or registered workers.

Inspect the plan:

```powershell
dbd-data campaign recover 8
```

The plan is read-only. It reports:

- source company count
- selected resource units
- DONE units that will be preserved
- N/A units that will be preserved
- unresolved/failed units that will be scheduled
- failed units represented as reopened work
- identity-conflict rows that remain quarantined

Create the recovery run explicitly:

```powershell
dbd-data campaign recover 8 --execute
```

Creation is lineage-fenced: if a recovery child already exists for the source run,
the command refuses to create a duplicate. A `building` child must be resumed with
`--recovery-run-id`; a `ready` child must be started instead of materialized again.

The command creates a new `fabric_v2` run with durable lineage:

```text
source Run 8
   |
   +-- worker_runs.parent_run_id
   |
   +--> recovery run
```

The source run is never rewritten.

If recovery materialization is interrupted while the new run is still `building`,
resume only that materialization:

```powershell
dbd-data campaign recover 8 --execute --recovery-run-id <id>
```

After the command reports `status=ready`, start the new run using the printed
`next_command`.

## Recovery semantics

For every company:

```text
preserved_done = source.done_mask & required_mask
preserved_na   = source.not_available_mask & required_mask & ~preserved_done
retry_mask     = required_mask & ~(preserved_done | preserved_na)
```

The recovery run:

- keeps `preserved_done`
- keeps `preserved_na`
- schedules `retry_mask`
- keeps old failed bits inside `failed_mask & dirty_mask` as reopened evidence, so
  they are no longer counted as terminal failures while retried
- resets fetch/project attempt counters
- does not mutate normalized projection tables or response archives
- leaves identity conflicts terminal/quarantined

This means a company that already completed committees and transformations does not
re-fetch those endpoints merely because its historical profile request was contaminated
by a temporary 403/auth/network incident.

## Transient upstream behavior

The async client now authenticates lazily. A DBD auth outage cannot make a newly spawned
worker die before its first heartbeat.

The client retries bounded 403/auth/network/upstream errors, but availability and
throughput pressure are now separate control signals:

- auth/session failures open the global availability circuit without reducing the
  configured fleet pacer
- ordinary 500/502/504 and network/timeout failures open endpoint availability circuits;
  distinct endpoint failures can escalate to the global availability circuit
- HTTP 429 remains a throughput-pressure signal and reduces adaptive pacing
- HTTP 503 is treated as rate pressure only when DBD supplies `Retry-After`; otherwise
  it follows the availability path

An open availability circuit stops upstream admission while preserving the configured
pacing target. Shared workers use one bounded HALF_OPEN probe and blocked coroutines
respect the circuit/probe window instead of polling Redis roughly once per second.

If bounded HTTP-level recovery still fails, the worker returns the owned task to
`retry_wait` through an attempt-neutral defer:

```text
claim
  -> upstream/auth failure
  -> bounded retry + availability circuit
  -> still unavailable
  -> durable defer
       status=retry_wait
       current fetch attempt rolled back
       failed_mask unchanged
```

`DBD_UPSTREAM_DEFER_SECONDS` controls the durable defer delay.

## Failure evidence

New failures/deferred outcomes are appended to `collection_failure_events` with:

- run/company/endpoint
- fetch or project stage
- outcome
- error code
- HTTP status when known
- attempt number
- archive ID when available
- UTC occurrence time

This event stream complements `company_collection_state.last_error`, which remains the
latest state-level context rather than a historical per-endpoint ledger.

## Drain and Guardian boundary

Durable fetch claiming is bounded near actual HTTP concurrency instead of the physical
pipeline queue capacity. On graceful drain, fetchers stop consuming queued-but-unstarted
work; active HTTP/archive/project work reaches a durable boundary and remaining queued
fetch leases are released to MySQL without consuming an attempt.

`Ctrl+C` remains an intentional pause and displays the real durable MySQL progress.
A second Ctrl+C force-stops confirmed local process trees, shields further interrupts
during the bounded lease-reconciliation critical section, and reclaims only leases
belonging to confirmed-dead workers.

Campaign status keeps durable and operational state separate: a durable `READY` run can
be operationally `PAUSED`/offline, and exact status exposes `RETRY WAIT` separately
from ordinary pending work.

If local worker restart capacity is exhausted, Guardian terminates as
`CAPACITY_EXHAUSTED` and requires operator diagnosis. It no longer churns indefinitely
through repeated supervisor generations while no healthy local worker can stay alive.

## Safety invariants

Unchanged:

- MySQL is durable truth.
- Redis is coordination/rate/control, not durable progress.
- No blind Redis cleanup.
- No automatic mass retry.
- No run-wide lease reclaim.
- Lease owner/version/expiry fencing remains authoritative.
- Archive-before-project remains authoritative.
- Natural-key projection freshness remains authoritative.
- The historical source run remains audit evidence.
