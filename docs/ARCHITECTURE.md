# Architecture

## Ownership and dependencies

- `cli/`: arguments, commands and the single-manager lock boundary.
- `client/`: curl_cffi connections, shared auth, retries, adaptive paced admission.
- `contracts/`: identity validation and pure normalization into endpoint records.
- `runtime/`: streaming imports, endpoint definitions, leased async consumers, replay.
- `pipelines/`: discovery orchestration and pre-0.2 compatibility pipelines.
- `storage/mysql/`: async queue repository, synchronous projection pool, transactions,
  raw archives, discovery checkpoints, and packaged Alembic revisions.
- `core/`: validated settings and low-cardinality telemetry.

## Two bounded database paths

The queue/archive path uses SQLAlchemy asyncio + asyncmy. Mature projection SQL
runs in bounded threads using SQLAlchemy's PyMySQL pool. A ContextVar binds nested
repositories to one outer transaction; inner commits become no-ops. This keeps
projection updates, endpoint state and task acknowledgement atomic without
rewriting every existing financial/detail SQL statement.

The manager owns at most `min(concurrency, DBD_QUEUE_SIZE)` endpoint payloads.
Projection threads are bounded by `DB_POOL_SIZE`. Imports and master scheduling
are paged; a million identities are never loaded into memory together.

## Data identities

`company_master.company_id` is the stable internal primary key. The source identity
is unique on `(jp_type_code, jp_no)`. Existing projection tables retain `jp_no`
keys for compatibility. A conflicting type is rejected before a worker updates
those projections. Resolving whether two source identities represent the same
legal entity remains an explicit data-governance decision.

`endpoint_tasks` deduplicates `(run_id, company_id, endpoint)`. Parameters, including
fiscal year, belong to immutable `worker_runs`. Each refresh is a distinct run.
`company_endpoint_state` records the archive and timestamp behind each endpoint
projection. Present null fields in a profile clear old values; absent fields do
not. Search only inserts missing company projections.

## Task lifecycle

`pending -> running -> done | not_available | retry_wait | failed`.
Claims lock one eligible task with SKIP LOCKED; heartbeat extends only unexpired
leases owned by the process. Expired leases are recovered in periodic maintenance.
Lease version plus owner is checked under a row lock before projecting. The
projection and acknowledgement commit together. Retry attempts are finite;
explicit retry resets failed attempts and reopens the run.

HTTP retries cover transient transport/status/token failures. Task retries cover
exhausted HTTP attempts and persistence failures. Both limits are explicit, so the
maximum number of requests is bounded rather than hidden in nested infinite loops.
401 refresh is single-flight: rejection of an old token cannot invalidate a newer
one. 429/503 cooldown is shared inside the one permitted manager process.

## Recovery and evidence

Decoded responses commit to `response_archive` before normalization. A malformed
payload survives even if normalization or projection fails. Replay uses the stored
request context and is fenced like a normal task. Legacy raw rows remain for
existing table foreign keys; this intentionally stores some payloads twice.

Discovery stores `(bucket_id, page_no)` independently. Page projection and its
checkpoint commit together. Successful page counts never stand in for page IDs.
A failed or invalid page prevents the bucket becoming done. Summary counts on
resume are computed from durable page rows.

## Deliberate boundaries

One API manager per configured database; high concurrency is within one process.
There is no distributed global limiter across independent databases. There is no
claim that live API delivery is exactly once, or that overlapping keyword discovery
proves coverage of every company. Benchmarks of the local queue do not measure DBD
server capacity. The current CLI is a batch runner; use an OS scheduler to start
new refresh runs. Interrupted imports remain visible as building runs for review.
