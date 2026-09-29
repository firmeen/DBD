# Architecture

## Ownership and dependencies

- `cli/`: arguments, commands, exit policy and manager-lock boundaries.
- `client/`: browser-like session/auth, crypto, retries, worker-local availability isolation and shared pacing.
- `contracts/`: identity/root-shape validation and pure normalization.
- `runtime/`: endpoint definitions, staged bulk orchestration and replay.
- `pipelines/`: discovery orchestration and compatibility pipelines.
- `storage/mysql/`: durable queue/archive metadata, projections and migrations.
- `storage/archive_codec.py`: deterministic JSON/zstd archive encoding.
- `storage/archive_backend.py`: optional client-side encrypted S3-compatible payload storage.
- `core/`: validated settings, exceptions and low-cardinality telemetry.

Default collection scope is tabs 1-2: seven endpoints. Tabs 3-4 remain opt-in.

## V2 staged bulk path

```text
endpoint_tasks
  -> batch claim + lease/version fence
  -> bounded fetch queue
  -> async DBD HTTP + endpoint/global admission
  -> bounded archive queue
  -> durable archive boundary
  -> stage=project
  -> bounded project queue
  -> strict normalization + natural-key freshness
  -> projection + terminal acknowledgement
```

Fetch and projection are intentionally decoupled so local database work does not
have to complete inside the HTTP acquisition loop. Queue sizes are bounded so a
fast producer cannot create unbounded memory growth.

Project backlog is scheduled before new fetch work because it is already durable and
requires no additional DBD request.

## Durable task lifecycle

`endpoint_tasks` deduplicates `(run_id, company_id, endpoint)` and has two durable
pipeline stages: `fetch` and `project`.

```text
fetch/pending -> fetch/running
              -> project/running -> done | not_available | retry_wait | failed
```

Claims use `FOR UPDATE SKIP LOCKED`, increment attempts and lease version, and record
owner/expiry. Heartbeat only extends leases still owned and unexpired. Expired leases
are recovered by maintenance. Owner + version + expiry is checked again before archive
transition and projection commit, fencing stale workers.

Projection and terminal acknowledgement commit in the same outer transaction. HTTP
delivery is at-least-once before the durable archive boundary, while project-stage
recovery is archive-first and must not refetch upstream.

## Archive durability boundary

### Inline MySQL

The encoded payload and `stage=project/raw_response_id` transition commit in one
MySQL transaction.

### External encrypted object storage

The encoded payload is uploaded first, encrypted client-side with AES-256-GCM. MySQL
then commits archive metadata/object reference and `stage=project` together. This
ordering prefers an encrypted orphan after an ambiguous DB failure over a committed
reference to an object that was never uploaded.

External archive metadata includes backend, object key, nonce, key id, ciphertext
SHA-256, canonical plaintext SHA-256, codec and size metadata. The decoded response
body is not stored inline for external rows.

External reads verify ciphertext SHA, AES-GCM authentication with object key as AAD,
plaintext SHA and JSON/zstd decode before projection. Missing/corrupt/unreadable
archive data never silently falls back to DBD refetch.

New objects use the active encryption key/id. A read-only decryption key ring keeps
retained archives from older key ids replayable during rotation.

Archive backend/key-shape configuration is validated before the bulk HTTP client
opens or claims work. Remote object-store permission/network failures remain runtime
archive failures and cannot advance a task to `project`.

## Database paths

Queue/archive metadata uses SQLAlchemy asyncio + asyncmy. Existing projection SQL
uses synchronous PyMySQL in bounded threads. A ContextVar binds nested projection
repositories to the outer transaction so apparent nested commits become no-ops until
the projection transaction decides to commit.

Projection concurrency is bounded by both configured projection workers and DB pool
size. Identity import/master scheduling are paged; million-row populations are not
materialized in one Python collection.

## Manager and distributed ownership

Discovery, run creation and ordinary single-process bulk use a MySQL
connection-scoped named manager lock. Long-running locked operations verify that the
same connection still owns it; lock loss is terminal.

Distributed bulk is a deliberate exception after run creation. Multiple processes
join the same run without the singleton manager lock and coordinate task ownership
through row leases/version fencing. Redis coordinates aggregate admission.

## HTTP/auth/rate path

The production distributed request path is:

```text
worker-local availability circuit
  -> worker-local endpoint/global rate caps
  -> Redis aggregate fleet hard-cap admission
  -> optional Redis shared endpoint pacing
  -> token ensure/refresh for this worker session
  -> curl_cffi request
  -> response classification
  -> archive/project pipeline
```

Each worker owns its own session, auth lifecycle and availability circuits.
Authentication failure, transport failure, ordinary 500/502/504, and 503 without
`Retry-After` therefore remain worker/endpoint-local availability signals and do
not lower the Redis fleet ceiling or block healthy peer workers.

Explicit upstream rate pressure remains respected. HTTP 429 and 503 with
`Retry-After` can reduce the shared pacer for the affected endpoint, while the
fleet-wide Redis pacer remains the configured aggregate hard ceiling. Adding worker
processes increases fault tolerance and available concurrency; it does not multiply
that ceiling.

The Supervisor observes progress per worker. A single worker whose admission,
request, response and completion counters stop advancing while fetches remain active
is recycled independently. Multiple worker-local availability failures freeze
autoscale growth rather than creating a replacement storm.

Redis is a coordination/safety mechanism, not a rate-limit bypass.

## Identity model

`company_master.company_id` is the internal stable identity. Source identity is
unique on `(jp_type_code, jp_no)`.

Numeric types require 13-digit strings. Production evidence showed type `A` can be
11-13 digits, so those strings are preserved exactly rather than padded.

Legacy projection tables still key many rows by `jp_no`; projection rejects a type
conflict before writing them. Identity conflicts remain durable quarantine state.

Latest scheduling status lives in `company_master.observed_jp_stat_code` /
`observed_status_at`, separate from the richer company projection.

## Discovery architecture

```text
seed -> bucket count/archive -> split if oversized -> page archive
     -> row normalization -> valid projection + quarantine -> checkpoint
```

Search exposes 10 rows/page and at most 1000 pages/query. Oversized buckets split by
juristic type, province then business size. An exhausted leaf still above the cap is
a hard failure.

Count page 1 can be reused as page 1. Normalization is row-granular: valid neighbors
survive rejected rows, rejected rows are durable, and a page remains `partial` until
resolved. A normal full discovery is incomplete while failed buckets, partial pages,
unresolved quarantine or known lossy split partitions remain.

Seed/prefix checks describe observed rows only and cannot prove external population
completeness.

## Replay and evidence

`response_archive` is the durable endpoint evidence boundary before normalization.
Payload bytes may be inline or externally encrypted; MySQL keeps archive/request/task
metadata either way.

Legacy `raw_responses` remains because projection tables still retain historical
foreign keys to it.

Endpoint replay uses the archive without network access. Search-page correction uses
discovery retry instead.

## Natural-key freshness

`company_endpoint_state` records request-scope observability. It is not the write
fence because request scope and mutated natural keys can differ.

`projection_write_state` fences actual write keys:

- profile/details collections: company + endpoint;
- submissions: whole company collection;
- statements: company + each returned fiscal year;
- benchmark: TSIC + fiscal year + business size + location type + location number.

Projection locks these keys before mutation. Older responses can be archived and
acknowledged stale but cannot overwrite newer natural-key state, including shared
benchmark rows across companies.

## Collection safety

Replace-on-refresh is allowed only after strict root-shape validation. Malformed
transformation, nation-history and benchmark roots cannot masquerade as legitimate
empty/no-data responses. Explicit valid empty collections can clear current data;
profile explicit null remains authoritative while absent fields do not erase data.

## Deliberate boundaries

- no exactly-once claim for pre-archive live HTTP delivery;
- no claim that seed coverage alone proves the complete DBD population;
- no claim that a short probe equals sustainable seven-endpoint capacity;
- no automatic destructive migration downgrade;
- Redis is optional and used only for distributed pacing coordination;
- external object storage is optional and does not replace MySQL task/archive metadata;
- object retention/orphan cleanup is an explicit operational policy, not automatic deletion;
- production 48-hour acceptance, authoritative population reconciliation and backup/restore drills require live evidence.

The correctness model remains conservative: unknown status is not guessed, malformed
data is quarantined/failed rather than converted to absence, project-stage archive
failure never silently becomes a new upstream fetch, and request-saving optimizations
are accepted only when their full natural scope is provable before HTTP.
