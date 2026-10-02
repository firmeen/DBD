# Architecture

## Ownership and dependencies

- `cli/`: arguments, commands, exit policy and manager-lock boundaries.
- `client/`: browser-like session/auth, crypto, retries and adaptive admission.
- `contracts/`: identity/root-shape validation and pure normalization.
- `runtime/`: endpoint definitions, staged bulk orchestration and replay.
- `pipelines/`: discovery orchestration and compatibility pipelines.
- `storage/mysql/`: durable queue/archive metadata, projections and migrations.
- `storage/archive_codec.py`: deterministic JSON/zstd archive encoding.
- `storage/archive_backend.py`: optional client-side encrypted S3-compatible payload storage.
- `core/`: validated settings, exceptions and low-cardinality telemetry.

Default collection scope is tabs 1-2: seven endpoints. Tabs 3-4 remain opt-in.

## Standalone Deep path

Standalone `dbd-deep` เป็นอีก execution path ที่แยกจาก Campaign/Fabric runtime โดยตั้งใจ และไม่ใช้ Redis, Supervisor, Guardian หรือ Campaign run ID เป็น dependency

```text
DBD upstream
    │
    ▼
Discovery
    │
    ├── companies / company_master / discovery_sightings
    │
    ▼
Deep bridge
    │
    ▼
dbd_company_master
waiting population
    │
    │ bounded promotion
    ▼
dbd_company_deep
authoritative Deep row + 7 endpoint state
    │
    ├── dbd_deep_runtime   bounded temporary work only
    │
    └── COMPLETE rows only
             │
             ▼
       dbd_master_deep
       flat MySQL serving
             │
             ▼
TIPDataIntelligence.dbo.DBD_MASTER_DEEP
```

Key invariants:

- company identity is `(jp_type_code, jp_no)`;
- `dbd_company_master ∩ dbd_company_deep = 0`;
- active standalone scope is exactly 7 endpoints: profile, committees, committee_signs, transformations, financial_submissions, financial_statements and industry_benchmark;
- legacy nations/history endpoints remain compatibility data and are not scheduled by standalone Deep;
- `dbd_company_deep` remains the authoritative current Deep state;
- `dbd_master_deep` is derived output only, contains scalar/human-readable fields and no endpoint JSON blobs;
- serving projection uses only `deep_status='COMPLETE'`; when an ACTIVE company is reopened for refresh, its previous serving row remains until a new COMPLETE snapshot is available;
- SQL Server synchronization is INSERT + UPDATE + SKIP by `row_hash`; target-only rows are reported and are not automatically deleted;
- `TIPDataIntelligence.dbo.DBD_COMPANY` is outside this serving pipeline.

Normal collector scheduling priority remains unresolved Deep work → waiting Master companies → stale ACTIVE COMPLETE refresh → idle. The runtime table is bounded temporary work, not a permanent company registry.

### Standalone presentation boundary

`src/dbd/deep/presentation.py` renders `dbd-deep` commands. `src/dbd/presentation/master_deep.py` renders bootstrap, MySQL serving and SQL Server sync. Both consume structured state only; neither owns MySQL/HTTP/scheduling decisions.

Interactive TTY output may use Rich colors and live repaint. Redirected output remains plain newline text, and explicit JSON modes remain machine-readable.

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

The V2 bulk request path is:

```text
concurrency semaphore
  -> endpoint local adaptive gate
  -> optional shared endpoint Redis gate
  -> global local adaptive gate
  -> optional shared global Redis gate
  -> token ensure/refresh
  -> curl_cffi request
  -> status penalty/cooldown
  -> decrypt/wrapper decode
  -> healthy recovery
```

401/token expiry refreshes through one auth lock. 403 is terminal. 429/503 penalizes
the endpoint and global safety layer.

In distributed mode Redis stores the aggregate schedule plus effective-rate,
cooldown and last-penalty state. A 429/503 seen by one process therefore constrains
other workers using the same namespace rather than only the process that received
it. Healthy decoded responses recover gradually toward the configured target.

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
