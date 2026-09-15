# Architecture

## Ownership and dependencies

- `cli/`: arguments, commands, exit policy and manager-lock boundaries.
- `client/`: browser-like session/auth, crypto, retries and adaptive paced admission.
- `contracts/`: identity and root-shape validation plus pure normalization.
- `runtime/`: endpoint definitions, streaming imports, leased consumers and replay.
- `pipelines/`: discovery orchestration and pre-0.2 compatibility pipelines.
- `storage/mysql/`: async queue/archive path, sync projection path, migrations and
  repositories.
- `core/`: validated settings, exceptions and low-cardinality telemetry.

The default collection scope is tabs 1-2: seven endpoints. Tabs 3-4 remain
implemented and test-covered but are opt-in.

## One manager, two bounded DB paths

The queue/archive path uses SQLAlchemy asyncio + asyncmy. Existing projection SQL
uses the synchronous PyMySQL path in bounded threads. A ContextVar binds nested
projection repositories to the outer transaction so their apparent commits become
no-ops until the projection transaction decides to commit.

The manager owns no more endpoint consumers than the configured queue/concurrency
bounds. Projection writes are separately limited by `DB_POOL_SIZE`. Identity import
and master scheduling are paged; a million identities are never required in one
Python set/list.

A MySQL connection-scoped named lock enforces one API manager per configured
database. Long-running bulk/discovery operations verify that the **same connection**
still owns the lock. Losing the connection/lock is a terminal manager-health error,
not permission to continue collecting without exclusivity.

## Identity model

`company_master.company_id` is the internal stable identity. Source identity is
unique on `(jp_type_code, jp_no)`.

Numeric types require 13-digit registration strings. Production evidence showed
that type `A` can be 11-13 digits, so those strings are preserved exactly rather
than padded by assumption.

Legacy projection tables still key many rows by `jp_no`; projection rejects a type
conflict before writing those tables. Identity-conflict resolution remains a data
governance decision rather than an automatic merge.

`company_master.observed_jp_stat_code` and `observed_status_at` hold the latest
status observation used for scheduling. This is separate from the richer
`companies` projection because discovery intentionally does not overwrite an
existing authoritative company profile with a shallow search row.

## Task lifecycle

`endpoint_tasks` deduplicates `(run_id, company_id, endpoint)`.

```text
pending -> running -> done | not_available | retry_wait | failed
```

Claim uses `FOR UPDATE SKIP LOCKED`, increments attempts and lease version, and
records owner/expiry. Heartbeat only extends leases still owned and unexpired.
Expired leases are recovered by maintenance. Lease owner + version + expiry is
checked again under row lock before projection, fencing stale workers.

Projection and endpoint-task acknowledgement commit in the same outer transaction.
A process can therefore deliver a request more than once, but an old lease cannot
successfully acknowledge after ownership has moved.

HTTP retries handle transient transport/status/token errors. Durable task retries
handle exhausted HTTP attempts and persistence failures. Both are finite.

## HTTP/auth/rate path

The async request path is:

```text
shared semaphore
  -> paced TokenBucket
  -> token ensure/refresh
  -> curl_cffi request
  -> status classification/cooldown
  -> decrypt/wrapper decode
  -> normalize/project
```

401/token expiry refreshes through a single auth lock. 403 is terminal. 429/503
reduce the effective process rate and apply shared cooldown. Healthy successes
recover the effective rate gradually toward the configured target.

Diagnostics expose safe stage/path/type/curl-code/status metadata and never dump
credentials or complete response bodies.

## Discovery architecture

Discovery is a durable search-plan engine rather than a simple crawler:

```text
seed -> bucket count/archive -> split if oversized -> per-page archive
     -> row normalization -> valid-row projection + quarantine -> checkpoint
```

The search backend exposes 10 rows/page and at most 1000 pages/query. Oversized
buckets are split by juristic type, province then business size. Exhausting those
dimensions while still over the cap is a hard failure.

The count request archives page 1. For a drainable bucket the same archive is
linked to page 1, avoiding an unnecessary duplicate request.

Each page is checkpointed independently. Normalization is row-granular:

- valid rows are persisted immediately;
- bad rows are recorded in `discovery_rejections`;
- the page becomes `partial` while any row remains rejected;
- retries re-fetch only non-done pages and resolve prior quarantine observations
  when the page later passes.

A normal full discovery run is incomplete while failed buckets, partial pages,
unresolved quarantines or known lossy split partitions remain.

Seed/prefix checks operate only on observed rows. They cannot prove the upstream
search returned every legal entity; final population completeness is an external
reconciliation problem.

## Raw evidence and replay

`response_archive` stores decoded runtime payloads before normalization together
with request context and processing status. Search pages and endpoint responses use
the same durable evidence principle.

Legacy `raw_responses` remains because projection tables still hold foreign keys to
it. Some payload data is therefore intentionally stored twice until a future
migration removes that dependency.

Endpoint replay uses the same projection/lease path without a network request.
Search-page correction uses discovery retry instead.

## Two freshness models

`company_endpoint_state` is request-scope observability: company + endpoint + scope
records which request/archive was most recently accepted for that scope.

It is **not** sufficient as the write fence because request scope and mutated rows
are not always identical. `projection_write_state` therefore fences the natural
keys actually written:

- profile/details collections: company + endpoint;
- submissions: whole company collection;
- financial statements: company + each returned fiscal year;
- industry benchmark: TSIC + fiscal year + business size + location type +
  location number.

The projection transaction locks these write keys before mutation. Older responses
can still be archived and acknowledged as stale, but cannot overwrite newer
natural-key state. This also protects shared benchmark rows across companies.

## Collection safety

Replace-on-refresh is allowed only after root-shape validation. Strict contracts
specifically prevent malformed transformation, nation-history and benchmark roots
from being interpreted as legitimate empty/no-data responses.

Explicit empty collections in a valid shape remain meaningful and may clear the
current collection. Profile explicit null remains authoritative; absent fields do
not erase existing values.

## Financial model

`financial_statements` stores one header per company/year and
`financial_statement_items` stores a tall item-code table. A single upstream
response may contain multiple fiscal years; each returned year is fenced
independently.

`financial_submissions` is a replace-all company collection, so its natural write
key is the entire company submission set.

`industry_benchmarks` is shared industry/location data, not a company-owned table.
The former TSIC/year/size pre-HTTP skip was removed because location fields are part
of the persisted key but cannot be safely inferred before the response.

## Deliberate boundaries

- one manager per database, not a distributed global limiter across databases;
- no exactly-once claim for live HTTP delivery;
- no claim that seed coverage alone proves the complete DBD population;
- no claim that a short profile probe equals sustainable mixed-endpoint capacity;
- no automatic destructive downgrade;
- no external broker required for the current single-manager architecture;
- no automatic long-term raw-data retention policy yet.

The current correctness model is intentionally conservative: unknown status is not
guessed, malformed data is quarantined/failed rather than converted to absence, and
request-saving optimizations are accepted only when their full natural scope is
provable before HTTP.