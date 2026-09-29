# Validation

This document separates code/CI guarantees from historical live observations and
from production acceptance evidence that still has to be measured.

## Current CI evidence

The authoritative validation record is the **latest GitHub Actions run for the
current PR head**. The workflow runs:

```text
python -m compileall -q src scripts tests
ruff check .
pytest -q
```

on Python 3.12 with disposable MySQL 8.4 and Redis services. Integration tests apply
Alembic migrations to real MySQL and exercise queue/projection/Redis behavior; they
are not mocks of SQL transaction semantics.

Do not copy a pass count or commit SHA from an older PR head into this document.
Every source, migration, test or documentation commit creates a new head and that
exact head must be green before the PR is considered validated.

## Correctness guarantees covered by tests

The current suite covers, among other cases:

- strict company identity parsing without numeric coercion or lost leading zeros;
- observed type `A` identities with 11, 12 and 13 digits;
- malformed collection roots cannot masquerade as valid empty data;
- row-level discovery quarantine preserves valid neighboring rows;
- partial discovery pages retry without re-fetching completed pages;
- async token refresh is single-flight and token expiry during decrypt retries safely;
- 403 remains terminal;
- lease fencing, expired-lease recovery and cancellation lease release;
- projection + acknowledgement transaction semantics;
- natural-key freshness for statements, replace-all submissions and shared benchmark
  scope across companies;
- durable identity-conflict quarantine excluded from generic retry;
- migration repeatability after partially committed MySQL DDL;
- batched `FOR UPDATE SKIP LOCKED` claims are disjoint across owners;
- fetch response archive + `stage=project` creates a crash/restart boundary that can
  resume without another DBD HTTP request;
- fetch and project retry budgets are independent through migration
  `0007_project_attempt_budget`;
- a response archived on the final allowed fetch attempt survives lease expiry and
  resumes projection without another DBD request;
- project backlog is drained before new fetch work;
- JSON/zstd archive encoding and replay compatibility;
- mixed-capacity probe workload construction, endpoint parsing and deadline/drain
  behavior;
- worker-local availability/circuit isolation for auth/session/transport failures;
- Redis pacers share one aggregate request schedule;
- explicit hard fleet ceilings remain independent of worker count;
- hard-cap admission ignores stale legacy global effective-rate/cooldown state, and
  fenced Supervisor activation resets the old reservation cursor without deleting
  Redis coordination keys;
- endpoint rate-pressure state propagates across workers using the same key without
  turning one worker's availability failure into a global fleet circuit;
- the per-worker request watchdog does not recycle a worker while every active fetch
  is intentionally waiting behind shared Redis pacing/cooldown, preserving explicit
  Retry-After behavior;
- legacy Guardian profile v1 fixed `4/4/4` fleets migrate to bounded `4..12` only
  when an explicit fleet hard cap already exists, while explicit operator overrides
  and uncapped legacy profiles remain unchanged;
- external S3-compatible archives are encrypted client-side with AES-256-GCM;
- external JSON and zstd payloads round-trip through encryption/decryption;
- ciphertext tamper and canonical serialized-payload checksum mismatch are rejected;
- old archive key ids remain readable through the explicit decryption key ring while
  new objects use only the active key;
- external archive MySQL rows retain reference/nonce/key/checksum metadata without
  retaining the decoded response body inline;
- an externally archived project-stage task resumes from the object without DBD HTTP;
- failed external PUT cannot create an archive reference/project-stage transition;
- invalid external archive configuration fails before the DBD client is opened;
- migrations `0005_high_throughput_pipeline`, `0006_external_archive_backend` and
  `0007_project_attempt_budget` adopt already-applied DDL repeat-safely.

## V2 durable archive semantics

### Inline MySQL

The serialized payload and task transition to `stage=project` commit in one MySQL
transaction. A committed project task therefore has a committed archive row.

### Encrypted external object storage

The encoded response is uploaded first, client-side encrypted with AES-256-GCM. Only
then may MySQL commit the object reference and task transition to `stage=project`.
This ordering can leave an unreferenced encrypted object if the database fails after
upload, but it prevents a committed project task from being created before the object
was successfully written.

External reads verify:

1. ciphertext SHA-256 against MySQL metadata;
2. AES-GCM authentication using object key as AAD;
3. restore canonical serialized JSON bytes from JSON/zstd;
4. serialized-payload SHA-256;
5. JSON decoding.

Object/key/integrity/decode failure does not silently refetch DBD. The archive remains
the recovery boundary.

Archive configuration is built before the bulk HTTP client opens or work is claimed,
so an invalid backend/bucket/key shape fails before spending an upstream request.
Remote object-store permission/network failure still occurs at upload time and follows
the fetch-stage retry/failure policy without advancing to `project`.

### Independent fetch/project attempts

`endpoint_tasks.attempts` is the fetch-stage retry counter and
`endpoint_tasks.project_attempts` is the projection-stage retry counter. Fetch
exhaustion therefore cannot discard a response that has already crossed the durable
archive boundary. Lease recovery uses the counter for the task's current stage, and
`bulk-retry` resets only the failed stage's budget.

## Distributed rate semantics

Distributed workers coordinate durable work through MySQL leases and admission
through one Redis namespace. With explicit fleet configuration, Redis enforces a
hard aggregate fleet ceiling while worker-local caps remain independent. Worker
auth/session and availability circuits are not shared. HTTP 429 and 503 with
`Retry-After` remain explicit rate-pressure evidence and can constrain the shared
pacer for the affected endpoint. Adding workers does not multiply the hard fleet
ceiling.

This is a safety/cooperation mechanism, not a way to increase or bypass an upstream
rate limit.

## Identity format: corrected statement

Numeric company-number types remain strict 13-digit strings. Historical production
evidence found type `A` rows outside that format, including 12-digit and 11-digit
values. The collector accepts 11-13 digits for type `A` and preserves the source
string exactly; it does not pad by assumption.

Therefore “every collected company number is 13 digits and starts with 0” is not a
valid population-coverage proof.

## Discovery guarantees and limits

Confirmed by implementation/integration coverage:

- decoded search payload is archived before row normalization;
- page-1 count response can be reused as page 1;
- valid rows survive invalid neighbors;
- rejected rows are durable in `discovery_rejections`;
- partial pages do not become completed checkpoints;
- a normal full run exits non-zero for failed buckets, partial pages, unresolved
  rejections or known lossy split partitions;
- explicit `099` seed is included alongside province-derived prefixes;
- manager-lock ownership is checked during discovery.

A zero seed-gap report still describes **observed** companies only. It cannot identify
a company never returned by any seed. Live data may also change while multi-page
pagination is running. Final complete-population acceptance therefore requires an
authoritative external reconciliation.

## Historical live API observations

These remain protocol evidence, not current throughput guarantees:

| Property | Observed value |
| --- | --- |
| search page size | 10 rows; larger `itemsPerPage` ignored |
| maximum page | 1000; effectively 10,000 accessible rows/query |
| search keyword | required, minimum 3 characters |
| search semantics | substring/company-name match |
| encryption | AES-GCM with HKDF-SHA256 from JWT `encKey` |
| AAD | `bdw\|v{kid}\|{path}` without query string |
| browser behavior | plain requests observed 403; Chrome-impersonating `curl_cffi` succeeds |

Financial statement variants were observed to return the same payload, so one
request can contain multiple fiscal years. This is why write freshness uses returned
natural keys rather than only the requested year.

`/fin/basics` is addressed by company but represents a shared industry benchmark.
Its persisted natural key includes TSIC, fiscal year, business size, location type
and location number. Location scope is not safely known before the response, so the
old pre-fetch cross-company skip remains intentionally removed.

## Historical capacity probe snapshot

Historical profile-only snapshot from 15 September 2026, concurrency 16:

| Target | Repeat | Window success/s | HTTP p95 | Transport errors | 429/503 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 20/s | 1 | 16.80 | 0.377 s | 0 | 0 |
| 20/s | 2 | 16.30 | 0.442 s | 0 | 0 |
| 25/s | 1 | 20.03 | 0.923 s | 6 | 0 |

The 20/s target did not deliver 20 successful responses/s in-window. The 25/s sample
showed transport instability without observed 429/503. This was a short profile-only
probe, not the current mixed seven-endpoint persisted V2 pipeline. The new mixed
probe must be rerun before selecting a production rate.

## Throughput model

Do not estimate completion as `requests / configured_rate` alone. End-to-end wall
clock also contains discovery, retries, cooldowns, auth/session work, DB/archive
latency, object-store latency when enabled, projection, source absences and restarts.

For the strict seven-endpoint upper-bound campaign, planning arithmetic is:

```text
required_success_per_second = (7 * companies) / 172800
```

That is only a theoretical lower bound before retry/operational margin. No current
code test proves that public DBD accepts the required sustained live rate.

## Data semantics

- `response_archive` is the durable archive metadata/source-of-truth boundary before
  normalization;
- payload bytes may be inline MySQL or encrypted external object storage;
- legacy `raw_responses` remains because projections retain historical foreign keys;
- profile explicit null can clear an old authoritative field; absent fields do not;
- collection writers replace current data only after strict normalization;
- statement items are tall rows keyed by company/year/item code;
- benchmark rows are shared natural-key data, not company-owned rows;
- `company_endpoint_state` is request-scope observability;
- `projection_write_state` is the natural-key freshness fence.

## Still not validated by this PR

1. Complete reconciliation against the authoritative set of Thai juristic persons.
2. A clean live discovery campaign after the current correctness changes.
3. The exact historical transport-error cause around the earlier 25/s profile probe.
4. A sustained safe DBD ceiling for the real seven-endpoint production mix.
5. A persisted production canary proving selected rate/concurrency under DB/archive
   and projection load.
6. A full immutable all-status campaign finishing in <= 48 hours.
7. Production-scale storage growth and object-store latency/cost at final population.
8. A real backup/restore drill covering MySQL + retained external objects + encryption
   key ring, if external storage is enabled.
9. The final long-term retention/lifecycle/orphan-reconciliation policy.

These require live operational evidence. They must not be converted into a code-level
“passed” claim.
