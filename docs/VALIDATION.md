# Validation

This document separates what is proven by current code/tests from historical live
measurements and from assumptions that still need production evidence.

## Current CI evidence

PR #1 head before the final documentation pass:

```text
commit 58ab1b01d972c0a708fe6964f36acd85129fef5e
python -m compileall -q src scripts tests   PASS
ruff check .                                PASS
pytest -q                                   50 passed in 7.24s
```

The workflow runs on Python 3.12 with a disposable MySQL 8.4 service using
`DB_NAME=dbd_test_ci` and `DBD_INTEGRATION=1`. Integration tests therefore apply
Alembic migrations to a real MySQL server and exercise queue/projection behavior;
they are not mocks of SQL semantics.

The final PR head must rerun this workflow after documentation/code changes.

## Correctness guarantees covered by tests

The current suite covers, among other cases:

- strict company identity parsing without numeric coercion or lost leading zeros;
- observed type `A` identities with 11, 12 and 13 digits;
- malformed collection root shapes cannot masquerade as empty data;
- explicit valid empty collections remain valid;
- row-level discovery quarantine preserves valid neighboring rows;
- partial discovery pages retry without re-fetching completed pages;
- count-page archive/reuse behavior;
- invalid/empty province responses fail as contract errors;
- async token refresh is single-flight;
- token expiry during decrypt retries safely;
- 429 shared cooldown and gradual healthy rate recovery;
- 403 remains terminal;
- lease fencing, expired-lease recovery and cancellation lease release;
- projection + acknowledgement transaction semantics;
- archive-before-normalize behavior;
- latest observed scheduling status overriding stale shallow status;
- natural-key freshness for financial statements across request-year scopes;
- replace-all submission freshness;
- shared industry benchmark freshness across different companies;
- all 11 implemented endpoints still project when explicitly requested;
- default endpoint scope remains tabs 1-2 only;
- migration repeatability on a disposable MySQL database.

## Identity format: corrected statement

Numeric company-number types remain strict 13-digit strings. Production evidence
from the earlier discovery snapshot found type `A` rows that do **not** fit that
format: 37 rows had 12 digits and one row had 11 digits. The collector now accepts
11-13 digits for type `A` and preserves the source string exactly. It never pads a
short type-A value by assumption.

Therefore the old statement “every collected company number is 13 digits and
starts with 0” is false and must not be used as a proof of population coverage.

## Discovery behavior

Confirmed from implementation and integration tests:

- decoded search payload is archived before row normalization;
- the count request for page 1 can be reused as page 1 when the bucket is drainable;
- valid rows are persisted independently of invalid rows on the same page;
- rejected rows are durable in `discovery_rejections`;
- partial pages do not become completed checkpoints;
- a normal full run exits non-zero if failed buckets, partial pages, unresolved
  rejections or known lossy split partitions remain;
- the explicit `099` seed is included alongside province-derived prefixes;
- manager lock ownership is checked while discovery runs.

### What discovery does not prove

A zero seed-gap report only says every **observed** company is explainable by a
registered seed. It cannot identify an entity never returned by any seed. A final
“all companies” statement therefore requires reconciliation against an authoritative
external DBD population/count source.

Live data may also change while a multi-page bucket is being read. Parent/child
count equality detects one class of partition problem, but equal counts do not
prove that the set of identities was unchanged during pagination.

## API behavior measured historically

The following findings were measured against the live public service and remain
useful protocol evidence:

| Property | Observed value |
| --- | --- |
| search page size | 10 rows; larger `itemsPerPage` ignored |
| maximum page | 1000; effectively 10,000 accessible rows/query |
| search keyword | required, minimum 3 characters |
| search semantics | substring match and company-name match |
| encryption | AES-GCM with HKDF-SHA256 from JWT `encKey` |
| AAD | `bdw\|v{kid}\|{path}` using path without query string |
| browser behavior | plain requests observed 403; Chrome-impersonating `curl_cffi` succeeds |

Financial statement compare-page variants were observed to return the same payload,
so one statements request can contain multiple fiscal years and multiple statement
views. This is why write freshness is now fenced by each returned fiscal year, not
by the request fiscal year alone.

`/fin/basics` is addressed by company but represents an industry benchmark scope.
The persisted natural key contains TSIC, fiscal year, business size, location type
and location number. Because location scope is not safely known before the response,
the previous TSIC/year/size pre-fetch skip was removed.

## Latest recorded capacity probe snapshot

Historical snapshot from 15 September 2026, concurrency 16, 20-second steps,
two repeats where available:

| Target | Repeat | Window success/s | HTTP p95 | Transport errors | 429/503 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 20/s | 1 | 16.80 | 0.377 s | 0 | 0 |
| 20/s | 2 | 16.30 | 0.442 s | 0 | 0 |
| 25/s | 1 | 20.03 | 0.923 s | 6 | 0 |

Interpretation:

- the 20/s target did not produce 20 successful responses/s in the timed window;
- 25/s produced repeated transport instability in historical runs despite no
  observed 429/503;
- absence of 429/503 is not proof that a higher load is safe;
- this was a profile-only short-window probe, not the full seven-endpoint pipeline;
- the probe has since been hardened to report safe transport class/curl-code
  diagnostics and separate admission/window throughput from post-window drain.

A new probe run is required before recommending a production rate. The historical
numbers are evidence, not a current ceiling.

## Throughput model

Do not use `requests / configured_rate` as the sole ETA. End-to-end wall time also
contains discovery, retries, cooldown, auth/session work, DB projection, queue
scheduling, source absences and incomplete-page recovery.

For scenario analysis only, a rough request model for the default scope is:

```text
R ≈ D + 4N + 2F + B + retry_overhead
```

where `D` is discovery requests, `N` discovered companies, `F` companies requiring
financial detail and `B` benchmark requests. The benchmark term is intentionally
not assumed to deduplicate safely before HTTP.

The earlier scenario range remains illustrative, not authoritative:

- 1.0M companies: roughly 4.55M-7.55M total requests;
- 1.9M companies: roughly 8.15M-13.85M total requests.

At 48 hours these imply high sustained successful rates. No current measurement
proves that the public service plus full local pipeline can sustain those rates.

## Local database evidence

The remote profile probe's local RTT check uses a temporary table and is explicitly
not a full projection benchmark. `scripts/benchmark_queue.py` exercises synthetic
queue claim/finish throughput on a guarded `dbd_test_*` database and never contacts
DBD. Neither measurement is a replacement for a full mixed-endpoint persistence
benchmark.

## Data semantics

- `response_archive` is the durable decoded runtime archive before normalization.
- legacy `raw_responses` remains because projection tables retain foreign keys to it.
- profile explicit null can clear an old authoritative field; absent fields do not.
- collection writers replace the current collection only after strict normalization.
- financial statement items are tall rows keyed by company/year/item code.
- industry benchmark rows are shared natural-key data, not company-owned rows.
- `company_endpoint_state` records request-scope observability.
- `projection_write_state` is the natural-key freshness fence used to prevent stale
  writes across request scopes or companies.

## Still not validated by this PR

1. Complete reconciliation against the authoritative total set of Thai juristic
   persons.
2. A clean live discovery campaign after deploying the type-A/quarantine changes.
3. The exact transport-error cause previously observed around the 25/s probe target;
   the new diagnostics make the next run actionable but cannot reconstruct old
   errors retrospectively.
4. A sustained safe DBD request ceiling for the seven-endpoint production mix.
5. A full all-status end-to-end campaign finishing in <= 48 hours.
6. Production-scale storage/queue behavior at the final population size.
7. Backup/restore drill and long-term archive-retention policy.

Those items require live operational evidence; they must not be converted into a
code-level “passed” claim.