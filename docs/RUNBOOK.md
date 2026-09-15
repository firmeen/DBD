# Runbook

Operational path from an empty MySQL schema to a durable DBD collection. Current
production scope is tabs 1-2: seven endpoints per company.

## 1. Prepare

```powershell
dbd-data check-db
dbd-data migrate
dbd-data --version
```

Back up the database before schema maintenance. Migrations are forward-only; the
supported rollback is restore-from-backup.

## 2. Discovery

```powershell
dbd-data discover
dbd-data discover-status
dbd-data master-status
```

With no explicit seeds, discovery upserts the current audited prefix set. It keeps
existing checkpoints and includes the explicit `099` exception observed in prior
production data. Search is substring-based, so overlap is normal and identities
are deduplicated on `(jp_type_code, jp_no)`.

### Search limits and splitting

The upstream search endpoint returns 10 rows/page and allows at most 1000 pages per
query. A bucket over 10,000 rows is split in this order:

1. juristic-person type;
2. province filter;
3. business size.

If a leaf still exceeds 10,000 after all dimensions are exhausted, it is marked
failed rather than silently truncated.

### Durable page handling

The page-1 count request is archived. If the bucket is small enough to drain, that
same response is linked as page 1 instead of issuing the request twice.

Every page is archived before row normalization. Rows are normalized independently:

- valid rows are committed;
- invalid rows are stored in `discovery_rejections` with the original decoded row,
  row index and safe error reason;
- a page containing rejected rows is `partial`, not `done`;
- retry re-fetches only pages not already `done` and resolves old quarantine rows
  when the corrected page passes.

A full discovery invocation exits non-zero while any of these blockers remain:
failed buckets, partial pages, unresolved quarantine rows or known lossy splits.
`--max-buckets` is intentionally a partial/trial run and does not apply that final
completeness exit rule.

### Coverage statement

`discover-status` checks collected identities against registered seed prefixes and
reports split-count mismatches. These checks are necessary but not sufficient for
an “all companies” claim: an entity never returned by any search cannot appear in
`discovery_sightings`. Final completeness therefore requires reconciliation with an
authoritative external DBD population/count source.

### Retry and refresh

```powershell
dbd-data discover-retry
dbd-data discover-refresh
dbd-data discover-refresh --bucket-id N
```

`discover-retry` reopens failed buckets. `discover-refresh` intentionally starts a
new read of existing leaf buckets by removing their page checkpoints while keeping
raw/archive evidence and company projections.

Do **not** reset discovery merely to recover network errors. The queue/checkpoints
are built for resume.

## 3. Full tabs 1-2 campaign

The accepted all-company scope includes every discovered status:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

Record the returned run ID. Resume **that exact run**:

```powershell
dbd-data bulk --run-id N --concurrency <MEASURED_CONCURRENCY> --rate <MEASURED_RATE>
dbd-data bulk-status N
dbd-data bulk-retry N
```

Do not use `bulk --master` again as a resume command; it creates a new refresh run.

Default endpoint set:

| Tab | Endpoints |
| --- | --- |
| 1 | `profile`, `committees`, `committee_signs`, `transformations` |
| 2 | `financial_submissions`, `financial_statements`, `industry_benchmark` |

Tabs 3-4 stay available through `--only nationality,history`, but are outside the
current 48-hour acceptance campaign.

### Operating-only mode

`dbd-data bulk --master` without `--all-statuses` filters on the latest observed
`jpStatCode` stored in `company_master`, not a potentially stale shallow company
projection. Only observed status `1` is selected. Unknown status is not guessed.

This mode is useful for an operating-only product workload, but it is **not** the
accepted all-company shortcut for the current campaign.

## 4. Financial prerequisites

Financial tasks wait for the profile task because fiscal-year metadata comes from
that projection unless an explicit run fiscal year is supplied. If no submitted
fiscal year exists, financial tasks become `not_available` with
`no_submitted_fiscal_year`.

`not_available` means confirmed source absence. Network errors, contract errors,
quarantined search rows and companies never discovered do not count as unavailable.

The old cross-company `industry_benchmark` pre-fetch skip is intentionally gone.
The benchmark natural key includes `location_type` and `location_no`, which are not
safely known from TSIC/year/size before the response. Natural-key freshness prevents
stale overwrites after the full response scope is known.

## 5. Interruption and leases

Only one API manager may operate on a database at a time. Bulk and discovery check
that the same MySQL connection still owns the named lock. If ownership disappears,
the manager fails instead of continuing without exclusivity.

Ctrl+C returns leases owned by the process to pending. If the process dies, expired
leases are recovered by maintenance. Lease owner, version and expiry are checked
again inside the projection transaction, so a stale worker cannot commit after its
lease is reclaimed.

## 6. Replay after contract fixes

A decoded endpoint response is archived before normalization. After correcting a
normalizer:

```powershell
dbd-data replay ARCHIVE_ID
```

Replay does not contact DBD. Projection freshness is based on natural write keys:

- statement: company + returned fiscal year;
- submissions: whole company collection;
- benchmark: TSIC + fiscal year + size + location type + location number;
- other resources: company + endpoint collection.

Older archives are therefore prevented from overwriting newer rows even when the
old and new requests used different request scopes.

Search pages use discovery retry, not endpoint replay.

## 7. Failure inspection

```sql
SELECT endpoint, status, last_error, COUNT(*)
FROM endpoint_tasks
WHERE run_id = :run
GROUP BY endpoint, status, last_error
ORDER BY endpoint, status, COUNT(*) DESC;

SELECT processing_status, error_code, endpoint, COUNT(*)
FROM response_archive
GROUP BY processing_status, error_code, endpoint
ORDER BY COUNT(*) DESC;

SELECT bucket_id, page_no, row_index, jp_type_code, jp_no,
       error_code, error_message
FROM discovery_rejections
WHERE resolved_at IS NULL
ORDER BY bucket_id, page_no, row_index;
```

Transport diagnostics intentionally report safe metadata only: stage, path,
exception type, curl code where available and HTTP status. They never print auth
tokens, cookies, encryption keys or whole response bodies.

## 8. Discovery reset tool

For genuinely incompatible historical checkpoint state only:

```powershell
python scripts\reset_discovery.py
python scripts\reset_discovery.py --apply
```

Dry-run is default. `--apply` acquires the same manager lock before mutation and
preserves company data, raw payloads, endpoint tasks and worker runs.

## 9. Capacity probe

Stop every collector first, then:

```powershell
python scripts\probe_capacity.py --collector-stopped --min-rate 5 --max-rate 25
```

The probe is profile-only and bounded. It reports admission/window throughput,
HTTP latency, drain time and transport error breakdown. It is **not** a sustained
full-endpoint benchmark and does not by itself prove a safe production rate or a
48-hour ETA.

## 10. 48-hour acceptance

Start the campaign clock at a recorded timestamp and report discovery plus depth as
one wall-clock campaign. Required evidence:

- discovery has no failed/partial/quarantine/lossy blockers;
- external population reconciliation is documented;
- the exact all-status worker run is recorded;
- no failed endpoint tasks remain after retry/replay policy is exhausted;
- `not_available` counts are source-supported;
- actual request mix, successful throughput, retry overhead, DB projection time
  and remaining backlog are recorded;
- total elapsed wall time is <= 48 hours.

A short profile probe, queue benchmark or arithmetic extrapolation alone is not an
acceptance result.