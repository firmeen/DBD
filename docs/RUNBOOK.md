# Runbook

How to take this from an empty database to a full collection, and what to do
when something goes wrong. Configuration reference lives in `OPERATIONS.md`;
design rationale lives in `ARCHITECTURE.md`.

## What is collected

Two tabs of the DBD company page, seven endpoints per company:

| Tab | Endpoints |
| --- | --- |
| 1 ข้อมูลนิติบุคคล | `profile`, `committees`, `committee_signs`, `transformations` |
| 2 ข้อมูลงบการเงิน | `financial_submissions`, `financial_statements`, `industry_benchmark` |

Tabs 3 (การลงทุนจำแนกตามสัญชาติ) and 4 (ประวัติการเปลี่ยนแปลง) are implemented
but not collected. Add them with `--only nationality,history` if ever needed.

## Two phases

```
discover  ->  finds companies, writes a shallow profile for each  (~hours)
bulk      ->  fills in depth for the companies found              (~days)
```

Discovery is cheap per company because one search page carries ten complete
records. Depth is expensive because it is one request per endpoint per company.

---

## Phase 1: discovery

```powershell
dbd-data migrate
dbd-data discover              # full pass; resumes if one is already underway
dbd-data discover-status
```

`discover` with no arguments starts a full pass when nothing is outstanding,
and resumes otherwise. It is safe to interrupt and re-run.

### How coverage is achieved

A Thai juristic person number is thirteen digits: `0` + two-digit registration
province + serial. Search matches numbers as a substring, so a search for
`0` + province returns everything whose number *starts* with it. Seeding one
keyword per province therefore covers the whole registration space by
construction — roughly 77 seeds.

Two consequences worth understanding before reporting coverage:

- **Overlap is expected.** Substring matching also returns companies that
  merely contain the seed elsewhere. They are deduplicated on
  `(jp_type_code, jp_no)`; duplicates cost requests, never correctness.
- **Registration province is not current province.** A company that relocates
  keeps its original number. Seeding is deliberately done on the number, not on
  the `pvCodeList` filter, which matches the current address and would miss
  relocated companies.

### Bucket splitting

The API caps any one query at 10,000 rows (`itemsPerPage` is locked to 10 and
`currentPage` stops at 1000). A larger bucket is split by jpType, then
province, then business size, until every leaf fits.

Each split records whether `SUM(children) == parent`. Read the delta, not just
the flag: a difference of one or two rows is live data changing between counts,
while a difference of thousands means the split is losing rows.

### Verifying the pass

```powershell
dbd-data discover-status
```

Expect `coverage : every collected number matches a seeded prefix`. A
`COVERAGE GAP` line means a company number was collected whose prefix no seed
covers — the one way the seeding assumption can be wrong. Seed those prefixes
explicitly:

```powershell
dbd-data discover 0XX 0YY
```

Note the limit of this check: it can only see numbers that *were* collected. It
cannot detect a company never returned by any seed. For an external check,
compare `discovery_sightings` against DBD's own published totals rather than
treating the row count as proof.

---

## Phase 2: depth

```powershell
dbd-data bulk --master              # enqueue every discovered company, then work it
dbd-data bulk --run-id N            # resume a specific run
dbd-data bulk-status                # progress
dbd-data bulk-status N              # that run's failures
dbd-data bulk-retry N               # requeue its failures
```

Progress is committed per task, so an interrupt costs at most the tasks in
flight. Re-running is always safe.

### Requests are avoided, not just throttled

Three gates remove work before any HTTP happens:

- a company that never filed financial statements skips all three financial
  endpoints (`no_submitted_fiscal_year`);
- an industry benchmark already fetched for the same TSIC/year/size in this run
  is not fetched again (`benchmark_already_current`) — the endpoint is addressed
  per company but answers about the whole industry group;
- financial endpoints are not claimed until that company's profile is `done`,
  because the fiscal year comes from the profile.

Those appear as `not_available` in `bulk-status`. They are skips, not failures.

---

## Scale and expectations

At one million companies, with `DBD_RATE_PER_SECOND=10`:

| Phase | Requests | Time |
| --- | ---: | ---: |
| discovery | ~250k (incl. overlap) | ~7 h |
| depth (7 endpoints) | ~5M | ~6 days |

Rate is the only lever. Time is `requests ÷ rate`; raising `DBD_CONCURRENCY`
above the rate cap changes nothing. Storage runs roughly 45-50 KB per company
in `response_archive`, so plan tens of gigabytes and expect
`financial_statement_items` in the hundreds of millions of rows.

Depth is a multi-day campaign, not a single command. Run it as a sustained job
and let the queue resume across interruptions — that is what it is for.

---

## When something goes wrong

**A run stopped.** Re-run the same command. Nothing needs unwinding.

**Tasks stuck `running`.** Their leases expire and periodic maintenance
recovers them. No manual step.

**Many failures.** Inspect before retrying:

```sql
SELECT endpoint, last_error, COUNT(*)
FROM endpoint_tasks WHERE run_id = :run AND status = 'failed'
GROUP BY endpoint, last_error ORDER BY 3 DESC;
```

**A normalizer was wrong.** The decoded payload is archived before
normalization, so reprocess without touching the network:

```powershell
dbd-data replay ARCHIVE_ID
```

**429 or 503 from DBD.** The shared limiter lowers the rate on its own and does
not retry into it. Leave it alone; do not raise concurrency to compensate.

**Discovery buckets from an older version.** Buckets created before per-page
checkpointing cannot be resumed correctly, because their progress was counted
rather than recorded. Clear the bookkeeping and start a clean pass — company
data is preserved:

```powershell
python scripts\reset_discovery.py           # dry run, shows what would change
python scripts\reset_discovery.py --apply
```

---

## Before reporting results

- Row counts in the thousands are partial runs, not results. Thailand has
  roughly 1.9 million registered juristic persons.
- `not_available` is a deliberate skip, not a gap in the data.
- Coverage is provable by construction *given* the number format; the
  `COVERAGE GAP` check tests that assumption against collected data, and an
  external reconciliation is still the honest way to claim completeness.
- Data is what DBD publishes for public service and, per their own notice,
  cannot be used as a legal reference.
