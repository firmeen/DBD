# Run 9 targeted data repair

This runbook is intentionally conservative. It repairs only projection failures whose
source payload is already durably archived and whose corrected projection semantics are
proven by the repository. It does not turn transport errors into business outcomes.

## Safety rules

- Keep the same durable Run 9.
- Stop the supervisor/workers before `--execute`.
- Run migrations before projection repair.
- Do not run `bulk-retry 9` as a substitute for targeted repair.
- Do not delete Redis rate/circuit keys.
- Do not globally map HTTP 404 to `not_available`.
- Do not resume the campaign until the dry-run output has been reviewed.

## 1. Collect exact repair evidence before changing schema

```powershell
dbd-data campaign evidence 9 --json
```

The report contains:

- endpoint + error code + HTTP status + stage + outcome as one exact matrix
- current terminal-resource intersection
- bounded company/archive samples
- financial-submission collision evidence without dumping raw payload values
- `setupObjNameKeyin` length evidence without dumping the text
- live schema/index/generated-column metadata
- benchmark HTTP 404 candidates marked `semantics_unverified`

The benchmark section is deliberately non-executable. A 404 transport result is not
proof that DBD means “no published benchmark”.

Review the payload-shape evidence before migrating. In particular, confirm that profile
overflow samples really contain a valid `setupObjNameKeyin` value longer than 500
characters and inspect whether submission collisions are exact duplicate rows or
conflicting same-year revisions.

## 2. Apply the guarded lossless schema migration

```powershell
dbd-data migrate
```

Revision `0012_run9_data_repair` widens
`companies.setup_obj_name_keyin` from `VARCHAR(500)` to `TEXT` only after
checking the live column type and refusing automatic change when an index or generated
column depends on that field. There is intentionally no standalone raw ALTER migration:
the guarded Alembic revision is the authoritative upgrade path.

## 3. Dry-run targeted repair

```powershell
dbd-data campaign repair 9 --json
```

Dry-run is the default. Eligible archive replay is limited to:

1. `financial_submissions + IntegrityError` where the archived source has one or
   more structurally identical duplicate rows for a submit year and no conflicting
   same-year revision.
2. `profile + DataError` where the durable failure names
   `setup_obj_name_keyin`, the archived value exceeds 500 characters, and the live
   column has already been migrated to a lossless text type.

Everything else remains untouched.

## 4. Execute only after reviewing the plan

```powershell
dbd-data campaign repair 9 --execute --json
```

Execution refuses an active campaign. It takes the manager lock, revalidates current
terminal state and archive identity, and reopens **at most one evidence-verified endpoint
per company scheduler state per execution** at `stage=project`. This prevents a later
Fabric transition from chaining into a different replayable archive that was not the
exact archive inspected by the repair evidence. If one state has several independently
repairable failed endpoints, run the dry-run/execute cycle again after the first replay
finishes.

No DBD fetch is scheduled for these resources. The normal worker projection path loads
the existing `response_archive` payload and preserves archive-before-project and
natural-key freshness fencing.

The operation is idempotent: once a state has been reopened, a repeated execution does
not reopen it again.

## 5. Benchmark HTTP 404 remains blocked

This PR does **not** implement:

```text
industry_benchmark + HTTP 404 -> not_available
```

Before that can exist, Run 9 evidence and controlled upstream observations must prove
that the endpoint uses 404 as a business-level “no published benchmark” signal rather
than a generic request failure. A durable negative-observation/freshness model must
also be designed because current `company_endpoint_state.archive_id` requires an
archive-backed observation.

Until then, benchmark 404 failures remain terminal evidence and are not mutated by
`campaign repair`.
