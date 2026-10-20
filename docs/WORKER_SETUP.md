# DBD v0.6 — Worker Setup (SQL Server)

## v0.6 Migration command #04 — safety contract (PR #102 draft)

`dbd-data migrate` is **read-only by default**. Use `--check` (or `--dry-run`) to inspect the selected Production database before authorizing DDL. `--backend sqlserver|mysql`, `--db` and `-d` select the backend; the `.env` default remains supported.

```powershell
dbd-data migrate --backend sqlserver --check
dbd-data migrate --backend mysql --check
# Review the actual plan, legacy evidence, permissions and BLOCKERS first.
dbd-data migrate --backend sqlserver --apply
dbd-data migrate --backend mysql --apply
# Noninteractive automation ONLY, after explicit operator approval:
dbd-data migrate --backend sqlserver --apply --yes
dbd-data check-db --backend sqlserver
```

Interactive `--apply` requires typing `MIGRATE`. Noninteractive Apply requires both `--apply --yes`. `--check` never executes DDL or writes Control, Generation, Discovery, Branch or business rows. The approved plan is re-read under the existing Production Manager Lock; another schema/state change blocks Apply.

SQL Server uses a transaction and rolls it back when an exception occurs. MySQL DDL commits implicitly: a failure may leave additive DDL already committed; inspect with `--check` before retrying. Never assume Ctrl+C reverses MySQL DDL. Neither backend is authorized to DROP or TRUNCATE old tables. Existing `DBD_COMPANY_BRANCH` and PR #100 summary fields are retained.

**Historical Branch Count warning:** PR #100 `branch_count` may exclude HQ; v0.6 `DBD_COMPANY.branch_count` includes HQ. Ambiguous PR #100 rows, conflicting counts, and historical detail rows without sufficient evidence block schema convergence rather than receive automatic `+1` or `1`. Review `dbd-data branch import -c -d sqlserver --source sqlserver-extension` and the original Journal first; do not manually mutate verified counts to bypass the gate. This migration does **not** import Branch details, collect HTTP, start a Generation, or reset checkpoints.

The six canonical business tables are `DBD_COMPANY`, `DBD_COMPANY_RELATION`, `DBD_COMPANY_EVENT`, `DBD_FINANCIAL`, `DBD_INDUSTRY`, `DBD_BRANCH`. SQL Server `DBD_CONTROL` and `DBD_DISCOVERY_WORK`, and MySQL `DBD_DISCOVERY_WORK`, are separate operational structures. Read-only `--check` must never be interpreted as test evidence for isolated database migration or Windows interruption compatibility. PR #102 remains Draft until those gates are independently validated.


A Worker joins an already-opened Core Generation. A Worker never creates a monthly Generation, starts Search Discovery, edits the shared calendar, or Finalizes data. The Admin can also collect locally with no Worker present.

## 1. Portable setup (no Windows Administrator)

```powershell
python -m pip install -e .
dbd-data setup
dbd-data check-db --backend sqlserver
```

Use a separate SQL Server login, not the Admin login. The Setup Wizard reads .env and prompts for missing values; DBD_OPERATOR_NAME is a display-only name. Machine ID is created once in the local user's DBDData config and does not belong in the shared .env or Git repository.

## SQL Server permissions (one-time, configured by the database administrator)

Each machine must have its own SQL Server login and mapped database user. The
Worker needs only the existing production-table permissions required to claim,
write and release Core Deep results, plus **SELECT on DBD_CONTROL** to read
its enrollment and Generation state, and **EXECUTE on DBD_CONTROL_ADMIT**
for the atomic shared HTTP rate gate.

For example, after the SQL Server database user is created and mapped to the
machine-specific login, the authorized database administrator can grant:

```sql
USE TIPDataIntelligence;
GRANT SELECT ON OBJECT::dbo.DBD_CONTROL TO [your_worker_database_user];
GRANT EXECUTE ON OBJECT::dbo.DBD_CONTROL_ADMIT TO [your_worker_database_user];
```

Replace the bracketed example with the actual database user. Grant the required
least-privilege SELECT/INSERT/UPDATE on the five existing business tables
separately according to the deployed collector contract; **never grant INSERT,
UPDATE, DELETE, ALTER or CONTROL on dbo.DBD_CONTROL to Workers**, or broad
`db_owner` / `db_ddladmin` membership. Do not reuse the Admin SQL login
for Workers, or the same login across two Worker computers.

The Setup Wizard will now refuse READY if the logged-in SQL identity cannot
execute the admission procedure. A Worker cannot grant these permissions to
itself through `dbd-data setup`.

## 2. Enrollment

Setup displays your Machine ID (node-...), your authenticated SQL Server Login and Operator name. Give all three to the Admin. The Admin approves the Machine ID/Login pair using dbd-data setup. Rerun Setup to verify APPROVED. Worker does not create or change its own SQL Server Control record.

An authorized DBA must give the Worker the existing SQL Server collection permissions on the five production business tables, SELECT on dbo.DBD_CONTROL and EXECUTE on dbo.DBD_CONTROL_ADMIT. Never grant UPDATE/INSERT/DELETE on dbo.DBD_CONTROL; never use a shared Admin password as a shortcut.

## 3. Join the current monthly Core Generation

```powershell
dbd-deep status --phase core --backend sqlserver
dbd-deep collect --phase core --backend sqlserver --continuous
```

Join after the Coordinator enters COLLECTING. All machines share one SQL Server Generation, existing Company Leases and one global HTTP admission budget. A claimed company is fenced by token + generation. Retry and recovery may send a request more than once, but durable writes are idempotent.

When the Coordinator switches to DRAINING, Worker must stop claiming new companies and allow in-flight work to finish or let the lease expire. Worker revocation immediately blocks new claims and subsequently committed outcomes. Admin waits for active leases before VERIFY/FINALIZE.

## 4. Startup and shutdown

Workers do not configure Monthly Windows Task Scheduler. Only the Admin runs cycle run --auto. If desired, the Worker starts a collector manually during the active month and presses Ctrl+C for graceful shutdown. The Admin cycle works even when there are zero remote Workers.

## 5. Troubleshooting

- Node missing or not approved: rerun dbd-data setup and provide the correct Node ID and SQL Login to Admin.
- SQL login mismatch: check per-machine .env; never copy the Admin credentials.
- No COLLECTING Generation: wait for Admin to start/resume the cycle.
- Rate admission failed: stop sending requests; there is no local-rate fallback.
- A lease expires during network loss: no manual database DELETE is needed; recovery can re-claim safely.
- SQL permissions must be enforced by SQL Server, not merely hidden Terminal menus.

See docs/ADMIN_SETUP.md for the monthly calendar and Windows Task Scheduler.
