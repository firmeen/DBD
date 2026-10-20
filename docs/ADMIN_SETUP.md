# DBD v0.6 — Admin Setup (SQL Server)

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


Production: TIPDataIntelligence. The Admin is the only coordinator. Local collection works with no remote workers.

## One-time preparation

```powershell
python -m venv .venv
.\\.venv\\Scripts\\python.exe -m pip install -e .
.\\.venv\\Scripts\\dbd-data.exe migrate --backend sqlserver
.\\.venv\\Scripts\\dbd-data.exe setup
.\\.venv\\Scripts\\dbd-data.exe check-db --backend sqlserver
```

The automatic `scripts\\dbd-auto.bat` deliberately uses
`.venv\\Scripts\\dbd-data.exe` on the Admin machine. If you instead
installed the CLI globally, manual commands can still work, but automatic
registration will refuse READY until this portable project `.venv` exists.
No Windows Administrator access is needed for the venv.

Setup loads .env, prompts for missing values, creates a stable per-machine Node ID, checks the SQL login and offers monthly schedule / Worker approval. DBD_OPERATOR_NAME is a display label only. Never share the Admin SQL credentials with Workers. Schema migration requires adequate SQL Server CREATE/ALTER privileges; no Windows elevation is requested.

The Admin SQL Server database user must be authorized to read and update `dbo.DBD_CONTROL`, write the five existing business tables as the current production workflow requires, and execute `dbo.DBD_CONTROL_ADMIT`. Initial migration of the control table/procedure needs the appropriate SQL Server schema permissions. `dbd-data setup` now checks admission EXECUTE permission before declaring READY. Do not grant Workers the Admin control-table permissions; see [Worker setup](WORKER_SETUP.md) for the exact restricted grants.

## Manual Core cycle (unchanged)

```powershell
dbd-data check-db --backend sqlserver
dbd-data cycle status --phase core --backend sqlserver
dbd-data cycle run --phase core --backend sqlserver
dbd-data cycle status --phase core --backend sqlserver
```

Manual starts before a scheduled day need explicit confirmation. Interrupted cycles resume the same Generation. A completed month cannot be rerun as a new Generation. The actual start and scheduled start remain recorded for that Generation.

## Windows automatic cycle

```powershell
dbd-data cycle run --phase core --backend sqlserver --auto
```

Run dbd-data setup on the Admin machine and optionally enable Windows Auto Run. It registers one per-user Task Scheduler task named DBD Monthly Core. Only the Admin machine may register the automatic coordinator task. The action is scripts\dbd-auto.bat; do not copy it to Startup as well.

### Task Scheduler — five tabs

| Tab | Recommended settings |
|---|---|
| General | Run only when user is logged on, Highest privileges OFF, Hidden OFF |
| Triggers | Daily at your chosen check hour + At log on; not a fixed Monthly Windows trigger |
| Actions | Start a program: C:\Windows\System32\cmd.exe |
| Arguments | /d /c ""C:\path\to\DBD\scripts\dbd-auto.bat"" |
| Start in | C:\path\to\DBD (unquoted) |
| Conditions | No idle requirement; use AC power where possible; Wake-to-run only if supported |
| Settings | Run missed tasks, Do not start a new instance, no forced stop after 3 days |

The SQL Server schedule is authoritative. Task Scheduler performs a quick daily/login check, exits immediately if not due, and opens the existing Terminal UI if due. No Python polling daemon is kept alive. Wake from Sleep depends on Windows power settings; Shutdown does not auto-power-on. Interactive Terminal requires a signed-in desktop session.

## Schedule changes

Setup lets Admin choose day 1–31 and time in Asia/Bangkok. Short months use their last day. A changed day while G2 is running takes effect from G3. The system warns if consecutive actual starts are fewer than 10 days apart (advice, not hard rejection). Delayed runs use current API data; they do not fabricate historical snapshots.

## Enrolling workers

Worker runs dbd-data setup on their own machine and gives Admin their Machine ID, SQL Login and Operator name. Admin runs setup and selects Approve/ Revoke Worker. Node approval binds a login to a machine identity; separate SQL credentials are essential.

Workers need SELECT on dbo.DBD_CONTROL and EXECUTE on dbo.DBD_CONTROL_ADMIT in addition to the existing minimal collection privileges on the five business tables. They must NOT receive INSERT, UPDATE or DELETE on dbo.DBD_CONTROL. An authorized SQL Server DBA grants permissions; Setup does not create elevated SQL principals.

## Recovery

Use cycle status then run the same cycle command. Do not manually wipe masks, Redis keys, Generation IDs or Leases. If Control Schema is missing, use migrate with an authorized login. SQL Connection/Rate Admission errors are fail-closed. No Gmail/yagmail dependency is installed. See docs/OPERATIONS.md for production diagnosis.
