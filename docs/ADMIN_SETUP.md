# DBD v0.6 — Admin Setup (SQL Server)

Production: TIPDataIntelligence. The Admin is the only coordinator. Local collection works with no remote workers.

## One-time preparation

```powershell
python -m pip install -e .
dbd-data migrate --backend sqlserver
dbd-data setup
dbd-data check-db --backend sqlserver
```

Setup loads .env, prompts for missing values, creates a stable per-machine Node ID, checks the SQL login and offers monthly schedule / Worker approval. DBD_OPERATOR_NAME is a display label only. Never share the Admin SQL credentials with Workers. Schema migration requires adequate SQL Server CREATE/ALTER privileges; no Windows elevation is requested.

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
