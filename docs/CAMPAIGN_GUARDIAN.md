> [!WARNING]
> **LEGACY / HISTORICAL DOCUMENT**
>
> เอกสารนี้ไม่ใช่คู่มือสำหรับ SQL Server v0.6 Production workflow ปัจจุบัน
> สำหรับการรัน Production ให้เริ่มจาก README.md และ docs/RUNBOOK.md
> เก็บไฟล์นี้ไว้เพื่ออ้างอิง runtime รุ่นเดิมเท่านั้น และไม่ควรนำ command หรือ state model ในไฟล์นี้มาปะปนกับ v0.6

# Campaign Guardian

The Campaign Guardian is the long-running operations layer for an existing durable DBD campaign. It protects the same Run ID across worker failures, supervisor failures, temporary Redis/MySQL outages, and terminal loss of the controlling shell.

The Guardian does not create a new campaign, change the run scope, issue bulk-retry, delete Redis coordination keys, or reinterpret terminal data failures.

## First activation

Run the Guardian once with the intended operational profile:

~~~powershell
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"
dbd-data campaign guard 8 --workers 6 --workers-min 4 --workers-max 10 --worker-concurrency 12 --fleet-rate-initial 16 --fleet-rate-max 20 --fleet-db-connections-max 96 --fleet-archive-workers-max 24 --fleet-projection-workers-max 48 --fleet-redis-connections-max 128 --rate-mode adaptive --max-restarts 3 --headless
~~~

The resolved profile is persisted under data/guardian/run-8.json. It contains operational settings only; credentials and Redis URLs are not copied into the profile.

After that, the normal operator command is only:

~~~powershell
dbd-data campaign guard 8
~~~

For a Windows outer watchdog that can also relaunch the Python Guardian after an unexpected interpreter or terminal loss:

~~~powershell
.\Run-DBD.ps1 8 -Headless
~~~

Use `-OpenPowerShell` instead when separate worker consoles are intentionally required. The two display switches are mutually exclusive.

`Run-DBD.ps1` is intentionally ASCII-only so the same checked-out file parses safely in both Windows PowerShell 5.1 and PowerShell 7 without depending on BOM/code-page behavior. The watchdog also prepends the repository `src` directory to its process-local `PYTHONPATH` and verifies `import dbd` before launch. This lets the watchdog execute the checked-out source even if an old editable-install entry point in `.venv` has become stale; it does not change the permanent user environment.

## Recovery contract

The Guardian uses MySQL as durable truth and Redis only as the ephemeral control plane.

Before resuming it requires Redis stability probes to pass, the previous supervisor lease to be absent or expired, MySQL durable run state to be readable, and remaining durable work to exist.

A replacement supervisor then uses the existing supervisor handoff logic to drain registered workers before it starts replacement capacity. Worker process deaths keep using the existing immediate lease reclaim logic; ordinary expired leases remain covered by scheduler maintenance.

Workers spawned by the Guardian carry a host-bound per-Guardian generation token in their durable lease owner ID (for example, `local-8-ga1b2c3d4e5f60718-...`). The Windows watchdog still uses the lost Guardian PID only for local process ancestry. After a hard Guardian loss it performs a bounded, fail-closed Win32 inventory that includes both PowerShell worker wrappers and Python workers, and writes a recovery marker only after that inventory confirms zero owned descendants. The marker carries both the dead PID and its generation token. Once Redis is stable and the previous supervisor lease is absent, the replacement Guardian reconciles only MySQL leases whose owner prefix matches that confirmed-dead generation. It returns those tasks to pending and rolls back one process-loss stage attempt. There is no run-wide or PID-only reclaim: independently supervised, manually launched, multi-host, or later PID-reuse workers for the same Run ID remain outside durable reconciliation.

The PowerShell watchdog waits for the Python Guardian process. If that process is unexpectedly lost, it reads the fresh Guardian PID from the current state file, gives only that Guardian's worker descendants a bounded grace period, and terminates only matching python -m dbd bulk descendants for the same Run ID. Process ownership verification is fail-closed: the CIM/WMI inventory has a bounded operation timeout, inaccessible descendant command lines never count as zero workers, and PowerShell wrappers count as owned work even before their Python child appears. Independently supervised or manually launched workers for the same Run ID are never selected by this containment path. If containment or recovery-marker persistence cannot be verified, the watchdog relaunches without immediate lease reconciliation and relies on Redis handoff / durable lease expiry instead. Command-line parsing errors (exit 2) are treated as fatal operator errors rather than crash-looped.

## Repairing a stale local CLI install

If `.venv\Scripts\dbd-data.exe` exists but running `dbd-data` fails with `ModuleNotFoundError: No module named 'dbd'`, the virtual environment contains a stale or incomplete editable installation. Repair it from the repository root:

~~~powershell
.\scripts\bootstrap.ps1 -Dev -InstallCommand
.\.venv\Scripts\python.exe -c "import dbd; print(dbd.__version__)"
.\.venv\Scripts\python.exe -m dbd --version
~~~

The bootstrap now treats both import and module-CLI smoke tests as required post-install checks. Guardian watchdog execution is separately protected by its source-checkout bootstrap described above.

## Failure classes

Automatic self-healing is intended for execution and infrastructure failures such as worker process exit or heartbeat loss, supervisor process failure, temporary Redis outage, temporary MySQL unavailability, stale supervisor lease fencing, and shell/interpreter loss handled by the PowerShell watchdog.

The Guardian protects and waits instead of performing destructive recovery while control-plane health is uncertain.

Terminal resource failures are deliberately separate. When pending, running, and retry-wait work reaches zero and failures remain, the final state is COMPLETED_WITH_FAILURES. The Guardian exits and leaves diagnosis or selective retry to an operator. It never requeues a large failed population automatically.

## Operator intent

Ctrl+C remains authoritative. A graceful pause is written to data/guardian/run-<id>.state.json and is not interpreted by the PowerShell watchdog as a crash that should be resumed.

The Guardian profile and state files contain no database password, Redis URL, or API credential.
