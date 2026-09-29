# Campaign Guardian

The Campaign Guardian is the long-running operations layer for an existing durable DBD campaign. It protects the same Run ID across worker failures, supervisor failures, temporary Redis/MySQL outages, and terminal loss of the controlling shell.

The Guardian does not create a new campaign, change the run scope, issue bulk-retry, delete Redis coordination keys, or reinterpret terminal data failures.

## First activation

Run the Guardian once with the intended operational profile:

~~~powershell
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"
dbd-data campaign guard 8 --workers 4 --workers-min 4 --workers-max 12 --worker-concurrency 8 --worker-rate 15 --fleet-rate-max 60 --fleet-db-connections-max 96 --fleet-archive-workers-max 24 --fleet-projection-workers-max 48 --fleet-redis-connections-max 128 --rate-mode adaptive --max-restarts 3 --headless
~~~

The resolved profile is persisted under data/guardian/run-8.json. It contains operational settings only; credentials and Redis URLs are not copied into the profile. Existing persisted profiles remain authoritative on resume except for one versioned safety migration: a v1 fixed `4/4/4` profile is promoted to `workers_max=12` only when it already carries an explicit `fleet_rate_max`, so added process capacity cannot multiply the upstream ceiling. Explicit CLI `--workers-max` always wins, and uncapped or non-default legacy fleet shapes are preserved.

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

Workers spawned by the Guardian carry a host-bound per-Guardian generation token in their durable lease owner ID (for example, `local-8-ga1b2c3d4e5f60718-...`). The Supervisor also persists an operational registry of its exact live child PIDs under `data/guardian/run-<id>.workers.json`. After a hard Guardian loss, the Windows watchdog prefers those exact PIDs and validates each command line against the same Run ID/generation context before containment; the older bounded full Win32 inventory remains a fallback. A missing or ambiguous registered PID fails closed rather than being treated as proof that all children exited. The recovery marker is written only after worker death is proven. Once Redis is stable and the previous supervisor lease is absent, the replacement Guardian reconciles only MySQL leases whose owner prefix matches that confirmed-dead generation. It returns those tasks to pending and rolls back one process-loss stage attempt. There is no run-wide or PID-only reclaim.

The PowerShell watchdog waits for the Python Guardian process. If that process is unexpectedly lost, it gives owned workers a bounded grace period and terminates only verified matching process trees. Production workers also require the Supervisor lease on every Redis heartbeat: loss of that lease moves the worker to draining, and a worker that cannot finish drain within the bounded drain window cancels its local pipeline so its final MySQL lease release can run. If containment or registry verification cannot be proven, the watchdog withholds immediate generation reclaim and relies on Redis fencing / durable lease expiry instead. Command-line parsing errors (exit 2) remain fatal operator errors rather than crash-looped.

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

Worker request-path health is evaluated independently. A stalled worker is stopped,
its confirmed-dead durable leases are returned attempt-neutrally, and a replacement
is started within its restart budget while healthy peers continue. Autoscale can
temporarily add spare capacity for one unhealthy worker, but multi-worker
availability evidence freezes scale-up to avoid a process storm during a real
upstream outage.

The Guardian protects and waits instead of performing destructive recovery while control-plane health is uncertain.

Terminal resource failures are deliberately separate. When pending, running, and retry-wait work reaches zero and failures remain, the final state is COMPLETED_WITH_FAILURES. The Guardian exits and leaves diagnosis or selective retry to an operator. It never requeues a large failed population automatically.

## Operator intent

Ctrl+C remains authoritative. A graceful pause is written to data/guardian/run-<id>.state.json and is not interpreted by the PowerShell watchdog as a crash that should be resumed.

The Guardian profile and state files contain no database password, Redis URL, or API credential.
