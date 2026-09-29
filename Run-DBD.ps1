[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateRange(1, [int]::MaxValue)]
    [int]$RunId,

    [ValidateRange(5, 600)]
    [int]$OrphanGraceSeconds = 40,

    [ValidateRange(1, 300)]
    [int]$RestartDelaySeconds = 5,

    [switch]$Headless,

    [switch]$OpenPowerShell,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$GuardianArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path -LiteralPath Variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$StatePath = Join-Path $Root "data\guardian\run-$RunId.state.json"
$RecoveryPath = Join-Path $Root "data\guardian\run-$RunId.recovery.json"
$WorkerRegistryPath = Join-Path $Root "data\guardian\run-$RunId.workers.json"
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
$SourceRoot = Join-Path $Root "src"

# Keep this script ASCII-only. Windows PowerShell 5.1 can decode UTF-8
# without a BOM through a legacy code page, which can corrupt source tokens.

function Write-GuardianHeader {
    param([string]$State, [string]$Detail)
    Write-Host ""
    Write-Host "+-------------------- DBD CAMPAIGN GUARDIAN / OUTER WATCHDOG --------------------+"
    Write-Host ("| RUN {0,-8} {1,-20} {2,-48} |" -f $RunId, $State, $Detail)
    Write-Host "+---------------------------------------------------------------------------------+"
}

function Resolve-Python {
    if (Test-Path -LiteralPath $VenvPython) {
        return $VenvPython
    }
    $command = Get-Command python -ErrorAction Stop
    return $command.Source
}

function Initialize-ProjectPythonPath {
    $moduleEntry = Join-Path $SourceRoot "dbd\__main__.py"
    if (-not (Test-Path -LiteralPath $moduleEntry)) {
        throw "Project source module not found at $moduleEntry"
    }

    $separator = [string][IO.Path]::PathSeparator
    $current = [Environment]::GetEnvironmentVariable("PYTHONPATH", "Process")
    $entries = @()
    if (-not [string]::IsNullOrWhiteSpace($current)) {
        $entries = @($current -split [regex]::Escape($separator))
    }

    if ($entries -notcontains $SourceRoot) {
        if ([string]::IsNullOrWhiteSpace($current)) {
            $env:PYTHONPATH = $SourceRoot
        }
        else {
            $env:PYTHONPATH = "$SourceRoot$separator$current"
        }
    }
}

function Assert-DbdImport {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Python
    )

    $probe = @(
        & $Python -c "import pathlib, dbd, dbd.cli.main; print(pathlib.Path(dbd.__file__).resolve())" 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        $detail = ($probe | ForEach-Object { [string]$_ }) -join " "
        throw (
            "Python cannot import the project module 'dbd' after source bootstrap. " +
            "Rebuild the environment with .\scripts\bootstrap.ps1 -Dev -InstallCommand. " +
            "Details: $detail"
        )
    }

    return [string]($probe | Select-Object -Last 1)
}

function Get-GuardianState {
    if (-not (Test-Path -LiteralPath $StatePath)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Get-WorkerRegistry {
    if (-not (Test-Path -LiteralPath $WorkerRegistryPath)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $WorkerRegistryPath -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Write-RecoveryMarker {
    param(
        [Parameter(Mandatory = $true)]
        [int]$SupervisorPid,

        [Parameter(Mandatory = $true)]
        [ValidatePattern('^[0-9a-f]{16}$')]
        [string]$SupervisorGeneration
    )

    $directory = Split-Path -Parent $RecoveryPath
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    $temporary = "$RecoveryPath.$PID.tmp"
    [ordered]@{
        run_id = $RunId
        supervisor_pid = $SupervisorPid
        supervisor_generation = $SupervisorGeneration
        confirmed_at = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    } |
    ConvertTo-Json -Compress |
    Set-Content -LiteralPath $temporary -Encoding ASCII
    Move-Item -LiteralPath $temporary -Destination $RecoveryPath -Force
}


function Test-GuardianDescendant {
    param(
        [Parameter(Mandatory = $true)]
        $Process,

        [Parameter(Mandatory = $true)]
        [int]$GuardianPid,

        [Parameter(Mandatory = $true)]
        [hashtable]$ProcessByPid
    )

    $current = $Process
    $seen = @{}

    for ($depth = 0; $depth -lt 32; $depth++) {
        $parentPid = [int]$current.ParentProcessId
        if ($parentPid -eq $GuardianPid) {
            return $true
        }
        if ($parentPid -le 0 -or $seen.ContainsKey($parentPid)) {
            return $false
        }

        $seen[$parentPid] = $true
        if (-not $ProcessByPid.ContainsKey($parentPid)) {
            return $false
        }
        $current = $ProcessByPid[$parentPid]
    }

    return $false
}

function Get-GuardianOwnedWorkers {
    param(
        [Parameter(Mandatory = $true)]
        [int]$GuardianPid,

        [string]$SupervisorGeneration = ""
    )

    if ($GuardianPid -le 0) {
        return @()
    }

    # Prefer exact PIDs persisted by the Supervisor. This avoids a full
    # Win32_Process scan on hosts where CIM enumeration can time out.
    if (-not [string]::IsNullOrWhiteSpace($SupervisorGeneration)) {
        $registry = Get-WorkerRegistry
        $stableProperty = $null
        if ($null -ne $registry) {
            $stableProperty = $registry.PSObject.Properties["stable"]
        }
        if (
            $null -ne $registry -and
            $null -ne $stableProperty -and
            [bool]$stableProperty.Value -and
            [int]$registry.run_id -eq $RunId -and
            [string]$registry.supervisor_generation -eq $SupervisorGeneration
        ) {
            $registered = @()
            foreach ($entry in @($registry.workers)) {
                $pidValue = 0
                try {
                    $pidValue = [int]$entry.pid
                }
                catch {
                    $pidValue = 0
                }
                if ($pidValue -le 0) {
                    continue
                }

                $matches = @(
                    Get-CimInstance Win32_Process -Filter ("ProcessId = {0}" -f $pidValue) -OperationTimeoutSec 5 -ErrorAction Stop
                )
                if ($matches.Count -eq 0) {
                    # A stable registry is a complete snapshot for this generation.
                    # A listed PID that no longer exists is therefore confirmed dead.
                    continue
                }

                $process = $matches[0]
                $name = [string]$process.Name
                $commandLine = [string]$process.CommandLine
                if ([string]::IsNullOrWhiteSpace($commandLine)) {
                    throw (
                        "Process registry PID {0} has no readable command line; " +
                        "recovery ownership cannot be proven." -f $pidValue
                    )
                }

                $runPattern = [regex]::Escape([string]$RunId)
                $workerId = [string]$entry.worker_id
                if ([string]::IsNullOrWhiteSpace($workerId)) {
                    throw (
                        "Process registry PID {0} has no worker identity; " +
                        "recovery ownership cannot be proven." -f $pidValue
                    )
                }
                $workerPattern = [regex]::Escape($workerId)
                if (
                    $name -notmatch '^(python(w)?(\.exe)?|powershell(\.exe)?|pwsh(\.exe)?)$' -or
                    $commandLine -notmatch '\-m\s+dbd\s+bulk' -or
                    $commandLine -notmatch ("\-\-run-id\s+" + $runPattern + "(\s|$)") -or
                    $commandLine -notmatch ("--worker-instance\s+" + $workerPattern + "(\s|$)")
                ) {
                    throw (
                        "Process registry PID {0} no longer matches Run {1} worker {2}; " +
                        "recovery ownership cannot be proven." -f $pidValue, $RunId, $workerId
                    )
                }
                $registered += $process
            }
            return @($registered)
        }
    }

    # Fallback for older/missing registries: bounded full inventory + ancestry proof.
    $processes = @(
        Get-CimInstance Win32_Process -OperationTimeoutSec 5 -ErrorAction Stop
    )
    $processByPid = @{}
    foreach ($process in $processes) {
        $processByPid[[int]$process.ProcessId] = $process
    }

    $runPattern = [regex]::Escape([string]$RunId)
    $owned = @()
    foreach ($process in $processes) {
        if (-not (Test-GuardianDescendant -Process $process -GuardianPid $GuardianPid -ProcessByPid $processByPid)) {
            continue
        }

        $name = [string]$process.Name
        if ($name -notmatch '^(python(w)?(\.exe)?|powershell(\.exe)?|pwsh(\.exe)?)$') {
            continue
        }

        if ([string]::IsNullOrWhiteSpace([string]$process.CommandLine)) {
            throw (
                "Process inventory is incomplete for Guardian descendant PID {0} ({1}); " +
                "recovery ownership cannot be proven." -f
                $process.ProcessId,
                $name
            )
        }

        $commandLine = [string]$process.CommandLine
        if (
            $commandLine -match '\-m\s+dbd\s+bulk' -and
            $commandLine -match ("\-\-run-id\s+" + $runPattern + "(\s|$)")
        ) {
            $owned += $process
        }
    }

    return @($owned)
}

function Stop-GuardianOwnedWorkers {
    param(
        [Parameter(Mandatory = $true)]
        [int]$GuardianPid,

        [string]$SupervisorGeneration = ""
    )

    $workers = @(
        Get-GuardianOwnedWorkers -GuardianPid $GuardianPid -SupervisorGeneration $SupervisorGeneration
    )
    if ($workers.Count -eq 0) {
        return 0
    }

    Write-GuardianHeader "CONTAINING" ("{0} owned orphan worker/wrapper process(es)" -f $workers.Count)
    $ownedPidSet = @{}
    foreach ($owned in $workers) {
        $ownedPidSet[[int]$owned.ProcessId] = $true
    }
    $roots = @(
        $workers |
        Where-Object { -not $ownedPidSet.ContainsKey([int]$_.ParentProcessId) }
    )
    foreach ($worker in $roots) {
        $stopped = $false
        try {
            & taskkill.exe /PID $worker.ProcessId /T /F 2>$null | Out-Null
            $stopped = ($LASTEXITCODE -eq 0)
        }
        catch {
            $stopped = $false
        }

        if (-not $stopped) {
            try {
                Stop-Process -Id $worker.ProcessId -Force -ErrorAction Stop
            }
            catch {
                Write-Warning (
                    "Could not stop Guardian-owned worker PID {0}: {1}" -f
                    $worker.ProcessId,
                    $_.Exception.Message
                )
            }
        }
    }
    return $workers.Count
}

if ($Headless -and $OpenPowerShell) {
    throw "-Headless and -OpenPowerShell are mutually exclusive."
}

Initialize-ProjectPythonPath
$Python = Resolve-Python
$DbdModulePath = Assert-DbdImport -Python $Python
$restartCount = 0

Write-GuardianHeader "BOOT" ("Python: {0}" -f $Python)
Write-Host ("DBD source: {0}" -f $DbdModulePath)

while ($true) {
    # Run-DBD is the production outer watchdog. Refuse an accidental dev
    # pacing namespace before Supervisor ownership or worker spawn.
    $commandArgs = @("-m", "dbd", "campaign", "guard", [string]$RunId, "--require-production-namespace")
    if ($Headless) {
        $commandArgs += "--headless"
    }
    elseif ($OpenPowerShell) {
        $commandArgs += "--open-powershell"
    }
    if ($GuardianArgs) {
        $commandArgs += $GuardianArgs
    }

    Write-GuardianHeader "STARTING" ("guardian process | restart {0}" -f $restartCount)
    $launchEpoch = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
    & $Python @commandArgs
    $exitCode = $LASTEXITCODE
    $state = Get-GuardianState

    $stateIsFresh = (
        $null -ne $state -and
        $null -ne $state.updated_at -and
        [double]$state.updated_at -ge $launchEpoch
    )
    if ($stateIsFresh -and [bool]$state.terminal) {
        $terminalExitCode = $exitCode
        if ($null -ne $state.exit_code) {
            $terminalExitCode = [int]$state.exit_code
        }
        Write-GuardianHeader ([string]$state.phase) ([string]$state.message)
        exit $terminalExitCode
    }

    if ($exitCode -eq 75) {
        Write-GuardianHeader "ALREADY MANAGED" "another local Guardian owns this run"
        exit 75
    }

    if ($exitCode -eq 2) {
        Write-GuardianHeader "FATAL CLI" "invalid Guardian arguments or command-line options"
        exit 2
    }

    if ($exitCode -eq 64) {
        Write-GuardianHeader "FATAL CONFIG" "Guardian requires operator correction"
        exit 64
    }

    if ($exitCode -eq 130) {
        Write-GuardianHeader "PAUSED" "operator requested graceful pause"
        exit 130
    }

    $restartCount++
    Write-GuardianHeader "PROCESS LOST" ("exit {0}; protecting Run {1}" -f $exitCode, $RunId)

    $lostGuardianPid = 0
    if ($stateIsFresh -and $null -ne $state.pid) {
        try {
            $lostGuardianPid = [int]$state.pid
        }
        catch {
            $lostGuardianPid = 0
        }
    }

    $lostSupervisorGeneration = ""
    if ($stateIsFresh -and $null -ne $state.supervisor_generation) {
        $candidateGeneration = ([string]$state.supervisor_generation).Trim().ToLowerInvariant()
        if ($candidateGeneration -match '^[0-9a-f]{16}$') {
            $lostSupervisorGeneration = $candidateGeneration
        }
    }

    if ($lostGuardianPid -gt 0) {
        $ownershipVerified = $false
        $ownedWorkers = @()

        try {
            $ownedWorkers = @(Get-GuardianOwnedWorkers -GuardianPid $lostGuardianPid -SupervisorGeneration $lostSupervisorGeneration)
            $ownershipVerified = $true
        }
        catch {
            Write-Warning (
                "Could not verify worker ownership for Guardian PID {0}: {1}. " +
                "Direct process containment and immediate lease reconciliation are withheld." -f
                $lostGuardianPid,
                $_.Exception.Message
            )
        }

        if ($ownershipVerified -and $ownedWorkers.Count -gt 0) {
            Write-GuardianHeader "DRAIN GRACE" (
                "{0} owned worker(s) | Guardian PID {1} | {2}s" -f
                $ownedWorkers.Count,
                $lostGuardianPid,
                $OrphanGraceSeconds
            )
            Start-Sleep -Seconds $OrphanGraceSeconds
            try {
                [void](Stop-GuardianOwnedWorkers -GuardianPid $lostGuardianPid -SupervisorGeneration $lostSupervisorGeneration)
            }
            catch {
                Write-Warning (
                    "Containment verification failed for Guardian PID {0}: {1}. " +
                    "Recovery will fall back to Redis handoff / durable lease expiry." -f
                    $lostGuardianPid,
                    $_.Exception.Message
                )
            }
        }

        if ($ownershipVerified) {
            try {
                $remainingOwned = @(Get-GuardianOwnedWorkers -GuardianPid $lostGuardianPid -SupervisorGeneration $lostSupervisorGeneration)
                if ($remainingOwned.Count -eq 0 -and $lostSupervisorGeneration) {
                    try {
                        Write-RecoveryMarker `
                            -SupervisorPid $lostGuardianPid `
                            -SupervisorGeneration $lostSupervisorGeneration
                    }
                    catch {
                        Write-Warning (
                            "Could not write recovery marker for Guardian PID {0}: {1}. " +
                            "Immediate MySQL lease reconciliation is skipped." -f
                            $lostGuardianPid,
                            $_.Exception.Message
                        )
                    }
                }
                elseif ($remainingOwned.Count -eq 0) {
                    Write-Warning (
                        "Guardian PID {0} has no owned worker descendants, but its generation " +
                        "identity is unavailable; immediate MySQL lease reconciliation is withheld." -f
                        $lostGuardianPid
                    )
                }
                else {
                    Write-Warning (
                        (
                            "Guardian PID {0} still owns {1} worker/wrapper process(es); " +
                            "immediate MySQL lease reconciliation is withheld."
                        ) -f $lostGuardianPid, $remainingOwned.Count
                    )
                }
            }
            catch {
                Write-Warning (
                    "Could not re-verify worker ownership for Guardian PID {0}: {1}. " +
                    "Recovery marker is withheld." -f
                    $lostGuardianPid,
                    $_.Exception.Message
                )
            }
        }
    }

    $effectiveDelay = [Math]::Min(
        60,
        $RestartDelaySeconds * [Math]::Pow(2, [Math]::Min([Math]::Max($restartCount - 1, 0), 4))
    )
    Write-GuardianHeader "RECOVERING" ("relaunch in {0}s | same Run ID" -f $effectiveDelay)
    Start-Sleep -Seconds ([int]$effectiveDelay)
}
