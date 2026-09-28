[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateRange(1, [int]::MaxValue)]
    [int]$RunId,

    [ValidateRange(1, 256)]
    [int]$Concurrency = 48,

    [ValidateRange(0.1, 1000.0)]
    [double]$Rate = 50.0,

    [switch]$Headless,

    [switch]$OpenPowerShell,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CollectorArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path -LiteralPath Variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
$SourceRoot = Join-Path $Root "src"

function Write-CollectorHeader {
    param([string]$State, [string]$Detail)
    Write-Host ""
    Write-Host "+-------------------------- DBD COLLECTOR --------------------------+"
    Write-Host ("| RUN {0,-8} {1,-16} {2,-35} |" -f $RunId, $State, $Detail)
    Write-Host "+-------------------------------------------------------------------+"
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
            "Python cannot import the project module 'dbd'. " +
            "Rebuild the environment with .\scripts\bootstrap.ps1 -Dev -InstallCommand. " +
            "Details: $detail"
        )
    }

    return [string]($probe | Select-Object -Last 1)
}

if ($Headless -and $OpenPowerShell) {
    throw "-Headless and -OpenPowerShell are mutually exclusive."
}

Initialize-ProjectPythonPath
$Python = Resolve-Python
$DbdModulePath = Assert-DbdImport -Python $Python

if ($OpenPowerShell) {
    Write-Warning "-OpenPowerShell is retained for command compatibility; the collector now runs in this process."
}

Write-CollectorHeader "STARTING" ("concurrency {0} | rate {1}/s" -f $Concurrency, $Rate)
Write-Host ("Python: {0}" -f $Python)
Write-Host ("DBD source: {0}" -f $DbdModulePath)

$commandArgs = @(
    "-m",
    "dbd",
    "collect",
    [string]$RunId,
    "--concurrency",
    [string]$Concurrency,
    "--rate",
    [string]$Rate
)

if ($CollectorArgs) {
    $commandArgs += $CollectorArgs
}

& $Python @commandArgs
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-CollectorHeader "STOPPED" "collector exited cleanly"
}
elseif ($exitCode -eq 130) {
    Write-CollectorHeader "PAUSED" "operator requested safe stop"
}
else {
    Write-CollectorHeader "FAILED" ("collector exit {0}" -f $exitCode)
}

exit $exitCode
