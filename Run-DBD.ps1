[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateRange(1, 2147483647)]
    [int]$RunId,

    # Compatibility switch from the former Guardian launcher. The simple
    # collector is always single-process/headless with one in-terminal UI.
    [switch]$Headless,

    [switch]$NoUI,

    [ValidateRange(1, 256)]
    [int]$Concurrency,

    [ValidateScript({ $_ -gt 0 })]
    [double]$Rate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $Python)) {
    throw "Project virtualenv Python was not found: $Python"
}

$srcPath = Join-Path $ProjectRoot "src"
if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
    $env:PYTHONPATH = $srcPath
}
elseif (($env:PYTHONPATH -split [IO.Path]::PathSeparator) -notcontains $srcPath) {
    $env:PYTHONPATH = $srcPath + [IO.Path]::PathSeparator + $env:PYTHONPATH
}

$modulePath = (& $Python -c "import dbd; print(dbd.__file__)").Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($modulePath)) {
    throw "Could not import the project dbd package with $Python"
}

Write-Host ""
Write-Host "+----------------------- DBD SIMPLE COLLECTOR -----------------------+"
Write-Host ("| RUN {0,-8} single process | MySQL durable state | no Redis hot path |" -f $RunId)
Write-Host "+--------------------------------------------------------------------+"
Write-Host ("DBD source: {0}" -f $modulePath)
if ($Headless) {
    Write-Host "Compatibility: -Headless accepted; collector already runs in one terminal."
}

$commandArgs = @("-m", "dbd", "collect", [string]$RunId)
if ($PSBoundParameters.ContainsKey("Concurrency")) {
    $commandArgs += @("--concurrency", [string]$Concurrency)
}
if ($PSBoundParameters.ContainsKey("Rate")) {
    $commandArgs += @("--rate", [string]$Rate)
}
if ($NoUI) {
    $commandArgs += "--no-ui"
}

& $Python @commandArgs
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host ""
    Write-Host ("Run {0} collector exited cleanly." -f $RunId)
}
else {
    Write-Host ""
    Write-Host ("Run {0} collector exited with code {1}." -f $RunId, $exitCode)
}

exit $exitCode
