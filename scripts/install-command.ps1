$ErrorActionPreference = 'Stop'
$dbdRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$dbdScripts = Join-Path $dbdRoot '.venv\Scripts'
$dbdExe = Join-Path $dbdScripts 'dbd-data.exe'
if (-not (Test-Path -LiteralPath $dbdExe)) {
    throw 'Run scripts\bootstrap.ps1 first to install dbd-data into .venv.'
}

# Store the user PATH without expanding or truncating its existing entries.
$dbdUserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$dbdEntries = @($dbdUserPath -split ';' | Where-Object { $_ })
if (-not ($dbdEntries | Where-Object { $_.TrimEnd('\') -ieq $dbdScripts.TrimEnd('\') })) {
    [Environment]::SetEnvironmentVariable('Path', (($dbdEntries + $dbdScripts) -join ';'), 'User')
}
if (-not (($env:Path -split ';') | Where-Object { $_.TrimEnd('\') -ieq $dbdScripts.TrimEnd('\') })) {
    $env:Path += ';' + $dbdScripts
}
& $dbdExe --version
if ($LASTEXITCODE -ne 0) { throw 'dbd-data verification failed.' }
Write-Host "Permanent command installed: $dbdExe"
Write-Host 'Open a new terminal to use dbd-data from any folder.'
