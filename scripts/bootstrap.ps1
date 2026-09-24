param([switch]$Dev, [switch]$InstallCommand)
$ErrorActionPreference = 'Stop'
$dbdRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Push-Location -LiteralPath $dbdRoot
try {
    if (-not (Test-Path -LiteralPath '.venv\Scripts\python.exe')) {
        py -m venv .venv
        if ($LASTEXITCODE -ne 0) { throw 'Virtual environment creation failed.' }
    }
    $dbdPackage = if ($Dev) { '.[dev]' } else { '.' }
    & .\.venv\Scripts\python.exe -m pip install -c requirements.lock -e $dbdPackage
    if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }

    & .\.venv\Scripts\python.exe -c "import dbd; print(dbd.__version__)"
    if ($LASTEXITCODE -ne 0) {
        throw "Editable install completed but the 'dbd' package is not importable."
    }

    & .\.venv\Scripts\python.exe -m dbd --version
    if ($LASTEXITCODE -ne 0) {
        throw "Package import works but the dbd CLI module smoke test failed."
    }
    if (-not (Test-Path -LiteralPath '.env')) {
        Copy-Item -LiteralPath '.env.example' -Destination '.env'
        Write-Host 'Created .env. Set DB_PASSWORD before connecting.'
    }
    if ($InstallCommand) { & .\scripts\install-command.ps1 }
    Write-Host 'Bootstrap complete. Run dbd-data migrate with schema privileges.'
} finally {
    Pop-Location
}
