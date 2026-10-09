@echo off
setlocal EnableExtensions DisableDelayedExpansion

rem Task Scheduler launches this file on the authorized Admin computer.
rem The repository is resolved relative to the batch file, not a user name.
for %%I in ("%~dp0..") do set "DBD_HOME=%%~fI"
set "DBD_CLI=%DBD_HOME%\.venv\Scripts\dbd-data.exe"

if not exist "%DBD_CLI%" (
    echo ERROR: DBD package executable not found: "%DBD_CLI%"
    exit /b 2
)
cd /d "%DBD_HOME%" || exit /b 3

rem The database owns the real monthly schedule. Not-due exits promptly.
rem No PAUSE: Task Scheduler must close finished consoles automatically.
"%DBD_CLI%" cycle run --phase core --backend sqlserver --auto
exit /b %ERRORLEVEL%
