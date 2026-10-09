"""One interactive setup command for the v0.6 SQL Server admin and workers.

Rich is used for presentation; prompt_toolkit supplies keyboard-driven
selection and masked input. No elevation, service, or daemon is installed.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

from dotenv import dotenv_values
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..control import ControlError, validate_schedule
from ..core.settings import ENV_FILE, PROJECT_ROOT, Settings, settings
from ..storage.sqlserver.control_repository import ControlRepository
from ..storage.sqlserver.engine import check_connection

NODE_PATH = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / ".config"))) / "DBDData" / "node.json"
TASK_NAME = "DBD Monthly Core"
FIELDS = ("DBD_OPERATOR_NAME", "MSSQL_SERVER", "MSSQL_DATABASE", "MSSQL_USER", "MSSQL_PASSWORD")


def local_node_id(*, create: bool = False) -> str | None:
    if NODE_PATH.is_file():
        node = json.loads(NODE_PATH.read_text(encoding="utf-8"))
        identifier = node.get("node_id")
        if isinstance(identifier, str) and identifier.startswith("node-"):
            return identifier
        raise ControlError(f"Invalid local node identity at {NODE_PATH}")
    if not create:
        return None
    identifier = "node-" + uuid4().hex[:20]
    NODE_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".node-", suffix=".tmp", dir=NODE_PATH.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump({"node_id": identifier}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, NODE_PATH)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return identifier


def _task_script() -> Path:
    return PROJECT_ROOT / "scripts" / "dbd-auto.bat"


def register_windows_task(*, check_hour: int = 9, check_minute: int = 0, wake: bool = False) -> None:
    """Create a single per-user Task with Daily + Logon triggers.

    Registration failures do not alter database state. This is run only after
    explicit admin confirmation; never requests elevated privileges.
    """
    if sys.platform != "win32":
        raise ControlError("Windows Task Scheduler is available only on Windows")
    script = _task_script()
    if not script.exists():
        raise ControlError(f"Auto-run script not found: {script}")
    task_path = str(script).replace("'", "''")
    root_path = str(PROJECT_ROOT).replace("'", "''")
    name = TASK_NAME.replace("'", "''")
    wake_flag = '-WakeToRun' if wake else ''
    # The principal is the current interactive Windows user, not an elevated
    # service account. No plain-text Windows password is needed.
    ps = f"""
$ErrorActionPreference = 'Stop'
$cmd = Join-Path $env:SystemRoot 'System32\\cmd.exe'
$action = New-ScheduledTaskAction -Execute $cmd -Argument '/d /c ""{task_path}""' -WorkingDirectory '{root_path}'
$triggers = @(
    (New-ScheduledTaskTrigger -Daily -At '{check_hour:02d}:{check_minute:02d}'),
    (New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\\$env:USERNAME")
)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) {wake_flag}
$user = "$env:USERDOMAIN\\$env:USERNAME"
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName '{name}' -Action $action -Trigger $triggers -Settings $settings -Principal $principal -Force | Out-Null
"""
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", ps],
        capture_output=True, text=True, check=False, timeout=40,
    )
    if result.returncode:
        raise ControlError(
            "Task Scheduler registration failed (no elevation was attempted): "
            + (result.stderr or result.stdout).strip()[:500]
        )


def _env_value(value: str) -> str:
    # Quote whitespace and #/quote characters without exposing a password in logs.
    return value if re.fullmatch(r"[A-Za-z0-9_./:@-]+", value) else json.dumps(value, ensure_ascii=False)


def _save_env(changes: dict[str, str]) -> None:
    """Preserve unrelated env lines and replace only selected known keys."""
    existing = ENV_FILE.read_text(encoding="utf-8") if ENV_FILE.is_file() else ""
    lines = existing.splitlines()
    found: set[str] = set()
    output: list[str] = []
    for line in lines:
        stripped = line.lstrip()
        key = stripped.split("=", 1)[0].strip() if "=" in stripped and not stripped.startswith("#") else None
        if key in changes:
            if key not in found:
                output.append(f"{key}={_env_value(changes[key])}")
                found.add(key)
        else:
            output.append(line)
    output.extend(f"{key}={_env_value(value)}" for key, value in changes.items() if key not in found)
    ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=".dbd-env-", dir=ENV_FILE.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            fp.write("\n".join(output).rstrip() + "\n")
            fp.flush()
            os.fsync(fp.fileno())
        if ENV_FILE.exists():
            os.chmod(temp, ENV_FILE.stat().st_mode & 0o777)
        else:
            os.chmod(temp, 0o600)
        os.replace(temp, ENV_FILE)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def _reload_settings() -> None:
    # Settings is a single frozen Pydantic instance imported across modules.
    # Updating its validated fields in-place is limited to initial Setup before
    # any collector/client has been instantiated in this process.
    validated = Settings()
    settings.__dict__.update(validated.__dict__)


class SetupWizard:
    def __init__(self) -> None:
        self.console = Console()
        self.interactive = bool(sys.stdin.isatty() and sys.stdout.isatty())

    def choice(self, title: str, options: list[tuple[str, str]]) -> str:
        from prompt_toolkit.shortcuts import radiolist_dialog
        selection = radiolist_dialog(
            title="DBD DATA  /  " + title,
            text="Use ↑ ↓ to select, Enter to confirm, Esc to cancel.",
            values=options,
            ok_text="Confirm", cancel_text="Back",
        ).run()
        if selection is None:
            raise KeyboardInterrupt
        return str(selection)

    def ask(self, title: str, initial: str = "", *, secret: bool = False) -> str:
        from prompt_toolkit.shortcuts import input_dialog
        value = input_dialog(
            title="DBD DATA  /  " + title,
            text="Enter a value, then press Enter. Esc cancels.",
            default=initial,
            password=secret,
            ok_text="Confirm", cancel_text="Back",
        ).run()
        if value is None:
            raise KeyboardInterrupt
        return str(value).strip()

    def confirm(self, question: str, *, default: bool = False) -> bool:
        from prompt_toolkit.shortcuts import yes_no_dialog
        return bool(yes_no_dialog(
            title="DBD DATA  /  Confirm",
            text=question, yes_text="Yes", no_text="No",
        ).run())

    def _header(self, node_id: str | None) -> None:
        self.console.print(Panel(
            "[bold cyan]DBD DATA  ·  PRODUCTION SETUP[/bold cyan]\n"
            "[dim]One SQL Server · One coordinator · Optional remote workers[/dim]\n"
            f"[dim]Local identity: {node_id or 'not created'}[/dim]",
            title="READINESS / CONFIGURATION", border_style="cyan",
        ))

    def run(self) -> int:
        node = local_node_id()
        self._header(node)
        parsed = dotenv_values(ENV_FILE)
        present = {k: os.environ.get(k) or parsed.get(k) for k in FIELDS}
        missing = [k for k in FIELDS if not present.get(k)]
        if not self.interactive:
            table = Table(title="Non-interactive setup: read-only")
            table.add_column("Setting")
            table.add_column("Status")
            for key in FIELDS:
                table.add_row(key, "READY" if present.get(key) else "MISSING")
            self.console.print(table)
            if missing or not node:
                self.console.print("[yellow]Interactive Terminal required to complete setup.[/yellow]")
                return 2
            try:
                check_connection()
                control = ControlRepository()
                status = control.node_status(node)
                self.console.print(f"Node: {status or 'NOT ENROLLED'}")
                admission_ready = control.can_execute_admission()
                if not admission_ready:
                    self.console.print("[red]SQL EXECUTE permission on DBD_CONTROL_ADMIT is missing.[/red]")
                return 0 if status and status.get("authorized") and admission_ready else 2
            except Exception as exc:
                self.console.print(f"[red]Readiness failed: {exc}[/red]")
                return 2

        try:
            edits: dict[str, str] = {}
            for key in missing:
                default = "TIPDataIntelligence" if key == "MSSQL_DATABASE" else ""
                value = self.ask(key, default, secret=(key == "MSSQL_PASSWORD"))
                if not value:
                    raise ControlError(f"{key} is required")
                if any(char in value for char in "\r\n"):
                    raise ControlError(f"{key} contains an invalid newline")
                edits[key] = value
            if edits:
                visible = ", ".join(edits)
                self.console.print(f"Missing environment values: [bold]{visible}[/bold]")
                if "MSSQL_PASSWORD" in edits:
                    self.console.print("[yellow]Saving MSSQL_PASSWORD in .env stores a local plain-text secret. Protect this file.[/yellow]")
                if not self.confirm("Save the listed missing settings to the local .env file?"):
                    return 2
                _save_env(edits)
                _reload_settings()

            self.console.print("[cyan]Checking SQL Server...[/cyan]")
            try:
                info = check_connection()
            except Exception as first_error:
                self.console.print(f"[yellow]Connection validation failed: {first_error}[/yellow]")
                if not self.confirm("Edit existing SQL Server connection values and retry?"):
                    return 2
                repaired = {}
                for key in ("MSSQL_SERVER", "MSSQL_DATABASE", "MSSQL_USER"):
                    repaired[key] = self.ask(key, str(present.get(key) or edits.get(key) or ""))
                password = self.ask("MSSQL_PASSWORD (masked; empty keeps existing)", secret=True)
                if password:
                    repaired["MSSQL_PASSWORD"] = password
                if not all(repaired.values()):
                    raise ControlError("SQL Server host/database/user cannot be empty")
                if not self.confirm("Save corrected connection settings to the local .env and retry?"):
                    return 2
                _save_env(repaired)
                _reload_settings()
                info = check_connection()
            self.console.print(f"[green]Connected[/green]  {info.get('database', 'TIPDataIntelligence')}")
            repo = ControlRepository()
            try:
                repo.validate_schema()
            except ControlError:
                if not self.confirm("DBD_CONTROL is missing. Create/validate only the control table and admission procedure now?"):
                    return 2
                repo.ensure_schema()
                repo.validate_schema()

            if node is None:
                if not self.confirm("Create a stable Machine ID on this computer?"):
                    return 2
                node = local_node_id(create=True)
            assert node is not None
            operator = edits.get("DBD_OPERATOR_NAME") or present.get("DBD_OPERATOR_NAME") or settings.dbd_operator_name
            current = repo.node_status(node)
            with repo_connection() as conn:
                admin_capable = repo._has_admin_permission(conn)
                sql_login = repo.original_login(conn)

            if current is None and admin_capable and repo.schedule() is None:
                if self.confirm("Initialize this machine as the first Admin Coordinator?"):
                    repo.register_admin(node, str(operator))
                    current = repo.node_status(node)
            if not current or not current.get("authorized"):
                self.console.print(Panel(
                    f"NODE: {node}\nSQL Login: {sql_login}\nOperator: {operator}\n"
                    "Give these values to the Admin Coordinator for approval, then rerun setup.",
                    title="WORKER ENROLLMENT REQUIRED", border_style="yellow",
                ))
                return 2

            if not repo.can_execute_admission():
                raise ControlError(
                    "SQL Server login cannot EXECUTE dbo.DBD_CONTROL_ADMIT; "
                    "ask the SQL Server administrator for the minimal grant "
                    "(see docs/WORKER_SETUP.md)"
                )
            self.console.print(
                f"[green]Authorized[/green] {current['role']}  |  Node {node}  |  SQL login {sql_login}"
            )
            if current["role"] == "ADMIN":
                self._admin_actions(repo, node)
                if repo.schedule() is None:
                    raise ControlError("Monthly schedule is required before the Admin is READY")
            else:
                self.console.print("[dim]Worker cannot change schedules or approve machines.[/dim]")
            self.console.print(Panel(
                "SQL Server READY · Node AUTHORIZED · Local Identity READY",
                title="SETUP COMPLETE", border_style="green",
            ))
            return 0
        except KeyboardInterrupt:
            self.console.print("[yellow]Setup cancelled; unconfirmed changes were not applied.[/yellow]")
            return 130
        except Exception as exc:
            self.console.print(f"[red]SETUP FAILED:[/red] {exc}")
            return 1

    def _admin_actions(self, repo: ControlRepository, node: str) -> None:
        from ..storage.sqlserver.repository import ProductionSqlServerRepository
        state = ProductionSqlServerRepository().generation_state()
        schedule = repo.schedule()
        if schedule is None or self.confirm("Edit Monthly Generation schedule and Global HTTP rate?"):
            day = int(self.ask("Monthly start day (1..31)", str((schedule or {}).get("day", 15))))
            hour = int(self.ask("Start hour (0..23)", str((schedule or {}).get("hour", 9))))
            minute = int(self.ask("Start minute (0..59)", str((schedule or {}).get("minute", 0))))
            rate = float(self.ask("Global HTTP requests/second", str(settings.dbd_rate_per_second)))
            validate_schedule(day, hour, minute)
            if self.confirm(
                f"Confirm monthly day {day} at {hour:02d}:{minute:02d} Asia/Bangkok, "
                f"shared rate {rate:g}/s? Active generation will NOT change."
            ):
                result = repo.set_schedule(
                    node, day=day, hour=hour, minute=minute,
                    rate=rate, generation_state=state,
                )
                self.console.print(f"[green]Saved schedule:[/green] {result}")

        action = self.choice("Admin operations", [
            ("none", "Continue without node changes"),
            ("approve", "Approve a Worker machine"),
            ("revoke", "Revoke a Worker machine"),
        ])
        if action == "approve":
            worker = self.ask("Worker Machine ID (node-...)")
            login = self.ask("Worker SQL Login (from Worker Setup)")
            op = self.ask("Worker Operator display name")
            if self.confirm(f"Approve {worker}, SQL Login {login}?"):
                repo.approve_worker(node, worker, login, op)
        elif action == "revoke":
            worker = self.ask("Worker Machine ID to revoke")
            if self.confirm(f"Revoke {worker}? Existing in-flight work will lose authorization."):
                repo.revoke_worker(node, worker)

        if sys.platform == "win32":
            task_action = self.choice("Windows Auto Run", [
                ("keep", "Leave current Windows Task unchanged"),
                ("enable", "Enable/update Daily + At Logon Task"),
                ("disable", "Disable existing DBD Monthly Core Task"),
            ])
            try:
                if task_action == "enable":
                    time_value = self.ask("Daily check time (HH:MM)", "09:00")
                    hour, minute = [int(x) for x in time_value.split(":")]
                    validate_schedule(15, hour, minute)
                    if self.confirm("Register/update the one per-user non-elevated Windows Task?"):
                        wake = self.confirm("Wake the computer from Sleep when Windows and hardware support it?")
                        register_windows_task(check_hour=hour, check_minute=minute, wake=wake)
                        self.console.print("[green]Windows Task registered.[/green]")
                elif task_action == "disable" and self.confirm("Disable the DBD Monthly Core Task?"):
                    result = subprocess.run(
                        ["schtasks.exe", "/Change", "/TN", TASK_NAME, "/DISABLE"],
                        capture_output=True, text=True, check=False, timeout=30,
                    )
                    if result.returncode:
                        raise ControlError((result.stderr or result.stdout).strip())
                    self.console.print("[green]Windows Task disabled.[/green]")
            except Exception as exc:
                self.console.print(f"[yellow]Task Scheduler not changed: {exc}[/yellow]")
                self.console.print("Manual instructions: docs/ADMIN_SETUP.md")


def repo_connection():
    from ..storage.sqlserver.engine import connection
    return connection()


def confirm_early_cycle() -> bool:
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return False
    return SetupWizard().confirm(
        "The next monthly generation is NOT DUE yet. Start early anyway? "
        "This is audited, and the month cannot be started again after completion."
    )
