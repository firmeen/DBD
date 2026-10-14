from __future__ import annotations

import sys
import time
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from threading import RLock
from typing import Any, Callable, Mapping, TextIO

from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

_CYCLE_PHASES = ("PRECHECK", "DISCOVER", "PREPARE", "COLLECT", "VERIFY", "FINALIZE")
_COLLECTION_COUNTERS = (
    "companies_claimed",
    "companies_finished",
    "companies_in_flight",
    "companies_retry_wait",
    "endpoints_done",
    "endpoints_not_available",
    "endpoints_failed",
    "leases_recovered",
)
_MUTATING_COMMANDS = {
    "migrate",
    "discover",
    "fetch-company",
    "cycle-run",
    "cycle-loop",
    "prepare",
    "collect",
    "retry",
    "branch",
}
_LONG_RUNNING_COMMANDS = {"discover", "fetch-company", "cycle-run", "cycle-loop", "collect", "branch"}
_COMMAND_LABELS = {
    "check-db": "CHECK DB",
    "migrate": "MIGRATE",
    "cutover": "CUTOVER",
    "discover": "DISCOVER",
    "branch": "BRANCH",
    "fetch-company": "FETCH COMPANY",
    "cycle-run": "CYCLE RUN",
    "cycle-loop": "CYCLE LOOP",
    "cycle-status": "CYCLE STATUS",
    "prepare": "PREPARE",
    "collect": "COLLECT",
    "retry": "RETRY",
    "status": "STATUS",
}


def _number(value: Any) -> str:
    if value is None:
        return ""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def _duration(seconds: float | int | None) -> str:
    total = max(0, int(float(seconds or 0)))
    return f"{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}"


def _local_datetime(value: datetime) -> datetime:
    return value if value.utcoffset() is not None else value.astimezone()


def _timestamp(value: datetime) -> str:
    local = _local_datetime(value)
    offset = local.strftime("%z")
    if len(offset) == 5:
        offset = f"{offset[:3]}:{offset[3:]}"
    return f"{local:%Y-%m-%d %H:%M:%S} {offset}".rstrip()


def _clock_time(value: datetime) -> str:
    return _local_datetime(value).strftime("%H:%M:%S")


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return {"value": value}


def _truthy_count(value: Any) -> bool:
    try:
        return int(value or 0) != 0
    except (TypeError, ValueError):
        return bool(value)


@dataclass
class V06TerminalState:
    app: str
    command: str
    backend: str = "sqlserver"
    operator_name: str | None = None
    node_id: str | None = None
    automatic: bool = False
    started_at: datetime | None = None
    started_monotonic: float = 0.0
    finished_at: datetime | None = None
    finished_monotonic: float | None = None
    status: str = "STARTING"
    phase: str | None = None
    generation: int | None = None
    dataset_phase: str | None = None
    resumed: bool | None = None
    phase_states: dict[str, str] = field(default_factory=dict)
    discovery: dict[str, Any] = field(default_factory=dict)
    branch: dict[str, Any] = field(default_factory=dict)
    last_plain_progress_at: float = 0.0
    collection: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    active_endpoints: tuple[str, ...] = ()
    endpoint_states: dict[str, str] = field(default_factory=dict)
    current_company_id: int | None = None
    current_registration_no: str | None = None
    current_endpoint: str | None = None
    juristic_type_code: str | None = None
    iteration: int | None = None
    wait_started_monotonic: float | None = None
    wait_seconds: float | None = None
    last_cycle: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    cutover: dict[str, Any] = field(default_factory=dict)
    last_warning: str | None = None
    last_error: str | None = None
    transient_kind: str | None = None
    transient_until: float = 0.0
    handoff_phase: str | None = None
    handoff_until: float = 0.0
    changed_at: dict[str, float] = field(default_factory=dict)
    last_plain_milestone: tuple[str, str, int | None] | None = None


class _Scene:
    def __init__(self, terminal: V06Terminal) -> None:
        self.terminal = terminal

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> RenderResult:
        try:
            yield self.terminal._render(max(40, int(options.max_width)))
        except Exception:
            self.terminal._renderer_failed = True
            state = self.terminal.state
            label = _COMMAND_LABELS.get(state.command, state.command.upper())
            fallback = Text()
            fallback.append(f"{state.app}  /  {label}\n", style="bold white")
            if state.started_at is not None:
                fallback.append(f"Started  {_timestamp(state.started_at)}\n", style="dim")
            fallback.append(
                "Terminal rendering unavailable; runtime continues.",
                style="bold yellow",
            )
            yield fallback


class V06Terminal:
    """Unified presentation-only terminal for the SQL Server v0.6 command path."""

    def __init__(
        self,
        command: str,
        *,
        app: str = "DBD DATA",
        stream: TextIO = sys.stdout,
        tty: bool | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.stream = stream
        self._clock = clock
        self._wall_clock = wall_clock or (lambda: datetime.now().astimezone())
        self._lock = RLock()
        self._renderer_failed = False
        self._closed = False
        detected = False
        try:
            detected = bool(stream.isatty())
        except Exception:
            detected = False
        self.interactive = detected if tty is None else bool(tty)
        self.console = Console(
            file=stream,
            soft_wrap=False,
            force_terminal=True if self.interactive else False,
            color_system="auto" if self.interactive else None,
        )
        self.state = V06TerminalState(app=app, command=command)
        self._scene = _Scene(self)
        self._live: Live | None = None

    def start(self, **context: Any) -> V06Terminal:
        now = self._clock()
        with self._lock:
            if context.get("backend"):
                self.state.backend = str(context["backend"])
            self.state.operator_name = (
                str(context["operator_name"]) if context.get("operator_name") else None
            )
            self.state.node_id = (
                str(context["node_id"]) if context.get("node_id") else None
            )
            self.state.automatic = bool(context.get("auto", False))
            self.state.started_at = self._wall_clock()
            self.state.started_monotonic = now
            self.state.status = "RUNNING"
            self.state.dataset_phase = (
                str(context.get("phase")).upper() if context.get("phase") else None
            )
            self.state.current_registration_no = context.get("registration_no")
            self.state.juristic_type_code = context.get("juristic_type_code")
            for key in (
                "concurrency",
                "rate",
                "http_retries",
                "continuous",
                "idle_seconds",
                "limit",
                "coverage",
            ):
                if key in context and context[key] is not None:
                    self.state.config[key] = context[key]
        if self.interactive:
            try:
                self._live = Live(
                    self._scene,
                    console=self.console,
                    auto_refresh=True,
                    refresh_per_second=4,
                    transient=False,
                    screen=False,
                    vertical_overflow="visible",
                    redirect_stdout=False,
                    redirect_stderr=False,
                )
                self._live.start(refresh=True)
            except Exception:
                self._renderer_failed = True
                self.interactive = False
                self._live = None
                self._plain_start()
        else:
            self._plain_start()
        return self

    def __call__(self, name: str, payload: dict[str, Any]) -> None:
        self.event(name, payload)

    def event(self, name: str, payload: dict[str, Any]) -> None:
        try:
            self._apply_event(str(name), dict(payload))
            if not self.interactive:
                self._plain_event(str(name), payload)
        except Exception:
            self._renderer_failed = True

    def finish(self, result: Any = None, *, status: str | None = None) -> None:
        payload = _mapping(result)
        final_status = status or self._final_status(payload)
        if self.interactive and self._live is not None and final_status in {
            "COMPLETE",
            "READY",
        }:
            with self._lock:
                self.state.transient_kind = "settle"
                self.state.transient_until = self._clock() + 0.10
            self._safe_refresh()
            time.sleep(0.06)

        with self._lock:
            self.state.result = payload
            self.state.status = final_status
            self.state.finished_at = self._wall_clock()
            self.state.finished_monotonic = self._clock()
            self.state.wait_started_monotonic = None
            self.state.wait_seconds = None
            if final_status in {"COMPLETE", "READY", "COMPLETE_WITH_FAILURES"}:
                self.state.transient_kind = None
                self.state.transient_until = 0.0

        if self.interactive:
            self._safe_refresh()
            self._stop_live()
            if self._renderer_failed:
                self._plain_finish(payload, final_status)
        else:
            self._plain_finish(payload, final_status)

    def fail(self, message: str) -> None:
        with self._lock:
            self.state.last_error = str(message)
            self.state.status = "FAILED"
            self.state.finished_at = self._wall_clock()
            self.state.finished_monotonic = self._clock()
        if self.interactive:
            self._safe_refresh()
            self._stop_live()
            if self._renderer_failed:
                self._plain_failure(message, exit_code=1)
        else:
            self._plain_failure(message, exit_code=1)

    def interrupt(self) -> None:
        if self._closed:
            return
        with self._lock:
            self.state.status = "STOPPING"
            self.state.transient_kind = "stopping"
            self.state.transient_until = self._clock() + 0.12
        if self.interactive:
            self._safe_refresh()
            time.sleep(0.08)
        with self._lock:
            self.state.status = "STOPPED"
            self.state.finished_at = self._wall_clock()
            self.state.finished_monotonic = self._clock()
        if self.interactive:
            self._safe_refresh()
            self._stop_live()
            if self._renderer_failed:
                self._plain_stopped()
        else:
            self._plain_stopped()

    def close(self) -> None:
        if self._closed:
            return
        self._stop_live()
        self._closed = True

    def _apply_event(self, name: str, payload: dict[str, Any]) -> None:
        now = self._clock()
        with self._lock:
            if self.state.command == "cutover" and name in {"started", "progress", "complete"}:
                self.state.cutover.update(payload)
                if name == "complete":
                    self.state.status = "COMPLETE"
                return
            generation = payload.get("generation")
            if generation is not None:
                self.state.generation = int(generation)

            if name == "phase_started":
                phase = str(payload.get("phase") or "WORK").upper()
                if (
                    self.state.command in {"cycle-run", "cycle-loop"}
                    and phase not in _CYCLE_PHASES
                ):
                    return
                if (
                    self.state.command == "cycle-loop"
                    and phase == "PRECHECK"
                    and self.state.status == "WAITING"
                ):
                    self.state.phase_states.clear()
                    self.state.discovery.clear()
                    self.state.collection.clear()
                    self.state.current_company_id = None
                    self.state.current_endpoint = None
                self.state.status = "RUNNING"
                self.state.phase = phase
                self.state.wait_started_monotonic = None
                self.state.wait_seconds = None
                if phase in _CYCLE_PHASES:
                    self.state.phase_states[phase] = "RUNNING"
                if payload.get("resumed") is not None:
                    self.state.resumed = bool(payload["resumed"])
                return

            if name == "phase_completed":
                phase = str(payload.get("phase") or self.state.phase or "WORK").upper()
                event_status = str(payload.get("status") or "COMPLETE").upper()
                phase_status = "FAILED" if event_status == "FAILED" else "COMPLETE"
                if phase in _CYCLE_PHASES:
                    self.state.phase_states[phase] = phase_status
                    self.state.handoff_phase = phase
                    self.state.handoff_until = now + 0.30
                if payload.get("resumed") is not None:
                    self.state.resumed = bool(payload["resumed"])
                if phase == "DISCOVER":
                    self._update_discovery(payload, now)
                elif phase == "COLLECT":
                    self._update_collection(payload, now)
                elif phase == "VERIFY":
                    self.state.config["verification_status"] = event_status
                self.state.phase = phase
                return

            if name == "branch_progress":
                self.state.branch.update(payload)
                return

            if name == "discovery_progress":
                self._update_discovery(payload, now)
                return

            if name == "discovery_split":
                self._update_discovery(payload, now)
                self.state.transient_kind = "split"
                self.state.transient_until = now + 0.85
                return

            if name == "started":
                self.state.phase = "COLLECT"
                self.state.generation = int(payload.get("generation") or self.state.generation or 0) or None
                if payload.get("phase"):
                    self.state.dataset_phase = str(payload["phase"]).upper()
                for key in (
                    "concurrency",
                    "rate_per_second",
                    "company_limit",
                    "continuous",
                    "idle_seconds",
                ):
                    if key in payload:
                        self.state.config[key] = payload[key]
                active = tuple(str(item) for item in payload.get("active_endpoints") or ())
                self.state.active_endpoints = active
                if self.state.command == "fetch-company":
                    self.state.endpoint_states = {key: "SELECTED" for key in active}
                self._update_collection(payload, now)
                return

            if name == "progress":
                if self.state.command == "collect" and self.state.status == "WAITING":
                    self.state.status = "RUNNING"
                    self.state.wait_started_monotonic = None
                    self.state.wait_seconds = None
                self._update_collection(payload, now)
                return

            if name == "company_claimed":
                if payload.get("company_id") is not None:
                    self.state.current_company_id = int(payload["company_id"])
                if payload.get("registration_no"):
                    self.state.current_registration_no = str(payload["registration_no"])
                return

            if name == "endpoint_started":
                endpoint = str(payload.get("endpoint") or "")
                if payload.get("company_id") is not None:
                    self.state.current_company_id = int(payload["company_id"])
                self.state.current_endpoint = endpoint or None
                if self.state.command == "fetch-company" and endpoint:
                    self.state.endpoint_states[endpoint] = "ACTIVE"
                return

            if name in {
                "endpoint_completed",
                "endpoint_not_available",
                "endpoint_failed",
            }:
                endpoint = str(payload.get("endpoint") or "")
                if payload.get("company_id") is not None:
                    self.state.current_company_id = int(payload["company_id"])
                self.state.current_endpoint = endpoint or self.state.current_endpoint
                if self.state.command == "fetch-company" and endpoint:
                    endpoint_status = {
                        "endpoint_completed": "COMPLETE",
                        "endpoint_not_available": "N/A",
                        "endpoint_failed": "FAILED",
                    }[name]
                    self.state.endpoint_states[endpoint] = endpoint_status
                if name == "endpoint_failed":
                    self.state.last_warning = (
                        f"{endpoint} failed" if endpoint else "Endpoint failed"
                    )
                    self.state.transient_kind = "warning"
                    self.state.transient_until = now + 1.2
                return

            if name == "endpoint_retry":
                endpoint = str(payload.get("endpoint") or "")
                self.state.current_endpoint = endpoint or self.state.current_endpoint
                if payload.get("company_id") is not None:
                    self.state.current_company_id = int(payload["company_id"])
                if self.state.command == "fetch-company" and endpoint:
                    self.state.endpoint_states[endpoint] = "RETRY"
                self.state.last_warning = str(payload.get("error") or "Endpoint retry")
                self.state.transient_kind = "retry"
                self.state.transient_until = now + 1.0
                return

            if name == "company_completed":
                if payload.get("company_id") is not None:
                    self.state.current_company_id = int(payload["company_id"])
                if self.state.command == "fetch-company" and payload.get("status"):
                    self.state.config["company_status"] = str(payload["status"])
                return

            if name == "warning":
                self.state.last_warning = str(
                    payload.get("message") or payload.get("error") or "Warning"
                )
                self.state.transient_kind = "warning"
                self.state.transient_until = now + 1.2
                return

            if name == "cycle_completed":
                self.state.last_cycle = dict(payload)
                self.state.result = dict(payload)
                self.state.status = str(payload.get("status") or "COMPLETE").upper()
                return

            if name == "cycle_waiting":
                self.state.status = "WAITING"
                self.state.phase = None
                if self.state.command == "collect":
                    self.state.current_company_id = None
                    self.state.current_registration_no = None
                    self.state.current_endpoint = None
                    self.state.transient_kind = None
                    self.state.transient_until = 0.0
                self.state.iteration = int(payload.get("iteration") or 0) or None
                self.state.wait_seconds = float(
                    payload.get("interval_seconds") or payload.get("idle_seconds") or 0
                )
                self.state.wait_started_monotonic = now
                if self.state.iteration is not None:
                    self.state.last_cycle["iteration"] = self.state.iteration
                return

    def _update_discovery(self, payload: Mapping[str, Any], now: float) -> None:
        aliases = {
            "partitions": "buckets_processed",
            "pages": "pages_fetched",
            "companies": "companies_written",
            "requests": "requests_made",
        }
        for target in (
            "buckets_processed", "pages_fetched", "companies_written",
            "requests_made", "invalid_rows",
            "work_id", "bucket", "stage", "pages_done", "pages_total",
            "total_buckets", "committed_pages", "pending", "counted",
            "fetching", "done", "blocked", "failed", "split",
            "rate_limit_hits", "configured_rate", "cooldown_seconds",
            "elapsed_seconds", "scope", "error",
        ):
            source = target
            if source not in payload:
                source = next((key for key, value in aliases.items() if value == target and key in payload), source)
            if source in payload:
                self._set_value(self.state.discovery, target, payload[source], f"discovery.{target}", now)

    def _update_collection(self, payload: Mapping[str, Any], now: float) -> None:
        for key in _COLLECTION_COUNTERS:
            if key in payload:
                self._set_value(self.state.collection, key, payload[key], f"collection.{key}", now)
        if "configured_rate" in payload:
            self.state.config["configured_rate"] = payload["configured_rate"]
        if "rate_per_second" in payload:
            self.state.config["rate_per_second"] = payload["rate_per_second"]
        for key in ("http_requests", "http_retries", "rate_limit_hits"):
            if key in payload:
                self.state.collection[key] = payload[key]

    def _set_value(
        self,
        target: dict[str, Any],
        key: str,
        value: Any,
        change_key: str,
        now: float,
    ) -> None:
        if target.get(key) != value:
            self.state.changed_at[change_key] = now
        target[key] = value

    def _final_status(self, payload: Mapping[str, Any]) -> str:
        if self.state.command == "check-db":
            return "READY"
        if self.state.command in {"cycle-run", "cycle-loop"}:
            value = payload.get("status")
            return str(value or "COMPLETE").upper()
        if self.state.command == "fetch-company":
            value = str(payload.get("status") or "COMPLETE").upper()
            return "COMPLETE" if value == "COMPLETE" else "COMPLETE_WITH_FAILURES"
        if self.state.command == "collect":
            durable = payload.get("status")
            if isinstance(durable, Mapping) and any(
                _truthy_count(durable.get(key))
                for key in ("retry_wait", "partial", "failed")
            ):
                return "COMPLETE_WITH_FAILURES"
            if _truthy_count(payload.get("endpoints_failed")):
                return "COMPLETE_WITH_FAILURES"
            return "COMPLETE"
        if self.state.command in {"cycle-status", "status"}:
            return "SNAPSHOT"
        return "COMPLETE"

    def _render(self, width: int):
        with self._lock:
            parts: list[Any] = [self._render_header()]
            blocks = self._render_command(width)
            if blocks:
                parts.append(Text())
                parts.extend(blocks)
            footer = self._render_footer()
            if footer is not None:
                parts.append(Text())
                parts.append(footer)
            return Group(*parts)

    def _render_header(self):
        started = self.state.started_at or self._wall_clock()
        command = _COMMAND_LABELS.get(self.state.command, self.state.command.upper())
        title = Text()
        title.append(self.state.app, style="bold white")
        title.append("  /  ", style="dim")
        title.append(command, style="bold white")
        title.append(f"  ·  {self.state.backend.upper()}", style="cyan")
        timing = Text()
        timing.append("Started  ", style="dim")
        timing.append(_timestamp(started), style="white")
        timing.append("   ·   ", style="dim")
        if self.state.finished_monotonic is None:
            elapsed = self._clock() - self.state.started_monotonic
            timing.append("Elapsed  ", style="dim")
        else:
            elapsed = self.state.finished_monotonic - self.state.started_monotonic
            timing.append("Duration  ", style="dim")
        timing.append(_duration(elapsed), style="white")
        identity = Text()
        if self.state.operator_name:
            identity.append("Operator  ", style="dim")
            identity.append(self.state.operator_name, style="white")
        if self.state.node_id:
            if identity.plain:
                identity.append("   ·   ", style="dim")
            identity.append("Node  ", style="dim")
            identity.append(self.state.node_id, style="cyan")
        if self.state.command == "cycle-run":
            if identity.plain:
                identity.append("   ·   ", style="dim")
            identity.append(
                "AUTO" if self.state.automatic else "MANUAL",
                style="bold cyan" if self.state.automatic else "dim",
            )
        return Group(title, timing, identity) if identity.plain else Group(title, timing)

    def _render_command(self, width: int) -> list[Any]:
        command = self.state.command
        if (
            self.state.finished_monotonic is not None
            and not self.state.result
            and self.state.status in {"FAILED", "STOPPED"}
        ):
            status = self.state.status
            message = (
                self.state.last_error
                if status == "FAILED" and self.state.last_error
                else "Command stopped before completion."
            )
            title = "ERROR" if status == "FAILED" else "STOPPED"
            return [
                self._panel(
                    title,
                    Text(message, style=self._status_style(status)),
                    status,
                )
            ]
        if command == "check-db":
            return [self._render_check_db(width)]
        if command == "migrate":
            return [self._render_migrate(width)]
        if command == "cutover":
            return [self._render_cutover(width)]
        if command == "discover":
            return [self._render_discovery(width)]
        if command == "branch":
            return [self._render_branch(width)]
        if command == "fetch-company":
            return self._render_fetch_company(width)
        if command in {"cycle-run", "cycle-loop"}:
            return self._render_cycle(width)
        if command == "cycle-status":
            return self._render_status(width, allow_coverage=False)
        if command == "prepare":
            return [self._render_prepare(width)]
        if command == "collect":
            return self._render_collect(width)
        if command == "retry":
            return [self._render_retry(width)]
        if command == "status":
            return self._render_status(width, allow_coverage=True)
        return [self._panel("WORK", Text("Running"), self.state.status)]

    def _render_check_db(self, width: int):
        if not self.state.result:
            body = self._centered(
                f"Checking {self.state.backend.upper()} production contract",
                width,
                "cyan",
            )
            return self._panel("DATABASE CONTRACT", body, "CHECKING", active=True)
        result = self.state.result
        items = [
            ("Server", result.get("server"), "check.server"),
            ("Database", result.get("database"), "check.database"),
            ("Login", result.get("login"), "check.login"),
            ("Schema", result.get("schema"), "check.schema"),
            ("Managed tables", result.get("managed_tables"), "check.tables"),
        ]
        permission = Text()
        for label, key in (("SELECT", "select"), ("INSERT", "insert"), ("UPDATE", "update")):
            if key not in result:
                continue
            if len(permission):
                permission.append("     ")
            permission.append("✓ " if result.get(key) else "× ", style="green" if result.get(key) else "red")
            permission.append(label, style="green" if result.get(key) else "red")
        body = Group(self._metric_grid(items, width), permission) if len(permission) else self._metric_grid(items, width)
        return self._panel("DATABASE CONTRACT", body, "READY")

    def _render_migrate(self, width: int):
        if not self.state.result:
            body = Group(
                self._centered("Applying safe schema contract", width, "cyan"),
                self._centered(self._motion("seal"), width, "cyan"),
            )
            return self._panel(f"{self.state.backend.upper()} SCHEMA", body, "APPLYING", active=True)
        result = self.state.result
        created = result.get("created")
        altered = result.get("altered")
        items = [
            ("Managed tables", result.get("managed_tables"), "migrate.tables"),
            ("Created", len(created) if isinstance(created, list) else created, "migrate.created"),
            ("Altered", len(altered) if isinstance(altered, list) else altered, "migrate.altered"),
        ]
        return self._panel("SQL SERVER SCHEMA", self._metric_grid(items, width), "COMPLETE")

    def _render_cutover(self, width: int):
        snapshot = self.state.result or self.state.cutover
        items = [
            ("Generation", snapshot.get("generation", 1), "cutover.generation"),
            ("Source rows", snapshot.get("source_rows", 0), "cutover.source"),
            ("Companies", snapshot.get("companies", 0), "cutover.companies"),
            ("Relations", snapshot.get("relations", 0), "cutover.relations"),
            ("Events", snapshot.get("events", 0), "cutover.events"),
            ("Financials", snapshot.get("financial", 0), "cutover.financial"),
            ("Industries", snapshot.get("industries", snapshot.get("industries_written", 0)), "cutover.industry"),
            ("Batches", snapshot.get("batches", 0), "cutover.batches"),
        ]
        state = "COMPLETE" if self.state.result else "COPYING"
        return self._panel(
            f"MYSQL LEGACY → {self.state.backend.upper()} PRODUCTION",
            self._metric_grid(items, width),
            state, active=not bool(self.state.result),
        )

    def _render_discovery(self, width: int):
        values = dict(self.state.discovery)
        if self.state.result:
            result = self.state.result
            values.update(
                {
                    "buckets_processed": result.get("partitions", result.get("buckets_processed")),
                    "pages_fetched": result.get("pages", result.get("pages_fetched")),
                    "companies_written": result.get("companies", result.get("companies_written")),
                    "requests_made": result.get("requests", result.get("requests_made")),
                    "invalid_rows": result.get("invalid_rows"),
                }
            )
        items: list[tuple[str, Any, str]] = []
        generation = self.state.result.get("generation", self.state.generation)
        if generation is not None:
            items.append(("Generation", generation, "discovery.generation"))
        items.extend(
            [
                ("Partitions", values.get("buckets_processed", 0), "discovery.buckets_processed"),
                ("Pages", values.get("pages_fetched", 0), "discovery.pages_fetched"),
                ("Companies", values.get("companies_written", 0), "discovery.companies_written"),
                ("Requests", values.get("requests_made", 0), "discovery.requests_made"),
                ("Invalid", values.get("invalid_rows", 0), "discovery.invalid_rows"),
            ]
        )
        content: list[Any] = [self._metric_grid(items, width)]
        if values.get("stage") or values.get("bucket"):
            detail = Text()
            detail.append("Stage  ", style="dim")
            detail.append(str(values.get("stage") or "INITIALIZING"), style="bold cyan")
            if values.get("bucket"):
                detail.append("   Bucket  ", style="dim")
                detail.append(str(values["bucket"])[:70])
            content.extend([Text(), detail])
        page_done = values.get("pages_done")
        page_total = values.get("pages_total")
        if page_done is not None and page_total:
            done = max(0, min(int(page_done),int(page_total)))
            total = max(1,int(page_total))
            progress = Text(
                f"Current leaf  {done:,}/{total:,}  ({done/total:.1%})",
                style="cyan",
            )
            content.append(progress)
        if values.get("total_buckets") is not None:
            content.extend([Text(), self._metric_grid([
                ("Durable buckets",values.get("total_buckets"),"discovery.total"),
                ("Done leaves",values.get("done",0),"discovery.done"),
                ("Blocked",values.get("blocked",0),"discovery.blocked"),
                ("Committed pages",values.get("committed_pages",0),"discovery.committed"),
            ],width)])
        if values.get("configured_rate") is not None:
            content.extend([Text(), self._metric_grid([
                ("Target req/s",values.get("configured_rate"),"discovery.rate"),
                ("HTTP 429",values.get("rate_limit_hits",0),"discovery.429"),
                ("Cooldown s",round(float(values.get("cooldown_seconds") or 0),1),"discovery.cooldown"),
            ],width)])
        if values.get("blocked") or values.get("failed"):
            content.append(Text("Global coverage: INCOMPLETE",style="yellow"))
        elif not self.state.result:
            content.append(Text("Global coverage: VERIFYING / UNKNOWN",style="dim"))
        if not self.state.result:
            content.append(Text())
            if self._transient("split"):
                content.append(self._centered("──────●──────  ↳ Splitting partition", width, "cyan"))
            else:
                content.append(self._centered(self._motion("sweep", width), width, "cyan"))
        status = "COMPLETE" if self.state.result else "RUNNING"
        return self._panel("DISCOVERY", Group(*content), status, active=not self.state.result)

    def _render_branch(self, width: int):
        values = self.state.result or self.state.branch
        items = [
            ("Checked",values.get("checked",0),"branch.checked"),
            ("Verified",values.get("verified",0),"branch.verified"),
            ("Branches",values.get("branches",0),"branch.branches"),
            ("Deferred",values.get("deferred",0),"branch.deferred"),
            ("HTTP attempts",values.get("http_requests",0),"branch.requests"),
        ]
        status = "COMPLETE" if self.state.result else "RUNNING"
        body = self._metric_grid(items,width)
        return self._panel("BRANCH ENRICHMENT",body,status,active=not bool(self.state.result))

    def _render_prepare(self, width: int):
        if self.state.result:
            result = self.state.result
            items = [
                ("Generation", result.get("generation"), "prepare.generation"),
                ("Phase", str(result.get("phase") or "").upper(), "prepare.phase"),
                ("Rows prepared", result.get("rows_prepared"), "prepare.rows"),
            ]
            return self._panel("PREPARATION", self._metric_grid(items, width), "COMPLETE")
        items = []
        if self.state.generation is not None:
            items.append(("Generation", self.state.generation, "prepare.generation"))
        if self.state.dataset_phase:
            items.append(("Phase", self.state.dataset_phase, "prepare.phase"))
        body = Group(
            self._metric_grid(items, width) if items else Text(),
            Text(),
            self._centered(self._motion("stitch", width), width, "cyan"),
            self._centered("Preparing required work", width, "cyan"),
        )
        return self._panel("PREPARATION", body, "RUNNING", active=True)

    def _render_retry(self, width: int):
        if self.state.result:
            result = self.state.result
            items = [
                ("Generation", result.get("generation"), "retry.generation"),
                ("Phase", str(result.get("phase") or "").upper(), "retry.phase"),
                ("Reopened", result.get("reopened"), "retry.reopened"),
            ]
            return self._panel("RETRY", self._metric_grid(items, width), "COMPLETE")
        items = []
        if self.state.generation is not None:
            items.append(("Generation", self.state.generation, "retry.generation"))
        if self.state.dataset_phase:
            items.append(("Phase", self.state.dataset_phase, "retry.phase"))
        body = Group(
            self._metric_grid(items, width) if items else Text(),
            Text(),
            self._centered("Reopening failed work", width, "cyan"),
        )
        return self._panel("RETRY", body, "RUNNING", active=True)

    def _render_collect(self, width: int) -> list[Any]:
        if self.state.result:
            return [self._render_collection_block(width, final=True)]
        runtime_items: list[tuple[str, Any, str]] = []
        if self.state.generation is not None:
            runtime_items.append(("Generation", self.state.generation, "runtime.generation"))
        if self.state.dataset_phase:
            runtime_items.append(("Phase", self.state.dataset_phase, "runtime.phase"))
        concurrency = self.state.config.get("concurrency")
        if concurrency is not None:
            runtime_items.append(("Concurrency", concurrency, "runtime.concurrency"))
        rate = self.state.config.get("rate_per_second", self.state.config.get("rate"))
        if rate is not None:
            runtime_items.append(("Configured rate", f"{rate:g} req/s", "runtime.rate"))
        runtime = self._panel(
            "RUNTIME",
            self._metric_grid(runtime_items, width),
            "ACTIVE",
            context=True,
        )
        return [runtime, self._render_collection_block(width, final=False)]

    def _render_collection_block(
        self,
        width: int,
        *,
        final: bool,
        include_config: bool = False,
    ):
        source = self.state.result if final else self.state.collection
        items = [
            ("Claimed", source.get("companies_claimed", 0), "collection.companies_claimed"),
            ("Finished", source.get("companies_finished", 0), "collection.companies_finished"),
            ("In flight", source.get("companies_in_flight", 0), "collection.companies_in_flight"),
            ("Retry wait", source.get("companies_retry_wait", 0), "collection.companies_retry_wait"),
            ("Endpoint complete", source.get("endpoints_done", 0), "collection.endpoints_done"),
            (
                "Endpoint N/A",
                source.get("endpoints_not_available", 0),
                "collection.endpoints_not_available",
            ),
            ("Endpoint failed", source.get("endpoints_failed", 0), "collection.endpoints_failed"),
        ]
        if final:
            for label, key in (
                ("HTTP requests", "http_requests"),
                ("HTTP retries", "http_retries"),
                ("Rate-limit hits", "rate_limit_hits"),
            ):
                if key in source:
                    items.append((label, source.get(key), f"collection.{key}"))
        body_parts: list[Any] = [self._metric_grid(items, width)]
        if not final and include_config:
            config_items: list[tuple[str, Any, str]] = []
            concurrency = self.state.config.get("concurrency")
            if concurrency is not None:
                config_items.append(
                    ("Concurrency", concurrency, "collection.concurrency")
                )
            rate = self.state.config.get(
                "rate_per_second",
                self.state.config.get("rate"),
            )
            if rate is not None:
                config_items.append(
                    ("Configured rate", f"{rate:g} req/s", "collection.rate")
                )
            if config_items:
                body_parts.append(Text())
                body_parts.append(self._metric_grid(config_items, width))

        waiting = not final and self.state.status == "WAITING"
        if waiting:
            total = max(0.0, float(self.state.wait_seconds or 0.0))
            started = self.state.wait_started_monotonic or self._clock()
            elapsed = max(0.0, self._clock() - started)
            remaining = max(0.0, total - elapsed)
            body_parts.extend(
                [
                    Text(),
                    self._centered(self._motion("wait"), width, "yellow"),
                    self._centered(self._wait_rail(elapsed, total, width), width, "yellow"),
                    self._centered(
                        f"Next check in  {_duration(remaining)}",
                        width,
                        "yellow",
                    ),
                ]
            )
        elif not final:
            body_parts.append(Text())
            body_parts.append(
                self._centered(self._motion("stream", width), width, "cyan")
            )
            activity = self._activity_text()
            if activity:
                body_parts.append(
                    self._centered(activity.plain, width, activity.style or "cyan")
                )
        status = self.state.status if final else ("WAITING" if waiting else self._active_status())
        if final and status not in {"COMPLETE_WITH_FAILURES", "FAILED"}:
            status = "COMPLETE"
        return self._panel(
            "COLLECTION",
            Group(*body_parts),
            status,
            active=not final,
        )

    def _activity_text(self) -> Text | None:
        if self._transient("retry"):
            prefix = "↻ "
            style = "bold yellow"
        else:
            prefix = ""
            style = "cyan"
        company = self.state.current_company_id
        endpoint = self.state.current_endpoint
        if company is None and endpoint is None:
            return None
        chunks = []
        if company is not None:
            chunks.append(f"company {company}")
        if endpoint:
            chunks.append(str(endpoint))
        return Text(prefix + "  ·  ".join(chunks), style=style)

    def _render_fetch_company(self, width: int) -> list[Any]:
        result = self.state.result
        company_items = [
            (
                "Registration",
                result.get("registration_no", self.state.current_registration_no),
                "company.registration",
            ),
            (
                "Juristic type",
                result.get("juristic_type_code", self.state.juristic_type_code),
                "company.type",
            ),
            ("Company ID", result.get("company_id", self.state.current_company_id), "company.id"),
            ("Generation", result.get("generation", self.state.generation), "company.generation"),
            (
                "Phase",
                str(result.get("phase") or self.state.dataset_phase or "").upper(),
                "company.phase",
            ),
        ]
        if result:
            company_items.append(("Status", result.get("status"), "company.status"))
            status = str(result.get("status") or self.state.status).upper()
            return [
                self._panel(
                    "COMPANY",
                    self._metric_grid(company_items, width),
                    status,
                )
            ]

        company = self._panel(
            "COMPANY",
            self._metric_grid(company_items, width),
            "ACTIVE",
            context=True,
        )
        endpoint_lines: list[Any] = []
        for endpoint in self.state.active_endpoints:
            state = self.state.endpoint_states.get(endpoint, "PENDING")
            endpoint_lines.append(self._endpoint_line(endpoint, state))
        if not endpoint_lines:
            endpoint_lines.append(Text(f"{self._motion('orbit')}  Waiting for endpoint plan", style="cyan"))
        endpoints = self._panel(
            "ENDPOINTS",
            Group(*endpoint_lines),
            self._active_status(),
            active=True,
        )
        return [company, endpoints]

    def _endpoint_line(self, endpoint: str, state: str) -> Text:
        normalized = state.upper()
        if normalized == "COMPLETE":
            marker, style = "✓", "green"
        elif normalized == "N/A":
            marker, style = "–", "dim"
        elif normalized == "FAILED":
            marker, style = "×", "bold red"
        elif normalized == "RETRY":
            marker, style = "↻", "bold yellow"
        elif normalized == "ACTIVE":
            marker, style = self._motion("orbit"), "bold cyan"
        elif normalized == "SELECTED":
            marker, style = "·", "dim"
        else:
            marker, style = "○", "dim"
        return Text(f"  {marker} {endpoint}", style=style)

    def _render_cycle(self, width: int) -> list[Any]:
        if self.state.command == "cycle-loop" and self.state.status == "WAITING":
            return self._render_cycle_wait(width)

        result = self.state.result if self.state.finished_monotonic is not None else {}
        generation = result.get("generation", self.state.generation)
        dataset_phase = str(result.get("phase") or self.state.dataset_phase or "").upper()
        resumed = result.get("resumed", self.state.resumed)
        context = []
        if generation is not None:
            context.append(f"Generation  {generation}")
        if dataset_phase:
            context.append(f"Phase  {dataset_phase}")
        if resumed is not None and not result:
            context.append("Resumed" if resumed else "New cycle")

        cycle_body: list[Any] = []
        if context:
            cycle_body.append(Text("   ·   ".join(context), style="white"))
            cycle_body.append(Text())
        cycle_body.append(self._phase_rail(width))
        cycle_status = self.state.status if result else self._active_status()
        cycle_panel = self._panel(
            "CYCLE",
            Group(*cycle_body),
            cycle_status,
            active=not bool(result),
            context=not bool(result),
        )

        if result:
            collection = dict(result.get("collection") or {})
            result_items = [
                (
                    "Companies finished",
                    collection.get("companies_finished"),
                    "cycle.companies_finished",
                ),
                ("Endpoint complete", collection.get("endpoints_done"), "cycle.endpoints_done"),
                (
                    "Endpoint N/A",
                    collection.get("endpoints_not_available"),
                    "cycle.endpoints_not_available",
                ),
                ("Endpoint failed", collection.get("endpoints_failed"), "cycle.endpoints_failed"),
                ("Finalized rows", result.get("finalized_rows"), "cycle.finalized_rows"),
            ]
            blocks = [
                cycle_panel,
                self._panel(
                    "RESULT",
                    self._metric_grid(result_items, width),
                    str(result.get("status") or "COMPLETE").upper(),
                ),
            ]
            verification = dict(result.get("verification") or {})
            if self._verification_significant(result, verification):
                blocks.append(self._render_verification_result(verification, width))
            return blocks

        active_phase = self.state.phase or "PRECHECK"
        if active_phase == "DISCOVER":
            active = self._render_discovery(width)
        elif active_phase == "PREPARE":
            active = self._render_prepare(width)
        elif active_phase == "COLLECT":
            active = self._render_collection_block(
                width,
                final=False,
                include_config=True,
            )
        elif active_phase == "VERIFY":
            active = self._render_activity_panel(
                "VERIFICATION",
                "Verifying generation",
                "radar",
                self._active_status(),
                width,
            )
        elif active_phase == "FINALIZE":
            active = self._render_activity_panel(
                "FINALIZATION",
                "Finalizing generation",
                "seal",
                self._active_status(),
                width,
            )
        else:
            active = self._render_activity_panel(
                "PRECHECK",
                "Checking production contract",
                "orbit",
                self._active_status(),
                width,
            )
        return [cycle_panel, active]

    def _render_cycle_wait(self, width: int) -> list[Any]:
        last = self.state.last_cycle
        items = [
            ("Iteration", self.state.iteration, "wait.iteration"),
            ("Generation", last.get("generation"), "wait.generation"),
            ("Result", last.get("status"), "wait.status"),
        ]
        last_panel = self._panel(
            "LAST CYCLE",
            self._metric_grid(items, width),
            str(last.get("status") or "COMPLETE").upper(),
            context=True,
        )
        total = max(0.0, float(self.state.wait_seconds or 0.0))
        started = self.state.wait_started_monotonic or self._clock()
        elapsed = max(0.0, self._clock() - started)
        remaining = max(0.0, total - elapsed)
        body = Group(
            self._centered(self._motion("wait"), width, "yellow"),
            Text(),
            self._centered(self._wait_rail(elapsed, total, width), width, "yellow"),
            Text(),
            self._centered(f"Next cycle in  {_duration(remaining)}", width, "yellow"),
        )
        return [
            last_panel,
            self._panel("NEXT CYCLE", body, "WAITING", active=True),
        ]

    def _render_status(self, width: int, *, allow_coverage: bool) -> list[Any]:
        result = self.state.result
        if not result:
            body = self._centered(
                "Reading durable state",
                width,
                "cyan",
            )
            return [self._panel("STATUS", body, "READING", active=True)]

        generation_state = dict(result.get("generation_state") or {})
        summary = dict(result.get("summary") or {})
        active_generation = generation_state.get("active_generation")
        generation_items = [
            ("Current", result.get("generation"), "status.generation"),
            (
                "Finalized",
                generation_state.get("finalized_generation"),
                "status.finalized",
            ),
            (
                "Last seen",
                generation_state.get("last_seen_generation"),
                "status.last_seen",
            ),
            (
                "Unfinished",
                generation_state.get("unfinished_rows"),
                "status.unfinished",
            ),
            ("Phase", str(result.get("phase") or "").upper(), "status.phase"),
        ]
        generation_panel = self._panel(
            "GENERATION",
            self._metric_grid(generation_items, width),
            "ACTIVE" if active_generation is not None else "READY",
            context=True,
        )
        work_items = [
            ("Total", summary.get("total"), "status.total"),
            ("Complete", summary.get("complete"), "status.complete"),
            ("Pending", summary.get("pending"), "status.pending"),
            ("Collecting", summary.get("collecting"), "status.collecting"),
            ("Retry wait", summary.get("retry_wait"), "status.retry_wait"),
            ("Partial", summary.get("partial"), "status.partial"),
            ("Failed", summary.get("failed"), "status.failed"),
            ("Unresolved", summary.get("unresolved"), "status.unresolved"),
            ("Active leases", summary.get("active_leases"), "status.active_leases"),
            ("Expired leases", summary.get("expired_leases"), "status.expired_leases"),
        ]
        blocks = [
            generation_panel,
            self._panel("WORK", self._metric_grid(work_items, width), "SNAPSHOT"),
        ]
        coverage = result.get("coverage")
        if allow_coverage and coverage:
            coverage_items = [
                ("Companies", coverage.get("companies"), "coverage.companies"),
                ("Relations", coverage.get("relations"), "coverage.relations"),
                ("Events", coverage.get("events"), "coverage.events"),
                ("Financials", coverage.get("financials"), "coverage.financials"),
                ("Industries", coverage.get("industries"), "coverage.industries"),
            ]
            blocks.append(
                self._panel(
                    "COVERAGE",
                    self._metric_grid(coverage_items, width),
                    "SNAPSHOT",
                    context=True,
                )
            )
        return blocks

    def _render_verification_result(self, verification: Mapping[str, Any], width: int):
        items: list[tuple[str, Any, str]] = []
        for label, key in (
            ("Unresolved", "unresolved"),
            ("Global unresolved", "global_unresolved"),
            ("Retry wait", "retry_wait"),
            ("Terminal failures", "terminal_failures"),
            ("Global terminal failures", "global_terminal_failures"),
            ("Active leases", "active_leases"),
            ("Expired leases", "expired_leases"),
            ("Unfinished rows", "unfinished_rows"),
        ):
            if _truthy_count(verification.get(key)):
                items.append((label, verification.get(key), f"verification.{key}"))
        if not items:
            items.append(("Status", verification.get("status", "FAILED"), "verification.status"))
        status = str(verification.get("status") or self.state.status).upper()
        if status == "COMPLETE" and self.state.status == "COMPLETE_WITH_FAILURES":
            status = "COMPLETE_WITH_FAILURES"
        return self._panel("VERIFICATION", self._metric_grid(items, width), status)

    def _verification_significant(
        self,
        result: Mapping[str, Any],
        verification: Mapping[str, Any],
    ) -> bool:
        if str(result.get("status") or "").upper() != "COMPLETE":
            return True
        return any(
            _truthy_count(verification.get(key))
            for key in (
                "unresolved",
                "global_unresolved",
                "retry_wait",
                "terminal_failures",
                "global_terminal_failures",
                "active_leases",
                "expired_leases",
                "unfinished_rows",
            )
        )

    def _render_activity_panel(
        self,
        title: str,
        message: str,
        motion: str,
        status: str,
        width: int,
    ):
        body = Group(
            self._centered(self._motion(motion, width), width, self._status_style(status)),
            self._centered(message, width, self._status_style(status)),
        )
        return self._panel(title, body, status, active=True)

    def _phase_rail(self, width: int):
        lines: list[Text] = []
        if width < 80:
            for phase in _CYCLE_PHASES:
                lines.append(self._phase_text(phase))
            return Group(*lines)

        groups = [_CYCLE_PHASES] if width >= 120 else (_CYCLE_PHASES[:4], _CYCLE_PHASES[4:])
        for group in groups:
            line = Text()
            for index, phase in enumerate(group):
                if index:
                    line.append(" ─ ", style="dim")
                line.append_text(self._phase_text(phase))
            lines.append(line)
        return Group(*lines)

    def _phase_text(self, phase: str) -> Text:
        state = self.state.phase_states.get(phase, "PENDING")
        now = self._clock()
        if self.state.handoff_phase == phase and now < self.state.handoff_until:
            marker, style = "◆", "bold cyan"
        elif state == "COMPLETE":
            marker, style = "✓", "green"
        elif state == "FAILED":
            marker, style = "×", "bold red"
        elif state == "RUNNING":
            marker, style = self._motion("orbit"), "bold cyan"
        else:
            marker, style = "○", "dim"
        return Text(f"{marker} {phase}", style=style)

    def _metric_grid(
        self,
        items: list[tuple[str, Any, str]],
        width: int,
    ) -> Table:
        filtered = [(label, value, key) for label, value, key in items if value is not None and value != ""]
        table = Table.grid(expand=True, padding=(0, 1))
        if width < 80:
            table.add_column(style="dim", no_wrap=True)
            table.add_column(justify="right")
            for label, value, key in filtered:
                table.add_row(label, self._value_text(value, key))
            return table

        table.add_column(style="dim", no_wrap=True)
        table.add_column(justify="right")
        table.add_column(style="dim", no_wrap=True)
        table.add_column(justify="right")
        for offset in range(0, len(filtered), 2):
            left = filtered[offset]
            right = filtered[offset + 1] if offset + 1 < len(filtered) else ("", "", "")
            table.add_row(
                left[0],
                self._value_text(left[1], left[2]),
                right[0],
                self._value_text(right[1], right[2]) if right[0] else "",
            )
        return table

    def _value_text(self, value: Any, key: str) -> Text:
        label = _number(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else str(value)
        changed = self.state.changed_at.get(key)
        if changed is not None and self._clock() - changed < 0.35:
            return Text(label, style="bold bright_cyan")
        normalized = str(value).upper()
        if normalized in {"FAILED", "ERROR"}:
            style = "bold red"
        elif normalized in {"COMPLETE_WITH_FAILURES", "RETRY", "WAITING", "DEGRADED"}:
            style = "bold yellow"
        elif normalized in {"COMPLETE", "READY"}:
            style = "bold green"
        else:
            style = "bold white"
        return Text(label, style=style)

    def _panel(
        self,
        title: str,
        body: Any,
        status: str,
        *,
        active: bool = False,
        context: bool = False,
    ) -> Panel:
        normalized = status.upper()
        title_text = Text()
        if active and normalized not in {
            "FAILED",
            "COMPLETE",
            "READY",
            "COMPLETE_WITH_FAILURES",
            "WAITING",
        }:
            title_text.append(f"{self._motion('orbit')} ", style="cyan")
        title_text.append(title, style="bold white")
        title_text.append("  ·  ", style="dim")
        title_text.append(normalized.replace("_", " "), style=self._status_style(normalized))
        style = self._status_style(normalized)
        if context and normalized in {"ACTIVE", "RUNNING"}:
            style = "dim cyan"
        return Panel(
            body,
            title=title_text,
            title_align="left",
            border_style=style,
            padding=(0, 1),
        )

    def _render_footer(self) -> Text | Group | None:
        status = self.state.status.upper()
        finished = self.state.finished_at
        if status == "STOPPED" and finished is not None:
            first = Text()
            first.append("■ Stopped  ", style="bold yellow")
            first.append(_timestamp(finished), style="white")
            first.append("   ·   Exit 130", style="dim")
            if self.state.command in _MUTATING_COMMANDS:
                second = Text(
                    "Committed checkpoints remain durable. Unfinished work remains resumable.",
                    style="dim",
                )
                return Group(first, second)
            return first
        if status == "FAILED" and finished is not None:
            first = Text()
            first.append("× Failed  ", style="bold red")
            first.append(_timestamp(finished), style="white")
            first.append("   ·   Exit 1", style="dim")
            if self.state.last_error:
                return Group(first, Text(self.state.last_error, style="red"))
            return first
        if status == "COMPLETE_WITH_FAILURES" and finished is not None:
            line = Text()
            line.append("! Completed with failures  ", style="bold yellow")
            line.append(_timestamp(finished), style="white")
            return line
        if finished is not None and status not in {"SNAPSHOT"}:
            line = Text()
            line.append("✓ Completed  ", style="bold green")
            line.append(_timestamp(finished), style="white")
            return line
        if self.state.command in _LONG_RUNNING_COMMANDS:
            if self.state.command == "cycle-loop" and status == "WAITING":
                return Text("Ctrl+C  Stop loop", style="dim")
            return Text("Ctrl+C  Stop safely", style="dim")
        return None

    def _active_status(self) -> str:
        if self.state.status in {"FAILED", "STOPPED", "STOPPING", "WAITING"}:
            return self.state.status
        if self._transient("retry") or self._transient("warning"):
            return "DEGRADED"
        return "RUNNING"

    def _status_style(self, status: str) -> str:
        normalized = status.upper().replace(" ", "_")
        if normalized in {"FAILED", "ERROR"}:
            return "bold red"
        if normalized in {
            "WAITING",
            "RETRY",
            "RETRY_WAIT",
            "PARTIAL",
            "DEGRADED",
            "STOPPING",
            "COMPLETE_WITH_FAILURES",
        }:
            return "bold yellow"
        if normalized in {"READY", "COMPLETE"}:
            return "bold green"
        if normalized in {
            "RUNNING",
            "ACTIVE",
            "CHECKING",
            "APPLYING",
            "READING",
            "DISCOVERING",
            "PREPARING",
            "COLLECTING",
            "VERIFYING",
            "FINALIZING",
        }:
            return "cyan"
        if normalized in {
            "IDLE", "N/A", "PENDING", "SKIPPED", "STOPPED", "SNAPSHOT",
            "NOT_DUE", "ALREADY_COMPLETED",
        }:
            return "dim"
        return "white"

    def _transient(self, kind: str) -> bool:
        return self.state.transient_kind == kind and self._clock() < self.state.transient_until

    def _motion(self, kind: str, width: int = 120) -> str:
        now = self._clock()
        if self._transient("settle"):
            if kind == "sweep":
                return "━" * max(18, min(44, width - 12))
            if kind == "stream":
                return "━" * max(20, min(48, width - 12))
            return "◆"

        speed = 0.35 if self.state.status == "STOPPING" else 1.0
        if kind == "orbit":
            frames = ("◐", "◓", "◑", "◒")
            return frames[int(now * 8 * speed) % len(frames)]
        if kind == "radar":
            frames = ("◌", "◔", "◑", "●", "◑", "◔")
            return frames[int(now * 6 * speed) % len(frames)]
        if kind == "wait":
            frames = ("◷", "◶", "◵", "◴")
            return frames[int(now * 3) % len(frames)]
        if kind == "stitch":
            frames = (
                "· › · › · › · › ·",
                "  · › · › · › · ›",
                "› · › · › · › ·  ",
            )
            return frames[int(now * 5 * speed) % len(frames)]
        if kind == "seal":
            frames = (
                ">         <",
                "  >     <  ",
                "    > <    ",
                "     ◆     ",
                "    > <    ",
                "  >     <  ",
            )
            return frames[int(now * 6 * speed) % len(frames)]
        if kind == "sweep":
            length = max(18, min(44, width - 12))
            cycle = max(2, length * 2 - 2)
            position = int(now * 8 * speed) % cycle
            if position >= length:
                position = cycle - position
            chars = ["━"] * length
            chars[position] = "╺"
            return "".join(chars)
        if kind == "stream":
            length = max(20, min(48, width - 12))
            chars = [" "] * length
            offset = int(now * 7 * speed) % 10
            for pos in range(offset, length, 10):
                pulse = "╺━━"
                for index, char in enumerate(pulse):
                    if pos + index < length:
                        chars[pos + index] = char
            return "".join(chars).rstrip()
        return ""

    def _wait_rail(self, elapsed: float, total: float, width: int) -> str:
        length = max(18, min(44, width - 12))
        if total <= 0:
            return "━" * length
        ratio = max(0.0, min(1.0, elapsed / total))
        filled = int(ratio * length)
        if filled >= length:
            return "━" * length
        if filled <= 0:
            return "╺" + "━" * (length - 1)
        return "━" * filled + "╺" + "━" * max(0, length - filled - 1)

    def _centered(self, value: str, width: int, style: str) -> Text:
        text = Text(value, style=style)
        text.justify = "center"
        return text

    def _safe_refresh(self) -> None:
        if not self.interactive or self._live is None or self._renderer_failed:
            return
        try:
            self._live.refresh()
        except Exception:
            self._renderer_failed = True

    def _stop_live(self) -> None:
        live = self._live
        self._live = None
        if live is None:
            return
        try:
            live.stop()
        except Exception:
            self._renderer_failed = True

    def _plain_start(self) -> None:
        started = self.state.started_at or self._wall_clock()
        label = _COMMAND_LABELS.get(self.state.command, self.state.command.upper())
        self._plain_write(f"{self.state.app} / {label} / {self.state.backend.upper()}")
        self._plain_write(f"Started: {_timestamp(started)}")

    def _plain_event(self, name: str, payload: Mapping[str, Any]) -> None:
        stamp = _clock_time(self._wall_clock())
        if name in {"phase_started", "phase_completed"}:
            phase = str(payload.get("phase") or "WORK").upper()
            if (
                self.state.command in {"cycle-run", "cycle-loop"}
                and phase not in _CYCLE_PHASES
            ):
                return
            generation = (
                int(payload["generation"])
                if payload.get("generation") is not None
                else None
            )
            signature = (name, phase, generation)
            if self.state.last_plain_milestone == signature:
                return
            self.state.last_plain_milestone = signature
        if name == "phase_started":
            suffix = (
                f" generation={payload.get('generation')}"
                if payload.get("generation") is not None
                else ""
            )
            self._plain_write(f"{stamp} {phase} started{suffix}")
            return
        if name == "phase_completed":
            details = []
            if payload.get("generation") is not None:
                details.append(f"generation={payload['generation']}")
            if phase == "DISCOVER":
                for label, key in (
                    ("partitions", "partitions"),
                    ("pages", "pages"),
                    ("companies", "companies"),
                    ("requests", "requests"),
                    ("invalid", "invalid_rows"),
                ):
                    if key in payload:
                        details.append(f"{label}={payload[key]}")
            suffix = f" {' '.join(details)}" if details else ""
            self._plain_write(f"{stamp} {phase} complete{suffix}")
            return
        if name in {"discovery_progress","branch_progress"}:
            now = self._clock()
            if now - self.state.last_plain_progress_at >= 15.0:
                self.state.last_plain_progress_at = now
                if name == "discovery_progress":
                    self._plain_write(
                        f"{stamp} DISCOVER stage={payload.get('stage','RUNNING')} "
                        f"bucket={payload.get('work_id','--')} "
                        f"pages={payload.get('pages_done','--')}/{payload.get('pages_total','--')} "
                        f"429={payload.get('rate_limit_hits','--')} "
                        f"cooldown={payload.get('cooldown_seconds','--')}"
                    )
                else:
                    self._plain_write(
                        f"{stamp} BRANCH checked={payload.get('checked',0)} "
                        f"verified={payload.get('verified',0)} "
                        f"deferred={payload.get('deferred',0)}"
                    )
            return
        if name == "cycle_waiting":
            seconds = float(
                payload.get("interval_seconds") or payload.get("idle_seconds") or 0
            )
            target = "next_check" if self.state.command == "collect" else "next_cycle"
            self._plain_write(f"{stamp} WAITING {target}={_duration(seconds)}")
            return
        if name == "warning":
            message = payload.get("message") or payload.get("error") or "warning"
            self._plain_write(f"{stamp} WARNING {message}")
            return
        if name == "cycle_completed":
            self._plain_write(
                f"{stamp} CYCLE {str(payload.get('status') or 'COMPLETE').upper()} "
                f"generation={payload.get('generation')}"
            )

    def _plain_finish(self, payload: Mapping[str, Any], status: str) -> None:
        result_line = self._plain_result(payload, status)
        if result_line:
            self._plain_write(result_line)
        finished = self.state.finished_at or self._wall_clock()
        if status == "FAILED":
            self._plain_write(f"Failed: {_timestamp(finished)} · Exit 1")
        elif status == "COMPLETE_WITH_FAILURES":
            self._plain_write(f"Completed with failures: {_timestamp(finished)}")
        else:
            self._plain_write(f"Completed: {_timestamp(finished)}")
        elapsed = (self.state.finished_monotonic or self._clock()) - self.state.started_monotonic
        self._plain_write(f"Duration: {_duration(elapsed)}")

    def _plain_result(self, payload: Mapping[str, Any], status: str) -> str:
        command = self.state.command
        if command == "check-db":
            return (
                f"Result: schema={payload.get('schema')} "
                f"managed_tables={payload.get('managed_tables')}"
            )
        if command == "migrate":
            created = payload.get("created")
            altered = payload.get("altered")
            return (
                f"Result: status={status} managed_tables={payload.get('managed_tables')} "
                f"created={len(created) if isinstance(created, list) else created} "
                f"altered={len(altered) if isinstance(altered, list) else altered}"
            )
        if command == "branch":
            return (
                f"Result: checked={payload.get('checked')} verified={payload.get('verified')} "
                f"branches={payload.get('branches')} deferred={payload.get('deferred')}"
            )
        if command == "discover":
            return (
                f"Result: generation={payload.get('generation')} "
                f"partitions={payload.get('partitions')} pages={payload.get('pages')} "
                f"companies={payload.get('companies')} invalid={payload.get('invalid_rows')}"
            )
        if command == "fetch-company":
            return (
                f"Result: status={payload.get('status')} generation={payload.get('generation')} "
                f"company_id={payload.get('company_id')}"
            )
        if command in {"cycle-run", "cycle-loop"}:
            return (
                f"Result: status={payload.get('status')} generation={payload.get('generation')} "
                f"finalized_rows={payload.get('finalized_rows')}"
            )
        if command == "prepare":
            return (
                f"Result: generation={payload.get('generation')} "
                f"rows_prepared={payload.get('rows_prepared')}"
            )
        if command == "retry":
            return (
                f"Result: generation={payload.get('generation')} "
                f"reopened={payload.get('reopened')}"
            )
        if command == "collect":
            return (
                f"Result: finished={payload.get('companies_finished')} "
                f"endpoint_complete={payload.get('endpoints_done')} "
                f"endpoint_na={payload.get('endpoints_not_available')} "
                f"endpoint_failed={payload.get('endpoints_failed')} "
                f"http_requests={payload.get('http_requests')}"
            )
        if command in {"status", "cycle-status"}:
            summary = dict(payload.get("summary") or {})
            return (
                f"Result: generation={payload.get('generation')} total={summary.get('total')} "
                f"complete={summary.get('complete')} unresolved={summary.get('unresolved')}"
            )
        return f"Result: status={status}"

    def _plain_failure(self, message: str, *, exit_code: int) -> None:
        finished = self.state.finished_at or self._wall_clock()
        self._plain_write(f"Failed: {_timestamp(finished)} · Exit {exit_code}")
        self._plain_write(str(message))
        elapsed = (self.state.finished_monotonic or self._clock()) - self.state.started_monotonic
        self._plain_write(f"Duration: {_duration(elapsed)}")

    def _plain_stopped(self) -> None:
        finished = self.state.finished_at or self._wall_clock()
        self._plain_write(f"Stopped: {_timestamp(finished)} · Exit 130")
        if self.state.command in _MUTATING_COMMANDS:
            self._plain_write(
                "Committed checkpoints remain durable. Unfinished work remains resumable."
            )
        elapsed = (self.state.finished_monotonic or self._clock()) - self.state.started_monotonic
        self._plain_write(f"Duration: {_duration(elapsed)}")

    def _plain_write(self, message: str) -> None:
        try:
            self.stream.write(str(message) + "\n")
            self.stream.flush()
        except Exception:
            self._renderer_failed = True
