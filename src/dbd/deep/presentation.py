from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Mapping, TextIO

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ._version import __version__


def _number(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return str(value)


def _limit(value: Any) -> str:
    return "∞" if value is None else _number(value)


def _duration(seconds: float | int | None) -> str:
    total = max(0, int(float(seconds or 0)))
    return f"{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}"


def _styled(value: Any, style: str | None) -> Text:
    return Text(str(value), style=style)


def _state(value: Any) -> Text:
    label = str(value)
    normalized = label.upper().replace("_", " ")
    if any(token in normalized for token in ("FAILED", "ERROR", "MISSING")):
        style = "bold red"
    elif any(
        token in normalized
        for token in ("DEGRADED", "WAITING", "RETRY WAIT", "NOT INITIALIZED", "INTERRUPTED")
    ):
        style = "bold yellow"
    elif any(token in normalized for token in ("COMPLETE", "READY", "HEALTHY")):
        style = "bold green"
    elif normalized == "IDLE":
        style = "dim"
    elif any(
        token in normalized
        for token in (
            "RUNNING",
            "ACTIVE",
            "STARTING",
            "PREFLIGHT",
            "PREPARE",
            "AUTH",
            "RECOVER",
            "STAGE MASTER",
        )
    ):
        style = "cyan"
    else:
        style = None
    return _styled(label, style)


def _priority_count(value: Any, positive_style: str, *, zero_style: str = "dim") -> Text:
    label = _number(value)
    try:
        active = int(value or 0) > 0
    except (TypeError, ValueError):
        active = bool(value)
    return _styled(label, positive_style if active else zero_style)


def _grid(rows: list[tuple[str, Any, str, Any]]) -> Table:
    table = Table.grid(expand=True, padding=(0, 1))
    table.add_column(style="dim", no_wrap=True)
    table.add_column(justify="right")
    table.add_column(style="dim", no_wrap=True)
    table.add_column(justify="right")
    for left_key, left_value, right_key, right_value in rows:
        table.add_row(
            left_key,
            left_value if isinstance(left_value, Text) else str(left_value),
            right_key,
            right_value if isinstance(right_value, Text) else str(right_value),
        )
    return table


@dataclass
class DeepTerminalEvents:
    """Presentation-only event sink for standalone Deep commands."""

    command: str
    stream: TextIO = sys.stdout

    def __post_init__(self) -> None:
        self.console = Console(file=self.stream, soft_wrap=False)
        isatty = getattr(self.stream, "isatty", None)
        self.interactive = bool(isatty and isatty())
        self.started_at = time.monotonic()
        self.step_started_at = self.started_at
        self.last_commit_at: float | None = None
        self.last_plain_progress = 0.0
        self.last_live_update = 0.0
        self.live: Live | None = None
        self.state: dict[str, Any] = {
            "phase": "STARTING",
            "rows_prepared": 0,
            # Legacy event keys remain accepted so retained compatibility tools
            # do not crash; the v0.6 production renderer no longer displays them.
            "prepare_step": "-",
            "legacy_scope": "CHECKING",
            "legacy_action": "-",
            "batch": 0,
            "batch_rows": 0,
            "batches_committed": 0,
            "master_staged": 0,
            "rows_per_second": 0.0,
            "batch_elapsed_seconds": 0.0,
            "batch_rows_per_second": 0.0,
            "rows_total": 0,
            "rows_scanned": 0,
            "rows_changed": 0,
            "leases_recovered": 0,
            "generation": "-",
            "scope": "-",
            "companies_claimed": 0,
            "companies_finished": 0,
            "companies_in_flight": 0,
            "companies_retry_wait": 0,
            "endpoints_done": 0,
            "endpoints_not_available": 0,
            "endpoints_failed": 0,
            "concurrency": "-",
            "rate_per_second": "-",
            "company_limit": None,
            "continuous": False,
            "idle_seconds": None,
        }

    def __call__(self, name: str, payload: dict[str, Any]) -> None:
        now = time.monotonic()

        if name in {"phase", "phase_started"}:
            self.state["phase"] = payload.get("phase", self.state["phase"])
            self.state.update(
                {
                    key: value
                    for key, value in payload.items()
                    if key in {"generation", "scope"}
                }
            )
        elif name == "phase_completed":
            self.state.update(payload)
        elif name == "prepare_scope":
            self.state.update(payload)
        elif name == "prepare_step":
            self.state["prepare_step"] = payload.get(
                "step",
                self.state["prepare_step"],
            )
            self.state["batch"] = payload.get("batch", self.state["batch"])
            self.state["batch_rows"] = payload.get(
                "batch_rows",
                self.state["batch_rows"],
            )
            self.step_started_at = now
        elif name == "prepare_stage":
            self.state.update(payload)
            self.state["batches_committed"] = payload.get(
                "batch",
                self.state["batches_committed"],
            )
            self.state["prepare_step"] = "COMMITTED"
            self.last_commit_at = now
            self.step_started_at = now
        elif name == "prepare_ready":
            self.state.update(payload)
            self.state["phase"] = "READY"
            self.state["prepare_step"] = "READY"
            self.step_started_at = now
        elif name in {"started", "progress", "prepare_progress"}:
            self.state.update(payload)
            if name == "started":
                self.state["phase"] = "RUNNING"
        elif name in {"idle", "cycle_waiting"}:
            self.state.update(payload)
            self.state["phase"] = "IDLE"
        elif name in {
            "retry_wait",
            "worker_error",
            "endpoint_retry",
            "endpoint_failed",
            "warning",
        }:
            self.state["last_event"] = name.upper()
            self.state["last_error"] = payload.get("error") or payload.get("message")
            self.state["last_endpoint"] = payload.get("endpoint")
        elif name == "heartbeat":
            pass

        if self.interactive:
            if self.live is None or now - self.last_live_update >= 0.5:
                self._refresh_live()
                self.last_live_update = now
        else:
            self._plain_event(name, payload)

    def _plain_event(self, name: str, payload: Mapping[str, Any]) -> None:
        if name in {"phase", "phase_started"}:
            generation = payload.get("generation")
            suffix = f" generation={generation}" if generation is not None else ""
            self.console.print(f"PHASE {payload.get('phase')}{suffix}")
            return
        if name == "phase_completed":
            return
        if name == "prepare_stage":
            self.console.print(
                "STAGED "
                f"batch={_number(payload.get('batch'))} "
                f"rows={_number(payload.get('batch_rows'))} "
                f"total={_number(payload.get('master_staged'))} "
                f"last={float(payload.get('batch_rows_per_second') or 0):,.0f} rows/s "
                f"avg={float(payload.get('rows_per_second') or 0):,.0f} rows/s"
            )
            return
        if name == "prepare_ready":
            self.console.print(
                "READY "
                f"staged={_number(payload.get('master_staged'))} "
                f"batches={_number(payload.get('batch'))}"
            )
            return
        if name in {"prepare_scope", "prepare_step", "heartbeat"}:
            return
        if name in {"idle", "cycle_waiting"}:
            delay = payload.get("idle_seconds", payload.get("interval_seconds", 0))
            self.console.print(f"IDLE next_check={float(delay or 0):g}s")
            return
        if name in {"retry_wait", "worker_error", "endpoint_retry", "endpoint_failed", "warning"}:
            endpoint = payload.get("endpoint") or "-"
            self.console.print(
                f"{name.upper()} endpoint={endpoint} error={payload.get('error')}"
            )
            return

        now = time.monotonic()
        if name not in {"progress", "prepare_progress"} or now - self.last_plain_progress < 2.0:
            return
        self.last_plain_progress = now
        if name == "prepare_progress":
            self.console.print(
                "LEGACY "
                f"{_number(payload.get('rows_scanned'))}/"
                f"{_number(payload.get('rows_total'))} "
                f"changed={_number(payload.get('rows_changed'))}"
            )
        else:
            self.console.print(
                "DEEP "
                f"claimed={_number(payload.get('companies_claimed'))} "
                f"finished={_number(payload.get('companies_finished'))} "
                f"in_flight={_number(payload.get('companies_in_flight'))} "
                f"retry={_number(payload.get('companies_retry_wait'))}"
            )

    def _refresh_live(self) -> None:
        renderable = self._render()
        if self.live is None:
            self.live = Live(
                renderable,
                console=self.console,
                refresh_per_second=2,
                transient=False,
            )
            self.live.start()
        else:
            self.live.update(renderable, refresh=False)

    def _render(self):
        if self.command == "prepare":
            elapsed = time.monotonic() - self.started_at
            return Panel(
                _grid(
                    [
                        (
                            "Phase",
                            _state(self.state["phase"]),
                            "Elapsed",
                            _styled(_duration(elapsed), "dim"),
                        ),
                        (
                            "Generation",
                            _styled(self.state.get("generation", "-"), "bold white"),
                            "Scope",
                            _styled(str(self.state.get("scope") or "-").upper(), "cyan"),
                        ),
                        (
                            "Prepared",
                            _priority_count(self.state.get("rows_prepared"), "green"),
                            "",
                            "",
                        ),
                    ]
                ),
                title=f"DBD DEEP PREPARE v{__version__}",
            )

        elapsed = int(time.monotonic() - self.started_at)
        header = Text()
        header.append(f"DBD DEEP v{__version__}", style="bold white")
        header.append(" • ", style="dim")
        header.append_text(_state(self.state["phase"]))
        header.append(" • ", style="dim")
        header.append(
            f"{elapsed // 3600:02d}:{elapsed % 3600 // 60:02d}:{elapsed % 60:02d}",
            style="dim",
        )

        collector = Panel(
            _grid(
                [
                    (
                        "Concurrency",
                        _styled(self.state.get("concurrency", "-"), "cyan"),
                        "Rate",
                        _styled(f"{self.state.get('rate_per_second', '-')} req/s", "cyan"),
                    ),
                    (
                        "Limit",
                        _styled(_limit(self.state.get("company_limit")), "bold white"),
                        "Mode",
                        _styled(
                            "CONTINUOUS" if self.state.get("continuous") else "DRAIN",
                            "cyan",
                        ),
                    ),
                    (
                        "Claimed",
                        _priority_count(self.state.get("companies_claimed"), "blue"),
                        "Finished",
                        _priority_count(self.state.get("companies_finished"), "green"),
                    ),
                    (
                        "In flight",
                        _priority_count(self.state.get("companies_in_flight"), "cyan"),
                        "Retry wait",
                        _priority_count(self.state.get("companies_retry_wait"), "yellow"),
                    ),
                ]
            ),
            title="Collector",
        )

        session = Panel(
            _grid(
                [
                    (
                        "Endpoint done",
                        _priority_count(self.state.get("endpoints_done"), "green"),
                        "N/A",
                        _priority_count(self.state.get("endpoints_not_available"), "dim"),
                    ),
                    (
                        "Endpoint failed",
                        _priority_count(self.state.get("endpoints_failed"), "bold red"),
                        "Recovered leases",
                        _priority_count(self.state.get("leases_recovered"), "cyan"),
                    ),
                ]
            ),
            title="Session",
        )

        footer = Text()
        if self.state["phase"] == "IDLE":
            footer.append("IDLE", style="dim")
            footer.append(" • waiting for SQL Server claimable work", style="dim")
        else:
            footer.append("ACTIVE", style="cyan")
            footer.append(" • direct DBD_COMPANY claim", style="dim")
        footer.append(" • Ctrl+C graceful shutdown", style="dim")
        return Group(Panel(header, title="DBD DEEP"), collector, session, footer)

    def close(self) -> None:
        if self.live is not None:
            self.live.stop()
            self.live = None


@dataclass
class DeepPresenter:
    """Static human/JSON presentation for dbd-deep command results."""

    json_mode: bool = False
    stream: TextIO = sys.stdout

    def __post_init__(self) -> None:
        self.console = Console(file=self.stream, soft_wrap=False)

    def json(self, payload: Any) -> None:
        self.stream.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
        self.stream.flush()

    def status(self, payload: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(payload))
            return
        summary = payload["summary"]
        generation_state = payload.get("generation_state") or {}
        self.console.print(
            Panel(
                _grid(
                    [
                        (
                            "Generation",
                            _styled(payload.get("generation", "-"), "bold white"),
                            "Phase",
                            _styled(str(payload.get("phase") or "-").upper(), "cyan"),
                        ),
                        (
                            "Finalized",
                            _styled(generation_state.get("finalized_generation", "-"), "dim"),
                            "Active",
                            _styled(generation_state.get("active_generation", "-"), "cyan"),
                        ),
                        (
                            "Total",
                            _styled(_number(summary.get("total")), "bold white"),
                            "Pending",
                            _priority_count(summary.get("pending"), "yellow"),
                        ),
                        (
                            "Collecting",
                            _priority_count(summary.get("collecting"), "cyan"),
                            "Retry wait",
                            _priority_count(summary.get("retry_wait"), "yellow"),
                        ),
                        (
                            "Complete",
                            _priority_count(summary.get("complete"), "green"),
                            "Partial",
                            _priority_count(summary.get("partial"), "yellow"),
                        ),
                        (
                            "Failed",
                            _priority_count(summary.get("failed"), "bold red"),
                            "Unresolved",
                            _priority_count(summary.get("unresolved"), "yellow"),
                        ),
                        (
                            "Active leases",
                            _priority_count(summary.get("active_leases"), "cyan"),
                            "Expired leases",
                            _priority_count(summary.get("expired_leases"), "bold red"),
                        ),
                    ]
                ),
                title=f"DBD DEEP STATUS v{__version__}",
            )
        )
        coverage = payload.get("coverage")
        if coverage:
            self.console.print(
                Panel(
                    _grid(
                        [
                            ("Companies", _number(coverage.get("companies")), "Relations", _number(coverage.get("relations"))),
                            ("Events", _number(coverage.get("events")), "Financials", _number(coverage.get("financials"))),
                            ("Industries", _number(coverage.get("industries")), "", ""),
                        ]
                    ),
                    title="Five-table coverage",
                )
            )

    def prepare(self, result: Mapping[str, Any], status: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json({"prepare": dict(result), "status": dict(status)})
            return
        summary = status["summary"]
        self.console.print(
            Panel(
                _grid(
                    [
                        (
                            "Generation",
                            _styled(result.get("generation", "-"), "bold white"),
                            "Phase",
                            _styled(str(result.get("phase") or "-").upper(), "cyan"),
                        ),
                        (
                            "Prepared",
                            _priority_count(result.get("rows_prepared"), "green"),
                            "Pending",
                            _priority_count(summary.get("pending"), "yellow"),
                        ),
                        (
                            "Complete",
                            _priority_count(summary.get("complete"), "green"),
                            "Unresolved",
                            _priority_count(summary.get("unresolved"), "yellow"),
                        ),
                    ]
                ),
                title=f"DBD DEEP PREPARE v{__version__}",
            )
        )

    def retry(self, result: Mapping[str, Any], status: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json({"retry": dict(result), "status": dict(status)})
            return
        summary = status["summary"]
        self.console.print(
            Panel(
                _grid(
                    [
                        (
                            "Generation",
                            _styled(result.get("generation", "-"), "bold white"),
                            "Phase",
                            _styled(str(result.get("phase") or "-").upper(), "cyan"),
                        ),
                        (
                            "Reopened",
                            _priority_count(result.get("reopened"), "yellow"),
                            "Unresolved",
                            _priority_count(summary.get("unresolved"), "yellow"),
                        ),
                        (
                            "Failed",
                            _priority_count(summary.get("failed"), "bold red"),
                            "Retry wait",
                            _priority_count(summary.get("retry_wait"), "yellow"),
                        ),
                    ]
                ),
                title=f"DBD DEEP RETRY v{__version__}",
            )
        )

    def collect(self, result: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(result))
            return
        self.console.print(
            Panel(
                _grid(
                    [
                        (
                            "Generation",
                            _styled(result.get("generation", "-"), "bold white"),
                            "Phase",
                            _styled(str(result.get("phase") or "-").upper(), "cyan"),
                        ),
                        (
                            "Claimed",
                            _priority_count(result.get("companies_claimed"), "cyan"),
                            "Finished",
                            _priority_count(result.get("companies_finished"), "green"),
                        ),
                        (
                            "In flight",
                            _priority_count(result.get("companies_in_flight"), "cyan"),
                            "Retry wait",
                            _priority_count(result.get("companies_retry_wait"), "yellow"),
                        ),
                        (
                            "Endpoint done",
                            _priority_count(result.get("endpoints_done"), "green"),
                            "N/A",
                            _styled(_number(result.get("endpoints_not_available")), "dim"),
                        ),
                        (
                            "Endpoint failed",
                            _priority_count(result.get("endpoints_failed"), "bold red"),
                            "Recovered leases",
                            _priority_count(result.get("leases_recovered"), "cyan"),
                        ),
                        (
                            "HTTP requests",
                            _priority_count(result.get("http_requests"), "cyan"),
                            "HTTP retries",
                            _priority_count(result.get("http_retries"), "yellow"),
                        ),
                        (
                            "429",
                            _priority_count(result.get("rate_limit_hits"), "yellow"),
                            "Configured rate",
                            _styled(f"{result.get('configured_rate', '-')} req/s", "cyan"),
                        ),
                    ]
                ),
                title=f"DBD DEEP COLLECT COMPLETE v{__version__}",
            )
        )
