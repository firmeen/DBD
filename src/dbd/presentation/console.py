from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence, TextIO

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _pct(current: int, total: int) -> float:
    return (100.0 * current / total) if total > 0 else 0.0


def _rate(value: Any) -> str:
    number = _float(value)
    return "--" if number is None else f"{number:,.2f}/s"


def _age(value: Any) -> str:
    number = _float(value)
    return "--" if number is None else f"{number:.1f}s"


def _bool(value: Any) -> str:
    return "YES" if bool(value) else "NO"


def _status_style(value: str) -> str:
    value = value.lower()
    if value in {"healthy", "running", "ready", "finished", "success", "current", "active"}:
        return "bold green"
    if value in {"failed", "error", "degraded", "stale", "lost"}:
        return "bold red"
    if value in {"draining", "retry_wait", "warning", "building", "paused"}:
        return "bold yellow"
    return "bold cyan"


@dataclass
class TerminalPresenter:
    """Single human/machine presentation boundary for CLI output.

    Runtime, repositories and Redis continue to return structured data. This layer
    decides how operators see it. JSON remains available explicitly for scripting.
    """

    json_mode: bool = False
    verbose: bool = False
    stream: TextIO = sys.stdout

    def __post_init__(self) -> None:
        self.console = Console(file=self.stream, soft_wrap=False)

    def json(self, payload: Any) -> None:
        self.stream.write(json.dumps(payload, indent=2, default=str, ensure_ascii=False) + "\n")
        self.stream.flush()

    def message(self, message: str, *, kind: str = "info") -> None:
        if self.json_mode:
            self.json({"status": kind, "message": message})
            return
        style = {
            "success": "bold green",
            "warning": "bold yellow",
            "error": "bold red",
            "info": "cyan",
        }.get(kind, "")
        prefix = {
            "success": "OK",
            "warning": "WARN",
            "error": "ERROR",
            "info": "INFO",
        }.get(kind, "INFO")
        self.console.print(f"[{style}]{prefix}[/]  {message}")

    def key_values(
        self,
        title: str,
        rows: Iterable[tuple[str, Any]],
        *,
        subtitle: str | None = None,
    ) -> None:
        if self.json_mode:
            self.json({key: value for key, value in rows})
            return
        table = Table.grid(padding=(0, 2))
        table.add_column(style="dim", no_wrap=True)
        table.add_column()
        for key, value in rows:
            table.add_row(str(key).upper(), str(value))
        self.console.print(
            Panel(
                table,
                title=title,
                subtitle=subtitle,
                border_style="cyan",
                padding=(1, 2),
            )
        )

    def table(
        self,
        title: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
        *,
        caption: str | None = None,
    ) -> None:
        if self.json_mode:
            self.json([dict(zip(columns, row, strict=False)) for row in rows])
            return
        table = Table(title=title, box=box.SIMPLE_HEAVY, expand=False)
        for column in columns:
            table.add_column(column)
        for row in rows:
            table.add_row(*(str(value) for value in row))
        if caption:
            table.caption = caption
        self.console.print(table)

    def check_db(self, info: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(info))
            return
        self.key_values(
            "DBD · DATABASE CONNECTION",
            (
                ("status", "CONNECTED"),
                ("database", info.get("db", "--")),
                ("user", info.get("user", "--")),
                ("mysql", info.get("version", "--")),
            ),
        )

    def master_status(self, data: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(data))
            return
        total = _int(data.get("total"))
        eligible = _int(data.get("eligible_all_statuses"))
        operating = _int(data.get("eligible_operating"))
        conflict = _int(data.get("conflicts"))
        unknown = _int(data.get("unknown_status"))
        table = Table.grid(padding=(0, 3))
        table.add_column(style="dim", no_wrap=True)
        table.add_column(justify="right")
        table.add_row("TOTAL", f"{total:,}")
        table.add_row("ELIGIBLE", f"{eligible:,}")
        table.add_row("OPERATING", f"{operating:,}")
        table.add_row("CONFLICT", f"{conflict:,}")
        table.add_row("UNKNOWN STATUS", f"{unknown:,}")
        progress = ProgressBar(total=max(1, total), completed=min(total, eligible), width=48)
        self.console.print(
            Panel(
                Group(
                    Text("Company master registry", style="bold"),
                    Text(""),
                    progress,
                    Text(f"{_pct(eligible, total):.2f}% eligible for scheduling"),
                    Text(""),
                    table,
                ),
                title="DBD · MASTER STATUS",
                border_style="cyan",
                padding=(1, 2),
            )
        )

    def campaign_status(self, state: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(state))
            return

        total = _int(state.get("total"))
        done = _int(state.get("done"))
        na = _int(state.get("not_available"))
        failed = _int(state.get("failed"))
        terminal = min(total, done + na + failed)
        remaining = max(0, total - terminal)
        source = str(state.get("progress_source") or "unknown")
        exact = bool(state.get("progress_exact"))
        run_id = _int(state.get("run_id"))
        run_status = str(state.get("run_status") or "unknown").upper()

        header = Text()
        header.append(f"RUN {run_id}  ")
        header.append(run_status, style=_status_style(run_status))

        source_text = (
            "EXACT · MySQL reconciliation"
            if exact
            else "LIVE · Redis worker heartbeat"
        )
        progress = ProgressBar(total=max(1, total), completed=terminal, width=52)

        counts = Table.grid(padding=(0, 3))
        counts.add_column(style="dim", no_wrap=True)
        counts.add_column(justify="right")
        counts.add_column(style="dim", no_wrap=True)
        counts.add_column(justify="right")
        counts.add_row("DONE", f"{done:,}", "N/A", f"{na:,}")
        counts.add_row("FAILED RESOURCES", f"{failed:,}", "REMAINING", f"{remaining:,}")
        counts.add_row("RUNNING", f"{_int(state.get('running')):,}", "PENDING", f"{_int(state.get('pending')):,}")

        workers = state.get("worker_states") or {}
        metrics = state.get("worker_metrics") or {}
        supervisor = state.get("supervisor") or {}

        fleet = Table.grid(padding=(0, 2))
        fleet.add_column(style="dim")
        fleet.add_column()
        fleet.add_row(
            "WORKERS",
            f"{_int(workers.get('active'))} active · {_int(workers.get('draining'))} draining · "
            f"{_int(workers.get('stale'))} stale · {_int(workers.get('unknown'))} unknown",
        )
        fleet.add_row(
            "CAPACITY",
            f"target {_int(supervisor.get('workers_target', state.get('workers')))} / "
            f"max {_int(supervisor.get('effective_worker_max', supervisor.get('workers_max')))} · "
            f"restarts {_int(supervisor.get('restarts'))} · lost {_int(supervisor.get('exhausted_capacity'))}",
        )
        fleet.add_row(
            "HTTP",
            f"observed {_rate(supervisor.get('http_rate_observed'))} · "
            f"effective {_rate(state.get('effective_rate'))} · "
            f"target {_rate(state.get('rate_target'))} · "
            f"retries {_int(metrics.get('http_retries')):,} · "
            f"429/503 {_int(metrics.get('rate_limit_hits')):,}",
        )
        fleet.add_row(
            "PIPELINE",
            f"fetch q {_int(metrics.get('fetch_queue')):,} / active {_int(metrics.get('fetch_active')):,} · "
            f"archive q {_int(metrics.get('archive_queue')):,} / active {_int(metrics.get('archive_active')):,} · "
            f"project q {_int(metrics.get('project_queue')):,} / active {_int(metrics.get('project_active')):,}",
        )
        redis_per_http = (
            _int(metrics.get("redis_commands")) / max(1, _int(metrics.get("requests")))
        )
        fleet.add_row(
            "REDIS",
            f"pool {_int(metrics.get('redis_pool_in_use'))}/{_int(metrics.get('redis_pool_max'))} · "
            f"cmd/http {redis_per_http:.2f} · "
            f"supervisor {'ACTIVE' if state.get('supervisor_active') else 'OFFLINE'}",
        )

        alerts: list[str] = []
        lost = _int(supervisor.get("exhausted_capacity"))
        if lost:
            alerts.append(f"Fleet lost {lost} worker-capacity slot after restart exhaustion.")
        if _int(metrics.get("http_retries")) > 0 and _int(metrics.get("rate_limit_hits")) == 0:
            alerts.append("HTTP retries exist without 429/503 hits; inspect transport/auth/5xx diagnostics.")
        diagnostics = state.get("failure_diagnostics") or {}
        endpoint_failures = list(diagnostics.get("by_endpoint") or [])
        if endpoint_failures:
            top = endpoint_failures[0]
            alerts.append(
                "Largest durable failure concentration: "
                f"{top.get('endpoint') or 'unknown endpoint'} · "
                f"{_int(top.get('resources')):,} resource(s) across "
                f"{_int(top.get('states')):,} state(s)."
            )

        body: list[Any] = [
            header,
            Text(source_text, style="dim"),
            Text(""),
            progress,
            Text(f"{_pct(terminal, total):.2f}% · {terminal:,} / {total:,} terminal resources"),
            Text(""),
            counts,
            Text(""),
            fleet,
        ]
        if alerts:
            body.extend((Text(""), Text("DIAGNOSTICS", style="bold yellow")))
            body.extend(Text(f"• {item}") for item in alerts)

        self.console.print(
            Panel(
                Group(*body),
                title="DBD DATA PLATFORM · CAMPAIGN STATUS",
                border_style="cyan",
                padding=(1, 2),
            )
        )

        if self.verbose:
            details = state.get("worker_details") or []
            if details:
                table = Table(title="WORKER DETAIL", box=box.SIMPLE_HEAVY)
                for column in ("WORKER", "PID", "STATE", "HB", "CLAIM", "DONE", "N/A", "FAIL", "RETRY", "INFLIGHT"):
                    table.add_column(column)
                for row in details:
                    table.add_row(
                        str(row.get("worker_id", "--")),
                        str(row.get("pid", "--")),
                        str(row.get("status", "--")),
                        _age(row.get("heartbeat_age_seconds")),
                        f"{_int(row.get('claimed')):,}",
                        f"{_int(row.get('done')):,}",
                        f"{_int(row.get('not_available')):,}",
                        f"{_int(row.get('failed')):,}",
                        f"{_int(row.get('http_retries')):,}",
                        f"{_int(row.get('inflight')):,}",
                    )
                self.console.print(table)

            self.failure_diagnostics(diagnostics)

    def failure_diagnostics(self, diagnostics: Mapping[str, Any]) -> None:
        by_endpoint = list(diagnostics.get("by_endpoint") or [])
        latest_errors = list(diagnostics.get("latest_errors") or [])
        if by_endpoint:
            table = Table(title="FAILED RESOURCES BY ENDPOINT · EXACT", box=box.SIMPLE_HEAVY)
            table.add_column("ENDPOINT")
            table.add_column("STATES", justify="right")
            table.add_column("RESOURCES", justify="right")
            for row in by_endpoint:
                table.add_row(
                    str(row.get("endpoint") or "unknown"),
                    f"{_int(row.get('states')):,}",
                    f"{_int(row.get('resources')):,}",
                )
            self.console.print(table)

        if latest_errors:
            scope = str(diagnostics.get("error_scope") or "")
            title = (
                "LATEST STATE ERROR CONTEXT · NOT PER-ENDPOINT"
                if scope == "state_latest_error_not_per_endpoint"
                else "FAILED TASK ERRORS · EXACT"
            )
            table = Table(title=title, box=box.SIMPLE_HEAVY)
            for name in ("ERROR", "STAGE", "STATUS", "STATES"):
                table.add_column(name)
            for row in latest_errors:
                table.add_row(
                    str(row.get("error_code") or "unknown"),
                    str(row.get("stage") or "unknown"),
                    str(row.get("status") or "unknown"),
                    f"{_int(row.get('states')):,}",
                )
            self.console.print(table)

    def bulk_status(self, payload: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(payload))
            return
        run_id = _int(payload.get("run_id"))
        scheduler = str(payload.get("scheduler_mode") or "unknown")
        status = payload.get("status") or {}
        terminal = _int(status.get("done")) + _int(status.get("not_available")) + _int(status.get("failed"))
        total = (
            terminal
            + _int(status.get("pending"))
            + _int(status.get("retry_wait"))
            + _int(status.get("running"))
        )
        header = Text()
        header.append(f"RUN {run_id} · ")
        header.append(scheduler, style="bold cyan")
        progress = ProgressBar(total=max(1, total), completed=terminal, width=52)

        counts = Table.grid(padding=(0, 3))
        counts.add_column(style="dim")
        counts.add_column(justify="right")
        counts.add_column(style="dim")
        counts.add_column(justify="right")
        counts.add_row("PENDING", f"{_int(status.get('pending')):,}", "RUNNING", f"{_int(status.get('running')):,}")
        counts.add_row("RETRY WAIT", f"{_int(status.get('retry_wait')):,}", "DONE", f"{_int(status.get('done')):,}")
        counts.add_row("N/A", f"{_int(status.get('not_available')):,}", "FAILED RESOURCES", f"{_int(status.get('failed')):,}")

        body: list[Any] = [
            header,
            Text("EXACT · MySQL durable scheduler state", style="dim"),
            Text(""),
            progress,
            Text(f"{_pct(terminal, total):.2f}% · {terminal:,} / {total:,} terminal resources"),
            Text(""),
            counts,
        ]

        refresh = payload.get("refresh")
        if refresh:
            refresh_table = Table.grid(padding=(0, 3))
            refresh_table.add_column(style="dim")
            refresh_table.add_column(justify="right")
            refresh_table.add_column(style="dim")
            refresh_table.add_column(justify="right")
            refresh_table.add_row("COMPANIES", f"{_int(refresh.get('companies')):,}", "STALE", f"{_int(refresh.get('stale')):,}")
            refresh_table.add_row("CHANGED", f"{_int(refresh.get('changed')):,}", "UNCHANGED", f"{_int(refresh.get('unchanged')):,}")
            refresh_table.add_row("SKIPPED", f"{_int(refresh.get('skipped')):,}", "FORCED", f"{_int(refresh.get('forced')):,}")
            refresh_table.add_row("SELECTED RES.", f"{_int(refresh.get('selected_resources')):,}", "PLANNED RES.", f"{_int(refresh.get('planned_resources')):,}")
            refresh_table.add_row("HTTP ARCHIVES", f"{_int(refresh.get('http_calls')):,}", "CALLS/COMPANY", str(refresh.get("http_calls_per_company", 0)))
            body.extend((Text(""), Text("REFRESH PLAN", style="bold"), refresh_table))

        self.console.print(
            Panel(
                Group(*body),
                title="DBD · DURABLE RUN STATUS",
                border_style="cyan",
                padding=(1, 2),
            )
        )

        stages = payload.get("stages") or []
        if stages:
            table = Table(title="COMPANY STATE / STAGE", box=box.SIMPLE_HEAVY)
            table.add_column("STAGE")
            table.add_column("STATUS")
            table.add_column("COMPANIES", justify="right")
            for row in stages:
                table.add_row(str(row.get("stage")), str(row.get("status")), f"{_int(row.get('n')):,}")
            self.console.print(table)

        self.failure_diagnostics(payload.get("failures") or {})

    def run_list(self, rows: Sequence[Mapping[str, Any]]) -> None:
        if self.json_mode:
            self.json(list(rows))
            return
        table = Table(title="DBD · RECENT RUNS", box=box.SIMPLE_HEAVY)
        for column in ("RUN", "STATUS", "SCHEDULER", "SOURCE", "CREATED"):
            table.add_column(column)
        for row in rows:
            table.add_row(
                str(row.get("id", "--")),
                str(row.get("status", "--")),
                str(row.get("scheduler_mode", "legacy")),
                str(row.get("source_label", "--")),
                str(row.get("created_at", "--"))[:19],
            )
        self.console.print(table)

    def discovery_status(self, data: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(data))
            return
        rows = (
            ("unique companies", f"{_int(data.get('unique_companies')):,}"),
            ("rows fetched", f"{_int(data.get('rows_fetched')):,}"),
            (
                "buckets",
                f"{_int(data.get('buckets')):,} total · {_int(data.get('buckets_done')):,} done · "
                f"{_int(data.get('buckets_split')):,} split · {_int(data.get('buckets_open')):,} open · "
                f"{_int(data.get('buckets_failed')):,} failed",
            ),
            (
                "backlog",
                f"{_int(data.get('buckets_uncounted')):,} uncounted buckets · "
                f"{_int(data.get('remaining_counted_pages')):,} known pages",
            ),
            (
                "pages",
                f"{_int(data.get('pages_done')):,} done · {_int(data.get('pages_partial')):,} partial · "
                f"{_int(data.get('pages_failed')):,} failed",
            ),
            ("quarantine", f"{_int(data.get('unresolved_rejections')):,} unresolved row(s)"),
            ("lossy partitions", f"{_int(data.get('partitions_lossy')):,}"),
        )
        self.key_values("DBD · DISCOVERY STATUS", rows)

    def command_result(self, title: str, payload: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(payload))
            return
        self.key_values(title, ((key.replace("_", " "), value) for key, value in payload.items()))
