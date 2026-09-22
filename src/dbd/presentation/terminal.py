from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, TextIO

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from ..core.progress import format_duration


@dataclass(frozen=True)
class OutputOptions:
    """Operator-output policy shared by every CLI command."""

    json: bool = False
    verbose: bool = False
    debug: bool = False
    no_color: bool = False


def _num(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _rate(value: Any) -> str:
    number = _float(value)
    return "--" if number is None else f"{number:,.2f}/s"


def _age(value: Any) -> str:
    number = _float(value)
    return "--" if number is None else f"{number:.1f}s"


def _pct(part: int, total: int) -> float:
    return 0.0 if total <= 0 else min(100.0, max(0.0, 100.0 * part / total))


def _worker_index(worker_id: str) -> str:
    match = re.search(r"-([0-9]+)-[0-9a-f]+$", str(worker_id or ""))
    return match.group(1) if match else "?"


def _status_style(value: str) -> str:
    value = str(value or "").lower()
    if value in {"running", "ready", "active", "finished", "done", "healthy", "current", "success"}:
        return "bold green"
    if value in {"draining", "retry_wait", "partial", "degraded", "warning"}:
        return "bold yellow"
    if value in {"failed", "stale", "error", "lost", "conflict"}:
        return "bold red"
    return "bold cyan"


class TerminalPresenter:
    """Human-first CLI renderer with an explicit machine-readable JSON mode.

    The presenter owns no database, Redis, HTTP, scheduler, or worker clients.
    It receives plain mappings/results and is therefore presentation-only.
    """

    def __init__(
        self,
        options: OutputOptions | None = None,
        *,
        stream: TextIO = sys.stdout,
        force_terminal: bool | None = None,
    ) -> None:
        self.options = options or OutputOptions()
        self.stream = stream
        self.console = Console(
            file=stream,
            force_terminal=force_terminal,
            color_system=None if self.options.no_color else "auto",
            soft_wrap=False,
            highlight=False,
        )

    @classmethod
    def from_args(cls, args, **kwargs) -> "TerminalPresenter":
        return cls(
            OutputOptions(
                json=bool(getattr(args, "json_output", False)),
                verbose=bool(getattr(args, "verbose", False)),
                debug=bool(getattr(args, "debug", False)),
                no_color=bool(getattr(args, "no_color", False)),
            ),
            **kwargs,
        )

    def json(self, value: Any) -> None:
        self.stream.write(json.dumps(value, indent=2, default=str, ensure_ascii=False) + "\n")
        self.stream.flush()

    def _title(self, title: str, subtitle: str | None = None) -> Text:
        text = Text(title, style="bold")
        if subtitle:
            text.append(f" · {subtitle}", style="dim")
        return text

    def _kv(self, rows: Iterable[tuple[str, Any]], *, value_style: str | None = None) -> Table:
        table = Table.grid(padding=(0, 2))
        table.add_column(style="dim", no_wrap=True)
        table.add_column(style=value_style)
        for key, value in rows:
            table.add_row(str(key), str(value))
        return table

    def _panel(self, title: str, *renderables, border_style: str = "cyan") -> None:
        body = Group(*renderables) if len(renderables) > 1 else renderables[0]
        self.console.print(
            Panel(
                body,
                title=title,
                title_align="left",
                border_style=border_style,
                padding=(1, 2),
            )
        )

    def success(self, title: str, *, fields: Mapping[str, Any] | None = None, raw=None) -> None:
        if self.options.json:
            self.json(raw if raw is not None else {"status": "success", **dict(fields or {})})
            return
        rows = [("STATUS", "[bold green]SUCCESS[/]")]
        rows.extend((str(k).upper(), v) for k, v in (fields or {}).items())
        self._panel(title, self._kv(rows), border_style="green")

    def warning(self, title: str, message: str, *, raw=None) -> None:
        if self.options.json:
            self.json(raw if raw is not None else {"status": "warning", "message": message})
            return
        self._panel(title, Text(message), border_style="yellow")

    def error(self, message: str) -> None:
        if self.options.json:
            self.json({"status": "error", "error": message})
            return
        self.console.print(Panel(Text(message), title="DBD DATA PLATFORM · ERROR", border_style="red"))

    def interrupted(self) -> None:
        if self.options.json:
            self.json({"status": "interrupted", "recoverable": True})
            return
        self._panel(
            "DBD DATA PLATFORM · INTERRUPTED",
            Text("Durable checkpoints remain recoverable.", style="yellow"),
            border_style="yellow",
        )

    def check_db(self, info: Mapping[str, Any]) -> None:
        if self.options.json:
            self.json(dict(info))
            return
        self._panel(
            "DATABASE CONNECTION",
            self._kv(
                (
                    ("STATUS", "[bold green]CONNECTED[/bold green]"),
                    ("DATABASE", info.get("db", "--")),
                    ("USER", info.get("user", "--")),
                    ("MYSQL", info.get("version", "--")),
                )
            ),
            border_style="green",
        )

    def master_status(self, data: Mapping[str, Any]) -> None:
        payload = {key: _num(value) for key, value in data.items()}
        if self.options.json:
            self.json(payload)
            return
        total = payload.get("total", 0)
        eligible = payload.get("eligible_all_statuses", 0)
        conflicts = payload.get("conflicts", 0)
        operating = payload.get("eligible_operating", 0)
        unknown = payload.get("unknown_status", 0)
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Population")
        table.add_column("Count", justify="right")
        table.add_column("% total", justify="right")
        for label, value in (
            ("Total master identities", total),
            ("Eligible · all statuses", eligible),
            ("Eligible · operating", operating),
            ("Unknown status", unknown),
            ("Identity conflicts", conflicts),
        ):
            table.add_row(label, f"{value:,}", f"{_pct(value, total):.2f}%")
        self._panel("MASTER REGISTRY", table)

    def master_population(self, data: Mapping[str, Any]) -> None:
        if self.options.json:
            self.json({"master_population": dict(data)})
            return
        selected = _num(data.get("selected_scope"))
        total = _num(data.get("total"))
        self._panel(
            "MASTER SCOPE",
            self._kv(
                (
                    ("SCOPE", str(data.get("scope") or "--")),
                    ("SELECTED", f"{selected:,}"),
                    ("TOTAL MASTER", f"{total:,}"),
                    ("CONFLICTS", f"{_num(data.get('conflicts')):,}"),
                    ("UNKNOWN STATUS", f"{_num(data.get('unknown_status')):,}"),
                )
            ),
        )

    def campaign_prepared(self, run_id: int) -> None:
        self.success("CAMPAIGN PREPARED", fields={"run": run_id}, raw={"run_id": run_id, "status": "prepared"})

    def campaign_stop(self, run_id: int) -> None:
        self.success(
            "CAMPAIGN DRAIN REQUESTED",
            fields={"run": run_id, "mode": "graceful"},
            raw={"run_id": run_id, "status": "drain_requested", "mode": "graceful"},
        )

    def _campaign_diagnostics(self, state: Mapping[str, Any]) -> list[tuple[str, str, str]]:
        diagnostics: list[tuple[str, str, str]] = []
        workers = state.get("worker_states") or {}
        metrics = state.get("worker_metrics") or {}
        supervisor = state.get("supervisor") or {}
        exhausted = _num(supervisor.get("exhausted_capacity"))
        restarts = _num(supervisor.get("restarts"))
        stale = _num(workers.get("stale"))
        unknown = _num(workers.get("unknown"))
        requests = _num(metrics.get("requests"))
        retries = _num(metrics.get("http_retries"))
        rate_hits = _num(metrics.get("rate_limit_hits"))
        failed = _num(state.get("failed"))
        last_failure = supervisor.get("last_worker_failure") or {}

        if last_failure:
            diagnostics.append(
                (
                    "WORKER",
                    (
                        f"Worker {last_failure.get('worker_index', '?')} "
                        f"{last_failure.get('reason', 'failed')} "
                        f"(restart {last_failure.get('restart_count', '?')}/"
                        f"{last_failure.get('restart_budget', '?')})."
                    ),
                    "red" if exhausted else "yellow",
                )
            )
        if exhausted:
            diagnostics.append(
                ("CRITICAL", f"Fleet lost {exhausted} worker capacity slot(s) after restart exhaustion.", "red")
            )
        elif restarts:
            diagnostics.append(("WARN", f"Workers have restarted {restarts} time(s).", "yellow"))
        if stale or unknown:
            diagnostics.append(
                ("WARN", f"Worker visibility: stale={stale}, unknown={unknown}.", "yellow")
            )
        if retries and requests:
            ratio = 100.0 * retries / requests
            style = "yellow" if ratio >= 5 else "dim"
            diagnostics.append(
                ("HTTP", f"{retries:,} retries across {requests:,} requests ({ratio:.1f}% retry/request).", style)
            )
        if retries and rate_hits == 0:
            diagnostics.append(
                (
                    "HTTP",
                    "Retries are occurring without recorded 429/503 hits; inspect durable failure/log diagnostics for transport, auth, decode or other 5xx causes.",
                    "yellow",
                )
            )
        if rate_hits:
            diagnostics.append(("RATE", f"{rate_hits:,} rate-limit hit(s) recorded.", "red"))
        if failed:
            diagnostics.append(
                (
                    "FAIL",
                    f"{failed:,} logical resources are currently terminal-failed in this live view; use bulk-status for exact durable failure breakdown.",
                    "red",
                )
            )
        if not bool(state.get("progress_exact", True)):
            diagnostics.append(
                (
                    "SOURCE",
                    "Progress is a live Redis heartbeat estimate, not an exact MySQL reconciliation.",
                    "dim",
                )
            )
        if not diagnostics:
            diagnostics.append(("OK", "No operator-level warning detected in the current snapshot.", "green"))
        return diagnostics

    def campaign_status(self, state: Mapping[str, Any]) -> None:
        if self.options.json:
            self.json(dict(state))
            return

        supervisor = state.get("supervisor") or {}
        workers = state.get("worker_states") or {}
        metrics = state.get("worker_metrics") or {}
        run_id = _num(state.get("run_id"))
        run_status = str(supervisor.get("status") or state.get("run_status") or "unknown").upper()
        source = str(state.get("progress_source") or "mysql_reconciled")
        exact = bool(state.get("progress_exact", True))
        source_label = (
            "EXACT · MySQL reconciliation"
            if exact
            else "LIVE · Redis worker heartbeat"
        )

        done = _num(state.get("done"))
        na = _num(state.get("not_available"))
        failed = _num(state.get("failed"))
        total = _num(state.get("total"))
        terminal = min(total, done + na + failed)
        progress = Table.grid(expand=True)
        progress.add_column(ratio=1)
        progress.add_column(justify="right")
        progress.add_row(
            ProgressBar(total=max(1, total), completed=min(terminal, max(1, total)), width=50),
            f"{_pct(terminal, total):6.2f}%   {terminal:,} / {total:,}",
        )

        summary = self._kv(
            (
                ("STATUS", f"[{_status_style(run_status)}]{run_status}[/]"),
                ("SOURCE", source_label),
                ("RUN", run_id),
                ("RUN STATUS", state.get("run_status", "--")),
                ("STOP REQUESTED", bool(state.get("stop_requested", False))),
            )
        )

        counts = Table(box=box.SIMPLE, expand=True)
        counts.add_column("Progress")
        counts.add_column("Count", justify="right")
        for label, value in (
            ("Done", done),
            ("Not available", na),
            ("Failed resources", failed),
            ("Running / inflight", _num(state.get("running"))),
            ("Pending", _num(state.get("pending"))),
        ):
            counts.add_row(label, f"{value:,}")

        fleet = Table(box=box.SIMPLE, expand=True)
        fleet.add_column("Fleet")
        fleet.add_column("Value", justify="right")
        active = _num(workers.get("active"))
        target = _num(supervisor.get("workers_target")) or _num(state.get("workers"))
        fleet.add_row("Active / target", f"{active:,} / {target:,}")
        fleet.add_row("Draining", f"{_num(workers.get('draining')):,}")
        fleet.add_row("Stale", f"{_num(workers.get('stale')):,}")
        fleet.add_row("Restarts", f"{_num(supervisor.get('restarts')):,}")
        fleet.add_row("Lost capacity", f"{_num(supervisor.get('exhausted_capacity')):,}")

        pipeline = Table(box=box.SIMPLE, expand=True)
        pipeline.add_column("Pipeline")
        pipeline.add_column("Queue", justify="right")
        pipeline.add_column("Active", justify="right")
        for label, qkey, akey in (
            ("FETCH", "fetch_queue", "fetch_active"),
            ("ARCHIVE", "archive_queue", "archive_active"),
            ("PROJECT", "project_queue", "project_active"),
        ):
            pipeline.add_row(label, f"{_num(metrics.get(qkey)):,}", f"{_num(metrics.get(akey)):,}")

        http_redis = Table(box=box.SIMPLE, expand=True)
        http_redis.add_column("Transport")
        http_redis.add_column("Value", justify="right")
        http_redis.add_row("HTTP observed", _rate(supervisor.get("http_rate_observed")))
        http_redis.add_row("HTTP effective", _rate(state.get("effective_rate")))
        http_redis.add_row("HTTP target", _rate(state.get("rate_target")))
        http_redis.add_row("HTTP retries", f"{_num(metrics.get('http_retries')):,}")
        http_redis.add_row("429 / 503 hits", f"{_num(metrics.get('rate_limit_hits')):,}")
        requests = _num(metrics.get("requests"))
        commands = _num(metrics.get("redis_commands"))
        http_redis.add_row("HTTP requests", f"{requests:,}")
        http_redis.add_row(
            "Redis cmd / HTTP",
            f"{commands / requests:.2f}" if requests else "0.00",
        )
        http_redis.add_row(
            "Redis pool",
            f"{_num(metrics.get('redis_pool_in_use')):,} / {_num(metrics.get('redis_pool_max')):,}",
        )

        diag = Table.grid(padding=(0, 1))
        diag.add_column(no_wrap=True)
        diag.add_column()
        for kind, message, style in self._campaign_diagnostics(state):
            diag.add_row(Text(kind, style=style), Text(message, style=style))

        body = [
            summary,
            Text(""),
            progress,
            counts,
            fleet,
            pipeline,
            http_redis,
            Text(""),
            Text("DIAGNOSTICS", style="bold"),
            diag,
        ]

        if self.options.verbose or self.options.debug:
            worker_table = Table(title="WORKERS", box=box.SIMPLE_HEAVY, expand=True)
            for heading, justify in (
                ("#", "right"),
                ("STATE", "left"),
                ("PID", "right"),
                ("HB", "right"),
                ("REQ", "right"),
                ("RETRY", "right"),
                ("FAIL", "right"),
                ("INFLIGHT", "right"),
                ("FETCH Q", "right"),
            ):
                worker_table.add_column(heading, justify=justify)
            for row in sorted(
                state.get("worker_details") or [],
                key=lambda item: int(_worker_index(item.get("worker_id", ""))) if _worker_index(item.get("worker_id", "")).isdigit() else 9999,
            ):
                worker_table.add_row(
                    _worker_index(row.get("worker_id", "")),
                    str(row.get("status") or "unknown").upper(),
                    str(row.get("pid") or "--"),
                    _age(row.get("heartbeat_age_seconds")),
                    f"{_num(row.get('requests')):,}",
                    f"{_num(row.get('http_retries')):,}",
                    f"{_num(row.get('failed')):,}",
                    f"{_num(row.get('inflight')):,}",
                    f"{_num(row.get('fetch_queue')):,}",
                )
            body.extend((Text(""), worker_table))

        if self.options.debug:
            debug = self._kv(
                (
                    ("PROGRESS SOURCE", source),
                    ("SUPERVISOR ACTIVE", bool(state.get("supervisor_active"))),
                    ("RATE INDEPENDENT", bool(supervisor.get("rate_independent"))),
                    ("WORKERS MIN/MAX", f"{supervisor.get('workers_min', '--')} / {supervisor.get('workers_max', '--')}"),
                    ("DB BUDGET", supervisor.get("db_connections_budget", "--")),
                    ("REDIS BUDGET", supervisor.get("redis_connections_budget", "--")),
                    (
                        "LAST WORKER LOG",
                        (supervisor.get("last_worker_failure") or {}).get("log_path") or "--",
                    ),
                )
            )
            body.extend((Text(""), Text("DEBUG CONTEXT", style="bold"), debug))

        self._panel(f"DBD DATA PLATFORM · CAMPAIGN {run_id}", *body)

    def bulk_runs(self, rows: Iterable[Mapping[str, Any]]) -> None:
        rows = list(rows)
        if self.options.json:
            self.json([dict(row) for row in rows])
            return
        table = Table(box=box.SIMPLE_HEAVY, expand=True)
        for name in ("RUN", "STATUS", "SCHEDULER", "PARTS", "SOURCE", "CREATED"):
            table.add_column(name, no_wrap=name in {"RUN", "STATUS", "SCHEDULER"})
        for row in rows:
            table.add_row(
                str(row.get("id", "--")),
                str(row.get("status", "--")).upper(),
                str(row.get("scheduler_mode", "legacy")),
                str(row.get("parts", "--")),
                str(row.get("source_label", "--")),
                str(row.get("created_at", "--"))[:19],
            )
        self._panel("DURABLE RUNS · LATEST 20", table)

    def bulk_status(self, run_id: int, payload: Mapping[str, Any]) -> None:
        if self.options.json:
            self.json(dict(payload))
            return
        status = dict(payload.get("status") or {})
        stages = list(payload.get("stages") or [])
        refresh = payload.get("refresh") or {}
        diagnostics = payload.get("diagnostics") or {}
        total = sum(_num(status.get(key)) for key in ("pending", "retry_wait", "running", "failed", "done", "not_available"))
        terminal = _num(status.get("failed")) + _num(status.get("done")) + _num(status.get("not_available"))

        progress = Table.grid(expand=True)
        progress.add_column(ratio=1)
        progress.add_column(justify="right")
        progress.add_row(
            ProgressBar(total=max(1, total), completed=min(terminal, max(1, total)), width=50),
            f"{_pct(terminal, total):6.2f}%   {terminal:,} / {total:,}",
        )

        summary = self._kv(
            (
                ("SOURCE", "EXACT · MySQL durable state"),
                ("RUN", run_id),
                ("SCHEDULER", payload.get("scheduler_mode", "--")),
                ("LOGICAL RESOURCES", f"{total:,}"),
            )
        )

        counts = Table(box=box.SIMPLE, expand=True)
        counts.add_column("Resource state")
        counts.add_column("Count", justify="right")
        for key, label in (
            ("pending", "Pending"),
            ("retry_wait", "Retry wait"),
            ("running", "Running"),
            ("done", "Done"),
            ("not_available", "Not available"),
            ("failed", "Failed resources"),
        ):
            counts.add_row(label, f"{_num(status.get(key)):,}")

        stage_table = Table(box=box.SIMPLE, expand=True)
        stage_table.add_column("Stage")
        stage_table.add_column("State")
        stage_table.add_column("Company/task rows", justify="right")
        for row in stages:
            stage_table.add_row(
                str(row.get("stage", "--")).upper(),
                str(row.get("status", "--")),
                f"{_num(row.get('n')):,}",
            )

        body: list[Any] = [summary, Text(""), progress, counts, stage_table]

        if refresh:
            refresh_table = Table(box=box.SIMPLE, expand=True)
            refresh_table.add_column("Incremental refresh")
            refresh_table.add_column("Count", justify="right")
            for key, label in (
                ("companies", "Companies evaluated"),
                ("changed", "Changed"),
                ("unchanged", "Unchanged"),
                ("stale", "Stale"),
                ("forced", "Forced"),
                ("skipped", "Skipped"),
                ("selected_resources", "Selected resources"),
                ("planned_resources", "Planned resources"),
                ("http_calls", "Archived HTTP calls"),
            ):
                refresh_table.add_row(label, f"{_num(refresh.get(key)):,}")
            refresh_table.add_row(
                "HTTP calls / company",
                f"{float(refresh.get('http_calls_per_company') or 0):.4f}",
            )
            body.extend((Text(""), refresh_table))

        if diagnostics:
            body.extend((Text(""), Text("FAILURE DIAGNOSTICS", style="bold red")))
            semantics = diagnostics.get("semantics")
            if semantics:
                body.append(Text(str(semantics), style="dim"))

            diag_table = Table(box=box.SIMPLE, expand=True)
            diag_table.add_column("Dimension")
            diag_table.add_column("Value")
            diag_table.add_column("Count", justify="right")
            for group, label in (
                ("by_error", "ERROR"),
                ("by_endpoint", "ENDPOINT"),
                ("by_stage", "STAGE"),
            ):
                for row in diagnostics.get(group) or []:
                    diag_table.add_row(
                        label,
                        str(row.get("value") or "unknown"),
                        f"{_num(row.get('n')):,}",
                    )
            if diag_table.row_count:
                body.append(diag_table)

        self._panel(f"DBD RUN {run_id} · DURABLE STATUS", *body)

    def bulk_retry(self, run_id: int, count: int) -> None:
        self.success(
            "FAILED WORK REQUEUED",
            fields={"run": run_id, "work items": f"{count:,}"},
            raw={"run_id": run_id, "requeued": int(count)},
        )

    def process_result(self, run_id: int, result: Mapping[str, Any]) -> None:
        payload = {"run_id": run_id, **dict(result)}
        if self.options.json:
            self.json(payload)
            return
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Outcome")
        table.add_column("Count", justify="right")
        for key, value in payload.items():
            if key == "run_id":
                continue
            table.add_row(str(key).replace("_", " ").title(), f"{_num(value):,}" if isinstance(value, (int, float)) else str(value))
        self._panel(
            f"RUN {run_id} · RESULT",
            self._kv((("RUN", run_id),)),
            table,
            border_style="red" if _num(result.get("failed")) else "green",
        )

    def discovery_status(self, summary: Mapping[str, Any]) -> None:
        data = {key: _num(value) for key, value in summary.items()}
        if self.options.json:
            self.json(data)
            return
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Discovery")
        table.add_column("Count", justify="right")
        for key, label in (
            ("unique_companies", "Unique companies"),
            ("rows_fetched", "Rows fetched"),
            ("buckets", "Buckets total"),
            ("buckets_done", "Buckets done"),
            ("buckets_split", "Buckets split"),
            ("buckets_open", "Buckets open"),
            ("buckets_failed", "Buckets failed"),
            ("buckets_uncounted", "Buckets uncounted"),
            ("remaining_counted_pages", "Known pages remaining"),
            ("pages_done", "Pages done"),
            ("pages_failed", "Pages failed"),
            ("pages_partial", "Pages partial"),
            ("unresolved_rejections", "Unresolved quarantines"),
            ("partitions_lossy", "Lossy partitions"),
        ):
            table.add_row(label, f"{data.get(key, 0):,}")
        warnings = []
        if data.get("partitions_lossy"):
            warnings.append(f"{data['partitions_lossy']:,} lossy partition(s) block completeness.")
        if data.get("pages_failed") or data.get("buckets_failed"):
            warnings.append(
                f"Failures remain: buckets={data.get('buckets_failed', 0):,}, pages={data.get('pages_failed', 0):,}."
            )
        if data.get("unresolved_rejections"):
            warnings.append(f"{data['unresolved_rejections']:,} row rejection(s) remain unresolved.")
        body: list[Any] = [table]
        if warnings:
            body.extend((Text(""), Text("DIAGNOSTICS", style="bold yellow")))
            body.extend(Text(item, style="yellow") for item in warnings)
        self._panel("DISCOVERY STATUS", *body, border_style="yellow" if warnings else "cyan")

    def discovery_result(self, result) -> None:
        payload = {
            "buckets_processed": _num(getattr(result, "buckets_processed", 0)),
            "pages_fetched": _num(getattr(result, "pages_fetched", 0)),
            "companies_written": _num(getattr(result, "companies_written", 0)),
            "requests_made": _num(getattr(result, "requests_made", 0)),
        }
        if self.options.json:
            self.json(payload)
            return
        self.success(
            "DISCOVERY PASS COMPLETE",
            fields={
                "buckets": f"{payload['buckets_processed']:,}",
                "pages": f"{payload['pages_fetched']:,}",
                "companies": f"{payload['companies_written']:,}",
                "requests": f"{payload['requests_made']:,}",
            },
            raw=payload,
        )

    def discovery_reset(self, action: str, result: Mapping[str, Any]) -> None:
        payload = {"action": action, **dict(result)}
        if self.options.json:
            self.json(payload)
            return
        self.success(
            "DISCOVERY QUEUE UPDATED",
            fields={key.replace("_", " "): value for key, value in payload.items()},
            raw=payload,
        )

    def replay_result(self, archive_id: int, status: Any) -> None:
        self.success(
            "ARCHIVE REPLAY",
            fields={"archive": archive_id, "projection": status},
            raw={"archive_id": archive_id, "status": str(status)},
        )
