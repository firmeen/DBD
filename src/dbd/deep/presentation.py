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


def _grid(rows: list[tuple[str, Any, str, Any]]) -> Table:
    table = Table.grid(expand=True, padding=(0, 1))
    table.add_column(style="dim", no_wrap=True)
    table.add_column(justify="right")
    table.add_column(style="dim", no_wrap=True)
    table.add_column(justify="right")
    for left_key, left_value, right_key, right_value in rows:
        table.add_row(
            left_key,
            str(left_value),
            right_key,
            str(right_value),
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
        self.last_plain_progress = 0.0
        self.live: Live | None = None
        self.state: dict[str, Any] = {
            "phase": "STARTING",
            "cycle": 0,
            "queue_source": "IDLE",
            "companies_claimed": 0,
            "companies_finished": 0,
            "companies_retry_wait": 0,
            "master_promoted": 0,
            "deep_adopted": 0,
            "active_refreshed": 0,
            "endpoints_done": 0,
            "endpoints_not_available": 0,
            "endpoints_failed": 0,
            "runtime_recovered": 0,
            "runtime": 0,
            "concurrency": "-",
            "rate_per_second": "-",
            "buffer_size": "-",
            "company_limit": None,
            "continuous": False,
            "idle_seconds": None,
            "rows_total": 0,
            "rows_scanned": 0,
            "rows_changed": 0,
        }

    def __call__(self, name: str, payload: dict[str, Any]) -> None:
        if name == "phase":
            self.state["phase"] = payload.get("phase", self.state["phase"])
        elif name in {"started", "progress", "cycle", "prepare_progress"}:
            self.state.update(payload)
            if name == "started":
                self.state["phase"] = "RUNNING"
        elif name == "idle":
            self.state.update(payload)
            self.state["phase"] = "IDLE"
        elif name in {"retry_wait", "worker_error"}:
            self.state["last_event"] = name.upper()
            self.state["last_error"] = payload.get("error")
            self.state["last_endpoint"] = payload.get("endpoint")

        if self.interactive:
            self._refresh_live()
        else:
            self._plain_event(name, payload)

    def _plain_event(self, name: str, payload: Mapping[str, Any]) -> None:
        if name == "phase":
            self.console.print(f"PHASE {payload.get('phase')}")
            return
        if name == "idle":
            self.console.print(
                f"IDLE cycle={self.state['cycle']} "
                f"next_check={float(payload.get('idle_seconds') or 0):g}s"
            )
            return
        if name == "cycle":
            self.console.print(
                f"CYCLE {self.state['cycle']} "
                f"source={self.state['queue_source']} "
                f"runtime={_number(payload.get('runtime'))}"
            )
            return
        if name in {"retry_wait", "worker_error"}:
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
                "PREPARE "
                f"{_number(payload.get('rows_scanned'))}/"
                f"{_number(payload.get('rows_total'))} "
                f"changed={_number(payload.get('rows_changed'))}"
            )
        else:
            self.console.print(
                "DEEP "
                f"claimed={_number(payload.get('companies_claimed'))} "
                f"finished={_number(payload.get('companies_finished'))} "
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
            self.live.update(renderable, refresh=True)

    def _render(self):
        if self.command == "prepare":
            rows_total = int(self.state.get("rows_total") or 0)
            rows_scanned = int(self.state.get("rows_scanned") or 0)
            pct = 100.0 if rows_total == 0 else rows_scanned / rows_total * 100.0
            return Panel(
                _grid(
                    [
                        ("Phase", self.state["phase"], "Progress", f"{pct:,.2f}%"),
                        (
                            "Scanned",
                            f"{rows_scanned:,}/{rows_total:,}",
                            "Changed",
                            _number(self.state.get("rows_changed")),
                        ),
                    ]
                ),
                title=f"DBD DEEP PREPARE v{__version__}",
            )

        elapsed = int(time.monotonic() - self.started_at)
        header = Text(
            f"DBD DEEP v{__version__} • {self.state['phase']} "
            f"• {elapsed // 3600:02d}:{elapsed % 3600 // 60:02d}:{elapsed % 60:02d} "
            f"• Cycle {self.state['cycle']}"
        )
        collector = Panel(
            _grid(
                [
                    (
                        "Concurrency",
                        self.state.get("concurrency", "-"),
                        "Rate",
                        f"{self.state.get('rate_per_second', '-')} req/s",
                    ),
                    (
                        "Buffer",
                        self.state.get("buffer_size", "-"),
                        "Limit",
                        _limit(self.state.get("company_limit")),
                    ),
                    (
                        "Claimed",
                        _number(self.state.get("companies_claimed")),
                        "Finished",
                        _number(self.state.get("companies_finished")),
                    ),
                    (
                        "Source",
                        self.state.get("queue_source", "IDLE"),
                        "Runtime",
                        _number(self.state.get("runtime")),
                    ),
                ]
            ),
            title="Collector",
        )
        runtime = Panel(
            _grid(
                [
                    (
                        "Promoted",
                        _number(self.state.get("master_promoted")),
                        "Adopted",
                        _number(self.state.get("deep_adopted")),
                    ),
                    (
                        "ACTIVE refresh",
                        _number(self.state.get("active_refreshed")),
                        "Retry wait",
                        _number(self.state.get("companies_retry_wait")),
                    ),
                    (
                        "Endpoint done",
                        _number(self.state.get("endpoints_done")),
                        "N/A",
                        _number(self.state.get("endpoints_not_available")),
                    ),
                    (
                        "Endpoint failed",
                        _number(self.state.get("endpoints_failed")),
                        "Recovered",
                        _number(self.state.get("runtime_recovered")),
                    ),
                ]
            ),
            title="Session",
        )
        footer = Text(
            (
                "IDLE • waiting for work"
                if self.state["phase"] == "IDLE"
                else f"ACTIVE • {self.state.get('queue_source', 'RUNTIME')}"
            )
            + " • Ctrl+C graceful shutdown"
        )
        return Group(Panel(header, title="DBD DEEP"), collector, runtime, footer)

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

    def status(self, summary: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json(dict(summary))
            return
        deep = summary["deep"]
        runtime = summary["runtime"]
        self.console.print(
            Panel(
                _grid(
                    [
                        (
                            "Prepared",
                            "YES" if summary["prepared"] else "NO",
                            "Runtime",
                            "READY" if summary["runtime_exists"] else "MISSING",
                        ),
                        (
                            "Master waiting",
                            _number(summary["master"]),
                            "Deep total",
                            _number(deep["total"]),
                        ),
                        (
                            "Complete",
                            _number(deep["complete"]),
                            "Unresolved",
                            _number(deep["unresolved"]),
                        ),
                        (
                            "Runtime pending",
                            _number(runtime["pending"]),
                            "Running",
                            _number(runtime["running"]),
                        ),
                        (
                            "Retry wait",
                            _number(runtime["retry_wait"]),
                            "Overlap",
                            "YES" if summary["overlap"] else "0",
                        ),
                    ]
                ),
                title=f"DBD DEEP STATUS v{__version__}",
            )
        )
        coverage = summary.get("coverage") or {}
        if coverage:
            table = Table(title="Endpoint Coverage")
            table.add_column("Endpoint")
            table.add_column("Data", justify="right")
            table.add_column("Done", justify="right")
            table.add_column("N/A", justify="right")
            table.add_column("Failed", justify="right")
            for endpoint, row in coverage.items():
                table.add_row(
                    endpoint,
                    _number(row["data_rows"]),
                    _number(row["done"]),
                    _number(row["not_available"]),
                    _number(row["failed"]),
                )
            self.console.print(table)

    def prepare(self, result: Mapping[str, Any], summary: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json({"prepare": dict(result), "status": dict(summary)})
            return
        self.console.print(
            Panel(
                _grid(
                    [
                        (
                            "Already prepared",
                            "YES" if result["already_prepared"] else "NO",
                            "Rows total",
                            _number(result["rows_total"]),
                        ),
                        (
                            "Rows scanned",
                            _number(result["rows_scanned"]),
                            "Rows changed",
                            _number(result["rows_changed"]),
                        ),
                        (
                            "Recovered",
                            _number(result["runtime_recovered"]),
                            "Overlap",
                            "YES" if summary["overlap"] else "0",
                        ),
                    ]
                ),
                title=f"DBD DEEP PREPARE v{__version__}",
            )
        )

    def retry(self, count: int, summary: Mapping[str, Any]) -> None:
        if self.json_mode:
            self.json({"requeued": count, "status": dict(summary)})
            return
        self.console.print(
            Panel(
                _grid(
                    [
                        ("Reopened", _number(count), "Runtime", _number(summary["runtime"]["total"])),
                        (
                            "Unresolved",
                            _number(summary["deep"]["unresolved"]),
                            "Overlap",
                            "YES" if summary["overlap"] else "0",
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
                            "Claimed",
                            _number(result["companies_claimed"]),
                            "Finished",
                            _number(result["companies_finished"]),
                        ),
                        (
                            "Promoted",
                            _number(result["master_promoted"]),
                            "ACTIVE refresh",
                            _number(result.get("active_refreshed")),
                        ),
                        (
                            "HTTP requests",
                            _number(result["http_requests"]),
                            "Retries",
                            _number(result["http_retries"]),
                        ),
                        (
                            "429",
                            _number(result["rate_limit_hits"]),
                            "Cycles",
                            _number(result.get("cycle")),
                        ),
                    ]
                ),
                title=f"DBD DEEP COLLECT COMPLETE v{__version__}",
            )
        )
