from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Any, Mapping, TextIO

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


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
class MasterDeepTerminal:
    """Human-only terminal presentation for bootstrap, serving and SQL sync."""

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
            "batch_size": "-",
            "limit": None,
            "scanned": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "missing_identity": 0,
            "normalization_error": 0,
            "errors": [],
            "company_master_identities_added": 0,
            "upserted": 0,
            "overlap_removed": 0,
            "overlap": 0,
            "active_endpoints": 7,
            "inactive_endpoints": 4,
            "source_rows": 0,
            "target_rows": 0,
            "target_only": 0,
            "nul_removed": 0,
        }

    def __call__(self, name: str, payload: Mapping[str, Any]) -> None:
        if name == "phase":
            self.state["phase"] = str(payload.get("phase") or self.state["phase"])
        else:
            self.state.update(dict(payload))
            if name == "started":
                self.state["phase"] = str(payload.get("phase") or "RUNNING")
            elif name == "complete":
                self.state["phase"] = "COMPLETE"
            elif name == "error":
                self.state["phase"] = "ERROR"

        if self.interactive:
            self._refresh()
        else:
            self._plain(name, payload)

    def _plain(self, name: str, payload: Mapping[str, Any]) -> None:
        if name == "phase":
            self.console.print(f"PHASE {self.state['phase']}")
            return
        if name == "error":
            self.console.print(f"ERROR {payload.get('error')}")
            return
        if name == "complete":
            if self.command == "bootstrap":
                self.console.print(
                    "COMPLETE "
                    f"scanned={_number(self.state.get('scanned'))} "
                    f"upserted={_number(self.state.get('upserted'))} "
                    f"identity_added={_number(self.state.get('company_master_identities_added'))} "
                    f"overlap={_number(self.state.get('overlap'))}"
                )
            else:
                self.console.print(
                    "COMPLETE "
                    f"scanned={_number(self.state.get('scanned'))} "
                    f"inserted={_number(self.state.get('inserted'))} "
                    f"updated={_number(self.state.get('updated'))} "
                    f"unchanged={_number(self.state.get('unchanged'))}"
                )
            return
        if name != "progress":
            return

        now = time.monotonic()
        if now - self.last_plain_progress < 1.0:
            return
        self.last_plain_progress = now
        if self.command == "bootstrap":
            self.console.print(
                "BOOTSTRAP "
                f"scanned={_number(self.state.get('scanned'))} "
                f"upserted={_number(self.state.get('upserted'))} "
                f"overlap_removed={_number(self.state.get('overlap_removed'))}"
            )
        elif self.command == "serving":
            self.console.print(
                "SERVING "
                f"scanned={_number(self.state.get('scanned'))} "
                f"inserted={_number(self.state.get('inserted'))} "
                f"updated={_number(self.state.get('updated'))} "
                f"unchanged={_number(self.state.get('unchanged'))}"
            )
        else:
            self.console.print(
                "SQLSYNC "
                f"scanned={_number(self.state.get('scanned'))} "
                f"inserted={_number(self.state.get('inserted'))} "
                f"updated={_number(self.state.get('updated'))} "
                f"unchanged={_number(self.state.get('unchanged'))}"
            )

    def _refresh(self) -> None:
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

    def _header(self, title: str, mode: str) -> Panel:
        elapsed = int(time.monotonic() - self.started_at)
        text = Text(
            f"{mode} • {self.state['phase']} "
            f"• {elapsed // 3600:02d}:{elapsed % 3600 // 60:02d}:{elapsed % 60:02d}"
        )
        return Panel(text, title=title)

    def _render_bootstrap(self):
        model = Panel(
            _grid(
                [
                    (
                        "Active endpoints",
                        _number(self.state.get("active_endpoints")),
                        "Legacy inactive",
                        _number(self.state.get("inactive_endpoints")),
                    ),
                    (
                        "Identity added",
                        _number(self.state.get("company_master_identities_added")),
                        "Overlap",
                        _number(self.state.get("overlap")),
                    ),
                ]
            ),
            title="Model",
        )
        backfill = Panel(
            _grid(
                [
                    (
                        "Scanned",
                        _number(self.state.get("scanned")),
                        "Upserted",
                        _number(self.state.get("upserted")),
                    ),
                    (
                        "Overlap removed",
                        _number(self.state.get("overlap_removed")),
                        "Batch",
                        _number(self.state.get("batch_size")),
                    ),
                ]
            ),
            title="Backfill",
        )
        return Group(
            self._header("DBD DEEP BOOTSTRAP", "CURRENT MODEL"),
            model,
            backfill,
            Text("SAFE • CURRENT 7 ENDPOINT MODEL • NO DEEP PAYLOAD RESET"),
        )

    def _render_serving(self):
        pipeline = Panel(
            _grid(
                [
                    ("Source", "dbd_company_deep", "Filter", "COMPLETE ONLY"),
                    ("Target", "dbd_master_deep", "Batch", _number(self.state.get("batch_size"))),
                    ("Limit", _limit(self.state.get("limit")), "Storage", "MYSQL"),
                ]
            ),
            title="Pipeline",
        )
        projection = Panel(
            _grid(
                [
                    (
                        "Scanned",
                        _number(self.state.get("scanned")),
                        "Inserted",
                        _number(self.state.get("inserted")),
                    ),
                    (
                        "Updated",
                        _number(self.state.get("updated")),
                        "Unchanged",
                        _number(self.state.get("unchanged")),
                    ),
                    (
                        "Missing identity",
                        _number(self.state.get("missing_identity")),
                        "Normalize errors",
                        _number(self.state.get("normalization_error")),
                    ),
                ]
            ),
            title="Projection",
        )
        return Group(
            self._header("DBD MASTER DEEP", "MYSQL SERVING"),
            pipeline,
            projection,
            Text("MYSQL ONLY • SQL SERVER NOT TOUCHED"),
        )

    def _render_sqlserver(self):
        pipeline = Panel(
            _grid(
                [
                    ("Source", "MySQL dbd_master_deep", "Target", "dbo.DBD_MASTER_DEEP"),
                    ("Database", "TIPDataIntelligence", "Batch", _number(self.state.get("batch_size"))),
                    ("Limit", _limit(self.state.get("limit")), "Mode", "INSERT + UPDATE + SKIP"),
                ]
            ),
            title="Pipeline",
        )
        transfer = Panel(
            _grid(
                [
                    (
                        "Source rows",
                        _number(self.state.get("source_rows")),
                        "Target rows",
                        _number(self.state.get("target_rows")),
                    ),
                    (
                        "Scanned",
                        _number(self.state.get("scanned")),
                        "Inserted",
                        _number(self.state.get("inserted")),
                    ),
                    (
                        "Updated",
                        _number(self.state.get("updated")),
                        "Unchanged",
                        _number(self.state.get("unchanged")),
                    ),
                    (
                        "Target-only",
                        _number(self.state.get("target_only")),
                        "NUL removed",
                        _number(self.state.get("nul_removed")),
                    ),
                ]
            ),
            title="Transfer",
        )
        return Group(
            self._header("DBD MASTER DEEP SYNC", "MYSQL → SQL SERVER"),
            pipeline,
            transfer,
            Text("SAFE SYNC • TARGET-ONLY REPORTED • NO AUTOMATIC DELETE"),
        )

    def _render(self):
        if self.command == "bootstrap":
            renderable = self._render_bootstrap()
        elif self.command == "serving":
            renderable = self._render_serving()
        else:
            renderable = self._render_sqlserver()

        error = self.state.get("error")
        if error:
            return Group(
                renderable,
                Panel(Text(str(error), style="bold red"), title="Error"),
            )
        return renderable

    def complete(self, result: Mapping[str, Any]) -> None:
        self("complete", result)
        self.close()

        errors = list(result.get("errors") or [])
        if errors:
            table = Table(title="Projection Errors", expand=False)
            table.add_column("Identity")
            table.add_column("Error")
            for item in errors[:20]:
                table.add_row(str(item.get("identity") or "-"), str(item.get("error") or "-"))
            self.console.print(table)

        target_only = list(result.get("target_only_sample") or [])
        if target_only:
            table = Table(title="SQL Server Target-only Sample", expand=False)
            table.add_column("Company ID", justify="right")
            table.add_column("Type")
            table.add_column("Registration")
            for item in target_only[:20]:
                table.add_row(
                    str(item.get("company_id") or "-"),
                    str(item.get("juristic_type_code") or "-"),
                    str(item.get("registration_no") or "-"),
                )
            self.console.print(table)

    def error(self, exc: BaseException) -> None:
        self("error", {"error": f"{type(exc).__name__}: {exc}"})
        self.close()

    def close(self) -> None:
        if self.live is not None:
            self.live.stop()
            self.live = None
