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


def _count(value: Any, style: str, *, zero_style: str = "dim") -> Text:
    try:
        active = int(value or 0) > 0
    except (TypeError, ValueError):
        active = bool(value)
    return Text(_number(value), style=style if active else zero_style)


@dataclass
class DiscoveryTerminal:
    """Presentation-only live view for Discovery.

    The pipeline supplies values it already owns. This renderer never queries
    MySQL, Redis, or HTTP and cannot change Discovery scheduling or durability.
    """

    stream: TextIO = sys.stdout
    enabled: bool = True
    interval: float = 0.5

    def __post_init__(self) -> None:
        self.console = Console(file=self.stream, soft_wrap=False)
        isatty = getattr(self.stream, "isatty", None)
        self.interactive = bool(isatty and isatty())
        self.started_at = time.monotonic()
        self.last_render_at = 0.0
        self.last_plain_at = 0.0
        self.live: Live | None = None
        self.closed = False
        self.state: dict[str, Any] = {
            "phase": "RUNNING",
            "bucket_id": None,
            "pages_done": 0,
            "pages_total": 0,
            "rows": 0,
            "page_failed": 0,
            "page_partial": 0,
            "rejected": 0,
            "batch_size": 0,
            "queue_depth": 0,
            "buckets_processed": 0,
            "pages_fetched": 0,
            "companies_written": 0,
            "requests": 0,
            "last_event": None,
            "last_event_kind": "info",
            "final_status": None,
        }

    def update_bucket(
        self,
        *,
        bucket_id: int,
        pages_done: int,
        pages_total: int,
        rows: int,
        failed: int,
        partial: int,
        rejected: int,
        batch_size: int,
        queue_depth: int,
    ) -> None:
        if not self.enabled or self.closed:
            return
        self.state.update(
            {
                "bucket_id": int(bucket_id),
                "pages_done": int(pages_done),
                "pages_total": int(pages_total),
                "rows": int(rows),
                "page_failed": int(failed),
                "page_partial": int(partial),
                "rejected": int(rejected),
                "batch_size": int(batch_size),
                "queue_depth": int(queue_depth),
            }
        )
        self._refresh()

    def update_totals(
        self,
        *,
        buckets_processed: int,
        pages_fetched: int,
        companies_written: int,
        requests: int,
    ) -> None:
        if not self.enabled or self.closed:
            return
        self.state.update(
            {
                "buckets_processed": int(buckets_processed),
                "pages_fetched": int(pages_fetched),
                "companies_written": int(companies_written),
                "requests": int(requests),
            }
        )
        self._refresh()

    def event(self, message: str, *, kind: str | None = None) -> None:
        if not self.enabled or self.closed:
            return
        clean = " ".join(str(message).strip().split())
        if not clean:
            return
        lowered = clean.lower()
        if kind is None:
            if "failed" in lowered or "lossy" in lowered or "error" in lowered:
                kind = "error"
            elif "partial" in lowered or "lease" in lowered or "retry" in lowered:
                kind = "warning"
            elif "split" in lowered:
                kind = "split"
            else:
                kind = "info"
        self.state["last_event"] = clean
        self.state["last_event_kind"] = kind
        if self.interactive:
            self._refresh(force=True)
        else:
            self.stream.write(f"DISCOVERY {kind.upper()} {clean}\n")
            self.stream.flush()

    def close(
        self,
        *,
        status: Mapping[str, Any] | None = None,
        message: str | None = None,
    ) -> None:
        if not self.enabled or self.closed:
            return
        if status is not None:
            final_status = dict(status)
            self.state["final_status"] = final_status
            degraded = any(
                int(final_status.get(key) or 0) > 0
                for key in (
                    "buckets_failed",
                    "pages_failed",
                    "pages_partial",
                    "unresolved_rejections",
                    "partitions_lossy",
                )
            )
            self.state["phase"] = "DEGRADED" if degraded else "COMPLETE"
        if self.interactive:
            self._refresh(force=True)
            if self.live is not None:
                self.live.stop()
                self.live = None
        if message:
            self.stream.write(message.rstrip() + "\n")
            self.stream.flush()
        self.closed = True

    def _refresh(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_render_at < self.interval:
            return
        self.last_render_at = now
        if self.interactive:
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
            return

        if now - self.last_plain_at < max(2.0, self.interval):
            return
        self.last_plain_at = now
        self.stream.write(self._plain_line() + "\n")
        self.stream.flush()

    def _phase(self) -> Text:
        phase = str(self.state.get("phase") or "RUNNING").upper()
        style = {
            "RUNNING": "cyan",
            "COMPLETE": "bold green",
            "DEGRADED": "bold yellow",
        }.get(phase, "bold white")
        return Text(phase, style=style)

    def _event_text(self) -> Text:
        message = str(self.state.get("last_event") or "No notable event")
        kind = str(self.state.get("last_event_kind") or "info")
        style = {
            "error": "bold red",
            "warning": "yellow",
            "split": "magenta",
            "info": "dim",
        }.get(kind, "dim")
        return Text(message, style=style)

    def _grid(self, rows: list[tuple[str, Any, str, Any]]) -> Table:
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

    def _render(self):
        elapsed = int(time.monotonic() - self.started_at)
        header = Text()
        header.append("DBD DISCOVERY", style="bold white")
        header.append(" • ", style="dim")
        header.append_text(self._phase())
        header.append(" • ", style="dim")
        header.append(
            f"{elapsed // 3600:02d}:{elapsed % 3600 // 60:02d}:{elapsed % 60:02d}",
            style="dim",
        )

        done = int(self.state.get("pages_done") or 0)
        total = int(self.state.get("pages_total") or 0)
        pct = 100.0 if total and done >= total else ((done / total * 100.0) if total else 0.0)
        current = Panel(
            self._grid(
                [
                    (
                        "Bucket",
                        Text(
                            "--" if self.state.get("bucket_id") is None else str(self.state["bucket_id"]),
                            style="bold white",
                        ),
                        "Progress",
                        Text(f"{pct:,.2f}%", style="bold white"),
                    ),
                    (
                        "Pages",
                        Text(f"{done:,}/{total:,}", style="green" if total and done >= total else "cyan"),
                        "Rows",
                        Text(_number(self.state.get("rows")), style="bold white"),
                    ),
                    (
                        "Partial",
                        _count(self.state.get("page_partial"), "yellow"),
                        "Failed",
                        _count(self.state.get("page_failed"), "bold red"),
                    ),
                    (
                        "Batch",
                        Text(_number(self.state.get("batch_size")), style="cyan"),
                        "Write queue",
                        _count(self.state.get("queue_depth"), "blue"),
                    ),
                ]
            ),
            title="Current Bucket",
        )

        session = Panel(
            self._grid(
                [
                    (
                        "Buckets processed",
                        _count(self.state.get("buckets_processed"), "blue"),
                        "Pages fetched",
                        _count(self.state.get("pages_fetched"), "green"),
                    ),
                    (
                        "Companies written",
                        _count(self.state.get("companies_written"), "green"),
                        "HTTP requests",
                        _count(self.state.get("requests"), "cyan"),
                    ),
                    (
                        "Rejected rows",
                        _count(self.state.get("rejected"), "yellow"),
                        "Mode",
                        Text("DISCOVERY", style="dim"),
                    ),
                ]
            ),
            title="Session",
        )

        sections: list[Any] = [header, current, session]
        final_status = self.state.get("final_status")
        if final_status:
            sections.append(
                Panel(
                    self._grid(
                        [
                            (
                                "Unique companies",
                                Text(_number(final_status.get("unique_companies")), style="bold white"),
                                "Rows fetched",
                                Text(_number(final_status.get("rows_fetched")), style="bold white"),
                            ),
                            (
                                "Buckets done",
                                _count(final_status.get("buckets_done"), "green"),
                                "Buckets open",
                                _count(final_status.get("buckets_open"), "blue"),
                            ),
                            (
                                "Buckets split",
                                _count(final_status.get("buckets_split"), "magenta"),
                                "Buckets failed",
                                _count(final_status.get("buckets_failed"), "bold red"),
                            ),
                            (
                                "Page partial",
                                _count(final_status.get("pages_partial"), "yellow"),
                                "Page failed",
                                _count(final_status.get("pages_failed"), "bold red"),
                            ),
                            (
                                "Quarantine",
                                _count(final_status.get("unresolved_rejections"), "yellow"),
                                "Lossy partitions",
                                _count(final_status.get("partitions_lossy"), "bold red"),
                            ),
                        ]
                    ),
                    title="Integrity",
                )
            )

        sections.append(Panel(self._event_text(), title="Last Event"))
        return Panel(Group(*sections), title="DBD DATA PLATFORM · DISCOVERY")

    def _plain_line(self) -> str:
        bucket = self.state.get("bucket_id")
        done = int(self.state.get("pages_done") or 0)
        total = int(self.state.get("pages_total") or 0)
        return (
            "DISCOVERY "
            f"bucket={bucket if bucket is not None else '-'} "
            f"pages={done}/{total} "
            f"rows={int(self.state.get('rows') or 0)} "
            f"partial={int(self.state.get('page_partial') or 0)} "
            f"failed={int(self.state.get('page_failed') or 0)} "
            f"processed={int(self.state.get('buckets_processed') or 0)} "
            f"companies={int(self.state.get('companies_written') or 0)} "
            f"requests={int(self.state.get('requests') or 0)}"
        )
