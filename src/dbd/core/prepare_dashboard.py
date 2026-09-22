from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import TextIO

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from .progress import format_duration


@dataclass(frozen=True)
class PreparationSnapshot:
    """Presentation-only state for campaign preparation."""

    run_id: int | None
    scheduler_mode: str
    phase: str
    processed: int = 0
    total: int = 0
    batches: int = 0
    batch_size: int = 0
    changed: int = 0
    stale: int = 0
    forced: int = 0
    skipped: int = 0
    work_bearing: int = 0
    elapsed_seconds: float = 0.0


class LiveCampaignPrepareDashboard:
    """Rich live dashboard for the otherwise silent campaign build phase.

    This renderer owns no database or Redis clients. The preparation runtime
    pushes immutable snapshots into it, so presentation failure cannot corrupt
    durable scheduler creation.
    """

    def __init__(
        self,
        *,
        interval: float = 0.25,
        stream: TextIO = sys.stdout,
        enabled: bool = True,
        tty: bool | None = None,
        clock=time.monotonic,
    ) -> None:
        self.interval = max(0.05, float(interval))
        self.stream = stream
        self.enabled = bool(enabled)
        self._clock = clock
        self._tty = self._detect_tty() if tty is None else bool(tty)
        self._last_render: float | None = None
        self._closed = False
        self._renderer_failed = False
        self._live: Live | None = None
        self._console = Console(
            file=stream,
            force_terminal=self._tty,
            color_system="auto" if self._tty else None,
            soft_wrap=False,
        )

    def _detect_tty(self) -> bool:
        try:
            return bool(self.stream.isatty())
        except Exception:
            return False

    @staticmethod
    def _rate(snapshot: PreparationSnapshot) -> float | None:
        if snapshot.elapsed_seconds <= 0 or snapshot.processed <= 0:
            return None
        return snapshot.processed / snapshot.elapsed_seconds

    @classmethod
    def _eta(cls, snapshot: PreparationSnapshot) -> float | None:
        rate = cls._rate(snapshot)
        if rate is None or rate <= 0 or snapshot.total <= snapshot.processed:
            return None
        return (snapshot.total - snapshot.processed) / rate

    @staticmethod
    def _percent(snapshot: PreparationSnapshot) -> float:
        if snapshot.total <= 0:
            return 0.0
        return min(100.0, 100.0 * snapshot.processed / snapshot.total)

    def _renderable(self, snapshot: PreparationSnapshot):
        run = "NEW" if snapshot.run_id is None else str(snapshot.run_id)
        phase = snapshot.phase.upper()
        percent = self._percent(snapshot)
        rate = self._rate(snapshot)
        eta = self._eta(snapshot)

        header = Text()
        header.append("DBD DATA PLATFORM", style="bold")
        header.append(f" · CAMPAIGN {run} · ")
        header.append(phase, style="bold")

        progress = ProgressBar(
            total=100.0,
            completed=percent,
            width=52,
        )
        progress_row = Table.grid(expand=True)
        progress_row.add_column(ratio=1)
        progress_row.add_column(justify="right", no_wrap=True)
        progress_row.add_row(
            progress,
            (
                f"{percent:6.2f}%   "
                f"{snapshot.processed:,} / {snapshot.total:,}"
                if snapshot.total
                else f"{snapshot.processed:,} processed"
            ),
        )

        details = Table.grid(expand=True, padding=(0, 2))
        details.add_column(style="bold", no_wrap=True)
        details.add_column()
        details.add_row(
            "MATERIALIZE",
            f"batch {snapshot.batch_size:,} · batches {snapshot.batches:,}",
        )
        details.add_row(
            "PLANNER",
            (
                f"changed {snapshot.changed:,} · stale {snapshot.stale:,} · "
                f"forced {snapshot.forced:,} · skip {snapshot.skipped:,} · "
                f"work {snapshot.work_bearing:,}"
            ),
        )
        details.add_row(
            "FLOW",
            (
                f"{rate:,.0f} companies/s" if rate is not None else "-- companies/s"
            )
            + f" · elapsed {format_duration(snapshot.elapsed_seconds)}"
            + f" · ETA {format_duration(eta)}",
        )
        return Panel(
            Group(header, Text(""), progress_row, Text(""), details),
            title="CAMPAIGN PREPARATION",
            border_style="cyan",
            padding=(1, 2),
        )

    def _compact(self, snapshot: PreparationSnapshot) -> str:
        run = "NEW" if snapshot.run_id is None else str(snapshot.run_id)
        rate = self._rate(snapshot)
        eta = self._eta(snapshot)
        progress = (
            f"{snapshot.processed:,}/{snapshot.total:,} {self._percent(snapshot):.2f}%"
            if snapshot.total
            else f"{snapshot.processed:,}"
        )
        rate_text = "--" if rate is None else f"{rate:,.0f}/s"
        return (
            f"CAMPAIGN {run} | {snapshot.phase.upper()} | {progress} | "
            f"batch={snapshot.batch_size:,} batches={snapshot.batches:,} | "
            f"changed={snapshot.changed:,} stale={snapshot.stale:,} "
            f"forced={snapshot.forced:,} skip={snapshot.skipped:,} | "
            f"rate={rate_text} ETA={format_duration(eta)}"
        )

    def render(self, snapshot: PreparationSnapshot, *, force: bool = False) -> None:
        if not self.enabled or self._closed:
            return
        now = self._clock()
        force = force or snapshot.phase.lower() in {"ready", "failed"}
        if (
            not force
            and self._last_render is not None
            and now - self._last_render < self.interval
        ):
            return
        self._last_render = now

        try:
            if not self._tty:
                self.stream.write(self._compact(snapshot) + "\n")
                self.stream.flush()
                return
            renderable = self._renderable(snapshot)
            if self._live is None:
                self._live = Live(
                    renderable,
                    console=self._console,
                    auto_refresh=False,
                    transient=False,
                    redirect_stdout=False,
                    redirect_stderr=False,
                    vertical_overflow="crop",
                )
                self._live.start(refresh=True)
            else:
                self._live.update(renderable, refresh=True)
        except Exception:
            self._renderer_failed = True
            self.enabled = False
            self._stop_live()

    def _stop_live(self) -> None:
        if self._live is None:
            return
        try:
            self._live.stop()
        except Exception:
            self._renderer_failed = True
        finally:
            self._live = None

    def close(self, snapshot: PreparationSnapshot | None = None) -> None:
        if self._closed:
            return
        try:
            if snapshot is not None:
                self.render(snapshot, force=True)
        finally:
            self._stop_live()
            self._closed = True
