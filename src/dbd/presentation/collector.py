from __future__ import annotations

import math
import time
from collections.abc import Mapping

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text


def _int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _rate(value) -> str:
    try:
        return f"{float(value):.2f}/s"
    except (TypeError, ValueError):
        return "--"


def _duration(value) -> str:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return "--"
    if seconds < 0 or not math.isfinite(seconds):
        return "--"
    if seconds < 1:
        return "<1s"

    whole = int(round(seconds))
    days, remainder = divmod(whole, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


class SimpleCollectorDashboard:
    """One live operator surface for the single-process collector."""

    def __init__(
        self,
        run_id: int,
        baseline: Mapping[str, object],
        *,
        target_rate: float,
        concurrency: int,
        enabled: bool = True,
    ) -> None:
        self.run_id = int(run_id)
        self.baseline = {key: _int(value) for key, value in baseline.items()}
        self.target_rate = float(target_rate)
        self.concurrency = int(concurrency)
        self.enabled = bool(enabled)
        self.started_at = time.monotonic()
        self._last_sample_at = self.started_at
        self._last_requests = 0
        self._last_completed = 0
        self._http_rate = 0.0
        self._resource_rate = 0.0
        self._last_snapshot: dict[str, object] = {}
        self._live = Live(
            self._render({}),
            refresh_per_second=4,
            transient=False,
        ) if self.enabled else None

    @property
    def total(self) -> int:
        return sum(
            self.baseline.get(key, 0)
            for key in ("pending", "retry_wait", "running", "done", "not_available", "failed")
        )

    def start(self) -> None:
        if self._live is not None:
            self._live.start()

    def update(self, snapshot: Mapping[str, object]) -> None:
        self._last_snapshot = dict(snapshot)
        now = time.monotonic()
        requests = _int(snapshot.get("requests"))
        completed = _int(snapshot.get("completed"))
        elapsed = max(0.001, now - self._last_sample_at)
        if now - self._last_sample_at >= 0.5:
            self._http_rate = max(0, requests - self._last_requests) / elapsed
            self._resource_rate = max(0, completed - self._last_completed) / elapsed
            self._last_requests = requests
            self._last_completed = completed
            self._last_sample_at = now
        if self._live is not None:
            self._live.update(self._render(snapshot), refresh=True)

    def close(
        self,
        progress: Mapping[str, object] | None = None,
        *,
        status: str = "PAUSED",
    ) -> None:
        if progress is not None:
            self.baseline = {key: _int(value) for key, value in progress.items()}
            self._last_snapshot = {}
            self._resource_rate = 0.0
        if self._live is not None:
            self._live.update(self._render(self._last_snapshot, status=status), refresh=True)
            self._live.stop()

    def _render(
        self,
        snapshot: Mapping[str, object],
        *,
        status: str | None = None,
    ):
        status = status or ("DRAINING" if snapshot.get("drain_requested") else "RUNNING")
        done = self.baseline.get("done", 0) + _int(snapshot.get("done"))
        not_available = self.baseline.get("not_available", 0) + _int(
            snapshot.get("not_available")
        )
        failed = self.baseline.get("failed", 0) + _int(snapshot.get("failed"))
        terminal = max(0, done + not_available + failed)
        total = max(1, self.total)
        remaining = max(0, total - terminal)

        header = Text()
        header.append(f"RUN {self.run_id}  ")
        header.append(status, style="bold yellow" if status == "DRAINING" else "bold green")

        counts = Table.grid(padding=(0, 3))
        counts.add_column(style="dim")
        counts.add_column(justify="right")
        counts.add_column(style="dim")
        counts.add_column(justify="right")
        counts.add_row("DONE", f"{done:,}", "N/A", f"{not_available:,}")
        counts.add_row("FAILED", f"{failed:,}", "REMAINING", f"{remaining:,}")

        pipeline = Table.grid(padding=(0, 2))
        pipeline.add_column(style="dim")
        pipeline.add_column()
        pipeline.add_row(
            "HTTP",
            f"actual {_rate(self._http_rate)} · "
            f"effective {_rate(snapshot.get('effective_rate', self.target_rate))} · "
            f"target {_rate(snapshot.get('target_rate', self.target_rate))} · "
            f"requests {_int(snapshot.get('requests')):,} · retries {_int(snapshot.get('http_retries')):,} · "
            f"429/pressure {_int(snapshot.get('rate_limit_hits')):,}",
        )
        pipeline.add_row(
            "FETCH",
            f"active {_int(snapshot.get('fetch_active'))}/{self.concurrency} · "
            f"queued {_int(snapshot.get('fetch_queue')):,}",
        )
        pipeline.add_row(
            "ARCHIVE",
            f"active {_int(snapshot.get('archive_active')):,} · "
            f"queued {_int(snapshot.get('archive_queue')):,}",
        )
        pipeline.add_row(
            "PROJECT",
            f"active {_int(snapshot.get('project_active')):,} · "
            f"queued {_int(snapshot.get('project_queue')):,}",
        )
        pipeline.add_row(
            "UPSTREAM",
            f"{str(snapshot.get('availability_state') or 'starting').upper()} · "
            f"auth {str(snapshot.get('auth_state') or 'cold').upper()}",
        )
        elapsed = max(0.001, time.monotonic() - self.started_at)
        completed = max(0, _int(snapshot.get("completed")))
        session_rate = completed / elapsed
        eta_rate = self._resource_rate if self._resource_rate > 0 else session_rate
        eta = (
            "done"
            if remaining == 0
            else _duration(remaining / eta_rate) if eta_rate > 0 else "--"
        )
        pipeline.add_row(
            "FLOW",
            f"recent {_rate(self._resource_rate)} · session {_rate(session_rate)} · "
            f"completed {completed:,} · ETA {eta}",
        )

        pieces = [
            header,
            Text("SINGLE PROCESS · MYSQL DURABLE STATE · NO REDIS HOT PATH", style="dim"),
            Text(""),
            ProgressBar(total=total, completed=min(total, terminal), width=50),
            Text(f"{(terminal / total) * 100:.2f}% · {terminal:,} / {total:,} terminal resources"),
            Text(""),
            counts,
            Text(""),
            pipeline,
        ]
        last_error = snapshot.get("last_http_error")
        if last_error:
            pieces.extend((Text(""), Text(f"LAST ERROR  {last_error}", style="yellow")))

        return Panel(
            Group(*pieces),
            title="DBD DATA PLATFORM · COLLECTOR",
            border_style="cyan",
            padding=(1, 2),
        )
