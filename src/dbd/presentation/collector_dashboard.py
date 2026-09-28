from __future__ import annotations

import time
from collections import deque

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from ..core.progress import format_duration


class CollectorDashboard:
    """One-screen operator view for the single-process collector."""

    def __init__(self, run_id: int, *, enabled: bool = True) -> None:
        self.run_id = int(run_id)
        self.enabled = bool(enabled)
        self.console = Console()
        self._live: Live | None = None
        self._started_at = time.monotonic()
        self._samples: deque[tuple[float, int]] = deque(maxlen=600)
        self._last_plain = 0.0

    def __enter__(self):
        if self.enabled and self.console.is_terminal:
            self._live = Live(
                Text(f"DBD COLLECTOR · RUN {self.run_id}"),
                console=self.console,
                refresh_per_second=4,
                transient=False,
            )
            self._live.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._live is not None:
            self._live.__exit__(exc_type, exc, tb)
            self._live = None

    def _rate(self, current: int) -> float | None:
        now = time.monotonic()
        self._samples.append((now, int(current)))
        cutoff = now - 60.0
        while len(self._samples) > 2 and self._samples[0][0] < cutoff:
            self._samples.popleft()
        if len(self._samples) < 2:
            return None
        started, first = self._samples[0]
        elapsed = now - started
        delta = int(current) - int(first)
        if elapsed < 2.0 or delta <= 0:
            return None
        return delta / elapsed

    @staticmethod
    def _fmt_rate(value: float | None) -> str:
        return "--" if value is None else f"{value:,.2f}/s"

    def _table(self, state: dict) -> Table:
        total = int(state.get("total", 0))
        done = int(state.get("done", 0))
        na = int(state.get("not_available", 0))
        failed = int(state.get("failed", 0))
        terminal = done + na + failed
        remaining = max(0, int(state.get("remaining", total - terminal)))
        retry_wait = int(state.get("retry_wait", 0))
        progress = (100.0 * terminal / total) if total else 0.0
        flow = self._rate(terminal)
        eta = remaining / flow if flow and flow > 0 else None

        table = Table(
            title=f"DBD COLLECTOR · RUN {self.run_id}",
            show_header=False,
            box=None,
            pad_edge=False,
        )
        table.add_column("key", style="bold")
        table.add_column("value")

        table.add_row(
            "PROGRESS",
            f"{progress:6.2f}%   {terminal:,} / {total:,} terminal",
        )
        table.add_row(
            "STATE",
            f"done {done:,}   N/A {na:,}   failed {failed:,}   retry~ {retry_wait:,}   remaining {remaining:,}",
        )
        table.add_row(
            "HTTP",
            (
                f"flow {self._fmt_rate(flow)}   "
                f"rate {float(state.get('effective_rate', 0.0)):.2f}/s "
                f"/ target {float(state.get('target_rate', 0.0)):.2f}/s   "
                f"active {int(state.get('fetch_active', 0))}   "
                f"queued {int(state.get('fetch_queue', 0))}"
            ),
        )
        table.add_row(
            "REQUESTS",
            (
                f"{int(state.get('requests', 0)):,}   "
                f"retries {int(state.get('http_retries', 0)):,}   "
                f"429 {int(state.get('rate_limit_hits', 0)):,}   "
                f"auth {state.get('auth_state', 'unknown')}   "
                f"upstream {state.get('upstream_state', 'healthy')}"
            ),
        )
        table.add_row(
            "PIPELINE",
            (
                f"fetch {int(state.get('fetch_active', 0))}   "
                f"archive {int(state.get('archive_active', 0))}   "
                f"project {int(state.get('project_active', 0))}"
            ),
        )
        table.add_row(
            "TIME",
            f"elapsed {format_duration(time.monotonic() - self._started_at)}   ETA {format_duration(eta)}",
        )

        last_error = state.get("last_error")
        if last_error:
            table.add_row("LAST ERROR", Text(str(last_error)[:180], style="yellow"))

        table.caption = "Ctrl+C = stop claiming and exit safely"
        return table

    def render(self, state: dict) -> None:
        if not self.enabled:
            return
        table = self._table(state)
        if self._live is not None:
            self._live.update(table, refresh=True)
            return

        now = time.monotonic()
        if now - self._last_plain < 5.0:
            return
        self._last_plain = now
        total = int(state.get("total", 0))
        done = int(state.get("done", 0))
        na = int(state.get("not_available", 0))
        failed = int(state.get("failed", 0))
        terminal = done + na + failed
        self.console.print(
            f"RUN {self.run_id} | {terminal:,}/{total:,} | "
            f"done {done:,} N/A {na:,} failed {failed:,} | "
            f"HTTP {int(state.get('requests', 0)):,} | "
            f"active {int(state.get('fetch_active', 0))}"
        )
