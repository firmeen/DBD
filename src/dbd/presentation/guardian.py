from __future__ import annotations

from typing import Any, Mapping

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..core.progress import render_bar


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _phase_style(phase: str) -> str:
    if phase in {"COMPLETED", "RUNNING"}:
        return "bold green"
    if phase in {"WAIT_REDIS", "WAIT_MYSQL", "FENCED", "RECONCILING", "RECOVERING", "PAUSED"}:
        return "bold yellow"
    if phase in {"FATAL", "COMPLETED_WITH_FAILURES"}:
        return "bold red"
    return "bold cyan"


class GuardianDashboard:
    """Fixed-region recovery UI that yields the terminal to the campaign supervisor."""

    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console(highlight=False)
        self._live: Live | None = None
        self._last_phase: str | None = None

    def _renderable(self, payload: Mapping[str, Any]) -> Panel:
        phase = str(payload.get("phase") or "UNKNOWN")
        counts = payload.get("counts") or {}
        redis = payload.get("redis") or {}
        profile = payload.get("profile") or {}
        safety = payload.get("safety") or {}

        total = _int(counts.get("total"))
        terminal = _int(counts.get("terminal"))
        percent = (terminal / total * 100.0) if total else 0.0

        hero = Text()
        hero.append(f"RUN {_int(payload.get('run_id'))}  ", style="bold")
        hero.append(phase, style=_phase_style(phase))
        hero.append(f"    RECOVERIES {_int(payload.get('recovery_count'))}", style="dim")

        progress = Table.grid(padding=(0, 2))
        progress.add_column(style="dim", no_wrap=True)
        progress.add_column()
        progress.add_row(
            "OVERALL",
            f"{render_bar(terminal, total, width=34)}  {percent:6.2f}%  "
            f"{terminal:,} / {total:,}",
        )
        progress.add_row(
            "STATE",
            f"done {_int(counts.get('done')):,}   "
            f"N/A {_int(counts.get('not_available')):,}   "
            f"failed {_int(counts.get('failed')):,}   "
            f"remaining {_int(counts.get('remaining')):,}",
        )

        if redis.get("latency_ms") is not None:
            redis_text = (
                f"{'HEALTHY' if redis.get('stable') else 'WAITING'}   "
                f"ping {redis.get('pings') or '--'}   "
                f"latency {float(redis['latency_ms']):.1f} ms"
            )
        else:
            redis_text = (
                f"{'HEALTHY' if redis.get('stable') else 'WAITING'}   "
                f"ping {redis.get('pings') or '--'}"
            )

        control = Table.grid(padding=(0, 2))
        control.add_column(style="dim", no_wrap=True)
        control.add_column()
        control.add_row("REDIS", redis_text)

        lease = redis.get("lease_ttl")
        if lease is None:
            lease_text = "--"
        elif int(lease) == -2:
            lease_text = "CLEAR"
        elif int(lease) == -1:
            lease_text = "NO EXPIRY · FENCED"
        else:
            lease_text = f"{int(lease)}s · FENCED"
        control.add_row(
            "CONTROL",
            f"supervisor lease {lease_text}   "
            f"workers live {_int(redis.get('live_workers'))}   "
            f"stale memberships {_int(redis.get('stale_memberships'))}",
        )
        fleet_cap = profile.get("fleet_rate_max") or profile.get("fleet_rate_initial") or "--"
        control.add_row(
            "PROFILE",
            f"workers {_int(profile.get('workers'))} "
            f"[{_int(profile.get('workers_min'))}-{_int(profile.get('workers_max'))}]   "
            f"concurrency {_int(profile.get('worker_concurrency'))}   "
            f"worker cap {profile.get('worker_rate') or '--'}/s   "
            f"fleet cap {fleet_cap}/s",
        )
        control.add_row("RATE KEY", str(profile.get("redis_rate_key") or "--"))

        recovery = Table.grid(padding=(0, 2))
        recovery.add_column(style="dim", no_wrap=True)
        recovery.add_column()
        recovery.add_row("ACTION", str(payload.get("message") or "--"))
        recovery.add_row("LAST FAILURE", str(payload.get("last_failure") or "—"))
        recovery.add_row("PACKAGE", str(payload.get("package_health") or "unknown").upper())

        guard = Table.grid(padding=(0, 2))
        guard.add_column(style="dim", no_wrap=True)
        guard.add_column()
        guard.add_row(
            "DUPLICATE SUPERVISOR",
            str(safety.get("duplicate_supervisor") or "FENCED"),
        )
        guard.add_row("AUTO BULK RETRY", str(safety.get("auto_bulk_retry") or "DISABLED"))
        guard.add_row(
            "BLIND REDIS CLEANUP",
            str(safety.get("blind_redis_cleanup") or "DISABLED"),
        )
        guard.add_row("DURABLE TRUTH", str(safety.get("durable_truth") or "MYSQL"))

        body = Group(
            hero,
            Text("Long-running same-run recovery controller", style="dim"),
            Text(""),
            Text("DURABLE PROGRESS", style="bold cyan"),
            progress,
            Text(""),
            Text("CONTROL PLANE", style="bold cyan"),
            control,
            Text(""),
            Text("RECOVERY", style="bold cyan"),
            recovery,
            Text(""),
            Text("SAFETY", style="bold cyan"),
            guard,
        )
        border = (
            "green"
            if phase in {"RUNNING", "COMPLETED"}
            else "red"
            if phase in {"FATAL", "COMPLETED_WITH_FAILURES"}
            else "yellow"
            if phase in {"WAIT_REDIS", "WAIT_MYSQL", "FENCED", "RECONCILING", "RECOVERING", "PAUSED"}
            else "cyan"
        )
        return Panel(
            body,
            title="DBD DATA PLATFORM · CAMPAIGN GUARDIAN",
            subtitle="Ctrl+C = graceful pause · same Run ID only",
            border_style=border,
            box=box.ROUNDED,
            padding=(1, 2),
        )

    def render(self, payload: Mapping[str, Any]) -> None:
        renderable = self._renderable(payload)
        phase = str(payload.get("phase") or "UNKNOWN")
        if self.console.is_terminal:
            if self._live is None:
                self._live = Live(
                    renderable,
                    console=self.console,
                    refresh_per_second=4,
                    transient=False,
                )
                self._live.start()
            else:
                self._live.update(renderable, refresh=True)
        elif phase != self._last_phase:
            self.console.print(renderable)
        self._last_phase = phase

    def suspend(self) -> None:
        if self._live is not None:
            self._live.stop()
            self._live = None

    def close(self, payload: Mapping[str, Any]) -> None:
        self.render(payload)
        self.suspend()
