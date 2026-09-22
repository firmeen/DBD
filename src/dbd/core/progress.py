from __future__ import annotations

import os
import shutil
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, TextIO


def render_bar(current: int, total: int, width: int = 24) -> str:
    total = max(0, int(total))
    current = max(0, int(current))
    width = max(1, int(width))
    if total <= 0:
        return "[" + ("·" * width) + "]"
    ratio = min(1.0, current / total)
    filled = min(width, int(round(width * ratio)))
    return "[" + ("█" * filled) + ("░" * (width - filled)) + "]"


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "--"
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        return f"{days}d {hours:02d}h {minutes:02d}m"
    if hours:
        return f"{hours}h {minutes:02d}m {seconds:02d}s"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


@dataclass(frozen=True)
class TerminalSnapshot:
    """Presentation-only campaign state.

    The snapshot deliberately contains plain values only. Renderers never own or
    call Redis/MySQL/HTTP clients, keeping terminal output out of runtime control.
    """

    run_id: int
    status: str
    current: int
    total: int
    done: int = 0
    not_available: int = 0
    failed: int = 0
    remaining: int = 0
    pending: int = 0
    running: int = 0

    fetch_queue: int = 0
    archive_queue: int = 0
    project_queue: int = 0
    queue_capacity: int = 1
    fetch_active: int = 0
    archive_active: int = 0
    project_active: int = 0

    workers_registered: int = 0
    workers_target: int = 0
    workers_active: int = 0
    workers_draining: int = 0
    workers_stale: int = 0
    workers_min: int = 0
    workers_max: int = 0
    restarts: int = 0
    exhausted_capacity: int = 0

    http_rate: float | None = None
    rate_effective: float = 0.0
    rate_target: float = 0.0
    rate_ceiling: float = 0.0
    requests: int = 0
    http_retries: int = 0
    rate_limit_hits: int = 0

    redis_pool_in_use: int = 0
    redis_pool_max: int = 0
    redis_commands: int = 0
    redis_admissions: int = 0
    control_healthy: bool = True
    heartbeat_age_seconds: float | None = None

    elapsed_seconds: float = 0.0


@dataclass
class ProgressLine:
    """Single-line throttled terminal progress for non-campaign commands."""

    label: str
    interval: float = 0.75
    stream: TextIO = sys.stdout
    enabled: bool = True

    def __post_init__(self) -> None:
        self._last_render: float | None = None
        self._last_width = 0
        self._closed = False

    def update(
        self,
        current: int,
        total: int,
        *,
        detail: str = "",
        force: bool = False,
    ) -> None:
        if not self.enabled or self._closed:
            return
        now = time.monotonic()
        if (
            not force
            and self._last_render is not None
            and now - self._last_render < self.interval
        ):
            return
        self._last_render = now
        percent = (100.0 * current / total) if total > 0 else 0.0
        text = f"{self.label} {render_bar(current, total)} {current:,}/{total:,} {percent:5.1f}%"
        if detail:
            text += f" | {detail}"
        padding = " " * max(0, self._last_width - len(text))
        self.stream.write("\r" + text + padding)
        self.stream.flush()
        self._last_width = len(text)

    def event(self, message: str) -> None:
        if not self.enabled or self._closed:
            return
        if self._last_width:
            self.stream.write("\r" + (" " * self._last_width) + "\r")
        self.stream.write(message.rstrip() + "\n")
        self.stream.flush()
        self._last_width = 0

    def close(self, *, message: str | None = None) -> None:
        if not self.enabled or self._closed:
            return
        if message:
            self.event(message)
        elif self._last_width:
            self.stream.write("\n")
            self.stream.flush()
        self._closed = True


class LiveCampaignDashboard:
    """Low-overhead fixed-region campaign terminal dashboard.

    Interactive TTYs are repainted in place. Redirected output uses compact
    newline-delimited snapshots. Rendering is best effort and cannot fail the
    campaign supervisor.
    """

    _WARNING_WORDS = (
        "failed",
        "failure",
        "timeout",
        "unresponsive",
        "exhausted",
        "quarantined",
        "degraded",
        "429",
        "503",
        "error",
    )

    def __init__(
        self,
        run_id: int,
        *,
        interval: float = 0.5,
        stream: TextIO = sys.stdout,
        enabled: bool = True,
        tty: bool | None = None,
        width_provider: Callable[[], int] | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self.run_id = int(run_id)
        self.interval = max(0.05, float(interval))
        self.stream = stream
        self.enabled = enabled
        self._clock = clock
        self._wall_clock = wall_clock
        self._width_provider = width_provider or self._terminal_width
        self._tty = self._detect_tty() if tty is None else bool(tty)
        self._closed = False
        self._last_render: float | None = None
        self._region_height = 0
        self._latest_event: tuple[float, str] | None = None
        self._latest_warning: tuple[float, str] | None = None
        self._samples: deque[tuple[float, int]] = deque(maxlen=1200)
        self._renderer_failed = False
        if self._tty and tty is None:
            self._tty = self._enable_virtual_terminal()

    def _detect_tty(self) -> bool:
        try:
            return bool(self.stream.isatty())
        except Exception:
            return False

    def _enable_virtual_terminal(self) -> bool:
        if os.name != "nt":
            return True
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            kernel32.GetStdHandle.restype = ctypes.c_void_p
            handle = kernel32.GetStdHandle(-11)
            if not handle:
                return False
            mode = ctypes.c_uint()
            if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                return False
            return bool(kernel32.SetConsoleMode(handle, mode.value | 0x0004))
        except Exception:
            return False

    @staticmethod
    def _terminal_width() -> int:
        return shutil.get_terminal_size(fallback=(120, 30)).columns

    @staticmethod
    def _clip(text: str, width: int) -> str:
        width = max(8, int(width))
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[: width - 1] + "…"

    @staticmethod
    def _percent(current: int, total: int) -> float:
        return (100.0 * current / total) if total > 0 else 0.0

    @staticmethod
    def _rate_text(value: float | None) -> str:
        return "--" if value is None else f"{value:.2f}/s"

    def _remember_sample(self, snapshot: TerminalSnapshot, now: float) -> None:
        if self._samples and snapshot.current < self._samples[-1][1]:
            self._samples.clear()
        if not self._samples or now > self._samples[-1][0]:
            self._samples.append((now, int(snapshot.current)))

    def _window_rate(
        self,
        now: float,
        window: float,
        *,
        minimum_elapsed: float = 2.0,
    ) -> float | None:
        if len(self._samples) < 2:
            return None
        latest_t, latest_value = self._samples[-1]
        candidate = None
        cutoff = now - window
        for sample in self._samples:
            candidate = sample
            if sample[0] >= cutoff:
                break
        if candidate is None:
            return None
        start_t, start_value = candidate
        elapsed = latest_t - start_t
        delta = latest_value - start_value
        if elapsed < min(minimum_elapsed, window) or delta <= 0:
            return None
        return delta / elapsed

    def _throughput(
        self,
        snapshot: TerminalSnapshot,
        now: float,
    ) -> tuple[float | None, float | None, float | None]:
        self._remember_sample(snapshot, now)
        current = self._window_rate(now, 5.0, minimum_elapsed=2.0)
        one_minute = self._window_rate(now, 60.0, minimum_elapsed=10.0)
        five_minutes = self._window_rate(now, 300.0, minimum_elapsed=30.0)
        return current, one_minute, five_minutes

    def _eta(
        self,
        snapshot: TerminalSnapshot,
        current_rate: float | None,
        one_minute_rate: float | None,
    ) -> float | None:
        rate = one_minute_rate or current_rate
        if rate is None or rate <= 0 or snapshot.remaining <= 0:
            return None
        return snapshot.remaining / rate

    def event(self, message: str, *, warning: bool | None = None) -> None:
        if not self.enabled or self._closed:
            return
        try:
            clean = " ".join(str(message).strip().split())
            if not clean:
                return
            now = self._wall_clock()
            self._latest_event = (now, clean)
            is_warning = (
                any(word in clean.lower() for word in self._WARNING_WORDS)
                if warning is None
                else bool(warning)
            )
            if is_warning:
                self._latest_warning = (now, clean)
            if not self._tty:
                kind = "WARN" if is_warning else "EVENT"
                stamp = time.strftime("%H:%M:%S", time.localtime(now))
                self.stream.write(f"[{kind} {stamp}] {clean}\n")
                self.stream.flush()
        except Exception:
            # Presentation must never become a supervisor failure mode.
            self._renderer_failed = True

    def _event_text(self, value: tuple[float, str] | None, width: int) -> str:
        if value is None:
            return "--"
        timestamp, message = value
        stamp = time.strftime("%H:%M:%S", time.localtime(timestamp))
        return self._clip(f"{stamp}  {message}", width)

    def _wide_lines(
        self,
        snapshot: TerminalSnapshot,
        width: int,
        now: float,
    ) -> list[str]:
        width = max(92, width)
        current_rate, one_minute_rate, five_minute_rate = self._throughput(snapshot, now)
        eta = self._eta(snapshot, current_rate, one_minute_rate)
        percent = self._percent(snapshot.current, snapshot.total)
        overall_width = min(42, max(24, width - 58))
        queue_width = 18
        http_load = (
            min(snapshot.rate_effective, snapshot.rate_ceiling)
            if snapshot.rate_ceiling > 0
            else 0.0
        )
        redis_per_http = (
            snapshot.redis_commands / snapshot.requests if snapshot.requests > 0 else 0.0
        )
        control = "HEALTHY" if snapshot.control_healthy else "DEGRADED"
        heartbeat = (
            "--"
            if snapshot.heartbeat_age_seconds is None
            else f"{snapshot.heartbeat_age_seconds:.1f}s"
        )
        status = snapshot.status.upper()
        header = f" DBD DATA PLATFORM · CAMPAIGN {snapshot.run_id} "
        header += f"{status:>12}   ELAPSED {format_duration(snapshot.elapsed_seconds)}"
        separator = "━" * min(width, 118)
        overall = (
            f" OVERALL   {render_bar(snapshot.current, snapshot.total, overall_width)} "
            f"{percent:6.2f}%   {snapshot.current:,} / {snapshot.total:,}"
        )
        counts = (
            f"           done {snapshot.done:,}   N/A {snapshot.not_available:,}   "
            f"failed {snapshot.failed:,}   remaining {snapshot.remaining:,}"
        )

        def pipeline(name: str, queued: int, active: int) -> str:
            return (
                f" {name:<9} {render_bar(queued, max(1, snapshot.queue_capacity), queue_width)} "
                f"q {queued:>5,}   active {active:>3,}"
            )

        fleet = (
            f" FLEET     workers {snapshot.workers_registered}/{snapshot.workers_target or snapshot.workers_max}   "
            f"active {snapshot.workers_active}   draining {snapshot.workers_draining}   "
            f"stale {snapshot.workers_stale}   restarts {snapshot.restarts}   "
            f"lost-cap {snapshot.exhausted_capacity}"
        )
        http = (
            f" HTTP      actual {self._rate_text(snapshot.http_rate):>9}   "
            f"effective {snapshot.rate_effective:.2f}/s   target {snapshot.rate_target:.2f}/s   "
            f"ceiling {snapshot.rate_ceiling:.2f}/s   retries {snapshot.http_retries:,}   "
            f"429/503 {snapshot.rate_limit_hits:,}"
        )
        http_bar = (
            f"           load {render_bar(int(http_load * 1000), int(max(snapshot.rate_ceiling, 0.001) * 1000), 30)} "
            f"requests {snapshot.requests:,}"
        )
        redis = (
            f" REDIS     pool {snapshot.redis_pool_in_use}/{snapshot.redis_pool_max or '-'}   "
            f"cmd/http {redis_per_http:.2f}   admissions {snapshot.redis_admissions:,}   "
            f"control {control}   heartbeat {heartbeat}"
        )
        throughput = (
            f" FLOW      now {self._rate_text(current_rate):>9}   "
            f"1m {self._rate_text(one_minute_rate):>9}   "
            f"5m {self._rate_text(five_minute_rate):>9}   ETA {format_duration(eta)}"
        )
        event = f" LAST EVENT  {self._event_text(self._latest_event, width - 13)}"
        warning = f" LAST WARN   {self._event_text(self._latest_warning, width - 13)}"
        return [
            self._clip(header, width),
            separator,
            "",
            self._clip(overall, width),
            self._clip(counts, width),
            "",
            " PIPELINE",
            self._clip(pipeline("FETCH", snapshot.fetch_queue, snapshot.fetch_active), width),
            self._clip(pipeline("ARCHIVE", snapshot.archive_queue, snapshot.archive_active), width),
            self._clip(pipeline("PROJECT", snapshot.project_queue, snapshot.project_active), width),
            "",
            self._clip(fleet, width),
            self._clip(http, width),
            self._clip(http_bar, width),
            self._clip(redis, width),
            self._clip(throughput, width),
            "",
            self._clip(event, width),
            self._clip(warning, width),
            separator,
            " Ctrl+C = graceful drain",
        ]

    def _narrow_lines(
        self,
        snapshot: TerminalSnapshot,
        width: int,
        now: float,
    ) -> list[str]:
        width = max(44, width)
        current_rate, one_minute_rate, _ = self._throughput(snapshot, now)
        eta = self._eta(snapshot, current_rate, one_minute_rate)
        percent = self._percent(snapshot.current, snapshot.total)
        status = snapshot.status.upper()
        control = "OK" if snapshot.control_healthy else "DEGRADED"
        lines = [
            f"DBD CAMPAIGN {snapshot.run_id} · {status}",
            f"{render_bar(snapshot.current, snapshot.total, max(12, min(24, width - 24)))} "
            f"{percent:.1f}%  {snapshot.current:,}/{snapshot.total:,}",
            f"done {snapshot.done:,}  N/A {snapshot.not_available:,}  failed {snapshot.failed:,}",
            f"remaining {snapshot.remaining:,}  ETA {format_duration(eta)}",
            f"q fetch/archive/project {snapshot.fetch_queue}/{snapshot.archive_queue}/{snapshot.project_queue}",
            f"active fetch/archive/project {snapshot.fetch_active}/{snapshot.archive_active}/{snapshot.project_active}",
            f"workers {snapshot.workers_active}/{snapshot.workers_target or snapshot.workers_max} "
            f"drain {snapshot.workers_draining} stale {snapshot.workers_stale}",
            f"HTTP {self._rate_text(snapshot.http_rate)} target {snapshot.rate_target:.2f}/s "
            f"429/503 {snapshot.rate_limit_hits:,}",
            f"Redis {control} pool {snapshot.redis_pool_in_use}/{snapshot.redis_pool_max or '-'}",
            f"Event {self._event_text(self._latest_event, max(16, width - 6))}",
        ]
        return [self._clip(line, width) for line in lines]

    def _compact_line(self, snapshot: TerminalSnapshot) -> str:
        percent = self._percent(snapshot.current, snapshot.total)
        return (
            f"CAMPAIGN {snapshot.run_id} | {snapshot.status.upper()} | {percent:.2f}% | "
            f"{snapshot.current:,}/{snapshot.total:,} | remaining={snapshot.remaining:,} | "
            f"workers={snapshot.workers_active}/{snapshot.workers_target or snapshot.workers_max} | "
            f"q={snapshot.fetch_queue}/{snapshot.archive_queue}/{snapshot.project_queue} | "
            f"rate={self._rate_text(snapshot.http_rate)} | 429/503={snapshot.rate_limit_hits:,} | "
            f"failed={snapshot.failed:,}"
        )

    def _paint_region(self, lines: list[str]) -> None:
        height = max(self._region_height, len(lines))
        if self._region_height:
            self.stream.write(f"\x1b[{self._region_height}F")
        for index in range(height):
            self.stream.write("\x1b[2K")
            if index < len(lines):
                self.stream.write(lines[index])
            self.stream.write("\n")
        self.stream.flush()
        self._region_height = height

    def render(self, snapshot: TerminalSnapshot, *, force: bool = False) -> None:
        if not self.enabled or self._closed:
            return
        now = self._clock()
        if (
            not force
            and self._last_render is not None
            and now - self._last_render < self.interval
        ):
            return
        self._last_render = now
        try:
            width = max(20, int(self._width_provider()))
            if self._tty:
                lines = (
                    self._wide_lines(snapshot, width, now)
                    if width >= 92
                    else self._narrow_lines(snapshot, width, now)
                )
                self._paint_region(lines)
            else:
                self._remember_sample(snapshot, now)
                self.stream.write(self._compact_line(snapshot) + "\n")
                self.stream.flush()
        except Exception:
            # One presentation failure degrades the renderer permanently to a
            # compact best-effort path and never escapes into the supervisor.
            self._renderer_failed = True
            self._tty = False
            self._region_height = 0
            try:
                self.stream.write(self._compact_line(snapshot) + "\n")
                self.stream.flush()
            except Exception:
                self.enabled = False

    def close(self, *, message: str | None = None) -> None:
        if not self.enabled or self._closed:
            return
        try:
            # _paint_region leaves the cursor directly below the fixed dashboard.
            # Keep the final dashboard visible for post-run inspection and append
            # only the terminal summary below it.
            if message:
                self.stream.write(message.rstrip() + "\n")
            self.stream.flush()
        except Exception:
            pass
        self._closed = True
