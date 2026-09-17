from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import TextIO


def progress_bar(current: int, total: int, *, width: int = 24) -> tuple[str, float]:
    """Return a compact fixed-width bar and its clamped percentage."""
    current = max(0, int(current))
    total = max(0, int(total))
    if total <= 0:
        return "[" + "-" * width + "]", 0.0
    ratio = min(1.0, current / total)
    filled = min(width, int(ratio * width))
    return "[" + "#" * filled + "-" * (width - filled) + "]", ratio * 100.0


@dataclass
class ProgressLine:
    """Low-overhead single-line terminal progress output.

    Progress is emitted only for interactive terminals, goes to stderr so machine-
    readable stdout remains clean, and is throttled to avoid terminal I/O becoming
    part of the ingestion hot path.
    """

    enabled: bool = True
    interval: float = 0.75
    width: int = 24
    stream: TextIO = sys.stderr
    _last_emit: float = field(default=0.0, init=False)
    _last_width: int = field(default=0, init=False)
    _active: bool = field(default=False, init=False)

    @property
    def interactive(self) -> bool:
        isatty = getattr(self.stream, "isatty", None)
        return bool(self.enabled and callable(isatty) and isatty())

    def render(
        self,
        label: str,
        current: int,
        total: int,
        *,
        suffix: str = "",
        force: bool = False,
    ) -> None:
        if not self.interactive:
            return
        now = time.monotonic()
        if not force and now - self._last_emit < self.interval:
            return
        bar, percent = progress_bar(current, total, width=self.width)
        text = f"{label} {bar} {current:,}/{total:,} {percent:5.1f}%"
        if suffix:
            text += f" | {suffix}"
        padding = " " * max(0, self._last_width - len(text))
        self.stream.write("\r" + text + padding)
        self.stream.flush()
        self._last_emit = now
        self._last_width = len(text)
        self._active = True

    def clear(self) -> None:
        if not self.interactive or not self._active:
            return
        self.stream.write("\r" + " " * self._last_width + "\r")
        self.stream.flush()
        self._active = False
        self._last_width = 0

    def finish(
        self,
        label: str | None = None,
        current: int | None = None,
        total: int | None = None,
        *,
        suffix: str = "",
    ) -> None:
        if not self.interactive:
            return
        if label is not None and current is not None and total is not None:
            self.render(label, current, total, suffix=suffix, force=True)
        if self._active:
            self.stream.write("\n")
            self.stream.flush()
        self._active = False
        self._last_width = 0
