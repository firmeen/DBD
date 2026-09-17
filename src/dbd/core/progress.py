from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import TextIO


def render_bar(current: int, total: int, width: int = 24) -> str:
    total = max(0, int(total))
    current = max(0, int(current))
    if total <= 0:
        return "[" + ("·" * width) + "]"
    ratio = min(1.0, current / total)
    filled = min(width, int(round(width * ratio)))
    return "[" + ("█" * filled) + ("░" * (width - filled)) + "]"


@dataclass
class ProgressLine:
    """Single-line throttled terminal progress with negligible hot-path overhead."""

    label: str
    interval: float = 0.75
    stream: TextIO = sys.stdout
    enabled: bool = True

    def __post_init__(self) -> None:
        self._last_render = 0.0
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
        if not force and now - self._last_render < self.interval:
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
