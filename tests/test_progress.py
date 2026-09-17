from __future__ import annotations

from io import StringIO

from dbd.core.progress import ProgressLine, render_bar


def test_render_bar_bounds() -> None:
    assert render_bar(0, 10, 10) == "[░░░░░░░░░░]"
    assert render_bar(5, 10, 10) == "[█████░░░░░]"
    assert render_bar(99, 10, 10) == "[██████████]"


def test_progress_line_throttles_and_keeps_single_line() -> None:
    stream = StringIO()
    progress = ProgressLine("TEST", interval=999, stream=stream)
    progress.update(1, 10, detail="rate 1/s")
    progress.update(2, 10, detail="rate 2/s")
    progress.close()
    output = stream.getvalue()
    assert "1/10" in output
    assert "2/10" not in output
    assert output.endswith("\n")


def test_progress_event_clears_line() -> None:
    stream = StringIO()
    progress = ProgressLine("TEST", interval=0, stream=stream)
    progress.update(1, 2)
    progress.event("HTTP 429 cooldown")
    progress.close()
    assert "HTTP 429 cooldown\n" in stream.getvalue()
