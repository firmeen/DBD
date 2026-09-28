import pytest
from rich.console import Console

from dbd.presentation.collector import SimpleCollectorDashboard, _duration
from dbd.runtime.simple_collector import _assert_resume_safe


def test_resume_safe_accepts_zero_running_leases():
    _assert_resume_safe(
        {
            "pending": 100,
            "retry_wait": 2,
            "done": 10,
            "not_available": 3,
            "failed": 1,
        }
    )


def test_resume_safe_rejects_live_running_leases():
    with pytest.raises(ValueError, match="7 live RUNNING lease"):
        _assert_resume_safe({"running": 7, "pending": 100})


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "<1s"),
        (59, "59s"),
        (61, "1m 1s"),
        (3660, "1h 1m"),
        (90000, "1d 1h"),
        (float("inf"), "--"),
    ],
)
def test_collector_duration_format(seconds, expected):
    assert _duration(seconds) == expected


def test_collector_dashboard_renders_resource_eta():
    dashboard = SimpleCollectorDashboard(
        9,
        {"pending": 100},
        target_rate=60,
        concurrency=32,
        enabled=False,
    )
    dashboard._resource_rate = 10.0

    panel = dashboard._render(
        {
            "done": 10,
            "completed": 10,
            "effective_rate": 60,
            "target_rate": 60,
        }
    )
    console = Console(record=True, width=200)
    console.print(panel)

    rendered = console.export_text()
    assert "ETA 9s" in rendered
    assert "recent 10.00/s" in rendered

