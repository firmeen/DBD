from __future__ import annotations

import inspect
from io import StringIO

from dbd.core.progress import (
    LiveCampaignDashboard,
    ProgressLine,
    TerminalSnapshot,
    format_duration,
    render_bar,
)


class FakeClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def snapshot(**overrides) -> TerminalSnapshot:
    values = {
        "run_id": 7,
        "status": "running",
        "current": 250,
        "total": 1000,
        "done": 200,
        "not_available": 40,
        "failed": 10,
        "remaining": 750,
        "pending": 700,
        "running": 50,
        "fetch_queue": 128,
        "archive_queue": 32,
        "project_queue": 64,
        "queue_capacity": 512,
        "fetch_active": 24,
        "archive_active": 4,
        "project_active": 8,
        "workers_registered": 6,
        "workers_target": 6,
        "workers_active": 6,
        "workers_draining": 0,
        "workers_stale": 0,
        "workers_min": 3,
        "workers_max": 6,
        "restarts": 1,
        "exhausted_capacity": 0,
        "http_rate": 11.8,
        "rate_effective": 12.0,
        "rate_target": 12.0,
        "rate_ceiling": 20.0,
        "requests": 10000,
        "http_retries": 12,
        "rate_limit_hits": 0,
        "redis_pool_in_use": 3,
        "redis_pool_max": 4,
        "redis_commands": 10000,
        "redis_admissions": 10000,
        "control_healthy": True,
        "heartbeat_age_seconds": 0.4,
        "elapsed_seconds": 3600,
    }
    values.update(overrides)
    return TerminalSnapshot(**values)


def test_render_bar_bounds() -> None:
    assert render_bar(0, 10, 10) == "[░░░░░░░░░░]"
    assert render_bar(5, 10, 10) == "[█████░░░░░]"
    assert render_bar(99, 10, 10) == "[██████████]"
    assert render_bar(5, 10, 0) == "[░]"


def test_format_duration() -> None:
    assert format_duration(None) == "--"
    assert format_duration(5) == "5s"
    assert format_duration(65) == "1m 05s"
    assert format_duration(3661) == "1h 01m 01s"
    assert format_duration(90061) == "1d 01h 01m"


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


def test_live_dashboard_renders_fixed_wide_region() -> None:
    stream = StringIO()
    clock = FakeClock()
    wall = FakeClock(36000)
    dashboard = LiveCampaignDashboard(
        7,
        interval=0.5,
        stream=stream,
        tty=True,
        width_provider=lambda: 120,
        clock=clock,
        wall_clock=wall,
    )
    dashboard.event("autoscale up: worker 6 started")
    dashboard.render(snapshot())

    output = stream.getvalue()
    assert "DBD DATA PLATFORM · CAMPAIGN 7" in output
    assert "OVERALL" in output
    assert "PIPELINE" in output
    assert "FETCH" in output
    assert "ARCHIVE" in output
    assert "PROJECT" in output
    assert "HTTP" in output
    assert "REDIS" in output
    assert "LAST EVENT" in output
    assert "autoscale up: worker 6 started" in output
    assert dashboard._region_height > 10


def test_live_dashboard_repaints_instead_of_appending_events() -> None:
    stream = StringIO()
    clock = FakeClock()
    wall = FakeClock(36000)
    dashboard = LiveCampaignDashboard(
        7,
        interval=0.5,
        stream=stream,
        tty=True,
        width_provider=lambda: 120,
        clock=clock,
        wall_clock=wall,
    )
    dashboard.event("worker 1 started")
    dashboard.event("worker 2 started")
    dashboard.render(snapshot())
    first_height = dashboard._region_height
    clock.value = 1.0
    dashboard.event("worker 3 started")
    dashboard.render(snapshot(current=300, remaining=700))

    output = stream.getvalue()
    assert "worker 3 started" in output
    assert "\x1b[" + str(first_height) + "F" in output
    assert dashboard._latest_event is not None
    assert dashboard._latest_event[1] == "worker 3 started"


def test_live_dashboard_narrow_terminal_degrades_cleanly() -> None:
    stream = StringIO()
    dashboard = LiveCampaignDashboard(
        7,
        interval=0,
        stream=stream,
        tty=True,
        width_provider=lambda: 60,
    )
    dashboard.render(snapshot())
    output = stream.getvalue()
    assert "DBD CAMPAIGN 7" in output
    assert "q fetch/archive/project" in output
    assert "workers 6/6" in output
    assert "Redis OK" in output
    assert "DBD DATA PLATFORM" not in output


def test_non_tty_uses_compact_line_oriented_output() -> None:
    stream = StringIO()
    dashboard = LiveCampaignDashboard(7, interval=0, stream=stream, tty=False)
    dashboard.render(snapshot())
    dashboard.render(snapshot(current=260, remaining=740))
    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    assert all(line.startswith("CAMPAIGN 7 | RUNNING |") for line in lines)
    assert "q=128/32/64" in lines[0]


def test_dashboard_refresh_is_throttled() -> None:
    stream = StringIO()
    clock = FakeClock()
    dashboard = LiveCampaignDashboard(
        7,
        interval=10,
        stream=stream,
        tty=False,
        clock=clock,
    )
    dashboard.render(snapshot(current=100))
    clock.value = 1
    dashboard.render(snapshot(current=200))
    assert stream.getvalue().count("\n") == 1
    assert "100/1,000" in stream.getvalue()
    assert "200/1,000" not in stream.getvalue()


def test_dashboard_eta_needs_enough_samples_then_uses_local_history() -> None:
    stream = StringIO()
    clock = FakeClock()
    dashboard = LiveCampaignDashboard(
        7,
        interval=0,
        stream=stream,
        tty=True,
        width_provider=lambda: 120,
        clock=clock,
    )
    dashboard.render(snapshot(current=100, remaining=900), force=True)
    assert "ETA --" in stream.getvalue()

    clock.value = 1.0
    dashboard.render(snapshot(current=110, remaining=890), force=True)
    assert dashboard._window_rate(clock.value, 60) is None

    clock.value = 3.0
    dashboard.render(snapshot(current=130, remaining=870), force=True)
    rate = dashboard._window_rate(clock.value, 60)
    assert rate is not None
    assert 9.9 < rate < 10.1
    assert "ETA 1m" in stream.getvalue()


def test_dashboard_warning_tracks_latest_warning_without_log_spam() -> None:
    stream = StringIO()
    wall = FakeClock(36000)
    dashboard = LiveCampaignDashboard(
        7,
        interval=0,
        stream=stream,
        tty=True,
        width_provider=lambda: 120,
        wall_clock=wall,
    )
    dashboard.event("worker 2 started")
    dashboard.event("Redis control plane timeout")
    dashboard.render(snapshot(control_healthy=False))
    assert dashboard._latest_warning is not None
    assert dashboard._latest_warning[1] == "Redis control plane timeout"
    assert "LAST WARN" in stream.getvalue()


def test_renderer_failure_is_isolated_from_campaign() -> None:
    class BrokenStream:
        def write(self, value):
            raise OSError("terminal disappeared")

        def flush(self):
            raise OSError("terminal disappeared")

        def isatty(self):
            return False

    dashboard = LiveCampaignDashboard(7, interval=0, stream=BrokenStream(), tty=False)
    dashboard.render(snapshot())
    dashboard.event("still running")
    dashboard.close(message="complete")
    assert dashboard._renderer_failed is True
    assert dashboard.enabled is False


def test_presentation_layer_has_no_runtime_clients() -> None:
    source = inspect.getsource(LiveCampaignDashboard)
    assert "Redis" not in source
    assert "MySQL" not in source
    assert "connect(" not in source
    dashboard = LiveCampaignDashboard(7, enabled=False)
    assert not hasattr(dashboard, "redis")
    assert not hasattr(dashboard, "db")
