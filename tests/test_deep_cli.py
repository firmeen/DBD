from __future__ import annotations

import json
from io import StringIO

import pytest

from dbd.deep._version import __version__ as deep_version
from dbd.deep.cli import _collector_pool_size, parse_args
from dbd.deep.presentation import DeepPresenter, DeepTerminalEvents


def _summary() -> dict:
    return {
        "prepared": True,
        "runtime_exists": True,
        "master": 12,
        "deep": {
            "total": 30,
            "pending": 1,
            "collecting": 2,
            "complete": 27,
            "partial": 0,
            "failed": 0,
            "unresolved": 3,
            "terminal_failures": 0,
        },
        "runtime": {
            "total": 4,
            "pending": 2,
            "running": 1,
            "retry_wait": 1,
        },
        "overlap": 0,
        "universe": 42,
        "active_endpoint_mask": 127,
        "coverage": {},
    }


def test_deep_version_is_separate_from_distribution_version(capsys) -> None:
    assert deep_version == "0.1.0"
    with pytest.raises(SystemExit) as exc:
        parse_args(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == "dbd-deep 0.1.0"


def test_collect_continuous_options_are_explicit() -> None:
    args = parse_args(
        [
            "collect",
            "--continuous",
            "--idle-seconds",
            "7.5",
            "--concurrency",
            "12",
            "--rate",
            "20",
            "--buffer-size",
            "64",
            "--limit",
            "100",
        ]
    )
    assert args.continuous is True
    assert args.idle_seconds == 7.5
    assert args.concurrency == 12
    assert args.rate == 20.0
    assert args.buffer_size == 64
    assert args.limit == 100


def test_redirected_event_output_has_no_live_repaint_sequences() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)
    events("phase", {"phase": "PREFLIGHT"})
    events(
        "cycle",
        {
            "cycle": 1,
            "queue_source": "NEW COMPANY",
            "runtime": 4,
            "adopted": 0,
            "promoted": 4,
            "refreshed": 0,
        },
    )
    events(
        "progress",
        {
            "companies_claimed": 1,
            "companies_finished": 1,
            "companies_retry_wait": 0,
        },
    )
    events.close()

    output = stream.getvalue()
    assert "\r" not in output
    assert "\x1b[" not in output
    assert "PHASE PREFLIGHT" in output
    assert "CYCLE 1" in output


def test_json_status_output_stays_machine_readable() -> None:
    stream = StringIO()
    DeepPresenter(json_mode=True, stream=stream).status(_summary())
    assert json.loads(stream.getvalue()) == _summary()


def test_live_state_returns_to_running_when_cycle_finds_work() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)
    events("idle", {"cycle": 1, "idle_seconds": 5.0, "queue_source": "IDLE"})
    assert events.state["phase"] == "IDLE"

    events(
        "cycle",
        {
            "cycle": 2,
            "queue_source": "ACTIVE REFRESH",
            "runtime": 1,
            "adopted": 0,
            "promoted": 0,
            "refreshed": 1,
        },
    )
    assert events.state["phase"] == "RUNNING"


def test_collector_pool_reserves_control_capacity(monkeypatch) -> None:
    fake_settings = type(
        "FakeSettings",
        (),
        {
            "db_pool_size": 8,
            "dbd_fleet_db_connections_max": 96,
        },
    )()
    monkeypatch.setattr("dbd.deep.cli.settings", fake_settings)

    assert _collector_pool_size(8) == 10
    assert _collector_pool_size(200) == 96


def test_collect_terminal_does_not_render_serving_panel() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)

    events.console.print(events._render())

    output = stream.getvalue()
    assert "Serving Projection" not in output
    assert "Collector" in output


def test_live_updates_do_not_force_terminal_refresh() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)

    class FakeLive:
        def __init__(self) -> None:
            self.calls = []

        def update(self, renderable, *, refresh):
            self.calls.append((renderable, refresh))

    fake = FakeLive()
    events.live = fake
    events._refresh_live()

    assert len(fake.calls) == 1
    assert fake.calls[0][1] is False


def test_interactive_progress_builds_at_most_two_ui_snapshots_per_second(monkeypatch) -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)

    class FakeLive:
        def __init__(self) -> None:
            self.calls = []

        def update(self, renderable, *, refresh):
            self.calls.append((renderable, refresh))

    fake = FakeLive()
    events.interactive = True
    events.live = fake
    events.last_live_update = 100.0
    clock = iter([100.1, 100.2, 100.7])
    monkeypatch.setattr("dbd.deep.presentation.time.monotonic", lambda: next(clock))

    payload = {
        "companies_claimed": 1,
        "companies_finished": 1,
        "companies_retry_wait": 0,
    }
    events("progress", payload)
    events("progress", payload)
    events("progress", payload)

    assert len(fake.calls) == 1
    assert fake.calls[0][1] is False
