from __future__ import annotations

import json
from io import StringIO

import pytest

from dbd.deep._version import __version__ as deep_version
from dbd.deep.cli import parse_args
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
            "--limit",
            "100",
        ]
    )
    assert args.continuous is True
    assert args.idle_seconds == 7.5
    assert args.concurrency == 12
    assert args.rate == 20.0
    assert args.limit == 100


def test_redirected_event_output_has_no_live_repaint_sequences() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)
    events("phase", {"phase": "PREFLIGHT"})
    events(
        "started",
        {
            "concurrency": 8,
            "rate_per_second": 15.0,
            "company_limit": None,
            "continuous": True,
            "idle_seconds": 5.0,
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
    assert "CYCLE" not in output


def test_json_status_output_stays_machine_readable() -> None:
    stream = StringIO()
    DeepPresenter(json_mode=True, stream=stream).status(_summary())
    assert json.loads(stream.getvalue()) == _summary()


def test_live_state_returns_to_running_when_collection_starts() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)
    events("idle", {"idle_seconds": 5.0})
    assert events.state["phase"] == "IDLE"

    events(
        "started",
        {
            "concurrency": 8,
            "rate_per_second": 15.0,
            "company_limit": None,
            "continuous": True,
            "idle_seconds": 5.0,
        },
    )
    assert events.state["phase"] == "RUNNING"


def test_collect_terminal_has_no_buffer_cycle_or_serving_panel() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)

    events.console.print(events._render())

    output = stream.getvalue()
    assert "Buffer" not in output
    assert "Cycle" not in output
    assert "Serving Projection" not in output
    assert "In flight" in output
