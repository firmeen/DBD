from __future__ import annotations

import json
from io import StringIO

import pytest

from dbd.deep._version import __version__ as deep_version
from dbd.deep.cli import parse_args
from dbd.deep.presentation import DeepPresenter, DeepTerminalEvents


def _summary() -> dict:
    return {
        "generation": 2,
        "phase": "core",
        "generation_state": {
            "finalized_generation": 1,
            "last_seen_generation": 2,
            "active_generation": 2,
            "unfinished_rows": 12,
            "next_generation": 2,
        },
        "summary": {
            "generation": 2,
            "phase_mask": 127,
            "total": 30,
            "discovered": 0,
            "pending": 1,
            "collecting": 2,
            "retry_wait": 1,
            "complete": 25,
            "partial": 1,
            "failed": 0,
            "unresolved": 4,
            "active_leases": 2,
            "expired_leases": 0,
        },
        "coverage": None,
    }


def test_deep_version_is_separate_from_distribution_version(capsys) -> None:
    assert deep_version == "0.1.0"
    with pytest.raises(SystemExit) as exc:
        parse_args(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == "dbd-deep 0.1.0"


def test_prepare_retry_and_status_have_phase_scope() -> None:
    assert parse_args(["prepare", "--phase", "extended"]).phase == "extended"
    assert parse_args(["retry", "--phase", "extended"]).phase == "extended"
    assert parse_args(["status", "--phase", "extended"]).phase == "extended"


def test_collect_options_remain_explicit_without_mysql_pool_surface() -> None:
    args = parse_args(
        [
            "collect",
            "--phase",
            "core",
            "--continuous",
            "--idle-seconds",
            "7.5",
            "--concurrency",
            "12",
            "--rate",
            "20",
            "--http-retries",
            "2",
            "--limit",
            "100",
        ]
    )
    assert args.phase == "core"
    assert args.continuous is True
    assert args.idle_seconds == 7.5
    assert args.concurrency == 12
    assert args.rate == 20.0
    assert args.http_retries == 2
    assert args.limit == 100


def test_redirected_event_output_has_no_live_repaint_sequences() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)
    events("phase_started", {"phase": "PREFLIGHT", "generation": 2})
    events(
        "started",
        {
            "generation": 2,
            "phase": "core",
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
    assert "PHASE PREFLIGHT generation=2" in output
    assert "dbd_company_deep" not in output.lower()


def test_json_status_output_stays_machine_readable() -> None:
    stream = StringIO()
    DeepPresenter(json_mode=True, stream=stream).status(_summary())
    assert json.loads(stream.getvalue()) == _summary()


def test_status_output_uses_generation_not_master_runtime_language() -> None:
    stream = StringIO()
    DeepPresenter(stream=stream).status(_summary())
    output = stream.getvalue()
    assert "Generation" in output
    assert "Pending" in output
    assert "Retry wait" in output
    assert "Master waiting" not in output
    assert "Deep runtime" not in output


def test_collect_terminal_describes_direct_company_claim() -> None:
    stream = StringIO()
    events = DeepTerminalEvents("collect", stream=stream)
    events.console.print(events._render())
    output = stream.getvalue()
    assert "Buffer" not in output
    assert "Serving Projection" not in output
    assert "In flight" in output
    assert "DBD_COMPANY" in output
    assert "dbd_company_deep" not in output.lower()
