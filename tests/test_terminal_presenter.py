from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from io import StringIO

from dbd.presentation import TerminalPresenter
from dbd.presentation.v06 import V06Terminal


def presenter(*, json_mode=False, verbose=False):
    stream = StringIO()
    return TerminalPresenter(json_mode=json_mode, verbose=verbose, stream=stream), stream


def test_campaign_status_is_human_first_and_explains_live_vs_exact() -> None:
    output, stream = presenter()
    output.campaign_status(
        {
            "run_id": 8,
            "run_status": "ready",
            "total": 9_206_715,
            "pending": 8_015_459,
            "running": 4_714,
            "failed": 52_112,
            "done": 610_898,
            "not_available": 523_532,
            "progress_source": "redis_worker_heartbeat",
            "progress_exact": False,
            "workers": 9,
            "worker_states": {"active": 9, "draining": 0, "stale": 0, "unknown": 0},
            "worker_metrics": {
                "fetch_queue": 4_607,
                "fetch_active": 108,
                "archive_queue": 0,
                "archive_active": 0,
                "project_queue": 0,
                "project_active": 0,
                "requests": 3_081,
                "http_retries": 977,
                "rate_limit_hits": 0,
                "redis_commands": 3_177,
                "redis_pool_in_use": 2,
                "redis_pool_max": 36,
            },
            "effective_rate": 20.0,
            "rate_target": 20.0,
            "supervisor_active": True,
            "supervisor": {
                "workers_target": 10,
                "workers_max": 10,
                "effective_worker_max": 9,
                "restarts": 3,
                "exhausted_capacity": 1,
                "http_rate_observed": 17.33,
            },
            "failure_diagnostics": {
                "by_endpoint": [
                    {"endpoint": "profile", "states": 100, "resources": 400},
                ],
                "latest_errors": [
                    {
                        "error_code": "TimeoutError",
                        "stage": "fetch",
                        "status": "failed",
                        "states": 90,
                    }
                ],
                "error_scope": "state_latest_error_not_per_endpoint",
            },
        }
    )

    text = stream.getvalue()
    assert "DBD DATA PLATFORM · CAMPAIGN STATUS" in text
    assert "LIVE · Redis worker heartbeat" in text
    assert "FAILED RESOURCES" in text
    assert "52,112" in text
    assert "Fleet lost 1 worker-capacity slot" in text
    assert "without 429/503" in text
    assert "profile" in text
    assert "400 resource(s)" in text


def test_campaign_verbose_separates_exact_endpoint_failures_from_latest_errors() -> None:
    output, stream = presenter(verbose=True)
    output.campaign_status(
        {
            "run_id": 8,
            "run_status": "ready",
            "total": 10,
            "failed": 3,
            "progress_exact": True,
            "worker_details": [
                {
                    "worker_id": "local-8-1-test",
                    "pid": 123,
                    "status": "active",
                    "heartbeat_age_seconds": 1.2,
                    "claimed": 5,
                    "done": 2,
                    "not_available": 1,
                    "failed": 1,
                    "http_retries": 2,
                    "inflight": 1,
                }
            ],
            "failure_diagnostics": {
                "by_endpoint": [
                    {"endpoint": "committees", "states": 2, "resources": 2},
                ],
                "latest_errors": [
                    {
                        "error_code": "TimeoutError",
                        "stage": "fetch",
                        "status": "failed",
                        "states": 1,
                    }
                ],
                "error_scope": "state_latest_error_not_per_endpoint",
            },
        }
    )

    text = stream.getvalue()
    assert "EXACT · MySQL reconciliation" in text
    assert "WORKER DETAIL" in text
    assert "FAILED RESOURCES BY ENDPOINT" in text
    assert "Exact durable endpoint-bit counts" in text
    assert "LATEST STATE ERRORS · CONTEXT ONLY" in text


def test_bulk_status_labels_resource_counts_and_company_stage_counts() -> None:
    output, stream = presenter()
    output.bulk_status(
        {
            "run_id": 8,
            "scheduler_mode": "fabric_v2",
            "status": {
                "pending": 100,
                "retry_wait": 2,
                "running": 3,
                "failed": 20,
                "done": 30,
                "not_available": 40,
            },
            "stages": [
                {"stage": "fetch", "status": "failed", "n": 5},
            ],
            "refresh": {
                "companies": 100,
                "changed": 0,
                "unchanged": 100,
                "stale": 80,
                "forced": 0,
                "skipped": 20,
                "selected_resources": 700,
                "planned_resources": 560,
                "http_calls": 42,
                "http_calls_per_company": 0.42,
            },
            "failures": {
                "by_endpoint": [
                    {"endpoint": "profile", "states": 4, "resources": 12},
                ],
                "latest_errors": [],
                "error_scope": "state_latest_error_not_per_endpoint",
            },
        }
    )

    text = stream.getvalue()
    assert "EXACT · MySQL durable scheduler state" in text
    assert "FAILED RESOURCES" in text
    assert "COMPANY STATE / STAGE" in text
    assert "REFRESH PLAN" in text
    assert "FAILED RESOURCES BY ENDPOINT" in text


def test_json_mode_preserves_machine_readable_status() -> None:
    output, stream = presenter(json_mode=True)
    payload = {"run_id": 8, "status": {"failed": 2}, "worker_details": [{"pid": 123}]}

    output.campaign_status(payload)

    assert json.loads(stream.getvalue()) == payload


def test_master_status_is_not_raw_json_by_default() -> None:
    output, stream = presenter()
    output.master_status(
        {
            "total": 100,
            "conflicts": 2,
            "eligible_all_statuses": 98,
            "eligible_operating": 70,
            "unknown_status": 3,
        }
    )

    text = stream.getvalue()
    assert "DBD · MASTER STATUS" in text
    assert "ELIGIBLE" in text
    assert not text.lstrip().startswith("{")


def test_package_about_renders_version_mismatch_and_actions() -> None:
    output, stream = presenter(verbose=True)
    output.package_about(
        {
            "health": "mismatch",
            "identity": {
                "name": "dbd-data",
                "source_version": "0.4.0",
                "installed_version": "0.3.0",
                "version_match": False,
                "summary": "DBD DataWarehouse company data ingestion into MySQL",
                "home_page": "https://github.com/firmeen/DBD",
                "author": "Mr. Afimeen Usman",
                "author_email": "Afirmeen13.us@gmail.com",
                "license": "-",
            },
            "runtime": {
                "python": "3.11.3",
                "implementation": "CPython",
                "executable": r"C:\project\DBD\.venv\Scripts\python.exe",
                "virtualenv": True,
                "venv_path": r"C:\project\DBD\.venv",
                "platform": "Windows-11",
            },
            "installation": {
                "package_path": r"C:\project\DBD\src\dbd",
                "project_root": r"C:\project\DBD",
                "distribution_location": r"C:\project\DBD\.venv\Lib\site-packages",
                "metadata_location": r"C:\project\DBD\.venv\Lib\site-packages\dbd_data-0.3.0.dist-info",
                "editable": True,
                "editable_project_location": r"C:\project\DBD",
                "dependencies": ["redis", "rich"],
                "dependency_count": 2,
                "required_by": [],
            },
            "integrity": {
                "invalid_distribution_entries": [
                    r"C:\project\DBD\.venv\Lib\site-packages\~bd-data"
                ],
                "invalid_distribution_count": 1,
                "issues": [
                    {
                        "code": "version_mismatch",
                        "severity": "error",
                        "message": "Source version 0.4.0 differs from installed metadata 0.3.0.",
                    },
                    {
                        "code": "invalid_distribution_remnants",
                        "severity": "warning",
                        "message": "Found 1 invalid '~*' distribution remnant(s) in site-packages.",
                    },
                ],
            },
            "actions": [
                "python -m pip install -e .",
                "Inspect the listed '~*' site-packages entries and remove only confirmed stale remnants.",
            ],
        }
    )

    text = stream.getvalue()
    assert "DBD · PACKAGE INTELLIGENCE" in text
    assert "VERSION MISMATCH" in text
    assert "SOURCE" in text and "0.4.0" in text
    assert "INSTALLED" in text and "0.3.0" in text
    assert "PACKAGE PROFILE" in text
    assert "VIRTUALENV" in text
    assert "EDITABLE" in text
    assert "~bd-data" in text
    assert "RECOMMENDED ACTIONS" in text
    assert "python -m pip install -e ." in text
    assert "DIRECT DEPENDENCIES" in text


def test_package_about_json_mode_preserves_snapshot() -> None:
    output, stream = presenter(json_mode=True)
    payload = {
        "health": "healthy",
        "identity": {"source_version": "0.4.0", "installed_version": "0.4.0"},
    }

    output.package_about(payload)

    assert json.loads(stream.getvalue()) == payload



def _v06_wall_clock():
    return datetime(2026, 10, 7, 10, 22, 41, tzinfo=timezone(timedelta(hours=7)))


def test_v06_plain_output_has_start_time_and_no_repaint_sequences() -> None:
    stream = StringIO()
    terminal = V06Terminal(
        "discover",
        app="DBD DATA",
        stream=stream,
        tty=False,
        clock=lambda: 100.0,
        wall_clock=_v06_wall_clock,
    ).start(phase="core")
    terminal.event("phase_started", {"phase": "DISCOVER", "generation": 12})
    terminal.event(
        "discovery_progress",
        {
            "buckets_processed": 8,
            "pages_fetched": 21,
            "companies_written": 144,
            "requests_made": 22,
            "invalid_rows": 0,
        },
    )
    terminal.event(
        "phase_completed",
        {
            "phase": "DISCOVER",
            "generation": 12,
            "partitions": 8,
            "pages": 21,
            "companies": 144,
            "requests": 22,
            "invalid_rows": 0,
        },
    )
    terminal.finish(
        {
            "generation": 12,
            "phase": "core",
            "partitions": 8,
            "pages": 21,
            "companies": 144,
            "requests": 22,
            "invalid_rows": 0,
        }
    )

    output = stream.getvalue()
    assert "DBD DATA / DISCOVER" in output
    assert "Started: 2026-10-07 10:22:41 +07:00" in output
    assert "DISCOVER complete" in output
    assert "Duration: 00:00:00" in output
    assert "\r" not in output
    assert "\x1b[" not in output
    assert "ETA" not in output
    assert "Actual rate" not in output
    assert not output.lstrip().startswith("{")


def test_v06_semantic_complete_with_failures_is_yellow_not_green() -> None:
    terminal = V06Terminal("collect", stream=StringIO(), tty=False)
    assert terminal._status_style("COMPLETE_WITH_FAILURES") == "bold yellow"
    assert terminal._status_style("COMPLETE") == "bold green"
    assert terminal._status_style("FAILED") == "bold red"


def test_v06_compact_render_keeps_exact_numbers_and_configured_rate() -> None:
    stream = StringIO()
    terminal = V06Terminal(
        "collect",
        app="DBD DEEP",
        stream=stream,
        tty=False,
        clock=lambda: 100.0,
        wall_clock=_v06_wall_clock,
    ).start(phase="core", concurrency=24, rate=60)
    terminal.event(
        "started",
        {
            "generation": 12,
            "phase": "core",
            "concurrency": 24,
            "rate_per_second": 60.0,
            "company_limit": None,
            "continuous": False,
            "idle_seconds": 5.0,
            "active_endpoints": ["profile"],
        },
    )
    terminal.event(
        "progress",
        {
            "companies_claimed": 14821,
            "companies_finished": 14797,
            "companies_in_flight": 24,
            "companies_retry_wait": 18,
            "endpoints_done": 61225,
            "endpoints_not_available": 7991,
            "endpoints_failed": 42,
            "leases_recovered": 0,
        },
    )
    terminal.console.print(terminal._render(72))
    terminal.close()

    output = stream.getvalue()
    assert "14,821" in output
    assert "14.8k" not in output
    assert "Configured rate" in output
    assert "60" in output
    assert "ETA" not in output


def test_v06_cycle_wait_is_truthful_local_countdown() -> None:
    current = [100.0]
    stream = StringIO()
    terminal = V06Terminal(
        "cycle-loop",
        stream=stream,
        tty=False,
        clock=lambda: current[0],
        wall_clock=_v06_wall_clock,
    ).start(phase="core")
    terminal.event(
        "cycle_completed",
        {
            "status": "COMPLETE",
            "generation": 12,
            "phase": "core",
            "resumed": False,
            "discovery": {},
            "prepare": {},
            "collection": {},
            "verification": {},
            "finalized_rows": 10,
        },
    )
    terminal.event("cycle_waiting", {"interval_seconds": 60.0, "iteration": 1})
    current[0] = 115.0
    terminal.console.print(terminal._render(100))
    terminal.close()

    output = stream.getvalue()
    assert "NEXT CYCLE" in output
    assert "WAITING" in output
    assert "00:00:45" in output


def test_v06_event_renderer_failure_isolated_from_runtime(monkeypatch) -> None:
    terminal = V06Terminal("collect", stream=StringIO(), tty=False).start(phase="core")

    def broken_event(_name, _payload):
        raise RuntimeError("renderer fault")

    monkeypatch.setattr(terminal, "_apply_event", broken_event)
    terminal.event("progress", {"companies_finished": 1})

    assert terminal._renderer_failed is True
    terminal.close()



def test_v06_plain_phase_milestones_dedupe_adjacent_runtime_sources() -> None:
    stream = StringIO()
    terminal = V06Terminal(
        "cycle-run",
        stream=stream,
        tty=False,
        clock=lambda: 100.0,
        wall_clock=_v06_wall_clock,
    ).start(phase="core")

    payload = {"phase": "COLLECT", "generation": 12}
    terminal.event("phase_started", payload)
    terminal.event("phase_started", payload)
    terminal.event("phase_completed", payload)
    terminal.event("phase_completed", payload)
    terminal.close()

    output = stream.getvalue()
    assert output.count("COLLECT started generation=12") == 1
    assert output.count("COLLECT complete generation=12") == 1


def test_v06_endpoint_failure_is_degraded_but_not_process_fatal() -> None:
    terminal = V06Terminal(
        "collect",
        stream=StringIO(),
        tty=False,
        clock=lambda: 100.0,
        wall_clock=_v06_wall_clock,
    ).start(phase="core")
    terminal.event(
        "endpoint_failed",
        {"company_id": 42, "endpoint": "profile", "http_status": 404},
    )

    assert terminal.state.status == "RUNNING"
    assert terminal._active_status() == "DEGRADED"
    assert terminal.state.last_warning == "profile failed"
    terminal.close()


def test_v06_completion_settle_uses_static_motion_frame() -> None:
    current = [100.0]
    terminal = V06Terminal(
        "collect",
        stream=StringIO(),
        tty=False,
        clock=lambda: current[0],
        wall_clock=_v06_wall_clock,
    ).start(phase="core")
    terminal.state.transient_kind = "settle"
    terminal.state.transient_until = 101.0

    assert set(terminal._motion("stream", 100)) == {"━"}
    assert terminal._motion("orbit") == "◆"
    terminal.close()
