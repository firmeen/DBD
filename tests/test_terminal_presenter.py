from __future__ import annotations

import json
from io import StringIO

from dbd.presentation import TerminalPresenter


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
