from __future__ import annotations

import json
from io import StringIO
from types import SimpleNamespace

import dbd.presentation.terminal as terminal_module
from dbd.presentation import OutputOptions, TerminalPresenter


def presenter(*, json_mode=False, verbose=False, debug=False):
    stream = StringIO()
    view = TerminalPresenter(
        OutputOptions(
            json=json_mode,
            verbose=verbose,
            debug=debug,
            no_color=True,
        ),
        stream=stream,
        force_terminal=False,
    )
    return view, stream


def campaign_payload():
    return {
        "total": 9_206_715,
        "pending": 8_015_459,
        "retry_wait": 0,
        "running": 4_714,
        "failed": 52_112,
        "done": 610_898,
        "not_available": 523_532,
        "progress_source": "redis_worker_heartbeat",
        "progress_exact": False,
        "run_id": 8,
        "run_status": "ready",
        "workers": 9,
        "worker_states": {
            "active": 9,
            "draining": 0,
            "stale": 0,
            "unknown": 0,
        },
        "worker_metrics": {
            "requests": 3_081,
            "http_retries": 977,
            "rate_limit_hits": 0,
            "redis_commands": 3_177,
            "redis_pool_in_use": 2,
            "redis_pool_max": 36,
            "fetch_queue": 4_607,
            "archive_queue": 0,
            "project_queue": 0,
            "fetch_active": 108,
            "archive_active": 0,
            "project_active": 0,
        },
        "worker_details": [
            {
                "worker_id": "local-8-1-2b0e231f",
                "status": "active",
                "pid": 520684,
                "heartbeat_age_seconds": 3.1,
                "requests": 179,
                "http_retries": 29,
                "failed": 108,
                "inflight": 524,
                "fetch_queue": 512,
            }
        ],
        "effective_rate": 20.0,
        "rate_target": 20.0,
        "stop_requested": False,
        "supervisor_active": True,
        "supervisor": {
            "status": "running",
            "workers_target": 10,
            "workers_min": 4,
            "workers_max": 10,
            "restarts": 3,
            "exhausted_capacity": 1,
            "http_rate_observed": 17.33,
            "rate_independent": True,
            "db_connections_budget": 96,
            "redis_connections_budget": 128,
            "last_worker_failure": {
                "worker_index": 10,
                "reason": "exited 1",
                "restart_count": 3,
                "restart_budget": 3,
                "log_path": "data/logs/campaign-8-worker-10.log",
            },
        },
    }


def test_campaign_status_is_human_first_and_explains_live_semantics():
    view, stream = presenter()

    view.campaign_status(campaign_payload())

    output = stream.getvalue()
    assert "DBD DATA PLATFORM · CAMPAIGN 8" in output
    assert "LIVE · Redis worker heartbeat" in output
    assert "Failed resources" in output
    assert "52,112" in output
    assert "Fleet lost 1 worker capacity" in output
    assert "Worker 10 exited 1" in output
    assert "without recorded 429/503 hits" in output
    assert '"worker_details"' not in output


def test_campaign_verbose_adds_worker_table_without_raw_dict_dump():
    view, stream = presenter(verbose=True)

    view.campaign_status(campaign_payload())

    output = stream.getvalue()
    assert "WORKERS" in output
    assert "520684" in output
    assert "local-8-1-2b0e231f" not in output
    assert "{'worker_id'" not in output


def test_campaign_debug_surfaces_worker_log_path():
    view, stream = presenter(debug=True)

    view.campaign_status(campaign_payload())

    output = stream.getvalue()
    assert "DEBUG CONTEXT" in output
    assert "campaign-8-worker-10.log" in output


def test_campaign_json_preserves_machine_readable_raw_payload():
    payload = campaign_payload()
    view, stream = presenter(json_mode=True)

    view.campaign_status(payload)

    decoded = json.loads(stream.getvalue())
    assert decoded["run_id"] == 8
    assert decoded["worker_details"][0]["pid"] == 520684
    assert decoded["supervisor"]["last_worker_failure"]["reason"] == "exited 1"


def test_bulk_status_explains_fabric_failure_units_and_refresh_plan():
    view, stream = presenter()
    payload = {
        "scheduler_mode": "fabric_v2",
        "status": {
            "pending": 8_014_207,
            "retry_wait": 843,
            "running": 4_716,
            "failed": 52_507,
            "done": 610_907,
            "not_available": 523_535,
        },
        "stages": [
            {"stage": "fetch", "status": "pending", "n": 1_156_496},
            {"stage": "fetch", "status": "failed", "n": 11_887},
        ],
        "refresh": {
            "companies": 1_315_245,
            "changed": 0,
            "unchanged": 1_315_245,
            "stale": 1_177_920,
            "forced": 0,
            "skipped": 153_194,
            "selected_resources": 9_206_715,
            "planned_resources": 8_084_697,
            "http_calls": 4_186,
            "http_calls_per_company": 0.0032,
        },
        "diagnostics": {
            "semantics": (
                "Fabric failed resources come from failed_mask bits; stage/error counts "
                "are failed company-state rows, so those units intentionally differ."
            ),
            "by_error": [{"value": "TimeoutError", "n": 9_000}],
            "by_endpoint": [{"value": "financial_statements", "n": 21_000}],
            "by_stage": [{"value": "fetch", "n": 11_887}],
        },
    }

    view.bulk_status(8, payload)

    output = stream.getvalue()
    assert "DBD RUN 8 · DURABLE STATUS" in output
    assert "EXACT · MySQL durable state" in output
    assert "Failed resources" in output
    assert "52,507" in output
    assert "11,887" in output
    assert "failed_mask bits" in output
    assert "financial_statements" in output
    assert "TimeoutError" in output
    assert "Stale" in output
    assert "1,177,920" in output


def test_master_and_discovery_status_are_tables_not_default_json():
    view, stream = presenter()
    view.master_status(
        {
            "total": 100,
            "conflicts": 2,
            "eligible_all_statuses": 98,
            "eligible_operating": 80,
            "unknown_status": 5,
        }
    )
    view.discovery_status(
        {
            "unique_companies": 240_597,
            "rows_fetched": 285_929,
            "buckets": 9_101,
            "buckets_done": 628,
            "buckets_split": 191,
            "buckets_open": 8_261,
            "buckets_failed": 21,
            "buckets_uncounted": 8_260,
            "partitions_lossy": 1,
            "pages_done": 28_923,
            "pages_failed": 40,
            "pages_partial": 0,
            "unresolved_rejections": 3,
            "remaining_counted_pages": 79,
        }
    )

    output = stream.getvalue()
    assert "MASTER REGISTRY" in output
    assert "DISCOVERY STATUS" in output
    assert "240,597" in output
    assert "Lossy partitions" in output
    assert "block completeness" in output
    assert '"unique_companies"' not in output


def test_process_result_json_is_one_document():
    view, stream = presenter(json_mode=True)

    view.process_result(12, {"done": 5, "failed": 0})

    assert json.loads(stream.getvalue()) == {
        "run_id": 12,
        "done": 5,
        "failed": 0,
    }
    assert stream.getvalue().count("\n") > 1  # pretty JSON, still one document


def test_presentation_layer_has_no_runtime_or_storage_clients():
    namespace = vars(terminal_module)
    forbidden = {"Redis", "connect", "create_engine", "WorkerRepository", "AsyncDbdClient"}
    assert forbidden.isdisjoint(namespace)
