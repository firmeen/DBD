from __future__ import annotations

from dbd.core.settings import PROJECT_ROOT


def test_windows_launcher_keeps_campaign_guard_as_production_entry() -> None:
    launcher = (PROJECT_ROOT / "Run-DBD.ps1").read_text(encoding="utf-8")

    assert '"campaign"' in launcher
    assert '"guard"' in launcher
    assert '"collect"' not in launcher
    assert "GuardianArgs" in launcher
    assert "OrphanGraceSeconds" in launcher
    assert "RestartDelaySeconds" in launcher


def test_collector_documentation_is_explicitly_non_production() -> None:
    collector_doc = (PROJECT_ROOT / "docs" / "COLLECTOR.md").read_text(encoding="utf-8")

    assert "Diagnostic / Canary" in collector_doc
    assert "not** the production runtime" in collector_doc
    assert "Guardian -> Supervisor -> autoscaled distributed workers" in collector_doc
    assert "Redis shared admission/rate control" in collector_doc


def test_cli_keeps_campaign_as_production_runtime_and_collector_explicit() -> None:
    cli = (PROJECT_ROOT / "src" / "dbd" / "cli" / "main.py").read_text(encoding="utf-8")

    assert 'help="Supervise the production coordinated worker fleet"' in cli
    assert "Explicit single-process diagnostic/canary runner" in cli
    assert "prefer dbd-data collect" not in cli


def test_readme_keeps_campaign_as_operator_path() -> None:
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    assert "## Campaign" in readme
    assert "dbd-data campaign start" in readme
    assert "dbd-data campaign resume" in readme
    assert "DBD_REDIS_RATE_KEY" in readme
    assert "Collector (recommended)" not in readme


def test_distributed_claim_hot_path_does_not_take_global_handoff_lock() -> None:
    worker = (PROJECT_ROOT / "src" / "dbd" / "runtime" / "worker.py").read_text(
        encoding="utf-8"
    )

    assert "claim_fence = runtime_handoff_lock()" not in worker
    assert "await _assert_distributed_manager_free(repo)" in worker


def test_supervisor_startup_reconciles_mysql_instead_of_reusing_redis_progress() -> None:
    supervisor = (
        PROJECT_ROOT / "src" / "dbd" / "runtime" / "supervisor.py"
    ).read_text(encoding="utf-8")

    startup_marker = "# Reconcile durable MySQL truth once at Supervisor startup."
    assert startup_marker in supervisor
    startup = supervisor.split(startup_marker, 1)[1].split("has_work = _has_active_work", 1)[0]
    assert "baseline = _task_progress(run_id)" in startup
    assert "_runtime_progress_baseline(redis, run_id)" not in startup
