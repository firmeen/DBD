from __future__ import annotations

from types import SimpleNamespace

from dbd.cli import main as cli_main
from dbd.runtime import guardian


def test_campaign_guard_passes_only_explicit_overrides(monkeypatch) -> None:
    captured = {}

    def guard_run(run_id, *, overrides):
        captured["run_id"] = run_id
        captured["overrides"] = overrides
        return 0

    monkeypatch.setattr(guardian, "guard_campaign_run", guard_run)

    args = SimpleNamespace(
        campaign_command="guard",
        run_id=8,
        workers=None,
        workers_min=None,
        workers_max=None,
        worker_concurrency=None,
        worker_rate=None,
        rate_mode=None,
        fleet_rate_initial=None,
        fleet_rate_max=20.0,
        fleet_db_connections_max=None,
        fleet_archive_workers_max=None,
        fleet_projection_workers_max=None,
        fleet_redis_connections_max=None,
        max_restarts=None,
        open_powershell=None,
        redis_rate_key=None,
        recovery_interval=None,
    )

    cli_main._campaign_command(args, SimpleNamespace())

    assert captured["run_id"] == 8
    assert captured["overrides"]["fleet_rate_max"] == 20.0
    assert captured["overrides"]["workers"] is None


def test_campaign_recover_plan_is_read_only_and_reports_next_command(monkeypatch) -> None:
    captured = {}

    monkeypatch.setattr(
        "dbd.runtime.supervisor.campaign_status",
        lambda run_id: {"run_id": run_id, "workers": 0, "running": 0},
    )

    async def fake_recover(args):
        return {
            "source_run_id": args.source_run_id,
            "retry_resources": 123,
            "action": "plan_only",
            "next_command": f"dbd-data campaign recover {args.source_run_id} --execute",
        }

    monkeypatch.setattr(cli_main, "_recover_campaign", fake_recover)

    class Presenter:
        def command_result(self, title, payload):
            captured["title"] = title
            captured["payload"] = payload

    args = SimpleNamespace(
        campaign_command="recover",
        source_run_id=8,
        execute=False,
        recovery_run_id=None,
    )

    cli_main._campaign_command(args, Presenter())

    assert captured["title"] == "DBD · FABRIC RECOVERY"
    assert captured["payload"]["source_run_id"] == 8
    assert captured["payload"]["retry_resources"] == 123
    assert captured["payload"]["next_command"].endswith("8 --execute")


def test_campaign_recover_refuses_active_source(monkeypatch) -> None:
    monkeypatch.setattr(
        "dbd.runtime.supervisor.campaign_status",
        lambda run_id: {"run_id": run_id, "workers": 1, "running": 1},
    )
    args = SimpleNamespace(
        campaign_command="recover",
        source_run_id=8,
        execute=False,
        recovery_run_id=None,
    )

    try:
        cli_main._campaign_command(args, SimpleNamespace())
    except ValueError as exc:
        assert "still active" in str(exc)
    else:
        raise AssertionError("active source run must be rejected")
