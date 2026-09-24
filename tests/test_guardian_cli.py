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
