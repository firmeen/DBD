from __future__ import annotations

from types import SimpleNamespace

import dbd.runtime.guardian as guardian
from dbd.presentation.guardian import GuardianDashboard


class QuietDashboard:
    def __init__(self) -> None:
        self.phases = []
        self.payloads = []

    def render(self, payload):
        self.phases.append(payload["phase"])
        self.payloads.append(payload)

    def suspend(self):
        pass

    def close(self, payload):
        self.phases.append(payload["phase"])
        self.payloads.append(payload)


def state(*, remaining: int, failed: int = 0):
    return {
        "run_status": "ready",
        "total": remaining + failed,
        "pending": remaining,
        "retry_wait": 0,
        "running": 0,
        "done": 0,
        "not_available": 0,
        "failed": failed,
    }


def probe():
    return guardian.RedisRecoveryProbe(
        stable=True,
        successful_pings=5,
        requested_pings=5,
        latency_ms=20.0,
        lease_ttl=-2,
    )


def test_profile_persists_operational_values_but_current_env_owns_rate_namespace(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        guardian,
        "settings",
        SimpleNamespace(dbd_redis_rate_key="dbd:production:global-rate"),
    )
    monkeypatch.setattr(
        guardian,
        "_runtime_profile",
        lambda run_id: {
            "workers": 6,
            "workers_min": 4,
            "workers_max": 10,
            "fleet_rate_initial": 16.0,
            "redis_rate_key": "dbd:dev:global-rate",
        },
    )

    config = guardian.resolve_guardian_config(
        8,
        overrides={
            "fleet_rate_max": 20.0,
            "redis_rate_key": "dbd:explicit:global-rate",
            "open_powershell": False,
        },
    )

    assert config.workers == 6
    assert config.workers_min == 4
    assert config.workers_max == 10
    assert config.fleet_rate_initial == 16.0
    assert config.fleet_rate_max == 20.0
    assert config.redis_rate_key == "dbd:explicit:global-rate"

    monkeypatch.setattr(
        guardian,
        "_runtime_profile",
        lambda run_id: {
            "workers": 2,
            "redis_rate_key": "dbd:dev:global-rate",
        },
    )
    restored = guardian.resolve_guardian_config(8)

    assert restored.workers == 6
    assert restored.fleet_rate_max == 20.0
    assert restored.redis_rate_key == "dbd:production:global-rate"


def test_supervisor_lease_is_clear_only_when_key_is_absent() -> None:
    assert guardian.RedisRecoveryProbe(True, 5, 5, lease_ttl=-2).lease_clear is True
    assert guardian.RedisRecoveryProbe(True, 5, 5, lease_ttl=-1).lease_clear is False
    assert guardian.RedisRecoveryProbe(True, 5, 5, lease_ttl=0).lease_clear is False
    assert guardian.RedisRecoveryProbe(True, 5, 5, lease_ttl=1).lease_clear is False


def test_invalid_redis_probe_timing_is_fatal(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)

    for config in (
        guardian.GuardianConfig(run_id=8, redis_sample_delay=-1.0),
        guardian.GuardianConfig(run_id=8, redis_latency_limit_ms=-1.0),
        guardian.GuardianConfig(run_id=8, redis_sample_delay=float("nan")),
        guardian.GuardianConfig(run_id=8, redis_latency_limit_ms=float("inf")),
    ):
        code = guardian.guard_campaign(config, dashboard=QuietDashboard())
        assert code == 64
        saved = guardian._read_json(guardian.guardian_state_path(8))
        assert saved["phase"] == "FATAL"
        assert saved["exit_code"] == 64


def test_redis_probe_is_read_only_and_requires_all_samples(monkeypatch) -> None:
    calls = []

    class FakeRedis:
        def ping(self):
            calls.append("ping")
            return True

        def ttl(self, key):
            calls.append(("ttl", key))
            return -2

        def smembers(self, key):
            calls.append(("smembers", key))
            return {"worker-a", "worker-stale"}

        def mget(self, keys):
            calls.append(("mget", tuple(keys)))
            return ['{"status":"active"}', None]

        def close(self):
            calls.append("close")

        def delete(self, *keys):
            raise AssertionError("Guardian recovery probe must never delete Redis keys")

    monkeypatch.setattr(guardian, "_redis_client", lambda: FakeRedis())
    monkeypatch.setattr(guardian.time, "sleep", lambda seconds: None)

    config = guardian.GuardianConfig(run_id=8, redis_stability_samples=5)
    result = guardian.probe_redis_control_plane(config)

    assert result.stable is True
    assert result.successful_pings == 5
    assert result.lease_ttl == -2
    assert result.registered_workers == 2
    assert result.live_workers == 1
    assert result.stale_memberships == 1
    assert calls.count("ping") == 5


def test_recovery_gate_waits_for_mysql_then_continues(monkeypatch) -> None:
    monkeypatch.setattr(guardian, "probe_redis_control_plane", lambda config: probe())
    monkeypatch.setattr(guardian.time, "sleep", lambda seconds: None)

    calls = []

    def status(run_id):
        calls.append(run_id)
        if len(calls) == 1:
            raise ConnectionError("temporary MySQL outage")
        return state(remaining=10)

    monkeypatch.setattr(guardian, "campaign_status", status)
    dashboard = QuietDashboard()

    durable, redis_probe = guardian._wait_for_recovery_gate(
        guardian.GuardianConfig(run_id=8),
        dashboard,
        recovery_count=0,
        package_health="healthy",
        last_failure=None,
    )

    assert calls == [8, 8]
    assert durable["pending"] == 10
    assert redis_probe.stable is True
    assert "WAIT_MYSQL" in dashboard.phases


def test_recovery_gate_treats_missing_run_as_fatal(monkeypatch) -> None:
    monkeypatch.setattr(guardian, "probe_redis_control_plane", lambda config: probe())
    monkeypatch.setattr(
        guardian,
        "campaign_status",
        lambda run_id: (_ for _ in ()).throw(ValueError("Run 999 not found")),
    )

    try:
        guardian._wait_for_recovery_gate(
            guardian.GuardianConfig(run_id=999),
            QuietDashboard(),
            recovery_count=0,
            package_health="healthy",
            last_failure=None,
        )
    except ValueError as exc:
        assert "not found" in str(exc)
    else:
        raise AssertionError("missing run must fail instead of waiting forever")


def test_recovery_gate_reclaims_only_confirmed_guardian_pid_leases(monkeypatch) -> None:
    monkeypatch.setattr(guardian, "probe_redis_control_plane", lambda config: probe())
    monkeypatch.setattr(guardian, "_recovery_supervisor_identity", lambda run_id: (4242, "a1b2c3d4e5f60718"))
    reclaimed = []
    monkeypatch.setattr(
        guardian,
        "reclaim_guardian_process_leases",
        lambda run_id, generation: reclaimed.append((run_id, generation)) or 12,
    )
    cleared = []
    monkeypatch.setattr(
        guardian,
        "_clear_recovery_marker",
        lambda run_id: cleared.append(run_id),
    )
    monkeypatch.setattr(guardian, "campaign_status", lambda run_id: state(remaining=10))

    dashboard = QuietDashboard()
    durable, redis_probe = guardian._wait_for_recovery_gate(
        guardian.GuardianConfig(run_id=8),
        dashboard,
        recovery_count=1,
        package_health="healthy",
        last_failure="hard_process_loss",
    )

    assert reclaimed == [(8, "a1b2c3d4e5f60718")]
    assert cleared == [8]
    assert durable["pending"] == 10
    assert redis_probe.stable is True
    assert "RECONCILING" in dashboard.phases


def test_recovery_gate_does_not_reclaim_without_confirmed_marker(monkeypatch) -> None:
    monkeypatch.setattr(guardian, "probe_redis_control_plane", lambda config: probe())
    monkeypatch.setattr(guardian, "_recovery_supervisor_identity", lambda run_id: None)

    def must_not_reclaim(run_id, generation):
        raise AssertionError("lease recovery requires a confirmed dead Guardian PID marker")

    monkeypatch.setattr(guardian, "reclaim_guardian_process_leases", must_not_reclaim)
    monkeypatch.setattr(guardian, "campaign_status", lambda run_id: state(remaining=10))

    durable, _ = guardian._wait_for_recovery_gate(
        guardian.GuardianConfig(run_id=8),
        QuietDashboard(),
        recovery_count=0,
        package_health="healthy",
        last_failure=None,
    )

    assert durable["pending"] == 10


def test_recovery_marker_survives_mysql_reconcile_failure(monkeypatch) -> None:
    monkeypatch.setattr(guardian, "probe_redis_control_plane", lambda config: probe())
    marker_values = iter([(4242, "a1b2c3d4e5f60718"), (4242, "a1b2c3d4e5f60718")])
    monkeypatch.setattr(
        guardian,
        "_recovery_supervisor_identity",
        lambda run_id: next(marker_values),
    )
    calls = []

    def reconcile(run_id, generation):
        calls.append((run_id, generation))
        if len(calls) == 1:
            raise ConnectionError("temporary MySQL outage")
        return 3

    monkeypatch.setattr(guardian, "reclaim_guardian_process_leases", reconcile)
    cleared = []
    monkeypatch.setattr(guardian, "_clear_recovery_marker", lambda run_id: cleared.append(run_id))
    monkeypatch.setattr(guardian, "campaign_status", lambda run_id: state(remaining=10))
    monkeypatch.setattr(guardian.time, "sleep", lambda seconds: None)

    durable, _ = guardian._wait_for_recovery_gate(
        guardian.GuardianConfig(run_id=8),
        QuietDashboard(),
        recovery_count=0,
        package_health="healthy",
        last_failure=None,
    )

    assert calls == [(8, "a1b2c3d4e5f60718"), (8, "a1b2c3d4e5f60718")]
    assert cleared == [8]
    assert durable["pending"] == 10


def test_guardian_recovers_same_run_after_supervisor_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(guardian, "package_environment_snapshot", lambda: {"health": "healthy"})
    monkeypatch.setattr(guardian.time, "sleep", lambda seconds: None)

    gate_values = iter([(state(remaining=10), probe()), (state(remaining=10), probe())])
    monkeypatch.setattr(guardian, "_wait_for_recovery_gate", lambda *a, **k: next(gate_values))

    calls = []

    def supervise(run_id, **kwargs):
        calls.append(run_id)
        if len(calls) == 1:
            raise RuntimeError("temporary Redis control plane failure")
        return 0

    monkeypatch.setattr(guardian, "supervise_campaign", supervise)
    monkeypatch.setattr(guardian, "campaign_status", lambda run_id: state(remaining=0))

    config = guardian.GuardianConfig(run_id=8, recovery_interval=0.01)
    dashboard = QuietDashboard()

    code = guardian._guard_loop(config, dashboard)

    assert code == 0
    assert calls == [8, 8]
    assert "RECOVERING" in dashboard.phases
    assert dashboard.phases[-1] == "COMPLETED"


def test_terminal_failures_never_trigger_supervisor_or_bulk_retry(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(guardian, "package_environment_snapshot", lambda: {"health": "healthy"})
    monkeypatch.setattr(
        guardian,
        "_wait_for_recovery_gate",
        lambda *a, **k: (state(remaining=0, failed=2300000), probe()),
    )

    def should_not_run(*args, **kwargs):
        raise AssertionError("must not resume a terminal run")

    monkeypatch.setattr(guardian, "supervise_campaign", should_not_run)

    code = guardian._guard_loop(
        guardian.GuardianConfig(run_id=8),
        QuietDashboard(),
    )

    assert code == 1


def test_operator_pause_is_not_auto_resumed(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(guardian, "package_environment_snapshot", lambda: {"health": "healthy"})
    monkeypatch.setattr(
        guardian,
        "_wait_for_recovery_gate",
        lambda *a, **k: (state(remaining=10), probe()),
    )
    calls = []
    monkeypatch.setattr(
        guardian,
        "supervise_campaign",
        lambda run_id, **kwargs: calls.append(run_id) or 130,
    )
    monkeypatch.setattr(
        guardian,
        "campaign_status",
        lambda run_id: state(remaining=7, failed=3),
    )
    dashboard = QuietDashboard()

    code = guardian._guard_loop(
        guardian.GuardianConfig(run_id=8),
        dashboard,
    )

    assert code == 130
    assert calls == [8]
    assert dashboard.phases[-1] == "PAUSED"
    assert dashboard.payloads[-1]["counts"]["total"] == 10
    assert dashboard.payloads[-1]["counts"]["failed"] == 3
    assert dashboard.payloads[-1]["counts"]["remaining"] == 7


def test_capacity_exhaustion_stops_guardian_recovery_churn(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(guardian, "package_environment_snapshot", lambda: {"health": "healthy"})
    monkeypatch.setattr(
        guardian,
        "_wait_for_recovery_gate",
        lambda *a, **k: (state(remaining=10), probe()),
    )
    calls = []
    monkeypatch.setattr(
        guardian,
        "supervise_campaign",
        lambda run_id, **kwargs: calls.append(run_id) or 2,
    )
    monkeypatch.setattr(
        guardian,
        "campaign_status",
        lambda run_id: state(remaining=10),
    )
    dashboard = QuietDashboard()

    code = guardian._guard_loop(
        guardian.GuardianConfig(run_id=8),
        dashboard,
    )

    assert code == 2
    assert calls == [8]
    assert dashboard.phases[-1] == "CAPACITY_EXHAUSTED"
    saved = guardian._read_json(guardian.guardian_state_path(8))
    assert saved["terminal"] is True
    assert saved["phase"] == "CAPACITY_EXHAUSTED"
    assert saved["exit_code"] == 2


def test_duplicate_guardian_cannot_persist_rejected_overrides(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    called = []

    class BusyLock:
        def __init__(self, run_id):
            self.run_id = run_id

        def __enter__(self):
            raise guardian.GuardianAlreadyRunning("already managed")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(guardian, "GuardianFileLock", BusyLock)
    monkeypatch.setattr(
        guardian,
        "resolve_guardian_config",
        lambda *args, **kwargs: called.append((args, kwargs)),
    )

    code = guardian.guard_campaign_run(
        8,
        overrides={"workers": 10, "fleet_rate_max": 99.0},
        dashboard=QuietDashboard(),
    )

    assert code == 75
    assert called == []
    assert not guardian.guardian_profile_path(8).exists()


def test_invalid_profile_resolution_returns_fatal_config(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)

    code = guardian.guard_campaign_run(
        8,
        overrides={"workers": 2, "workers_min": 4, "workers_max": 10},
        dashboard=QuietDashboard(),
    )

    assert code == 64
    saved = guardian._read_json(guardian.guardian_state_path(8))
    assert saved["phase"] == "FATAL"
    assert saved["exit_code"] == 64


def test_malformed_persisted_profile_type_is_fatal(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    profile = guardian.guardian_profile_path(8)
    profile.parent.mkdir(parents=True, exist_ok=True)
    profile.write_text('{"workers":"four"}\n', encoding="utf-8")
    monkeypatch.setattr(guardian, "_runtime_profile", lambda run_id: {})

    code = guardian.guard_campaign_run(
        8,
        dashboard=QuietDashboard(),
    )

    assert code == 64
    saved = guardian._read_json(guardian.guardian_state_path(8))
    assert saved["phase"] == "FATAL"
    assert saved["exit_code"] == 64
    assert "TypeError" in saved["message"]


def test_deterministic_supervisor_value_error_is_not_retried(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(guardian, "package_environment_snapshot", lambda: {"health": "healthy"})
    monkeypatch.setattr(
        guardian,
        "_wait_for_recovery_gate",
        lambda *a, **k: (state(remaining=10), probe()),
    )
    calls = []

    def invalid_supervisor(run_id, **kwargs):
        calls.append(run_id)
        raise ValueError("fleet resource budget is invalid")

    monkeypatch.setattr(guardian, "supervise_campaign", invalid_supervisor)

    try:
        guardian._guard_loop(
            guardian.GuardianConfig(run_id=8),
            QuietDashboard(),
        )
    except ValueError as exc:
        assert "resource budget" in str(exc)
    else:
        raise AssertionError("deterministic supervisor ValueError must escape recovery loop")

    assert calls == [8]


def test_local_guardian_lock_prevents_duplicate_controller(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    first = guardian.GuardianFileLock(8)
    second = guardian.GuardianFileLock(8)

    with first:
        try:
            second.__enter__()
        except guardian.GuardianAlreadyRunning:
            pass
        else:
            second.__exit__(None, None, None)
            raise AssertionError("duplicate local Guardian lock unexpectedly succeeded")


def test_powershell_watchdog_contract() -> None:
    script = (guardian.PROJECT_ROOT / "Run-DBD.ps1").read_text(encoding="utf-8")

    # Windows PowerShell 5.1 can misdecode UTF-8 without BOM using a legacy
    # code page. Keeping the watchdog source ASCII-only makes parsing invariant.
    assert script.isascii()
    assert '"-m", "dbd", "campaign", "guard"' in script
    assert '[switch]$Headless' in script
    assert '[switch]$OpenPowerShell' in script
    assert '$commandArgs += "--headless"' in script
    assert '$commandArgs += "--open-powershell"' in script
    assert '$SourceRoot = Join-Path $Root "src"' in script
    assert 'Initialize-ProjectPythonPath' in script
    assert 'PYTHONPATH' in script
    assert 'Assert-DbdImport' in script
    assert "bulk-retry" not in script.lower()
    assert "flushdb" not in script.lower()
    assert "flushall" not in script.lower()
    assert "run-id" in script
    assert "taskkill.exe" in script
    assert "Get-GuardianOwnedWorkers" in script
    assert "$WorkerRegistryPath" in script
    assert "Get-WorkerRegistry" in script
    assert '-Filter ("ProcessId = {0}" -f $pidValue)' in script
    assert "Test-GuardianDescendant" in script
    assert "ParentProcessId" in script
    assert "$state.pid" in script
    assert "Get-OrphanWorkers" not in script
    assert "if ($exitCode -eq 2)" in script
    assert "Write-RecoveryMarker" in script
    assert "supervisor_pid" in script
    assert "$remainingOwned.Count -eq 0" in script
    assert "Get-CimInstance Win32_Process -OperationTimeoutSec 5 -ErrorAction Stop" in script
    assert "-SupervisorGeneration $lostSupervisorGeneration" in script
    assert "Get-CimInstance Win32_Process -ErrorAction SilentlyContinue" not in script
    assert "powershell(\\.exe)?|pwsh" in script
    assert "IsNullOrWhiteSpace" in script
    assert "supervisor_generation" in script
    assert "$ownershipVerified = $false" in script
    assert "$LASTEXITCODE -eq 0" in script
    assert "Recovery marker is withheld" in script


def test_dashboard_can_render_recovery_snapshot_without_terminal() -> None:
    from io import StringIO

    from rich.console import Console

    stream = StringIO()
    dashboard = GuardianDashboard(
        Console(file=stream, force_terminal=False, width=120, highlight=False)
    )
    dashboard.render(
        {
            "run_id": 8,
            "phase": "RECOVERING",
            "message": "Redis outage",
            "recovery_count": 2,
            "package_health": "healthy",
            "counts": {
                "total": 100,
                "terminal": 40,
                "done": 20,
                "not_available": 10,
                "failed": 10,
                "remaining": 60,
            },
            "redis": {
                "stable": False,
                "pings": "2/5",
                "lease_ttl": -2,
                "live_workers": 0,
                "stale_memberships": 0,
            },
            "profile": {
                "workers": 6,
                "workers_min": 4,
                "workers_max": 10,
                "worker_concurrency": 12,
                "fleet_rate_initial": 16,
                "fleet_rate_max": 20,
                "redis_rate_key": "dbd:production:global-rate",
            },
            "safety": {
                "duplicate_supervisor": "FENCED",
                "auto_bulk_retry": "DISABLED",
                "blind_redis_cleanup": "DISABLED",
                "durable_truth": "MYSQL",
            },
        }
    )

    rendered = stream.getvalue()
    assert "CAMPAIGN GUARDIAN" in rendered
    assert "RECOVERING" in rendered
    assert "AUTO BULK RETRY" in rendered
    assert "DISABLED" in rendered


def test_request_stall_reenters_same_run_recovery(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(guardian, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        guardian,
        "package_environment_snapshot",
        lambda: {"health": "healthy"},
    )

    gates = {"count": 0}

    def recovery_gate(*args, **kwargs):
        gates["count"] += 1
        if gates["count"] > 1:
            raise KeyboardInterrupt
        return state(remaining=10), probe()

    monkeypatch.setattr(guardian, "_wait_for_recovery_gate", recovery_gate)
    monkeypatch.setattr(
        guardian,
        "supervise_campaign",
        lambda run_id, **kwargs: guardian._EXIT_REQUEST_STALLED,
    )
    monkeypatch.setattr(
        guardian,
        "campaign_status",
        lambda run_id: state(remaining=10),
    )
    monkeypatch.setattr(guardian.time, "sleep", lambda seconds: None)

    dashboard = QuietDashboard()
    try:
        guardian._guard_loop(
            guardian.GuardianConfig(run_id=9, recovery_interval=1.0),
            dashboard,
        )
    except KeyboardInterrupt:
        pass

    assert "RECOVERING_STALL" in dashboard.phases
    saved = guardian._read_json(guardian.guardian_state_path(9))
    assert saved["phase"] == "RECOVERING_STALL"
    assert saved["terminal"] is False
    assert "same run" in saved["message"].lower()
