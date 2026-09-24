from __future__ import annotations

import hashlib
import json
import math
import os
import socket
import time
from contextlib import suppress
from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from redis import Redis

from ..core.package_info import package_environment_snapshot
from ..core.settings import PROJECT_ROOT, settings
from ..presentation.guardian import GuardianDashboard
from .supervisor import (
    campaign_status,
    reclaim_guardian_process_leases,
    supervise_campaign,
)

_GUARDIAN_PROFILE_VERSION = 1
_GUARDIAN_STATE_VERSION = 1
_EXIT_CONFIG = 64
_EXIT_ALREADY_MANAGED = 75
_EXIT_PAUSED = 130


class GuardianAlreadyRunning(RuntimeError):
    pass


@dataclass(frozen=True)
class GuardianConfig:
    run_id: int
    supervisor_generation: str = ""
    workers: int = 4
    workers_min: int = 4
    workers_max: int = 4
    worker_concurrency: int = 8
    worker_rate: float = settings.dbd_rate_per_second
    rate_mode: str = "adaptive"
    fleet_rate_initial: float | None = None
    fleet_rate_max: float | None = None
    fleet_db_connections_max: int = settings.dbd_fleet_db_connections_max
    fleet_archive_workers_max: int = settings.dbd_fleet_archive_workers_max
    fleet_projection_workers_max: int = settings.dbd_fleet_projection_workers_max
    fleet_redis_connections_max: int = settings.dbd_fleet_redis_connections_max
    max_restarts: int = 3
    open_powershell: bool = False
    redis_rate_key: str = settings.dbd_redis_rate_key
    recovery_interval: float = settings.dbd_redis_recovery_interval_seconds
    redis_stability_samples: int = 5
    redis_sample_delay: float = 1.0
    redis_latency_limit_ms: float = 2000.0

    def supervisor_kwargs(self) -> dict[str, Any]:
        return {
            "workers": self.workers,
            "workers_min": self.workers_min,
            "workers_max": self.workers_max,
            "worker_concurrency": self.worker_concurrency,
            "worker_rate": self.worker_rate,
            "rate_mode": self.rate_mode,
            "fleet_rate_initial": self.fleet_rate_initial,
            "fleet_rate_max": self.fleet_rate_max,
            "redis_rate_key": self.redis_rate_key,
            "supervisor_generation": self.supervisor_generation or None,
            "fleet_db_connections_max": self.fleet_db_connections_max,
            "fleet_archive_workers_max": self.fleet_archive_workers_max,
            "fleet_projection_workers_max": self.fleet_projection_workers_max,
            "fleet_redis_connections_max": self.fleet_redis_connections_max,
            "open_powershell": self.open_powershell,
            "max_restarts": self.max_restarts,
        }


@dataclass(frozen=True)
class RedisRecoveryProbe:
    stable: bool
    successful_pings: int
    requested_pings: int
    latency_ms: float | None = None
    lease_ttl: int | None = None
    registered_workers: int = 0
    live_workers: int = 0
    stale_memberships: int = 0
    error: str | None = None

    @property
    def lease_clear(self) -> bool:
        # Redis TTL: -2 = key absent, -1 = key exists without expiry.
        # Only an absent supervisor lease is safe for replacement ownership.
        return self.lease_ttl == -2


def _guardian_dir() -> Path:
    return PROJECT_ROOT / "data" / "guardian"


def guardian_profile_path(run_id: int) -> Path:
    return _guardian_dir() / f"run-{int(run_id)}.json"


def guardian_state_path(run_id: int) -> Path:
    return _guardian_dir() / f"run-{int(run_id)}.state.json"


def guardian_recovery_path(run_id: int) -> Path:
    return _guardian_dir() / f"run-{int(run_id)}.recovery.json"


def _new_supervisor_generation() -> str:
    """Create a host-bound, per-Guardian generation token safe for VARCHAR(64) lease owners."""
    seed = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex}".encode("utf-8")
    return hashlib.sha256(seed).hexdigest()[:16]


def _recovery_supervisor_identity(run_id: int) -> tuple[int, str] | None:
    payload = _read_json(guardian_recovery_path(run_id))
    try:
        marker_run = int(payload.get("run_id"))
        supervisor_pid = int(payload.get("supervisor_pid"))
        generation = str(payload.get("supervisor_generation") or "").strip().lower()
    except (TypeError, ValueError):
        return None
    valid_generation = (
        len(generation) == 16
        and all(char in "0123456789abcdef" for char in generation)
    )
    if marker_run != int(run_id) or supervisor_pid < 1 or not valid_generation:
        return None
    return supervisor_pid, generation


def _clear_recovery_marker(run_id: int) -> None:
    with suppress(FileNotFoundError):
        guardian_recovery_path(run_id).unlink()


def _lock_path(run_id: int) -> Path:
    return _guardian_dir() / f"run-{int(run_id)}.lock"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temp.write_text(
        json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, path)


def _runtime_profile(run_id: int) -> dict[str, Any]:
    try:
        state = campaign_status(run_id)
    except Exception:
        return {}
    runtime = state.get("supervisor") or {}
    if not isinstance(runtime, Mapping):
        return {}
    mapping = {
        "workers_initial": "workers",
        "workers_min": "workers_min",
        "workers_max": "workers_max",
        "worker_concurrency": "worker_concurrency",
        "worker_rate": "worker_rate",
        "rate_mode": "rate_mode",
        "fleet_rate_initial": "fleet_rate_initial",
        "fleet_rate_max": "fleet_rate_max",
        "db_connections_budget": "fleet_db_connections_max",
        "archive_workers_budget": "fleet_archive_workers_max",
        "projection_workers_budget": "fleet_projection_workers_max",
        "redis_connections_budget": "fleet_redis_connections_max",
        "max_restarts": "max_restarts",
        "redis_rate_key": "redis_rate_key",
    }
    result: dict[str, Any] = {}
    for source, target in mapping.items():
        value = runtime.get(source)
        if value is not None:
            result[target] = value
    return result


def _validate_config(config: GuardianConfig) -> GuardianConfig:
    if config.run_id < 1:
        raise ValueError("run_id must be positive")
    if min(config.workers, config.workers_min, config.workers_max) < 1:
        raise ValueError("workers must be positive")
    if not config.workers_min <= config.workers <= config.workers_max:
        raise ValueError("workers must satisfy workers-min <= workers <= workers-max")
    if config.worker_concurrency < 1:
        raise ValueError("worker concurrency must be positive")
    if config.worker_rate <= 0:
        raise ValueError("worker rate must be positive")
    if config.rate_mode not in {"shared", "multiply", "adaptive"}:
        raise ValueError(f"unsupported rate mode {config.rate_mode!r}")
    if config.max_restarts < 0:
        raise ValueError("max restarts must be non-negative")
    if config.recovery_interval <= 0:
        raise ValueError("recovery interval must be positive")
    if config.redis_stability_samples < 1:
        raise ValueError("Redis stability samples must be positive")
    sample_delay = float(config.redis_sample_delay)
    latency_limit = float(config.redis_latency_limit_ms)
    if not math.isfinite(sample_delay) or sample_delay < 0:
        raise ValueError("Redis sample delay must be finite and non-negative")
    if not math.isfinite(latency_limit) or latency_limit <= 0:
        raise ValueError("Redis latency limit must be finite and positive")
    if config.supervisor_generation:
        generation = str(config.supervisor_generation).strip().lower()
        if len(generation) != 16 or any(
            char not in "0123456789abcdef" for char in generation
        ):
            raise ValueError("supervisor generation must be a 16-character lowercase hex token")
    if not str(config.redis_rate_key).strip():
        raise ValueError("Redis rate key must not be empty")
    return config


def resolve_guardian_config(
    run_id: int,
    *,
    overrides: Mapping[str, Any] | None = None,
    persist: bool = True,
) -> GuardianConfig:
    """Resolve defaults -> previous runtime -> persisted profile -> explicit CLI values."""
    allowed = {item.name for item in fields(GuardianConfig)} - {"supervisor_generation"}
    values = asdict(GuardianConfig(run_id=int(run_id)))

    for source in (
        _runtime_profile(run_id),
        _read_json(guardian_profile_path(run_id)),
        dict(overrides or {}),
    ):
        for key, value in source.items():
            if key in allowed and key != "run_id" and value is not None:
                values[key] = value

    values["run_id"] = int(run_id)
    config = _validate_config(GuardianConfig(**{key: values[key] for key in allowed}))
    if persist:
        _persist_guardian_config(config)
    return config


def _persist_guardian_config(config: GuardianConfig) -> None:
    payload = asdict(config)
    payload.pop("supervisor_generation", None)
    _write_json_atomic(
        guardian_profile_path(config.run_id),
        {
            "profile_version": _GUARDIAN_PROFILE_VERSION,
            **payload,
        },
    )


def _write_guardian_state(
    config: GuardianConfig,
    *,
    phase: str,
    terminal: bool,
    recovery_count: int,
    message: str,
    exit_code: int | None = None,
) -> None:
    _write_json_atomic(
        guardian_state_path(config.run_id),
        {
            "state_version": _GUARDIAN_STATE_VERSION,
            "run_id": config.run_id,
            "pid": os.getpid(),
            "supervisor_generation": config.supervisor_generation or None,
            "phase": phase,
            "terminal": bool(terminal),
            "exit_code": exit_code,
            "recovery_count": int(recovery_count),
            "message": message,
            "updated_at": time.time(),
        },
    )


class GuardianFileLock:
    """One local Guardian per run. The Redis supervisor lease remains the fleet fence."""

    def __init__(self, run_id: int) -> None:
        self.path = _lock_path(run_id)
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self.path, "a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise GuardianAlreadyRunning(
                f"Campaign {self.path.stem.removeprefix('run-')} already has a local Guardian"
            ) from exc
        self.handle = handle
        return self

    def __exit__(self, exc_type, exc, tb):
        handle = self.handle
        self.handle = None
        if handle is None:
            return False
        with suppress(OSError):
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        return False


def _redis_client() -> Redis:
    if not settings.dbd_redis_url:
        raise ValueError("campaign guard requires DBD_REDIS_URL")
    return Redis.from_url(
        settings.dbd_redis_url,
        decode_responses=True,
        socket_connect_timeout=settings.dbd_redis_connect_timeout_seconds,
        socket_timeout=settings.dbd_redis_socket_timeout_seconds,
        health_check_interval=settings.dbd_redis_health_check_interval_seconds,
        max_connections=max(2, settings.dbd_redis_max_connections),
    )


def probe_redis_control_plane(config: GuardianConfig) -> RedisRecoveryProbe:
    """Read-only Redis recovery gate. It never deletes lease, worker or rate keys."""
    requested = int(config.redis_stability_samples)
    success = 0
    latencies: list[float] = []
    redis = None
    error: str | None = None
    try:
        redis = _redis_client()
        for index in range(requested):
            started = time.perf_counter()
            try:
                redis.ping()
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
            else:
                elapsed = (time.perf_counter() - started) * 1000
                latencies.append(elapsed)
                success += 1
                error = None
            if index + 1 < requested:
                time.sleep(config.redis_sample_delay)

        max_latency = max(latencies) if latencies else None
        stable = (
            success == requested
            and max_latency is not None
            and max_latency <= config.redis_latency_limit_ms
        )
        if not stable:
            if success == requested and max_latency is not None:
                error = (
                    f"Redis latency {max_latency:.1f} ms exceeds "
                    f"{config.redis_latency_limit_ms:.1f} ms recovery ceiling"
                )
            return RedisRecoveryProbe(
                stable=False,
                successful_pings=success,
                requested_pings=requested,
                latency_ms=max_latency,
                error=error or f"Redis stability only {success}/{requested}",
            )

        base = f"dbd:campaign:{config.run_id}"
        lease_ttl = int(redis.ttl(f"{base}:supervisor:lease"))
        ids = sorted(redis.smembers(f"{base}:workers"))
        values = redis.mget([f"{base}:worker:{worker_id}" for worker_id in ids]) if ids else []
        live = sum(value is not None for value in values)
        stale = len(ids) - live
        return RedisRecoveryProbe(
            stable=True,
            successful_pings=success,
            requested_pings=requested,
            latency_ms=max_latency,
            lease_ttl=lease_ttl,
            registered_workers=len(ids),
            live_workers=live,
            stale_memberships=stale,
        )
    except Exception as exc:
        return RedisRecoveryProbe(
            stable=False,
            successful_pings=success,
            requested_pings=requested,
            latency_ms=max(latencies) if latencies else None,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        if redis is not None:
            with suppress(Exception):
                redis.close()


def _counts(state: Mapping[str, Any] | None) -> dict[str, int]:
    state = state or {}
    pending = int(state.get("pending", 0) or 0)
    retry_wait = int(state.get("retry_wait", 0) or 0)
    running = int(state.get("running", 0) or 0)
    if state.get("pending_includes_retry_wait"):
        remaining = pending + running
    else:
        remaining = pending + retry_wait + running
    done = int(state.get("done", 0) or 0)
    not_available = int(state.get("not_available", 0) or 0)
    failed = int(state.get("failed", 0) or 0)
    total = int(state.get("total", 0) or 0)
    terminal = done + not_available + failed
    return {
        "pending": pending,
        "retry_wait": retry_wait,
        "running": running,
        "remaining": remaining,
        "done": done,
        "not_available": not_available,
        "failed": failed,
        "terminal": terminal,
        "total": total,
    }


def _view(
    config: GuardianConfig,
    *,
    phase: str,
    recovery_count: int,
    message: str,
    state: Mapping[str, Any] | None = None,
    redis_probe: RedisRecoveryProbe | None = None,
    package_health: str = "unknown",
    last_failure: str | None = None,
) -> dict[str, Any]:
    counts = _counts(state)
    return {
        "run_id": config.run_id,
        "phase": phase,
        "message": message,
        "recovery_count": recovery_count,
        "last_failure": last_failure,
        "package_health": package_health,
        "run_status": str((state or {}).get("run_status") or "--"),
        "counts": counts,
        "redis": {
            "stable": redis_probe.stable if redis_probe else False,
            "pings": (
                f"{redis_probe.successful_pings}/{redis_probe.requested_pings}"
                if redis_probe
                else "--"
            ),
            "latency_ms": redis_probe.latency_ms if redis_probe else None,
            "lease_ttl": redis_probe.lease_ttl if redis_probe else None,
            "registered_workers": redis_probe.registered_workers if redis_probe else 0,
            "live_workers": redis_probe.live_workers if redis_probe else 0,
            "stale_memberships": redis_probe.stale_memberships if redis_probe else 0,
            "error": redis_probe.error if redis_probe else None,
        },
        "profile": {
            "workers": config.workers,
            "workers_min": config.workers_min,
            "workers_max": config.workers_max,
            "worker_concurrency": config.worker_concurrency,
            "fleet_rate_initial": config.fleet_rate_initial,
            "fleet_rate_max": config.fleet_rate_max,
            "rate_mode": config.rate_mode,
            "redis_rate_key": config.redis_rate_key,
        },
        "safety": {
            "duplicate_supervisor": "FENCED",
            "auto_bulk_retry": "DISABLED",
            "blind_redis_cleanup": "DISABLED",
            "durable_truth": "MYSQL",
        },
    }


def _wait_for_recovery_gate(
    config: GuardianConfig,
    dashboard: GuardianDashboard,
    *,
    recovery_count: int,
    package_health: str,
    last_failure: str | None,
) -> tuple[dict[str, Any], RedisRecoveryProbe]:
    while True:
        probe = probe_redis_control_plane(config)
        if not probe.stable:
            message = probe.error or "Redis control plane is not stable"
            dashboard.render(
                _view(
                    config,
                    phase="WAIT_REDIS",
                    recovery_count=recovery_count,
                    message=message,
                    redis_probe=probe,
                    package_health=package_health,
                    last_failure=last_failure,
                )
            )
            _write_guardian_state(
                config,
                phase="WAIT_REDIS",
                terminal=False,
                recovery_count=recovery_count,
                message=message,
            )
            time.sleep(config.recovery_interval)
            continue

        if not probe.lease_clear:
            ttl = int(probe.lease_ttl if probe.lease_ttl is not None else 0)
            message = (
                "Previous supervisor lease exists without an expiry; ownership remains fenced"
                if ttl == -1
                else f"Previous supervisor lease is still fenced ({ttl}s TTL)"
            )
            dashboard.render(
                _view(
                    config,
                    phase="FENCED",
                    recovery_count=recovery_count,
                    message=message,
                    redis_probe=probe,
                    package_health=package_health,
                    last_failure=last_failure,
                )
            )
            _write_guardian_state(
                config,
                phase="FENCED",
                terminal=False,
                recovery_count=recovery_count,
                message=message,
            )
            time.sleep(min(config.recovery_interval, max(1.0, float(ttl))))
            continue

        recovery_identity = _recovery_supervisor_identity(config.run_id)
        if recovery_identity is not None:
            recovery_pid, recovery_generation = recovery_identity
            try:
                reclaimed = reclaim_guardian_process_leases(
                    config.run_id,
                    recovery_generation,
                )
            except Exception as exc:
                message = (
                    "MySQL orphan-lease reconciliation unavailable: "
                    f"{type(exc).__name__}"
                )
                dashboard.render(
                    _view(
                        config,
                        phase="WAIT_MYSQL",
                        recovery_count=recovery_count,
                        message=message,
                        redis_probe=probe,
                        package_health=package_health,
                        last_failure=last_failure,
                    )
                )
                _write_guardian_state(
                    config,
                    phase="WAIT_MYSQL",
                    terminal=False,
                    recovery_count=recovery_count,
                    message=message,
                )
                time.sleep(config.recovery_interval)
                continue

            _clear_recovery_marker(config.run_id)
            message = (
                f"Reconciled {reclaimed:,} MySQL running lease(s) owned by "
                f"confirmed-dead Guardian generation {recovery_generation} "
                f"(PID {recovery_pid})."
            )
            dashboard.render(
                _view(
                    config,
                    phase="RECONCILING",
                    recovery_count=recovery_count,
                    message=message,
                    redis_probe=probe,
                    package_health=package_health,
                    last_failure=last_failure,
                )
            )
            _write_guardian_state(
                config,
                phase="RECONCILING",
                terminal=False,
                recovery_count=recovery_count,
                message=message,
            )

        try:
            state = campaign_status(config.run_id)
        except ValueError:
            raise
        except Exception as exc:
            message = f"MySQL durable state unavailable: {type(exc).__name__}"
            dashboard.render(
                _view(
                    config,
                    phase="WAIT_MYSQL",
                    recovery_count=recovery_count,
                    message=message,
                    redis_probe=probe,
                    package_health=package_health,
                    last_failure=last_failure,
                )
            )
            _write_guardian_state(
                config,
                phase="WAIT_MYSQL",
                terminal=False,
                recovery_count=recovery_count,
                message=message,
            )
            time.sleep(config.recovery_interval)
            continue

        if state.get("supervisor_active"):
            message = "Another supervisor acquired the lease; waiting behind the fence"
            dashboard.render(
                _view(
                    config,
                    phase="FENCED",
                    recovery_count=recovery_count,
                    message=message,
                    state=state,
                    redis_probe=probe,
                    package_health=package_health,
                    last_failure=last_failure,
                )
            )
            time.sleep(config.recovery_interval)
            continue

        return dict(state), probe


def _terminal_result(
    config: GuardianConfig,
    dashboard: GuardianDashboard,
    *,
    state: Mapping[str, Any],
    probe: RedisRecoveryProbe | None,
    package_health: str,
    recovery_count: int,
) -> int | None:
    counts = _counts(state)
    if counts["remaining"] != 0:
        return None
    if counts["failed"]:
        phase = "COMPLETED_WITH_FAILURES"
        message = (
            f"Durable work is terminal; {counts['failed']:,} failed resources require diagnosis. "
            "Automatic bulk retry remains disabled."
        )
        code = 1
    else:
        phase = "COMPLETED"
        message = "All durable campaign work reached a terminal successful/not-available state."
        code = 0
    payload = _view(
        config,
        phase=phase,
        recovery_count=recovery_count,
        message=message,
        state=state,
        redis_probe=probe,
        package_health=package_health,
    )
    dashboard.close(payload)
    _write_guardian_state(
        config,
        phase=phase,
        terminal=True,
        recovery_count=recovery_count,
        message=message,
        exit_code=code,
    )
    return code


def _guard_loop(config: GuardianConfig, dashboard: GuardianDashboard) -> int:
    package_health = "unknown"
    with suppress(Exception):
        package_health = str(package_environment_snapshot().get("health") or "unknown")

    recovery_count = 0
    consecutive_failures = 0
    last_failure: str | None = None

    while True:
        state, probe = _wait_for_recovery_gate(
            config,
            dashboard,
            recovery_count=recovery_count,
            package_health=package_health,
            last_failure=last_failure,
        )
        terminal = _terminal_result(
            config,
            dashboard,
            state=state,
            probe=probe,
            package_health=package_health,
            recovery_count=recovery_count,
        )
        if terminal is not None:
            return terminal

        message = "Safety gates passed; resuming the existing durable run."
        dashboard.render(
            _view(
                config,
                phase="RESUMING",
                recovery_count=recovery_count,
                message=message,
                state=state,
                redis_probe=probe,
                package_health=package_health,
                last_failure=last_failure,
            )
        )
        _write_guardian_state(
            config,
            phase="RUNNING",
            terminal=False,
            recovery_count=recovery_count,
            message=message,
        )
        dashboard.suspend()

        try:
            code = supervise_campaign(
                config.run_id,
                **config.supervisor_kwargs(),
            )
        except KeyboardInterrupt:
            code = _EXIT_PAUSED
        except ValueError:
            raise
        except Exception as exc:
            recovery_count += 1
            consecutive_failures += 1
            last_failure = f"{type(exc).__name__}: {str(exc)[:240]}"
            delay = min(
                60.0,
                config.recovery_interval * (2 ** min(max(0, consecutive_failures - 1), 4)),
            )
            message = (
                "Supervisor exited unexpectedly; durable state is preserved. "
                f"Recovery gate reopens in {delay:.0f}s."
            )
            dashboard.render(
                _view(
                    config,
                    phase="RECOVERING",
                    recovery_count=recovery_count,
                    message=message,
                    state=state,
                    redis_probe=probe,
                    package_health=package_health,
                    last_failure=last_failure,
                )
            )
            _write_guardian_state(
                config,
                phase="RECOVERING",
                terminal=False,
                recovery_count=recovery_count,
                message=message,
            )
            time.sleep(delay)
            continue

        if code == _EXIT_PAUSED:
            message = "Operator requested a graceful pause; Guardian will not auto-resume."
            payload = _view(
                config,
                phase="PAUSED",
                recovery_count=recovery_count,
                message=message,
                package_health=package_health,
            )
            dashboard.close(payload)
            _write_guardian_state(
                config,
                phase="PAUSED",
                terminal=True,
                recovery_count=recovery_count,
                message=message,
                exit_code=_EXIT_PAUSED,
            )
            return _EXIT_PAUSED

        try:
            after = campaign_status(config.run_id)
        except ValueError:
            raise
        except Exception as exc:
            recovery_count += 1
            consecutive_failures += 1
            last_failure = f"{type(exc).__name__}: {str(exc)[:240]}"
            message = "Supervisor returned but MySQL reconciliation is unavailable; re-entering recovery."
            dashboard.render(
                _view(
                    config,
                    phase="RECOVERING",
                    recovery_count=recovery_count,
                    message=message,
                    package_health=package_health,
                    last_failure=last_failure,
                )
            )
            time.sleep(config.recovery_interval)
            continue

        terminal = _terminal_result(
            config,
            dashboard,
            state=after,
            probe=None,
            package_health=package_health,
            recovery_count=recovery_count,
        )
        if terminal is not None:
            return terminal

        recovery_count += 1
        if code == 0:
            consecutive_failures = 0
            delay = config.recovery_interval
        else:
            consecutive_failures += 1
            delay = min(
                60.0,
                config.recovery_interval * (2 ** min(max(0, consecutive_failures - 1), 4)),
            )
        last_failure = f"supervisor_exit_{code}"
        message = (
            f"Supervisor exited with code {code} while durable work remains; "
            f"re-entering fenced recovery in {delay:.0f}s."
        )
        dashboard.render(
            _view(
                config,
                phase="RECOVERING",
                recovery_count=recovery_count,
                message=message,
                state=after,
                package_health=package_health,
                last_failure=last_failure,
            )
        )
        _write_guardian_state(
            config,
            phase="RECOVERING",
            terminal=False,
            recovery_count=recovery_count,
            message=message,
        )
        time.sleep(delay)


def _run_owned_guardian(config: GuardianConfig, dashboard: GuardianDashboard) -> int:
    if not settings.dbd_redis_url:
        raise ValueError("campaign guard requires DBD_REDIS_URL")
    _persist_guardian_config(config)
    try:
        return _guard_loop(config, dashboard)
    except KeyboardInterrupt:
        message = "Operator interrupted Guardian before supervisor ownership; campaign remains durable."
        dashboard.close(
            _view(
                config,
                phase="PAUSED",
                recovery_count=0,
                message=message,
            )
        )
        _write_guardian_state(
            config,
            phase="PAUSED",
            terminal=True,
            recovery_count=0,
            message=message,
            exit_code=_EXIT_PAUSED,
        )
        return _EXIT_PAUSED


def _fatal_guardian(
    config: GuardianConfig,
    dashboard: GuardianDashboard,
    exc: BaseException,
) -> int:
    message = f"Guardian configuration/preflight failed: {type(exc).__name__}: {exc}"
    dashboard.close(
        _view(
            config,
            phase="FATAL",
            recovery_count=0,
            message=message,
        )
    )
    _write_guardian_state(
        config,
        phase="FATAL",
        terminal=True,
        recovery_count=0,
        message=message,
        exit_code=_EXIT_CONFIG,
    )
    return _EXIT_CONFIG


def guard_campaign(
    config: GuardianConfig,
    *,
    dashboard: GuardianDashboard | None = None,
) -> int:
    """Protect one resolved configuration; persist it only after ownership is acquired."""
    dashboard = dashboard or GuardianDashboard()
    try:
        with GuardianFileLock(config.run_id):
            owned = _validate_config(config)
            if not owned.supervisor_generation:
                owned = replace(
                    owned,
                    supervisor_generation=_new_supervisor_generation(),
                )
            return _run_owned_guardian(owned, dashboard)
    except GuardianAlreadyRunning as exc:
        dashboard.close(
            _view(
                config,
                phase="ALREADY_MANAGED",
                recovery_count=0,
                message=str(exc),
            )
        )
        return _EXIT_ALREADY_MANAGED
    except (ValueError, TypeError, OSError) as exc:
        return _fatal_guardian(config, dashboard, exc)


def guard_campaign_run(
    run_id: int,
    *,
    overrides: Mapping[str, Any] | None = None,
    dashboard: GuardianDashboard | None = None,
) -> int:
    """CLI entrypoint: acquire ownership before resolving or persisting profile overrides."""
    dashboard = dashboard or GuardianDashboard()
    fallback = GuardianConfig(run_id=int(run_id))
    config = fallback
    try:
        with GuardianFileLock(run_id):
            config = resolve_guardian_config(
                run_id,
                overrides=overrides,
                persist=False,
            )
            config = replace(
                config,
                supervisor_generation=_new_supervisor_generation(),
            )
            return _run_owned_guardian(config, dashboard)
    except GuardianAlreadyRunning as exc:
        dashboard.close(
            _view(
                fallback,
                phase="ALREADY_MANAGED",
                recovery_count=0,
                message=str(exc),
            )
        )
        return _EXIT_ALREADY_MANAGED
    except (ValueError, TypeError, OSError) as exc:
        return _fatal_guardian(config, dashboard, exc)
