from __future__ import annotations

import asyncio
import time

import pytest

from dbd.client.async_client import PermanentHttpError, TransientHttpError
from dbd.client.simple_client import SimpleAsyncDbdClient, SimpleGlobalLimiter
from dbd.core.exceptions import DbdAuthError
import dbd.runtime.collector as collector_runtime
from dbd.runtime.collector import _assert_postflight_invariants, _live_state


@pytest.mark.asyncio
async def test_auth_refresh_is_single_flight_for_concurrent_fetchers(monkeypatch) -> None:
    client = SimpleAsyncDbdClient(concurrency=32, rate_per_second=500, max_retries=0)
    calls = 0

    async def refresh_once():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        client._token = "shared-token"
        client._auth._expires_at = time.time() + 900
        return client._token

    monkeypatch.setattr(client, "_refresh_token_once", refresh_once)

    tokens = await asyncio.gather(*(client._ensure_token() for _ in range(32)))

    assert calls == 1
    assert tokens == ["shared-token"] * 32


@pytest.mark.asyncio
async def test_failed_auth_refresh_is_shared_then_retried_once_after_backoff(monkeypatch) -> None:
    client = SimpleAsyncDbdClient(concurrency=32, rate_per_second=500, max_retries=0)
    calls = 0
    error = DbdAuthError("auth_refresh failed: HTTP 502", status_code=502)

    async def fail_once():
        nonlocal calls
        calls += 1
        client._auth_last_error = error
        client._auth_backoff_until = time.monotonic() + 0.02
        raise error

    monkeypatch.setattr(client, "_refresh_token_once", fail_once)

    first = await asyncio.gather(
        *(client._ensure_token() for _ in range(32)),
        return_exceptions=True,
    )
    assert calls == 1
    assert all(isinstance(item, DbdAuthError) for item in first)

    await asyncio.sleep(0.03)
    second = await asyncio.gather(
        *(client._ensure_token() for _ in range(32)),
        return_exceptions=True,
    )
    assert calls == 2
    assert all(isinstance(item, DbdAuthError) for item in second)





@pytest.mark.asyncio
async def test_collector_auth_refresh_is_one_bounded_attempt() -> None:
    captured = {}

    class FakeAuth:
        _expires_at = 0.0

        def get_token(self, session, **kwargs):
            captured.update(kwargs)
            self._expires_at = time.time() + 900
            return "bounded-token"

        def invalidate(self, *, reset_warmup=False):
            self._expires_at = 0.0

    client = SimpleAsyncDbdClient(concurrency=1, rate_per_second=50, max_retries=0)
    client._auth = FakeAuth()
    client._auth_session = object()

    token = await client._ensure_token()

    assert token == "bounded-token"
    assert captured["max_attempts"] == 1
    assert 0 < captured["timeout_seconds"] <= 10.0


@pytest.mark.asyncio
async def test_rate_penalty_returns_directly_to_configured_target() -> None:
    limiter = SimpleGlobalLimiter(50)

    await limiter.penalize(retry_after_seconds=0, quiet_seconds=0.01)

    assert limiter.snapshot()["effective_rate"] == 25

    await asyncio.sleep(0.02)

    assert limiter.snapshot()["effective_rate"] == 50


def test_live_state_preserves_existing_run_totals() -> None:
    baseline = {
        "pending": 7_549_301,
        "retry_wait": 2_100,
        "running": 0,
        "failed": 12_476,
        "done": 909_310,
        "not_available": 733_528,
    }
    runtime = {
        "done": 12,
        "not_available": 3,
        "failed": 0,
        "retry_wait": 1,
        "retry_wait_claimed": 12,
        "requests": 18,
    }

    state = _live_state(9, baseline, runtime)

    assert state["total"] == 9_206_715
    assert state["done"] == 909_322
    assert state["not_available"] == 733_531
    assert state["failed"] == 12_476
    assert state["remaining"] == 7_551_386
    assert state["retry_wait"] == 2_089
    assert state["requests"] == 18



@pytest.mark.asyncio
async def test_permanent_404_marks_upstream_reachable() -> None:
    class Response:
        status_code = 404
        headers = {}
        content = b"{}"

    class Session:
        async def request(self, *args, **kwargs):
            return Response()

    client = SimpleAsyncDbdClient(concurrency=1, rate_per_second=500, max_retries=0)
    client._session = Session()
    client._token = "valid-token"
    client._auth._expires_at = time.time() + 900
    client._upstream.failures = 5
    client._upstream.pause_until = time.monotonic() + 0.01

    with pytest.raises(PermanentHttpError, match="HTTP 404"):
        await client.fetch("GET", "/synthetic/not-found")

    assert client._upstream.failures == 0
    assert client._upstream.pause_until == 0.0
    assert client.upstream_state == "healthy"



@pytest.mark.asyncio
async def test_503_retry_after_pauses_upstream_immediately() -> None:
    class Response:
        status_code = 503
        headers = {"retry-after": "2"}
        content = b"{}"

    class Session:
        async def request(self, *args, **kwargs):
            return Response()

    client = SimpleAsyncDbdClient(concurrency=1, rate_per_second=500, max_retries=0)
    client._session = Session()
    client._token = "valid-token"
    client._auth._expires_at = time.time() + 900

    with pytest.raises(TransientHttpError, match="HTTP 503"):
        await client.fetch("GET", "/synthetic/unavailable")

    assert client._upstream.failures == 1
    assert client._upstream.pause_until > time.monotonic()
    assert client.upstream_state == "paused"



def test_postflight_invariants_accept_monotonic_same_run_progress() -> None:
    baseline = {
        "pending": 10,
        "retry_wait": 2,
        "running": 0,
        "failed": 3,
        "done": 5,
        "not_available": 4,
    }
    exact = {
        "pending": 7,
        "retry_wait": 1,
        "running": 0,
        "failed": 4,
        "done": 7,
        "not_available": 5,
    }

    _assert_postflight_invariants(baseline, exact)


@pytest.mark.parametrize(
    ("exact", "message"),
    [
        (
            {
                "pending": 9,
                "retry_wait": 2,
                "running": 0,
                "failed": 3,
                "done": 5,
                "not_available": 4,
            },
            "total changed",
        ),
        (
            {
                "pending": 11,
                "retry_wait": 2,
                "running": 0,
                "failed": 3,
                "done": 4,
                "not_available": 4,
            },
            "DONE decreased",
        ),
        (
            {
                "pending": 10,
                "retry_wait": 2,
                "running": 1,
                "failed": 3,
                "done": 4,
                "not_available": 4,
            },
            "RUNNING is 1",
        ),
    ],
)
def test_postflight_invariants_fail_closed(exact, message) -> None:
    baseline = {
        "pending": 10,
        "retry_wait": 2,
        "running": 0,
        "failed": 3,
        "done": 5,
        "not_available": 4,
    }

    with pytest.raises(RuntimeError, match=message):
        _assert_postflight_invariants(baseline, exact)


@pytest.mark.asyncio
async def test_legacy_handoff_refuses_supervisor_or_registered_workers(monkeypatch) -> None:
    monkeypatch.setattr(
        "dbd.runtime.supervisor.collector_handoff_status",
        lambda run_id: {
            "redis_configured": True,
            "control_status": "active",
            "supervisor_active": True,
            "workers": 2,
        },
    )

    with pytest.raises(RuntimeError, match="legacy runtime is still active"):
        await collector_runtime._assert_legacy_runtime_offline(9)


@pytest.mark.asyncio
async def test_collector_preflight_refuses_live_foreign_leases_before_reclaim(
    monkeypatch,
) -> None:
    class FakeRepo:
        archive_backend = None

        async def collector_lease_summary(self, run_id):
            return {"running": 2, "live": 2, "expired": 0, "invalid": 0}

        async def reclaim_expired_collector_leases(self, run_id):
            raise AssertionError("live leases must not be reclaimed")

    async def schema_ready(repo):
        return "0012_run9_data_repair"

    async def legacy_offline(run_id):
        return {
            "control_status": "offline",
            "supervisor_active": False,
            "workers": 0,
        }

    monkeypatch.setattr(collector_runtime, "_assert_schema_ready", schema_ready)
    monkeypatch.setattr(
        collector_runtime,
        "_assert_legacy_runtime_offline",
        legacy_offline,
    )

    with pytest.raises(RuntimeError, match="still has 2 live durable lease"):
        await collector_runtime._collector_preflight(FakeRepo(), 9)


@pytest.mark.asyncio
async def test_collector_preflight_reclaims_only_expired_process_loss(
    monkeypatch,
) -> None:
    class FakeRepo:
        archive_backend = None

        def __init__(self):
            self.reclaimed = False

        async def collector_lease_summary(self, run_id):
            if self.reclaimed:
                return {"running": 0, "live": 0, "expired": 0, "invalid": 0}
            return {"running": 3, "live": 0, "expired": 3, "invalid": 0}

        async def reclaim_expired_collector_leases(self, run_id):
            self.reclaimed = True
            return 3

    async def schema_ready(repo):
        return "0012_run9_data_repair"

    async def legacy_offline(run_id):
        return {
            "control_status": "offline",
            "supervisor_active": False,
            "workers": 0,
        }

    monkeypatch.setattr(collector_runtime, "_assert_schema_ready", schema_ready)
    monkeypatch.setattr(
        collector_runtime,
        "_assert_legacy_runtime_offline",
        legacy_offline,
    )

    repo = FakeRepo()
    result = await collector_runtime._collector_preflight(repo, 9)

    assert repo.reclaimed is True
    assert result["reclaimed_leases"] == 3
    assert result["schema_revision"] == "0012_run9_data_repair"
