from __future__ import annotations

import asyncio
import time

import pytest

from dbd.client.async_client import PermanentHttpError, TransientHttpError
from dbd.client.simple_client import SimpleAsyncDbdClient, SimpleGlobalLimiter
from dbd.core.exceptions import DbdAuthError
from dbd.runtime.collector import _live_state


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
