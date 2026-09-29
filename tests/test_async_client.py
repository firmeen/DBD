import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import dbd.client.async_client as async_client_module
from dbd.client import rate_limit
from dbd.client.async_client import AsyncDbdClient
from dbd.client.auth import DbdAuth
from dbd.client.rate_limit import TokenBucket, protective_cooldown
from dbd.core.exceptions import DbdAuthError, DbdForbiddenError, DbdTokenExpiredError


def response(status=200, headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {}, content=b'{"ok":true}')


def client():
    c = AsyncDbdClient(concurrency=3, rate_per_second=1000, max_retries=1)
    c._token = "old"
    c._auth._expires_at = time.time() + 100
    c._session = SimpleNamespace(request=AsyncMock(return_value=response()), cookies={})
    c.rate.acquire = AsyncMock()
    return c


def test_production_client_exposes_worker_local_health_telemetry():
    c = client()
    assert c.auth_state == "ready"
    assert c.upstream_state == "closed"
    assert c.responses_received == 0
    assert c.last_request_at is None
    assert c.last_response_at is None


async def test_expired_token_and_concurrent_rejections_refresh_once():
    c = client()
    calls = []

    def refresh():
        calls.append(1)
        c._auth._expires_at = time.time() + 100
        return "new"

    c._blocking_token = refresh
    c._auth._expires_at = 0
    assert await c._ensure_token() == "new"
    await asyncio.gather(*(c._ensure_token(rejected="old") for _ in range(10)))
    assert len(calls) == 1


async def test_expiry_during_decryption_retries():
    c = client()
    c._ensure_token = AsyncMock(return_value="new")
    attempts = []

    def decode(*args):
        attempts.append(1)
        if len(attempts) == 1:
            raise DbdTokenExpiredError("expired")
        return {"ok": True}

    c._decode = decode
    result = await c.fetch("GET", "/test")
    assert result.data == {"ok": True}
    assert c.requests_made == 2
    assert c.retries_made == 1


async def test_429_sets_shared_cooldown_before_retry():
    c = client()
    c._session.request.side_effect = [response(429, {"retry-after": "5"}), response()]
    await c.fetch("GET", "/test")
    assert c.rate.rate == 500
    assert c.rate._cooldown_until > time.monotonic()


async def test_endpoint_429_penalizes_endpoint_without_global_collapse():
    c = client()
    c.endpoint_rate.acquire = AsyncMock()
    c._session.request.side_effect = [response(429, {"retry-after": "0"}), response()]

    await c.fetch("GET", "/profile", rate_key="profile")

    assert c.rate.rate == 1000
    assert c.endpoint_rate.effective_rate("profile") == 500
    assert c.endpoint_rate.effective_rate("committees") == 1000
    assert c.endpoint_rate.global_effective_rate() == 1000


async def test_distinct_endpoint_pressure_does_not_collapse_worker_global_rate():
    c = client()
    c.endpoint_rate.acquire = AsyncMock()
    c._session.request.side_effect = [
        response(429, {"retry-after": "0"}),
        response(),
        response(429, {"retry-after": "0"}),
        response(),
    ]

    await c.fetch("GET", "/profile", rate_key="profile")
    assert c.endpoint_rate.global_effective_rate() == 1000

    await c.fetch("GET", "/committees", rate_key="committees")
    assert c.endpoint_rate.effective_rate("profile") == 500
    assert c.endpoint_rate.effective_rate("committees") == 500
    assert c.endpoint_rate.global_effective_rate() == 1000


def test_protective_cooldown_respects_server_and_repeated_window_pressure():
    assert protective_cooldown(8, 2.5, 1) == 8
    first = protective_cooldown(1, 2.5, 1)
    second = protective_cooldown(1, 1.25, 2)
    third = protective_cooldown(1, 0.625, 3)
    assert first >= 2
    assert second > first
    assert third > second


def test_rate_recovers_gradually_and_never_exceeds_target(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: clock[0])
    bucket = TokenBucket(20)
    assert bucket.slow_down() == 10
    assert bucket.recover() is None

    clock[0] = 31
    assert bucket.recover() == pytest.approx(12.5)
    assert bucket.recover() is None

    clock[0] = 62
    assert bucket.recover() == pytest.approx(15.625)
    clock[0] = 93
    assert bucket.recover() == pytest.approx(19.53125)
    clock[0] = 124
    assert bucket.recover() == 20
    clock[0] = 155
    assert bucket.recover() is None


async def test_403_refreshes_auth_and_retries():
    c = client()
    c._session.request.side_effect = [response(403), response()]
    c._ensure_token = AsyncMock(side_effect=["old", "new", "new"])

    result = await c.fetch("GET", "/test")

    assert result.status == 200
    assert c.requests_made == 2
    assert c.retries_made == 1
    assert c._ensure_token.await_count == 3
    c._ensure_token.assert_any_await(rejected="old", reset_warmup=True)


async def test_repeated_403_is_bounded_by_http_retry_budget():
    c = client()
    c._session.request.return_value = response(403)
    c._ensure_token = AsyncMock(return_value="token")

    with pytest.raises(DbdForbiddenError, match="403"):
        await c.fetch("GET", "/test")

    assert c.requests_made == 2
    assert c.retries_made == 1


async def test_context_entry_does_not_require_upstream_auth(monkeypatch):
    c = AsyncDbdClient(concurrency=1, rate_per_second=1, max_retries=0)
    c.endpoint_rate.open = AsyncMock()
    c.endpoint_rate.close = AsyncMock()
    fake_async_session = SimpleNamespace(close=AsyncMock(), cookies={})
    fake_auth_session = SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(
        async_client_module,
        "AsyncSession",
        lambda **kwargs: fake_async_session,
    )
    monkeypatch.setattr(
        async_client_module,
        "create_session",
        lambda: fake_auth_session,
    )
    c._ensure_token = AsyncMock(side_effect=DbdAuthError("offline"))

    entered = await c.__aenter__()
    assert entered is c
    c._ensure_token.assert_not_awaited()

    await c.__aexit__()


def test_auth_invalidate_can_force_session_rewarm():
    auth = DbdAuth()
    auth._token = "old"
    auth._expires_at = time.time() + 60
    auth._warmed_up = True

    auth.invalidate(reset_warmup=True)

    assert auth._token is None
    assert auth._expires_at == 0.0
    assert auth._warmed_up is False


async def test_auth_outage_opens_availability_circuit_without_rate_penalty():
    c = client()
    c._ensure_token = AsyncMock(side_effect=DbdAuthError("auth_refresh failed: HTTP 502"))
    c.endpoint_rate.availability_failure = AsyncMock(return_value="closed")

    with pytest.raises(DbdAuthError):
        await c.fetch("GET", "/profile", rate_key="profile")

    assert c.endpoint_rate.availability_failure.await_count == 2
    for call in c.endpoint_rate.availability_failure.await_args_list:
        assert call.args == ()
        assert call.kwargs == {}
    assert c.endpoint_rate.global_effective_rate() == 1000
    assert c.endpoint_rate.effective_rate("profile") == 1000


async def test_502_is_availability_failure_without_rate_penalty():
    c = client()
    c.endpoint_rate.acquire = AsyncMock()
    c.endpoint_rate.availability_failure = AsyncMock(return_value="closed")
    c._session.request.side_effect = [response(502), response()]

    result = await c.fetch("GET", "/profile", rate_key="profile")

    assert result.status == 200
    c.endpoint_rate.availability_failure.assert_awaited_once_with("profile")
    assert c.endpoint_rate.global_effective_rate() == 1000
    assert c.endpoint_rate.effective_rate("profile") == 1000


async def test_503_without_retry_after_is_availability_not_rate_pressure():
    c = client()
    c.endpoint_rate.acquire = AsyncMock()
    c.endpoint_rate.availability_failure = AsyncMock(return_value="closed")
    c._session.request.side_effect = [response(503), response()]

    result = await c.fetch("GET", "/profile", rate_key="profile")

    assert result.status == 200
    assert c.rate_limit_hits == 0
    c.endpoint_rate.availability_failure.assert_awaited_once_with("profile")
    assert c.endpoint_rate.effective_rate("profile") == 1000


async def test_503_with_retry_after_remains_rate_pressure():
    c = client()
    c.endpoint_rate.acquire = AsyncMock()
    c.endpoint_rate.availability_failure = AsyncMock(return_value="closed")
    c._session.request.side_effect = [
        response(503, {"retry-after": "0"}),
        response(),
    ]

    result = await c.fetch("GET", "/profile", rate_key="profile")

    assert result.status == 200
    assert c.rate_limit_hits == 1
    c.endpoint_rate.availability_failure.assert_not_awaited()
    assert c.endpoint_rate.effective_rate("profile") == 500
