import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import dbd.client.auth as auth_module
from dbd.client.auth import DbdAuth
from dbd.client.simple_client import SimpleDbdClient
from dbd.core.exceptions import DbdAuthError


def response(status=200, headers=None):
    return SimpleNamespace(
        status_code=status,
        headers=headers or {},
        content=b'{"ok":true}',
    )


def client(*, retries=1):
    value = SimpleDbdClient(
        concurrency=2,
        rate_per_second=60,
        max_retries=retries,
    )
    value._token = "token"
    value._auth._expires_at = 10**12
    value._session = SimpleNamespace(
        request=AsyncMock(return_value=response()),
        cookies={},
    )
    value.rate.acquire = AsyncMock()
    assert value._auth.max_attempts == 1
    return value


async def test_502_pauses_without_lowering_fixed_target():
    value = client(retries=1)
    value._session.request.side_effect = [response(502), response()]
    value._pause_availability = AsyncMock(return_value=2.0)

    result = await value.fetch("GET", "/profile", rate_key="profile")

    assert result.status == 200
    assert value.requests_made == 2
    assert value.retries_made == 1
    assert value.target_rate == 60
    assert value.rate.rate == 60


async def test_429_penalty_is_temporary_and_success_restores_target_directly():
    value = client(retries=1)
    value._session.request.side_effect = [
        response(429, {"retry-after": "0"}),
        response(),
    ]
    value.rate.cooldown = lambda seconds: None

    result = await value.fetch("GET", "/profile", rate_key="profile")

    assert result.status == 200
    assert value.rate_limit_hits == 1
    assert value.target_rate == 60
    assert value.rate.rate == 60


async def test_429_without_retry_budget_leaves_effective_rate_penalized():
    value = client(retries=0)
    value.rate.cooldown = lambda seconds: None
    value._session.request.return_value = response(429, {"retry-after": "0"})

    with pytest.raises(Exception, match="HTTP 429"):
        await value.fetch("GET", "/profile", rate_key="profile")

    assert value.target_rate == 60
    assert value.rate.rate == 30


async def test_auth_failure_uses_auth_budget_only_not_outer_http_retry():
    value = client(retries=2)
    value._ensure_token = AsyncMock(side_effect=DbdAuthError("auth unavailable"))
    value._pause_availability = AsyncMock(return_value=2.0)

    with pytest.raises(DbdAuthError, match="auth unavailable"):
        await value.fetch("GET", "/profile", rate_key="profile")

    assert value.requests_made == 0
    assert value.retries_made == 0
    value._pause_availability.assert_awaited_once()


async def test_auth_refresh_failure_publishes_pause_before_refresh_lock_is_released():
    value = client(retries=0)
    value._token = None

    def fail_refresh():
        raise DbdAuthError("refresh unavailable")

    value._blocking_token = fail_refresh

    with pytest.raises(DbdAuthError, match="refresh unavailable"):
        await value._ensure_token()

    assert value.auth_state == "unavailable"
    assert value._pause_until > time.monotonic()


async def test_concurrent_auth_failure_is_shared_without_refresh_convoy():
    value = client(retries=0)
    value._token = None
    refresh_calls = 0

    def fail_refresh():
        nonlocal refresh_calls
        refresh_calls += 1
        raise DbdAuthError("shared refresh unavailable", status_code=503)

    value._blocking_token = fail_refresh

    results = await asyncio.gather(
        *(value._ensure_token() for _ in range(8)),
        return_exceptions=True,
    )

    assert refresh_calls == 1
    assert all(isinstance(result, DbdAuthError) for result in results)
    assert all(str(result) == "shared refresh unavailable" for result in results)
    assert all(result.status_code == 503 for result in results)
    assert value.auth_state == "unavailable"


async def test_shared_auth_failure_allows_one_refresh_after_pause_window():
    value = client(retries=0)
    value._token = None
    value._auth_error = DbdAuthError("previous refresh unavailable")
    value._auth_error_until = time.monotonic() - 1
    refresh_calls = 0

    def refresh():
        nonlocal refresh_calls
        refresh_calls += 1
        return "fresh-token"

    value._blocking_token = refresh

    token = await value._ensure_token()

    assert token == "fresh-token"
    assert refresh_calls == 1
    assert value._auth_error is None
    assert value._auth_error_until == 0
    assert value.auth_state == "healthy"


async def test_business_404_clears_availability_without_reclassifying_response():
    value = client(retries=0)
    value._pause_until = time.monotonic() + 30
    value._availability_failures = 3
    value._wait_availability = AsyncMock()
    value._session.request.return_value = response(404)

    with pytest.raises(Exception, match="HTTP 404"):
        await value.fetch("GET", "/industry-benchmark", rate_key="industry_benchmark")

    assert value._availability_failures == 0
    assert value._pause_until == 0
    assert value.auth_state == "healthy"


@pytest.mark.parametrize("status", [401, 403, 409, 502])
def test_single_attempt_auth_does_not_sleep_after_terminal_refresh_response(
    monkeypatch,
    status,
):
    auth = DbdAuth(max_attempts=1)
    auth._warmed_up = True
    sleeps = []
    monkeypatch.setattr(auth_module.time, "sleep", lambda seconds: sleeps.append(seconds))
    session = SimpleNamespace(
        post=lambda *args, **kwargs: SimpleNamespace(
            status_code=status,
            ok=False,
            headers={},
            json=lambda: {},
        )
    )

    with pytest.raises(DbdAuthError):
        auth.get_token(session)

    assert sleeps == []
