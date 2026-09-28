from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

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


async def test_429_cooldown_keeps_configured_rate():
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


async def test_auth_failure_uses_auth_budget_only_not_outer_http_retry():
    value = client(retries=2)
    value._ensure_token = AsyncMock(side_effect=DbdAuthError("auth unavailable"))
    value._pause_availability = AsyncMock(return_value=2.0)

    with pytest.raises(DbdAuthError, match="auth unavailable"):
        await value.fetch("GET", "/profile", rate_key="profile")

    assert value.requests_made == 0
    assert value.retries_made == 0
    value._pause_availability.assert_awaited_once()
