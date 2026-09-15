import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dbd.client import rate_limit
from dbd.client.async_client import AsyncDbdClient
from dbd.client.rate_limit import TokenBucket
from dbd.core.exceptions import DbdTokenExpiredError


def response(status=200, headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {}, content=b'{"ok":true}')


def client():
    c = AsyncDbdClient(concurrency=3, rate_per_second=1000, max_retries=1)
    c._token = "old"
    c._auth._expires_at = time.time() + 100
    c._session = SimpleNamespace(request=AsyncMock(return_value=response()), cookies={})
    c.rate.acquire = AsyncMock()
    return c


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


async def test_endpoint_429_penalizes_only_that_endpoint():
    c = client()
    c.endpoint_rate.acquire = AsyncMock()
    c._session.request.side_effect = [response(429, {"retry-after": "0"}), response()]

    await c.fetch("GET", "/profile", rate_key="profile")

    assert c.rate.rate == 1000
    assert c.endpoint_rate.effective_rate("profile") == 500
    assert c.endpoint_rate.effective_rate("committees") == 1000


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


async def test_403_is_not_retried():
    c = client()
    c._session.request.return_value = response(403)
    with pytest.raises(Exception, match="403"):
        await c.fetch("GET", "/test")
    assert c.requests_made == 1
