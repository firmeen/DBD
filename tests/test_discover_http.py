"""Focused transport and Discovery routing regression tests (no real HTTP)."""
import asyncio
import json
import time
from types import SimpleNamespace

import pytest

from dbd.deep import http as http_module
from dbd.pipelines import discovery


class Response:
    def __init__(self, code=200, payload=None, headers=None):
        self.status_code = code
        self.headers = headers or {}
        self.content = json.dumps(payload if payload is not None else {"ok": True}).encode()


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    async def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        return next(self.responses)


def client_for(monkeypatch, responses, retries=0, rate=1000):
    client = http_module.FixedRateDbdClient(
        concurrency=2, rate_per_second=rate, max_retries=retries
    )
    client._session = Session(responses)

    async def admitted():
        return "token"

    monkeypatch.setattr(client, "_admitted_token", admitted)
    return client


@pytest.mark.asyncio
async def test_collect_get_transport_remains_backward_compatible(monkeypatch):
    client = client_for(monkeypatch, [Response(payload={"value": 12}), Response(payload={"value": 13})])
    result = await client.fetch("GET", "/api/test", params={"x": 1})
    method, path, kw = client._session.calls[0]
    assert isinstance(result, http_module.DeepResponse)
    assert result.data == {"value": 12}
    assert method == "GET" and path.endswith("/api/test")
    assert kw["params"] == {"x": 1}
    assert kw["timeout"] == client.timeout
    assert "json" not in kw
    assert await client.get("/api/test", params={"x": 1}) == {"value": 13}
    assert len(client._session.calls) == 2
    assert "json" not in client._session.calls[1][2]


@pytest.mark.asyncio
async def test_discovery_post_json_and_count_override(monkeypatch):
    payload = {"keyword": "010", "currentPage": 1, "pvCodeList": []}
    client = client_for(monkeypatch, [Response(payload={"meta": {"totalItems": 2}})])
    decoded = await client.post(
        "/api/v1/company-profiles/infos", payload,
        referer="https://datawarehouse.dbd.go.th/juristic/searchInfo",
        timeout=120, rate_key="search",
    )
    method, _, kw = client._session.calls[0]
    assert method == "POST" and kw["json"] == payload
    assert kw["headers"]["Referer"].endswith("/juristic/searchInfo")
    assert kw["headers"]["Content-Type"] == "application/json"
    assert kw["timeout"] == 120
    assert decoded["meta"]["totalItems"] == 2
    assert client.rate_per_second == 1000


@pytest.mark.asyncio
async def test_discovery_page_request_uses_default_timeout(monkeypatch):
    client = client_for(monkeypatch, [Response()])
    await client.post("/api/v1/company-profiles/infos", {"currentPage": 2}, rate_key="search")
    assert client._session.calls[0][2]["timeout"] == client.timeout


@pytest.mark.asyncio
async def test_403_refreshes_auth_session_with_bounded_retry(monkeypatch):
    client = client_for(monkeypatch, [Response(403), Response()], retries=1)
    auth_calls = []

    async def ensure(**kw):
        auth_calls.append(kw)
        return "new-token"

    monkeypatch.setattr(client, "_ensure_token", ensure)
    monkeypatch.setattr(client, "_backoff", lambda *a, **kw: 0)
    output = await client.post("/search", {"keyword": "010"})
    assert output == {"ok": True}
    assert auth_calls == [{"rejected": "token", "reset_session": True}]
    assert len(client._session.calls) == 2
    assert client.retries_made == 1


@pytest.mark.asyncio
async def test_429_503_retry_after_do_not_reduce_configured_rate(monkeypatch):
    client = client_for(monkeypatch, [
        Response(429, headers={"retry-after": "2"}),
        Response(503, headers={"retry-after": "5"}),
        Response(),
    ], retries=2, rate=15)
    cooldowns = []

    async def cooldown(seconds):
        cooldowns.append(seconds)

    monkeypatch.setattr(client, "_set_cooldown", cooldown)
    monkeypatch.setattr(client, "_backoff", lambda attempt, seconds=0: max(seconds, 0))
    assert await client.post("/search", {"keyword": "010"}) == {"ok": True}
    assert cooldowns == [2, 5]
    assert client.rate_per_second == 15
    assert client.rate_limit_hits == 1
    assert client.retries_made == 2


@pytest.mark.asyncio
async def test_http_retry_exhaustion_returns_error_without_infinite_loop(monkeypatch):
    client = client_for(monkeypatch, [Response(429), Response(429)], retries=1)
    async def cooldown(seconds):
        pass
    monkeypatch.setattr(client, "_set_cooldown", cooldown)
    monkeypatch.setattr(client, "_backoff", lambda *a, **k: 0)
    with pytest.raises(http_module.DeepTransientHttpError):
        await client.post("/search", {})
    assert len(client._session.calls) == 2


@pytest.mark.asyncio
async def test_fixed_admission_applies_pacing_after_shared_cooldown():
    client = http_module.FixedRateDbdClient(
        concurrency=2, rate_per_second=20, max_retries=0
    )
    client._cooldown_until = time.monotonic() + 0.03
    start = time.monotonic()
    await asyncio.gather(client._admit(), client._admit())
    assert time.monotonic() - start >= 0.06
    assert client.rate_per_second == 20


@pytest.mark.asyncio
@pytest.mark.parametrize("distributed", [False, True])
async def test_discovery_selects_correct_http_client_without_touching_pipeline(
    monkeypatch, distributed
):
    constructed = []
    closed = []

    class FakeClient:
        def __init__(self, **kwargs):
            constructed.append(kwargs)
            self.requests_made = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            closed.append(True)

    class FakeProgress:
        def __init__(self, *args, **kw):
            pass
        def close(self, **kw):
            pass
        def event(self, *args, **kw):
            pass

    async def fake_work(client, *args, **kw):
        assert client is instance
        return discovery.DiscoveryResult(0, 0, 0, 0)

    # Do not use Redis or MySQL in a transport-selection unit test.
    original = discovery.settings
    monkeypatch.setattr(
        discovery, "settings",
        SimpleNamespace(
            dbd_redis_url="redis://unused" if distributed else None,
            dbd_rate_per_second=15,
            dbd_redis_rate_key="dbd:global-rate",
            dbd_concurrency=8,
            dbd_http_retries=3,
            request_timeout=30,
            dbd_lease_seconds=300,
        ),
    )
    assert original is not None
    monkeypatch.setattr(discovery, "FixedRateDbdClient", FakeClient if not distributed else lambda **kw: pytest.fail("wrong fixed client"))
    monkeypatch.setattr(discovery, "AsyncDbdClient", FakeClient if distributed else lambda **kw: pytest.fail("wrong adaptive client"))
    monkeypatch.setattr(discovery, "ProgressLine", FakeProgress)
    monkeypatch.setattr(discovery, "_run_work", fake_work)
    monkeypatch.setattr(discovery, "discovery_summary", lambda: {})
    monkeypatch.setattr(discovery, "release_bucket_claims", lambda *a: None)
    instance = None
    # fake_work observes the active client's __aenter__ result.
    original_enter = FakeClient.__aenter__
    async def enter(self):
        nonlocal instance
        instance = self
        return await original_enter(self)
    monkeypatch.setattr(FakeClient, "__aenter__", enter)

    await discovery.run(["010"], distributed=distributed, numeric_split=False, verbose=False)
    assert len(constructed) == 1 and closed == [True]
    assert "max_retries" in constructed[0] if not distributed else "endpoint_rates" in constructed[0]


@pytest.mark.asyncio
async def test_internal_recovery_shared_client_is_not_closed(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository as repository

    monkeypatch.setattr(
        repository, "recovery_candidates",
        lambda limit, parent_id: [{"reason": "stale_zero", "bucket_id": 42}],
    )
    monkeypatch.setattr(
        repository, "recovery_family",
        lambda bucket: ({"status": "failed"}, []),
    )
    monkeypatch.setattr(discovery, "observed_status_codes", lambda: [])
    monkeypatch.setattr(discovery, "validate_splits", lambda verbose: 0)

    class Shared:
        def __init__(self):
            self.exited = False
        async def __aexit__(self, *args):
            self.exited = True

    shared = Shared()
    report = await discovery.recover_discovery(execute=True, shared_client=shared)
    assert report["mode"] == "execute"
    assert report["unresolved"] >= 1
    assert not shared.exited


@pytest.mark.asyncio
async def test_real_discovery_count_passes_separate_timeout_without_changing_payload(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository
    from dbd.client.endpoints.search import SearchFilters

    class Stub:
        async def post(self, path, body, **kw):
            self.received = (path, body, kw)
            return {
                "meta": {
                    "currentPage": 1, "totalItems": 1, "totalPages": 1,
                    "itemsPerPage": 10,
                },
                "contents": [{"jpTypeCode": "5", "jpNo": "0100000000001"}],
            }

    monkeypatch.setattr(discovery_repository, "archive_search", lambda *a, **kw: 99)
    client = Stub()
    page, archive_id, _, _ = await discovery._count(client, 42, SearchFilters(keyword="010"))
    path, body, kwargs = client.received
    assert path == "/api/v1/company-profiles/infos"
    assert body["keyword"] == "010" and body["currentPage"] == 1
    assert kwargs["timeout"] == discovery.settings.dbd_count_timeout_seconds
    assert kwargs["rate_key"] == "search"
    assert page.total_items == 1 and archive_id == 99
