from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def client():
    return _load_module("dbd_remote_client_test", "scripts/remote_access/dbd_client.py")


@pytest.fixture(scope="module")
def gateway():
    return _load_module("dbd_remote_gateway_test", "scripts/remote_access/dbd_gateway.py")


def test_client_health_url(client):
    assert (
        client.get_health_url("wss://example.trycloudflare.com/dbd")
        == "https://example.trycloudflare.com/healthz"
    )


def test_client_health_url_requires_wss(client):
    with pytest.raises(RuntimeError, match="wss://"):
        client.get_health_url("https://example.test/dbd")


def test_gateway_generated_token_is_256_bit_hex(gateway, monkeypatch):
    monkeypatch.delenv("DBD_GATEWAY_TOKEN", raising=False)
    token, generated = gateway.resolve_gateway_token()
    assert generated is True
    assert len(token) == 64
    int(token, 16)


def test_gateway_uses_configured_token(gateway, monkeypatch):
    token = "x" * 32
    monkeypatch.setenv("DBD_GATEWAY_TOKEN", token)
    resolved, generated = gateway.resolve_gateway_token()
    assert resolved == token
    assert generated is False


def test_quick_tunnel_url_regex(gateway):
    line = "INF | https://alpha-beta.trycloudflare.com |"
    match = gateway.TRY_CLOUDFLARE_RE.search(line)
    assert match is not None
    assert match.group(0) == "https://alpha-beta.trycloudflare.com"


def test_gateway_bindings_are_loopback_only(client, gateway):
    assert client.LOCAL_HOST == "127.0.0.1"
    assert gateway.BRIDGE_HOST == "127.0.0.1"
    assert gateway.MYSQL_HOST == "127.0.0.1"
