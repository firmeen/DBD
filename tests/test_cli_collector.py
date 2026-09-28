from __future__ import annotations

import sys
from contextlib import contextmanager

import pytest

from dbd.cli import main as cli_main


def _fake_presenter(captured):
    class FakePresenter:
        def __init__(self, *, json_mode=False, verbose=False):
            captured["json_mode"] = json_mode
            captured["verbose"] = verbose

        def command_result(self, title, payload):
            captured["title"] = title
            captured["payload"] = payload

        def error(self, message):
            captured["error"] = message

    return FakePresenter


def test_collect_cli_uses_single_process_defaults(monkeypatch) -> None:
    captured = {}

    @contextmanager
    def fake_lock():
        yield object()

    async def fake_collect(args, health=None):
        captured["run_id"] = args.run_id
        captured["concurrency"] = args.concurrency
        captured["rate"] = args.rate
        captured["limit"] = args.limit
        captured["health_present"] = health is not None
        return {"run_id": args.run_id, "paused": False}

    monkeypatch.setattr(cli_main, "TerminalPresenter", _fake_presenter(captured))
    monkeypatch.setattr(cli_main, "manager_lock", fake_lock)
    monkeypatch.setattr(cli_main, "assert_manager_lock", lambda conn: None)
    monkeypatch.setattr(cli_main, "configure", lambda port: None)
    monkeypatch.setattr(cli_main, "_collect_command", fake_collect)
    monkeypatch.setattr(sys, "argv", ["dbd-data", "collect", "9", "--json"])

    cli_main.main()

    assert captured["run_id"] == 9
    assert captured["concurrency"] == 48
    assert captured["rate"] == 50.0
    assert captured["limit"] is None
    assert captured["health_present"] is True
    assert captured["payload"]["paused"] is False


def test_collect_cli_returns_130_after_graceful_pause(monkeypatch) -> None:
    captured = {}

    @contextmanager
    def fake_lock():
        yield object()

    async def fake_collect(args, health=None):
        return {"run_id": args.run_id, "paused": True}

    monkeypatch.setattr(cli_main, "TerminalPresenter", _fake_presenter(captured))
    monkeypatch.setattr(cli_main, "manager_lock", fake_lock)
    monkeypatch.setattr(cli_main, "assert_manager_lock", lambda conn: None)
    monkeypatch.setattr(cli_main, "configure", lambda port: None)
    monkeypatch.setattr(cli_main, "_collect_command", fake_collect)
    monkeypatch.setattr(sys, "argv", ["dbd-data", "collect", "9", "--json"])

    with pytest.raises(SystemExit) as exc:
        cli_main.main()

    assert exc.value.code == 130
