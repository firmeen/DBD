from __future__ import annotations

import sys

from dbd.cli import main as cli_main
from dbd.core import package_info


def test_about_debug_implies_verbose(monkeypatch) -> None:
    captured = {}

    class FakePresenter:
        def __init__(self, *, json_mode=False, verbose=False):
            captured["json_mode"] = json_mode
            captured["verbose"] = verbose

        def package_about(self, payload):
            captured["payload"] = payload

    payload = {
        "health": "healthy",
        "identity": {"source_version": "0.4.0", "installed_version": "0.4.0"},
    }
    monkeypatch.setattr(cli_main, "TerminalPresenter", FakePresenter)
    monkeypatch.setattr(package_info, "package_environment_snapshot", lambda: payload)
    monkeypatch.setattr(sys, "argv", ["dbd-data", "about", "--debug"])

    cli_main.main()

    assert captured["json_mode"] is False
    assert captured["verbose"] is True
    assert captured["payload"] == payload


def test_about_json_selects_machine_mode(monkeypatch) -> None:
    captured = {}

    class FakePresenter:
        def __init__(self, *, json_mode=False, verbose=False):
            captured["json_mode"] = json_mode
            captured["verbose"] = verbose

        def package_about(self, payload):
            captured["payload"] = payload

    payload = {"health": "healthy"}
    monkeypatch.setattr(cli_main, "TerminalPresenter", FakePresenter)
    monkeypatch.setattr(package_info, "package_environment_snapshot", lambda: payload)
    monkeypatch.setattr(sys, "argv", ["dbd-data", "about", "--json"])

    cli_main.main()

    assert captured["json_mode"] is True
    assert captured["verbose"] is False
    assert captured["payload"] == payload
