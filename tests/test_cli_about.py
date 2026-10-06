from __future__ import annotations

import sys

import pytest

import dbd.workflow as workflow
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


def test_cycle_commands_do_not_expand_json_or_scoped_discovery_surface(
    monkeypatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["dbd-data", "cycle", "run", "--json"])
    with pytest.raises(SystemExit) as exc:
        cli_main.main()
    assert exc.value.code == 2

    monkeypatch.setattr(sys, "argv", ["dbd-data", "cycle", "run", "010"])
    with pytest.raises(SystemExit) as exc:
        cli_main.main()
    assert exc.value.code == 2


def test_failed_cycle_returns_nonzero_exit_code(monkeypatch) -> None:
    captured = {}

    class FakePresenter:
        def __init__(self, *, json_mode=False, verbose=False):
            self.json_mode = json_mode

        def command_result(self, title, payload):
            captured["title"] = title
            captured["payload"] = payload

        def message(self, message, *, kind="info"):
            captured["message"] = (message, kind)

    async def failed_cycle(**_kwargs):
        return workflow.CycleResult(
            status="FAILED",
            generation=2,
            phase="core",
            resumed=False,
            discovery={},
            prepare={},
            collection={},
            verification={},
            finalized_rows=0,
        )

    monkeypatch.setattr(cli_main, "TerminalPresenter", FakePresenter)
    monkeypatch.setattr(workflow, "cycle_run", failed_cycle)
    monkeypatch.setattr(sys, "argv", ["dbd-data", "cycle", "run"])

    with pytest.raises(SystemExit) as exc:
        cli_main.main()

    assert exc.value.code == 1
    assert captured["payload"]["status"] == "FAILED"
