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



@pytest.mark.parametrize(
    "argv",
    [
        ["dbd-data", "check-db", "--json"],
        ["dbd-data", "migrate", "--json"],
        ["dbd-data", "discover", "--json"],
        ["dbd-data", "fetch-company", "70107556000051", "--json"],
    ],
)
def test_v06_data_commands_reject_json_without_breaking_legacy_json(monkeypatch, argv) -> None:
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exc:
        cli_main.main()
    assert exc.value.code == 2


def test_cycle_status_does_not_add_coverage_surface(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["dbd-data", "cycle", "status", "--coverage"])
    with pytest.raises(SystemExit) as exc:
        cli_main.main()
    assert exc.value.code == 2

def test_failed_cycle_returns_nonzero_exit_code(monkeypatch) -> None:
    captured = {}

    class FakePresenter:
        def __init__(self, *, json_mode=False, verbose=False):
            self.json_mode = json_mode
            self.verbose = verbose

    class FakeTerminal:
        def __init__(self, command, *, app):
            captured["command"] = command
            captured["app"] = app

        def start(self, **context):
            captured["context"] = context
            return self

        def event(self, _name, _payload):
            pass

        def finish(self, payload):
            captured["payload"] = payload

        def fail(self, message):
            captured["failure"] = message

        def interrupt(self):
            captured["interrupted"] = True

        def close(self):
            captured["closed"] = True

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
    monkeypatch.setattr(cli_main, "V06Terminal", FakeTerminal)
    monkeypatch.setattr(workflow, "cycle_run", failed_cycle)
    monkeypatch.setattr(sys, "argv", ["dbd-data", "cycle", "run"])

    with pytest.raises(SystemExit) as exc:
        cli_main.main()

    assert exc.value.code == 1
    assert captured["payload"]["status"] == "FAILED"
    assert captured["closed"] is True
