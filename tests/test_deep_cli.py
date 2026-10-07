from __future__ import annotations

from types import SimpleNamespace

import pytest

import dbd.deep.cli as deep_cli
from dbd.deep._version import __version__ as deep_version
from dbd.workflow import PrepareResult, RetryResult


class _FakeTerminal:
    def __init__(self):
        self.finished = None
        self.events = []

    def event(self, name, payload):
        self.events.append((name, payload))

    def finish(self, payload):
        self.finished = payload


def test_deep_version_is_separate_from_distribution_version(capsys) -> None:
    assert deep_version == "0.1.0"
    with pytest.raises(SystemExit) as exc:
        deep_cli.parse_args(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == "dbd-deep 0.1.0"


def test_prepare_retry_and_status_have_phase_scope() -> None:
    assert deep_cli.parse_args(["prepare", "--phase", "extended"]).phase == "extended"
    assert deep_cli.parse_args(["retry", "--phase", "extended"]).phase == "extended"
    assert deep_cli.parse_args(["status", "--phase", "extended"]).phase == "extended"


def test_collect_options_remain_explicit_without_mysql_pool_surface() -> None:
    args = deep_cli.parse_args(
        [
            "collect",
            "--phase",
            "core",
            "--continuous",
            "--idle-seconds",
            "7.5",
            "--concurrency",
            "12",
            "--rate",
            "20",
            "--http-retries",
            "2",
            "--limit",
            "100",
        ]
    )
    assert args.phase == "core"
    assert args.continuous is True
    assert args.idle_seconds == 7.5
    assert args.concurrency == 12
    assert args.rate == 20.0
    assert args.http_retries == 2
    assert args.limit == 100


def test_status_coverage_option_remains_explicit() -> None:
    args = deep_cli.parse_args(["status", "--phase", "core", "--coverage"])
    assert args.coverage is True


@pytest.mark.parametrize(
    "argv",
    [
        ["prepare", "--json"],
        ["collect", "--json"],
        ["retry", "--json"],
        ["status", "--json"],
    ],
)
def test_v06_deep_commands_reject_json(argv) -> None:
    with pytest.raises(SystemExit) as exc:
        deep_cli.parse_args(argv)
    assert exc.value.code == 2


@pytest.mark.asyncio
async def test_prepare_does_not_issue_presentation_only_status_read(monkeypatch) -> None:
    async def fake_prepare(**kwargs):
        assert kwargs["phase"] == "core"
        assert callable(kwargs["event"])
        return PrepareResult(generation=4, phase="core", rows_prepared=123)

    async def forbidden_status(**_kwargs):
        raise AssertionError("prepare presentation must not query status")

    monkeypatch.setattr(deep_cli, "prepare", fake_prepare)
    monkeypatch.setattr(deep_cli, "status", forbidden_status)
    terminal = _FakeTerminal()
    code = await deep_cli._prepare(SimpleNamespace(phase="core"), terminal)

    assert code == 0
    assert terminal.finished == {
        "generation": 4,
        "phase": "core",
        "rows_prepared": 123,
    }


@pytest.mark.asyncio
async def test_retry_does_not_issue_presentation_only_status_read(monkeypatch) -> None:
    async def fake_retry(**kwargs):
        assert kwargs["phase"] == "core"
        assert kwargs["limit"] == 50
        assert callable(kwargs["event"])
        return RetryResult(generation=4, phase="core", reopened=17)

    async def forbidden_status(**_kwargs):
        raise AssertionError("retry presentation must not query status")

    monkeypatch.setattr(deep_cli, "retry", fake_retry)
    monkeypatch.setattr(deep_cli, "status", forbidden_status)
    terminal = _FakeTerminal()
    code = await deep_cli._retry(SimpleNamespace(phase="core", limit=50), terminal)

    assert code == 0
    assert terminal.finished == {
        "generation": 4,
        "phase": "core",
        "reopened": 17,
    }


def test_deep_main_preserves_ctrl_c_exit_130(monkeypatch) -> None:
    captured = {}

    class FakeV06Terminal:
        def __init__(self, command, *, app):
            captured["command"] = command
            captured["app"] = app

        def start(self, **context):
            captured["context"] = context
            return self

        def interrupt(self):
            captured["interrupted"] = True

        def close(self):
            captured["closed"] = True

        def fail(self, message):
            captured["failed"] = message

        def event(self, _name, _payload):
            pass

    async def interrupted_collect(_args, _terminal):
        raise KeyboardInterrupt

    monkeypatch.setattr(deep_cli, "V06Terminal", FakeV06Terminal)
    monkeypatch.setattr(deep_cli, "_collect", interrupted_collect)

    with pytest.raises(SystemExit) as exc:
        deep_cli.main(["collect", "--limit", "1"])

    assert exc.value.code == 130
    assert captured["interrupted"] is True
    assert captured["closed"] is True
