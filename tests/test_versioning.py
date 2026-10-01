from __future__ import annotations

import sys
import tomllib
from importlib.metadata import version as installed_version
from pathlib import Path

import pytest

from dbd import __version__
from dbd.cli.main import main


def test_release_version_is_0_5_0() -> None:
    assert __version__ == "0.5.0"


def test_pyproject_uses_source_version_as_single_authority() -> None:
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    assert "version" not in project["project"]
    assert "version" in project["project"]["dynamic"]
    assert (
        project["tool"]["setuptools"]["dynamic"]["version"]["attr"]
        == "dbd._version.__version__"
    )


def test_installed_distribution_matches_source_version() -> None:
    assert installed_version("dbd-data") == __version__


def test_cli_version_matches_source_version(monkeypatch, capsys) -> None:
    monkeypatch.setattr(sys, "argv", ["dbd-data", "--version"])

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"dbd-data {__version__}"
