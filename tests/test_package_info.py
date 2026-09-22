from __future__ import annotations

import json
from pathlib import Path

from dbd._version import __version__
from dbd.core import package_info


class FakeDistribution:
    def __init__(
        self,
        *,
        version: str,
        root: Path,
        editable_project: Path | None = None,
    ) -> None:
        self.version = version
        self._root = root
        self._editable_project = editable_project
        self.metadata = {
            "Name": "dbd-data",
            "Summary": "DBD DataWarehouse company data ingestion into MySQL",
            "Home-page": "https://github.com/firmeen/DBD",
            "Author": "Mr. Afimeen Usman",
            "Author-email": "Afirmeen13.us@gmail.com",
            "License": "-",
        }
        self.requires = ["redis>=5,<7", "rich>=14,<15"]
        self.files = ()

    def locate_file(self, item):
        return self._root / str(item)

    def read_text(self, name: str):
        if name != "direct_url.json" or self._editable_project is None:
            return None
        return json.dumps(
            {
                "url": self._editable_project.as_uri(),
                "dir_info": {"editable": True},
            }
        )


def test_package_snapshot_detects_stale_editable_metadata_and_invalid_remnants(
    monkeypatch,
    tmp_path,
) -> None:
    project = tmp_path / "DBD"
    project.mkdir()
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()
    invalid = site_packages / "~bd-data"
    invalid.mkdir()

    fake = FakeDistribution(
        version="0.3.0",
        root=site_packages,
        editable_project=project,
    )
    monkeypatch.setattr(package_info, "distribution", lambda name: fake)
    monkeypatch.setattr(package_info, "distributions", lambda: [])
    monkeypatch.setattr(package_info, "_PROJECT_ROOT", project)
    monkeypatch.setattr(
        package_info,
        "_invalid_distribution_entries",
        lambda: [str(invalid)],
    )

    snapshot = package_info.package_environment_snapshot()

    assert snapshot["health"] == "mismatch"
    assert snapshot["identity"]["source_version"] == __version__
    assert snapshot["identity"]["installed_version"] == "0.3.0"
    assert snapshot["identity"]["version_match"] is False
    assert snapshot["installation"]["editable"] is True
    assert snapshot["installation"]["editable_project_location"] == str(project)
    assert snapshot["installation"]["dependency_count"] == 2
    assert snapshot["integrity"]["invalid_distribution_count"] == 1
    assert {issue["code"] for issue in snapshot["integrity"]["issues"]} == {
        "version_mismatch",
        "invalid_distribution_remnants",
    }
    assert "python -m pip install -e ." in snapshot["actions"]


def test_package_snapshot_is_healthy_when_source_and_metadata_match(
    monkeypatch,
    tmp_path,
) -> None:
    project = tmp_path / "DBD"
    project.mkdir()
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()

    fake = FakeDistribution(
        version=__version__,
        root=site_packages,
        editable_project=project,
    )
    monkeypatch.setattr(package_info, "distribution", lambda name: fake)
    monkeypatch.setattr(package_info, "distributions", lambda: [])
    monkeypatch.setattr(package_info, "_PROJECT_ROOT", project)
    monkeypatch.setattr(package_info, "_invalid_distribution_entries", lambda: [])

    snapshot = package_info.package_environment_snapshot()

    assert snapshot["health"] == "healthy"
    assert snapshot["identity"]["version_match"] is True
    assert snapshot["integrity"]["issues"] == []
    assert snapshot["actions"] == []


def test_package_snapshot_detects_editable_location_mismatch(
    monkeypatch,
    tmp_path,
) -> None:
    project = tmp_path / "DBD"
    project.mkdir()
    other = tmp_path / "OTHER"
    other.mkdir()
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()

    fake = FakeDistribution(
        version=__version__,
        root=site_packages,
        editable_project=other,
    )
    monkeypatch.setattr(package_info, "distribution", lambda name: fake)
    monkeypatch.setattr(package_info, "distributions", lambda: [])
    monkeypatch.setattr(package_info, "_PROJECT_ROOT", project)
    monkeypatch.setattr(package_info, "_invalid_distribution_entries", lambda: [])

    snapshot = package_info.package_environment_snapshot()

    assert snapshot["health"] == "cleanup_needed"
    assert snapshot["integrity"]["issues"][0]["code"] == "editable_location_mismatch"
