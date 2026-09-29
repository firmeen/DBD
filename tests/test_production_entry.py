from __future__ import annotations

from dbd.core.settings import PROJECT_ROOT


def test_windows_launcher_keeps_campaign_guard_as_production_entry() -> None:
    launcher = (PROJECT_ROOT / "Run-DBD.ps1").read_text(encoding="utf-8")

    assert '"campaign"' in launcher
    assert '"guard"' in launcher
    assert '"collect"' not in launcher
    assert "GuardianArgs" in launcher
    assert "OrphanGraceSeconds" in launcher
    assert "RestartDelaySeconds" in launcher


def test_collector_documentation_is_explicitly_non_production() -> None:
    collector_doc = (PROJECT_ROOT / "docs" / "COLLECTOR.md").read_text(encoding="utf-8")

    assert "Diagnostic / Canary" in collector_doc
    assert "not** the production runtime" in collector_doc
    assert "Guardian -> Supervisor -> autoscaled distributed workers" in collector_doc
    assert "Redis shared admission/rate control" in collector_doc
