"""Mixed-capacity probe regression tests. No database or network calls."""

import importlib.util
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "probe_mixed_capacity",
    Path(__file__).resolve().parents[1] / "scripts" / "probe_mixed_capacity.py",
)
probe = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = probe
spec.loader.exec_module(probe)


def test_workload_skips_financial_requests_without_real_fiscal_year():
    workload = probe.build_workload([probe.Target("7", "0107556000051")])
    assert [item.endpoint for item in workload] == [
        "profile",
        "committees",
        "committee_signs",
        "transformations",
    ]


def test_workload_contains_all_seven_endpoints_when_fiscal_year_known():
    workload = probe.build_workload([probe.Target("7", "0107556000051", "2568")])
    assert [item.endpoint for item in workload] == list(probe.DEFAULT_ENDPOINTS)
    financial = [item for item in workload if item.endpoint in probe.FINANCIAL]
    assert len(financial) == 3
    assert {item.fiscal_year for item in financial} == {"2568"}


def test_probe_endpoint_rate_parser_and_429_signature():
    parsed = probe.endpoint_rate_map("profile=25,industry_benchmark=8")
    assert parsed == {"profile": 25.0, "industry_benchmark": 8.0}

    class Limited(RuntimeError):
        pass

    assert probe._error_signature(Limited("HTTP 429")) == "Limited[429]"
