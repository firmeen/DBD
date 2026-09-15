"""Mixed-capacity probe regression tests. No database or network calls."""

import asyncio
import importlib.util
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

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


class FakeSession:
    def __init__(self, delay=0):
        self.delay = delay
        self.calls = 0

    async def request(self, *args, **kwargs):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return SimpleNamespace(status_code=200)


async def test_timed_transport_refuses_late_admission_without_network_call():
    session = FakeSession()
    budget = probe.Budget(10)
    transport = probe.TimedTransport(session, budget)
    transport.deadline = time.perf_counter() - 0.001

    with pytest.raises(probe.AdmissionClosed):
        await transport.request("GET", "https://example.invalid")

    assert session.calls == 0
    assert budget.used == 0


async def test_timed_transport_budget_counts_actual_transport_admissions():
    session = FakeSession()
    budget = probe.Budget(1)
    transport = probe.TimedTransport(session, budget)

    response = await transport.request("GET", "https://example.invalid")
    assert response.status_code == 200
    with pytest.raises(probe.AdmissionClosed):
        await transport.request("GET", "https://example.invalid")

    assert session.calls == 1
    assert budget.used == 1
    assert len(transport.samples) == 1


async def test_timed_transport_tracks_real_inflight_request_until_drained():
    session = FakeSession(delay=0.03)
    budget = probe.Budget(2)
    transport = probe.TimedTransport(session, budget)

    task = asyncio.create_task(transport.request("GET", "https://example.invalid"))
    await asyncio.sleep(0.005)
    assert task in transport.inflight_tasks
    await task
    assert task not in transport.inflight_tasks
