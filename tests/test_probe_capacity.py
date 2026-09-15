"""Probe measurement regression tests. No database or network calls."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location(
    "probe_capacity", Path(__file__).resolve().parents[1] / "scripts" / "probe_capacity.py"
)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class Session:
    def __init__(self, status=200):
        self.calls = 0
        self.status = status

    async def request(self, *args, **kwargs):
        self.calls += 1
        return SimpleNamespace(status_code=self.status)


async def test_transport_budget_is_enforced_before_sending():
    raw = Session()
    transport = probe.TimedSession(raw, probe.Budget(2))
    await transport.request("GET", "/")
    await transport.request("GET", "/")
    with pytest.raises(probe.AdmissionClosed):
        await transport.request("GET", "/")
    assert raw.calls == transport.budget.used == 2


async def test_429_stops_new_admissions():
    raw = Session(429)
    transport = probe.TimedSession(raw, probe.Budget(10))
    await transport.request("GET", "/")
    with pytest.raises(probe.AdmissionClosed):
        await transport.request("GET", "/")
    assert raw.calls == 1
    assert transport.samples[0][2] == 429


async def test_expired_window_sends_nothing():
    raw = Session()
    transport = probe.TimedSession(raw, probe.Budget(10))
    transport.deadline = 0
    with pytest.raises(probe.AdmissionClosed):
        await transport.request("GET", "/")
    assert raw.calls == transport.budget.used == 0


async def test_late_completions_are_not_counted_in_window(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(probe.time, "perf_counter", lambda: clock[0])

    class SlowSession(Session):
        async def request(self, *args, **kwargs):
            clock[0] += .6
            return await super().request(*args, **kwargs)

    transport = probe.TimedSession(SlowSession(), probe.Budget(10))

    class Client:
        concurrency = 1

        async def fetch(self, *args):
            await transport.request(*args)
            return SimpleNamespace(data={"jpTypeCode": "7", "jpNo": "0107556000051"})

    result = await probe.run_step(Client(), transport, [("7", "0107556000051")], 5, 1)
    assert result["success"] == 2
    assert result["drained"] == 1
    assert result["window_rps"] == 1
    assert result["overall_rps"] == pytest.approx(2 / 1.2)
    assert result["http_p95"] == pytest.approx(.6)


def test_incomplete_repeats_cannot_qualify(capsys):
    probe.summarise(
        [{"rate": 10, "clean": True, "overall_rps": 9}],
        SimpleNamespace(repeats=2, workload=None),
    )
    assert "inconclusive" in capsys.readouterr().out
