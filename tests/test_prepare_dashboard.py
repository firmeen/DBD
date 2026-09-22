from __future__ import annotations

from io import StringIO
from types import SimpleNamespace

import pytest

import dbd.core.prepare_dashboard as prepare_dashboard_module
import dbd.runtime.worker as worker_module
from dbd.core.prepare_dashboard import (
    LiveCampaignPrepareDashboard,
    PreparationSnapshot,
)
from dbd.runtime.worker import prepare


class FakeClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def snapshot(**overrides) -> PreparationSnapshot:
    values = {
        "run_id": 8,
        "scheduler_mode": "fabric_v2",
        "phase": "materializing",
        "processed": 250_000,
        "total": 1_000_000,
        "batches": 250,
        "batch_size": 1000,
        "changed": 10_000,
        "stale": 20_000,
        "forced": 0,
        "skipped": 200_000,
        "work_bearing": 50_000,
        "elapsed_seconds": 25.0,
    }
    values.update(overrides)
    return PreparationSnapshot(**values)


def test_prepare_dashboard_non_tty_is_compact_and_informative() -> None:
    stream = StringIO()
    dashboard = LiveCampaignPrepareDashboard(stream=stream, tty=False, interval=0)

    dashboard.render(snapshot(), force=True)

    output = stream.getvalue()
    assert "CAMPAIGN 8 | MATERIALIZING" in output
    assert "250,000/1,000,000 25.00%" in output
    assert "batch=1,000" in output
    assert "changed=10,000" in output
    assert "stale=20,000" in output
    assert "skip=200,000" in output
    assert "rate=10,000/s" in output


def test_prepare_dashboard_rich_tty_renders_live_panel() -> None:
    stream = StringIO()
    dashboard = LiveCampaignPrepareDashboard(stream=stream, tty=True, interval=0)

    dashboard.render(snapshot(), force=True)
    dashboard.close()

    output = stream.getvalue()
    assert "CAMPAIGN PREPARATION" in output
    assert "DBD DATA PLATFORM" in output
    assert "MATERIALIZE" in output
    assert "PLANNER" in output
    assert "FLOW" in output


def test_prepare_dashboard_throttles_but_ready_forces_final_render() -> None:
    stream = StringIO()
    clock = FakeClock()
    dashboard = LiveCampaignPrepareDashboard(
        stream=stream,
        tty=False,
        interval=10,
        clock=clock,
    )

    dashboard.render(snapshot(processed=100_000))
    clock.value = 1
    dashboard.render(snapshot(processed=200_000))
    dashboard.render(snapshot(phase="ready", processed=1_000_000))

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    assert "100,000/1,000,000" in lines[0]
    assert "READY" in lines[1]
    assert "1,000,000/1,000,000" in lines[1]


def test_prepare_dashboard_has_no_runtime_clients() -> None:
    namespace = vars(prepare_dashboard_module)
    assert "Redis" not in namespace
    assert "connect" not in namespace
    assert "create_engine" not in namespace


@pytest.mark.asyncio
async def test_master_prepare_uses_dedicated_large_batch_and_reports_progress(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        worker_module,
        "settings",
        SimpleNamespace(
            dbd_materialize_batch_size=1000,
            dbd_write_batch_size=100,
        ),
    )

    class FakeScheduler:
        def __init__(self) -> None:
            self.calls = 0
            self.batch_sizes = []
            self.metrics = {
                "companies": 0,
                "changed": 0,
                "stale": 0,
                "forced": 0,
                "skipped": 0,
                "work_bearing": 0,
            }
            self.ready_run = None

        def preparation_metrics(self):
            return dict(self.metrics)

        async def enqueue_master(self, run_id, resources, after, batch_size, statuses):
            self.batch_sizes.append(batch_size)
            self.calls += 1
            if self.calls == 1:
                self.metrics.update(
                    companies=1000,
                    changed=100,
                    stale=50,
                    skipped=700,
                    work_bearing=300,
                )
                return 1000
            if self.calls == 2:
                self.metrics.update(
                    companies=1500,
                    changed=125,
                    stale=75,
                    skipped=1100,
                    work_bearing=400,
                )
                return 1500
            return None

        async def ready(self, run_id):
            self.ready_run = run_id

    class FakeRepo:
        def __init__(self) -> None:
            self.scheduler = FakeScheduler()
            self.created = None

        async def create_run(
            self,
            source,
            parts,
            fiscal_year,
            *,
            scheduler_mode,
            force_refresh_mask,
        ):
            self.created = {
                "source": source,
                "parts": parts,
                "fiscal_year": fiscal_year,
                "scheduler_mode": scheduler_mode,
                "force_refresh_mask": force_refresh_mask,
            }
            return 8

        def for_run(self, run):
            assert run["scheduler_mode"] == "fabric_v2"
            return self.scheduler

        async def master_population(self):
            return {
                "total": 1500,
                "conflicts": 0,
                "eligible_all_statuses": 1500,
                "eligible_operating": 1000,
                "unknown_status": 0,
            }

    repo = FakeRepo()
    snapshots = []

    run_id = await prepare(
        repo,
        master=True,
        statuses=None,
        scheduler_mode="fabric_v2",
        progress=snapshots.append,
    )

    assert run_id == 8
    assert repo.scheduler.ready_run == 8
    assert repo.scheduler.batch_sizes == [1000, 1000, 1000]
    assert snapshots[0].phase == "counting"
    assert any(item.phase == "materializing" for item in snapshots)
    assert snapshots[-1].phase == "ready"
    assert snapshots[-1].processed == 1500
    assert snapshots[-1].total == 1500
    assert snapshots[-1].changed == 125
    assert snapshots[-1].stale == 75
    assert snapshots[-1].skipped == 1100
    assert snapshots[-1].work_bearing == 400
