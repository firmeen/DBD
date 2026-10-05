from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dbd.deep.repository import DeepRepository


class RefillHarness:
    def __init__(self, *, unresolved: int, master: int, stale: int) -> None:
        self.runtime = 0
        self.unresolved = unresolved
        self.master = master
        self.stale = stale
        self.reopened = 0
        self.events: list[tuple[str, int]] = []

    async def runtime_size(self) -> int:
        return self.runtime

    async def seed_existing_deep(self, limit: int) -> int:
        source = "unresolved" if self.unresolved else "refresh"
        available = self.unresolved if self.unresolved else self.reopened
        take = min(limit, available)
        if self.unresolved:
            self.unresolved -= take
        else:
            self.reopened -= take
        self.runtime += take
        self.events.append((f"seed:{source}", take))
        return take

    async def promote_master(self, limit: int) -> int:
        take = min(limit, self.master)
        self.master -= take
        self.runtime += take
        self.events.append(("promote", take))
        return take

    async def reopen_stale_active(self, limit: int) -> int:
        take = min(limit, self.stale)
        self.stale -= take
        self.reopened += take
        self.events.append(("refresh", take))
        return take


@pytest.mark.asyncio
async def test_refill_priority_is_unresolved_then_master_then_refresh() -> None:
    repo = RefillHarness(unresolved=1, master=1, stale=100)

    result = await DeepRepository.refill(repo, 3)

    assert result == {
        "runtime": 3,
        "adopted": 1,
        "promoted": 1,
        "refreshed": 1,
        "refresh_scanned": True,
    }
    assert repo.events == [
        ("seed:unresolved", 1),
        ("promote", 1),
        ("refresh", 1),
        ("seed:refresh", 1),
    ]
    assert repo.stale == 99


@pytest.mark.asyncio
async def test_refill_does_not_reopen_stale_when_higher_priority_fills_buffer() -> None:
    repo = RefillHarness(unresolved=2, master=5, stale=100)

    result = await DeepRepository.refill(repo, 4)

    assert result["runtime"] == 4
    assert result["adopted"] == 2
    assert result["promoted"] == 2
    assert result["refreshed"] == 0
    assert not any(name == "refresh" for name, _ in repo.events)


@pytest.mark.asyncio
async def test_promotion_sql_uses_incoming_alias_and_one_atomic_attempt() -> None:
    selected = SimpleNamespace(
        mappings=lambda: SimpleNamespace(
            all=lambda: [{"jp_type_code": "7", "jp_no": "9000000000001"}]
        )
    )
    conn = SimpleNamespace(execute=AsyncMock(side_effect=[
        selected, SimpleNamespace(), SimpleNamespace(), SimpleNamespace(rowcount=1),
    ]))

    async def transaction(operation, *, attempts):
        assert attempts == 1  # Never replay an ambiguous promotion commit.
        return await operation(conn)

    repo = object.__new__(DeepRepository)
    repo._transaction = AsyncMock(side_effect=transaction)
    assert await repo.promote_master(1) == 1

    calls = conn.execute.call_args_list
    selection, promotion, deletion, seed = [str(call.args[0]) for call in calls]
    assert "FOR UPDATE SKIP LOCKED" in selection
    assert calls[0].args[1] == {"limit": 1}
    assert re.search(r"SELECT\s+\*\s+FROM\s*\(", promotion)
    assert ") AS new" in promotion
    assert not re.search(r"\bVALUES\s*\(", promotion, re.IGNORECASE)
    assert "DELETE FROM dbd_company_master" in deletion
    assert "INSERT IGNORE INTO dbd_deep_runtime" in seed
    assert all(call.args[1] == {"t0": "7", "n0": "9000000000001"} for call in calls[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1])
async def test_promotion_nonpositive_limit_does_not_start_transaction(limit) -> None:
    repo = object.__new__(DeepRepository)
    repo._transaction = AsyncMock()
    assert await repo.promote_master(limit) == 0
    repo._transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_refill_can_poll_new_work_without_repeating_stale_scan() -> None:
    repo = RefillHarness(unresolved=0, master=0, stale=100)

    result = await DeepRepository.refill(repo, 4, include_refresh=False)

    assert result == {
        "runtime": 0,
        "adopted": 0,
        "promoted": 0,
        "refreshed": 0,
        "refresh_scanned": False,
    }
    assert not any(name == "refresh" for name, _ in repo.events)


@pytest.mark.asyncio
async def test_repository_forwards_explicit_pool_size(monkeypatch) -> None:
    engine = SimpleNamespace(dispose=AsyncMock())
    calls: list[int | None] = []

    def fake_engine(*, pool_size: int | None = None):
        calls.append(pool_size)
        return engine

    monkeypatch.setattr("dbd.deep.repository.create_deep_engine", fake_engine)

    repo = DeepRepository(pool_size=10)
    assert calls == [10]

    await repo.close()
    engine.dispose.assert_awaited_once()
