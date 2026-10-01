from __future__ import annotations

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
