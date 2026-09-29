from __future__ import annotations

from contextlib import contextmanager

import pytest

import dbd.runtime.locking as locking


class FakeCursor:
    def __init__(self, *, free=1, acquired=1, calls=None):
        self.free = free
        self.acquired = acquired
        self.calls = calls if calls is not None else []
        self._last = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params=None):
        self._last = str(sql)
        self.calls.append((self._last, params))

    def fetchone(self):
        if "IS_FREE_LOCK" in self._last:
            return {"free": self.free}
        if "GET_LOCK" in self._last:
            return {"acquired": self.acquired}
        return {"owned": 1}


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


@contextmanager
def fake_connect(cursor):
    yield FakeConnection(cursor)


def test_manager_lock_free_refuses_active_collector(monkeypatch) -> None:
    cursor = FakeCursor(free=0)
    monkeypatch.setattr(locking, "connect", lambda: fake_connect(cursor))

    with pytest.raises(RuntimeError, match="collector/API manager is already active"):
        locking.assert_manager_lock_free()


def test_runtime_handoff_lock_acquires_and_releases(monkeypatch) -> None:
    calls = []
    cursor = FakeCursor(acquired=1, calls=calls)
    monkeypatch.setattr(locking, "connect", lambda: fake_connect(cursor))

    with locking.runtime_handoff_lock(timeout_seconds=3):
        calls.append(("inside", None))

    assert any("GET_LOCK" in sql for sql, _ in calls if isinstance(sql, str))
    assert ("inside", None) in calls
    assert any("RELEASE_LOCK" in sql for sql, _ in calls if isinstance(sql, str))


def test_runtime_handoff_lock_fails_when_busy(monkeypatch) -> None:
    cursor = FakeCursor(acquired=0)
    monkeypatch.setattr(locking, "connect", lambda: fake_connect(cursor))

    with pytest.raises(RuntimeError, match="runtime handoff is busy"):
        with locking.runtime_handoff_lock(timeout_seconds=0):
            pass
