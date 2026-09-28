import pytest

from dbd.runtime.worker import _assert_distributed_manager_free, process


class BrokenArchiveRepo:
    @property
    def archive_backend(self):
        raise ValueError("synthetic archive configuration failure")


class MustNotOpenClient:
    opened = False

    def __init__(self, **kwargs):
        self.__class__.opened = True
        raise AssertionError("DBD client must not be constructed before archive preflight")


async def test_archive_preflight_runs_before_http_client_or_task_claims():
    MustNotOpenClient.opened = False

    with pytest.raises(ValueError, match="synthetic archive configuration failure"):
        await process(
            BrokenArchiveRepo(),
            1,
            concurrency=1,
            client_factory=MustNotOpenClient,
        )

    assert MustNotOpenClient.opened is False



class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class _ManagerFenceConnection:
    def __init__(self, available):
        self.available = available

    async def execute(self, statement):
        return _ScalarResult(self.available)


class _ManagerFenceContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _ManagerFenceEngine:
    def __init__(self, available):
        self.connection = _ManagerFenceConnection(available)

    def connect(self):
        return _ManagerFenceContext(self.connection)


class _ManagerFenceRepo:
    def __init__(self, available):
        self.engine = _ManagerFenceEngine(available)


async def test_legacy_manager_fence_allows_worker_when_collector_is_absent():
    await _assert_distributed_manager_free(_ManagerFenceRepo(1))


async def test_legacy_manager_fence_stops_worker_when_collector_is_active():
    with pytest.raises(RuntimeError, match="collector/API manager became active"):
        await _assert_distributed_manager_free(_ManagerFenceRepo(0))
