import pytest

from dbd.runtime.worker import process


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
