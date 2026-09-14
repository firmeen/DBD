"""Shared fixtures and the integration gate.

Fixtures only propagate across test modules from a conftest, which is why
`repo` and `schema` live here: importing them from another test module makes
them look imported-but-unused to linters, and a module that forgets the
import fails with "fixture not found" instead of skipping.

Integration tests are gated by a marker rather than by a skipif copied into
every module, so a new integration module only has to declare
`pytestmark = pytest.mark.integration`.
"""

from __future__ import annotations

import os

import pytest

from dbd.core.settings import settings
from dbd.storage.mysql.async_engine import create_engine
from dbd.storage.mysql.migrations import migrate
from dbd.storage.mysql.repositories.worker_repository import WorkerRepository

# Never point integration tests at a real database: both the explicit opt-in
# and the disposable dbd_test_* name are required.
INTEGRATION_ENABLED = os.getenv("DBD_INTEGRATION") == "1" and settings.db_name.startswith("dbd_test_")

_SKIP_REASON = "Requires DBD_INTEGRATION=1 and a disposable dbd_test_* database"


def pytest_collection_modifyitems(config, items):
    if INTEGRATION_ENABLED:
        return

    skip = pytest.mark.skip(reason=_SKIP_REASON)
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def schema():
    if INTEGRATION_ENABLED:
        migrate()
        migrate()  # Real adoption and repeatability, not just DDL string checks.


@pytest.fixture
async def repo():
    engine = create_engine()
    try:
        yield WorkerRepository(engine)
    finally:
        assert engine.pool.checkedout() == 0, "Worker left a database connection checked out"
        await engine.dispose()
