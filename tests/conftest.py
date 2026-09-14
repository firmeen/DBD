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
import sys

import pytest

from dbd.core.settings import settings
from dbd.storage.mysql.async_engine import create_engine
from dbd.storage.mysql.migrations import migrate
from dbd.storage.mysql.repositories.worker_repository import WorkerRepository

# Never point integration tests at a real database: both the explicit opt-in
# and the disposable dbd_test_* name are required.
_OPTED_IN = os.getenv("DBD_INTEGRATION") == "1"
_DISPOSABLE_DB = settings.db_name.startswith("dbd_test_")

INTEGRATION_ENABLED = _OPTED_IN and _DISPOSABLE_DB

_SKIP_REASON = "Requires DBD_INTEGRATION=1 and a disposable dbd_test_* database"


def pytest_collection_modifyitems(config, items):
    if INTEGRATION_ENABLED:
        return

    skip = pytest.mark.skip(reason=_SKIP_REASON)
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


def pytest_report_header(config):
    if INTEGRATION_ENABLED:
        return f"integration: ENABLED against {settings.db_name}"
    return "integration: disabled (set DBD_INTEGRATION=1 and a dbd_test_* DB_NAME to enable)"


def pytest_configure(config):
    """Shout when integration was requested but did not actually turn on.

    Asking for integration tests and silently getting none is indistinguishable
    from a passing run: the suite still reports "passed". The usual cause is the
    database variable never reaching pydantic - `DBD_NAME` instead of `DB_NAME`,
    for instance - which leaves settings pointed at the real database, so the
    guard correctly refuses, quietly.

    Written to stderr rather than returned as a header because `-q` hides
    headers, and `-q` is exactly how this suite is normally run.
    """
    if not _OPTED_IN or _DISPOSABLE_DB:
        return

    print(
        "\n"
        "  ===============================================================\n"
        "   DBD_INTEGRATION=1 but integration tests will NOT run.\n"
        f"   DB_NAME resolved to {settings.db_name!r}, which is not a dbd_test_* database,\n"
        "   so the safety guard is refusing to touch it.\n"
        "\n"
        '   The variable is DB_NAME, not DBD_NAME:  $env:DB_NAME="dbd_test_local"\n'
        "  ===============================================================\n",
        file=sys.stderr,
    )


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
