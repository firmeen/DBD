import pytest

from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.migrations import migrate

pytestmark = pytest.mark.integration


def test_correctness_migration_adopts_existing_objects():
    """Simulate Alembic metadata lagging behind DDL after an interrupted MySQL migration.

    The session fixture already applied 0004. Moving only alembic_version back to
    0003 makes the next migrate() replay 0004 against objects that already exist,
    which must succeed and restore the head revision.
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0003_queue_indexes'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == "0004_correctness_guards"
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='company_master'
              AND column_name IN ('observed_jp_stat_code','observed_status_at')
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "observed_jp_stat_code",
            "observed_status_at",
        ]
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name='discovery_sightings'
              AND index_name='PRIMARY'
            ORDER BY seq_in_index
            """
        )
        assert [row["name"] for row in cur.fetchall()] == ["jp_type_code", "jp_no"]
