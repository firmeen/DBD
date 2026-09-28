from contextlib import contextmanager

from ..storage.mysql.engine import connect


def assert_manager_lock(conn) -> None:
    """Fail if this exact connection no longer owns the manager lock.

    The lock is connection-scoped. A dead/reconnected connection must therefore
    stop the collector instead of silently continuing while another manager can
    acquire the lock and create a second independent rate limiter.
    """
    conn.ping(reconnect=False)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT IS_USED_LOCK(CONCAT(DATABASE(), ':dbd-api-manager')) = CONNECTION_ID() AS owned"
        )
        if cur.fetchone()["owned"] != 1:
            raise RuntimeError("DBD API manager lock was lost")


def assert_manager_lock_free() -> None:
    """Fail if the single-process API manager lock is currently owned.

    Legacy distributed workers call this once before registration/claiming so a
    collector that already owns the manager lock cannot be joined later by the
    old runtime.
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT IS_FREE_LOCK(CONCAT(DATABASE(), ':dbd-api-manager')) AS free"
        )
        if cur.fetchone()["free"] != 1:
            raise RuntimeError(
                "Single-process DBD collector/API manager is already active"
            )


@contextmanager
def manager_lock():
    """One API manager per database: an in-memory rate limit has one owner."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT GET_LOCK(CONCAT(DATABASE(), ':dbd-api-manager'), 0) AS acquired")
            if cur.fetchone()["acquired"] != 1:
                raise RuntimeError("Another DBD API manager is running against this database")
        try:
            yield conn
        finally:
            # If the connection was already lost, MySQL released the lock with it.
            # Do not mask the original failure with a release error.
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT RELEASE_LOCK(CONCAT(DATABASE(), ':dbd-api-manager'))")
            except Exception:
                pass
