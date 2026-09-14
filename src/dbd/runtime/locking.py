from contextlib import contextmanager

from ..storage.mysql.engine import connect


@contextmanager
def manager_lock():
    """One API manager per database: an in-memory rate limit has one owner.

    MySQL releases this connection-scoped lock if the process dies. Legacy
    discovery and bulk commands use the same lock as the async manager.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT GET_LOCK(CONCAT(DATABASE(), ':dbd-api-manager'), 0) AS acquired")
            if cur.fetchone()["acquired"] != 1:
                raise RuntimeError("Another DBD API manager is running against this database")
        try:
            yield conn
        finally:
            with conn.cursor() as cur:
                cur.execute("SELECT RELEASE_LOCK(CONCAT(DATABASE(), ':dbd-api-manager'))")
