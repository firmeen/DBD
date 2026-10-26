"""Independent pooled MySQL production connections (never legacy DB_*)."""
from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache

from pymysql.constants import CLIENT
from sqlalchemy import URL, create_engine

from ...core.settings import settings


def _configuration():
    if not settings.prod_mysql_user or not settings.prod_mysql_password:
        raise RuntimeError("PROD_MYSQL_USER and PROD_MYSQL_PASSWORD are required")
    if (
        settings.prod_mysql_database == settings.db_name
        and settings.prod_mysql_host == settings.db_host
        and settings.prod_mysql_port == settings.db_port
    ):
        raise RuntimeError(
            "Production MySQL database must differ from legacy DB_NAME; "
            "refusing to mix Generation 1 source and production target"
        )


@lru_cache(maxsize=1)
def pooled_engine():
    _configuration()
    return create_engine(
        URL.create(
            "mysql+pymysql",
            username=settings.prod_mysql_user,
            password=settings.prod_mysql_password,
            host=settings.prod_mysql_host,
            port=settings.prod_mysql_port,
            database=settings.prod_mysql_database,
        ),
        pool_size=settings.prod_mysql_pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=settings.db_timeout,
        isolation_level="READ COMMITTED",
        connect_args={
            "charset": "utf8mb4",
            "client_flag": CLIENT.FOUND_ROWS,
            "init_command": "SET time_zone = '+00:00'",
            "connect_timeout": settings.db_timeout,
            "read_timeout": settings.db_timeout,
            "write_timeout": settings.db_timeout,
        },
    )


@contextmanager
def connection():
    conn = pooled_engine().raw_connection()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction():
    with connection() as conn:
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise


def check_connection() -> dict[str, object]:
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT @@hostname, DATABASE(), CURRENT_USER(), VERSION()")
            host, database, login, version = cursor.fetchone()
    return {
        "server": host,
        "database": database,
        "login": login,
        "version": version,
        "backend": "mysql",
    }
