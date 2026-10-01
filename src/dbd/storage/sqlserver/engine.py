from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from ...core.settings import settings


class SqlServerConfigurationError(RuntimeError):
    pass


class SqlServerDependencyError(RuntimeError):
    pass


def _pymssql():
    try:
        import pymssql  # type: ignore
    except ImportError as exc:
        raise SqlServerDependencyError(
            "SQL Server sync requires pymssql. Install pymssql in the active environment "
            "before invoking dbd.pipelines.sqlserver_sync."
        ) from exc
    return pymssql


def validate_sqlserver_settings() -> None:
    missing = []
    if not settings.mssql_server:
        missing.append("MSSQL_SERVER")
    if not settings.mssql_user:
        missing.append("MSSQL_USER")
    if not settings.mssql_password:
        missing.append("MSSQL_PASSWORD")
    if missing:
        raise SqlServerConfigurationError(
            "Missing SQL Server configuration: " + ", ".join(missing)
        )
    if settings.mssql_database != "TIPDataIntelligence":
        raise SqlServerConfigurationError(
            "MSSQL_DATABASE must be 'TIPDataIntelligence' for this pipeline"
        )


def open_connection():
    validate_sqlserver_settings()
    pymssql = _pymssql()
    kwargs: dict[str, Any] = {
        "server": settings.mssql_server,
        "user": settings.mssql_user,
        "password": settings.mssql_password,
        "database": settings.mssql_database,
        "login_timeout": settings.mssql_login_timeout,
        "timeout": settings.mssql_timeout,
        "charset": "UTF-8",
        "autocommit": False,
        "use_datetime2": True,
    }
    if settings.mssql_port is not None:
        kwargs["port"] = str(settings.mssql_port)
    return pymssql.connect(**kwargs)


@contextmanager
def connection():
    conn = open_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def check_connection() -> dict[str, object]:
    with connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT DB_NAME(), SUSER_SNAME(), @@VERSION")
            database, login, version = cur.fetchone()
        finally:
            cur.close()
    return {
        "database": database,
        "login": login,
        "version": str(version).splitlines()[0],
    }
