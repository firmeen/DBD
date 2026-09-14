from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache

import pymysql
from sqlalchemy import URL, create_engine

from ...core.settings import settings

_bound = ContextVar("dbd_connection", default=None)


@lru_cache(maxsize=1)
def pooled_engine():
    return create_engine(
        URL.create(
            "mysql+pymysql",
            username=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
        ),
        pool_size=settings.db_pool_size,
        max_overflow=2,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
        pool_recycle=1800,
        pool_timeout=settings.db_timeout,
        connect_args={
            "init_command": "SET time_zone = '+00:00'",
            "charset": "utf8mb4",
            "connect_timeout": settings.db_timeout,
            "read_timeout": settings.db_timeout,
            "write_timeout": settings.db_timeout,
        },
    )


class BoundConnection:
    def __init__(self, connection):
        self.connection = connection

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        pass  # The outer projection owns the transaction.

    def rollback(self):
        pass


class PooledConnection:
    def __init__(self, raw):
        self.raw = raw

    def cursor(self):
        return self.raw.cursor(pymysql.cursors.DictCursor)

    def __getattr__(self, name):
        return getattr(self.raw, name)


@contextmanager
def connect():
    current = _bound.get()
    if current is not None:
        yield BoundConnection(current)
        return
    connection = pooled_engine().raw_connection()
    try:
        yield PooledConnection(connection)
    finally:
        connection.close()


@contextmanager
def use_connection(connection):
    token = _bound.set(connection)
    try:
        yield
    finally:
        _bound.reset(token)
