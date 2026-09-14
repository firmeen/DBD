from sqlalchemy import URL
from sqlalchemy.ext.asyncio import create_async_engine

from ...core.settings import settings


def create_engine():
    return create_async_engine(
        URL.create(
            "mysql+asyncmy",
            username=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
        ),
        pool_size=settings.db_pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
        pool_recycle=1800,
        pool_timeout=settings.db_timeout,
        connect_args={
            "init_command": "SET time_zone = '+00:00'",
            "charset": "utf8mb4",
            "connect_timeout": settings.db_timeout,
        },
    )
