from __future__ import annotations

import sys
from pathlib import Path

import sqlparse
from alembic import command
from alembic.config import Config
from sqlalchemy import URL, create_engine

from ...core.settings import PROJECT_ROOT, settings


def migrate():
    # Connect without a selected database, so a clean installation can bootstrap.
    engine = create_engine(
        URL.create(
            "mysql+pymysql",
            username=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
        ),
        connect_args={"init_command": "SET time_zone = '+00:00'", "connect_timeout": settings.db_timeout},
    )
    try:
        with engine.connect() as conn:
            acquired = conn.exec_driver_sql(
                "SELECT GET_LOCK(%s, 0)", (settings.db_name + ":migrations",)
            ).scalar()
            if acquired != 1:
                raise RuntimeError("Another migration is running")
            try:
                conn.exec_driver_sql(
                    f"CREATE DATABASE IF NOT EXISTS `{settings.db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
                )
                conn.exec_driver_sql(f"USE `{settings.db_name}`")
                conn.dialect.default_schema_name = settings.db_name
                conn.commit()
                cfg = Config()
                cfg.set_main_option("script_location", str(Path(__file__).parent / "alembic"))
                cfg.attributes["connection"] = conn
                command.upgrade(cfg, "head")
                conn.commit()
            finally:
                conn.exec_driver_sql("SELECT RELEASE_LOCK(%s)", (settings.db_name + ":migrations",))
    finally:
        engine.dispose()


def statements(sql):
    return [s.strip() for s in sqlparse.split(sqlparse.format(sql, strip_comments=True)) if s.strip()]


def apply_sql(connection, filename):
    directory = PROJECT_ROOT / "sql"
    if not directory.is_dir():
        directory = Path(sys.prefix) / "share" / "dbd-data" / "sql"
    for statement in statements((directory / filename).read_text(encoding="utf-8")):
        upper = statement.upper()
        if upper.startswith("USE ") or upper.startswith("CREATE DATABASE"):
            continue
        if upper.startswith("ALTER TABLE COMPANIES"):
            # The historic enrichment migration grouped columns in one ALTER.
            # Adopt partially upgraded databases one column at a time.
            import re

            for clause in re.split(
                r",\s*(?=ADD COLUMN)", statement.split("\n", 1)[1].rstrip(";"), flags=re.IGNORECASE
            ):
                name = re.search(r"ADD COLUMN\s+(\w+)", clause, re.IGNORECASE).group(1)
                exists = connection.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='companies' AND column_name=%s",
                    (name,),
                ).first()
                if not exists:
                    connection.exec_driver_sql("ALTER TABLE companies " + clause)
        else:
            connection.exec_driver_sql(statement)
