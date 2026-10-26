"""The only v0.6 production storage selector.

Legacy DB_* settings and campaign storage are deliberately excluded.
"""
from __future__ import annotations

from typing import Literal, TypeAlias

from ..core.settings import settings

Backend: TypeAlias = Literal["sqlserver", "mysql"]


class ProductionLeaseLost(RuntimeError):
    """The claim is no longer owned; stale HTTP results must never be committed."""


def production_repository(backend: Backend | None = None):
    selected = backend or settings.dbd_production_backend
    if selected == "sqlserver":
        from .sqlserver.repository import ProductionSqlServerRepository

        return ProductionSqlServerRepository()
    if selected == "mysql":
        from .mysql.production_repository import ProductionMySqlRepository

        return ProductionMySqlRepository()
    raise ValueError(f"Unsupported production backend: {selected!r}")
