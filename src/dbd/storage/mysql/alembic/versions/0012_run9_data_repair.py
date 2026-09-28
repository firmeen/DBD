"""Widen valid Run 9 profile objective text without truncation."""

import re

from alembic import op

revision = "0012_run9_data_repair"
down_revision = "0011_fabric_recovery"


def _canonical_metadata(row):
    """Normalize driver-specific information_schema label casing.

    MySQL/PyMySQL/SQLAlchemy combinations can expose information_schema labels
    as DATA_TYPE, data_type, or another case variant. Migration decisions must
    not depend on that presentation detail.
    """
    if row is None:
        return None
    normalized = {}
    for key, value in row.items():
        name = str(key).casefold()
        if name in normalized and normalized[name] != value:
            raise RuntimeError(
                f"Ambiguous information_schema metadata for {name!r}; "
                "refusing automatic migration"
            )
        normalized[name] = value
    return normalized


def _column(connection):
    return connection.exec_driver_sql(
        """
        SELECT
          data_type,column_type,character_maximum_length,
          character_octet_length,is_nullable,
          character_set_name,collation_name,column_default,
          extra,column_comment
        FROM information_schema.columns
        WHERE table_schema=DATABASE()
          AND table_name='companies'
          AND column_name='setup_obj_name_keyin'
        """
    ).mappings().first()


def _indexes(connection):
    return connection.exec_driver_sql(
        """
        SELECT DISTINCT index_name
        FROM information_schema.statistics
        WHERE table_schema=DATABASE()
          AND table_name='companies'
          AND column_name='setup_obj_name_keyin'
        ORDER BY index_name
        """
    ).scalars().all()


def _generated_dependencies(connection):
    return connection.exec_driver_sql(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=DATABASE()
          AND table_name='companies'
          AND generation_expression IS NOT NULL
          AND generation_expression<>''
          AND LOWER(generation_expression) LIKE '%setup_obj_name_keyin%'
        ORDER BY column_name
        """
    ).scalars().all()


def _identifier(value, label: str) -> str:
    value = str(value or "")
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise RuntimeError(
            f"Unexpected {label} {value!r}; refusing unsafe automatic migration"
        )
    return value


def upgrade():
    connection = op.get_bind()
    column = _canonical_metadata(_column(connection))
    if column is None:
        raise RuntimeError(
            "companies.setup_obj_name_keyin is missing; refusing an unverified repair migration"
        )

    data_type = str(column["data_type"] or "").lower()
    if data_type in {"text", "mediumtext", "longtext"}:
        return
    if data_type != "varchar":
        raise RuntimeError(
            "Unexpected companies.setup_obj_name_keyin type "
            f"{column['column_type']!r}; manual review is required"
        )

    indexes = list(_indexes(connection))
    generated = list(_generated_dependencies(connection))
    if indexes or generated:
        raise RuntimeError(
            "companies.setup_obj_name_keyin has schema dependencies; "
            f"indexes={indexes!r} generated_columns={generated!r}. "
            "Refusing automatic widening."
        )
    if column.get("column_default") is not None:
        raise RuntimeError(
            "companies.setup_obj_name_keyin has a non-null default; "
            "manual migration review is required"
        )
    if str(column.get("extra") or "").strip():
        raise RuntimeError(
            "companies.setup_obj_name_keyin has unexpected EXTRA metadata; "
            "manual migration review is required"
        )
    if str(column.get("column_comment") or ""):
        raise RuntimeError(
            "companies.setup_obj_name_keyin has a column comment; "
            "manual migration review is required so it is not silently dropped"
        )

    character_set = _identifier(column.get("character_set_name"), "character set")
    collation = _identifier(column.get("collation_name"), "collation")
    is_nullable = str(column.get("is_nullable") or "").upper()
    if is_nullable not in {"YES", "NO"}:
        raise RuntimeError(
            "Unexpected nullability metadata for companies.setup_obj_name_keyin; "
            "manual migration review is required"
        )
    nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
    connection.exec_driver_sql(
        "ALTER TABLE companies MODIFY COLUMN setup_obj_name_keyin "
        f"TEXT CHARACTER SET {character_set} COLLATE {collation} {nullable}"
    )


def downgrade():
    raise RuntimeError(
        "Automatic downgrade could truncate valid text; restore a backup instead"
    )
