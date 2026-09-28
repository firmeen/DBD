"""Widen valid Run 9 profile objective text without truncation."""

from alembic import op

revision = "0012_run9_data_repair"
down_revision = "0011_fabric_recovery"


def _column(connection):
    return connection.exec_driver_sql(
        """
        SELECT data_type,column_type,character_maximum_length,is_nullable
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


def upgrade():
    connection = op.get_bind()
    column = _column(connection)
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

    connection.exec_driver_sql(
        "ALTER TABLE companies MODIFY COLUMN setup_obj_name_keyin TEXT NULL"
    )


def downgrade():
    raise RuntimeError(
        "Automatic downgrade could truncate valid text; restore a backup instead"
    )
