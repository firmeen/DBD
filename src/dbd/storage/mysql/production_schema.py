"""Five-table MySQL production schema with SQL Server-equivalent data semantics.

The existing SQL Server ColumnSpec definitions are the canonical logical contract.
MySQL-only DDL stays here. Legacy MySQL schema and Alembic are not modified.
"""
from __future__ import annotations

import re

from ..sqlserver.schema import (
    EXTRA_CHECKS,
    FOREIGN_KEYS,
    INDEXES,
    JSON_CHECKS,
    TABLE_COLUMNS,
)

TABLES = tuple(name.split(".", 1)[1] for name in TABLE_COLUMNS)


def _type(value: str) -> str:
    upper = value.upper()
    if upper.startswith("NVARCHAR(MAX)"):
        return "LONGTEXT"
    if upper.startswith("NVARCHAR("):
        return "VARCHAR" + upper[len("NVARCHAR"):]
    if upper.startswith("DATETIME2("):
        return "DATETIME" + upper[len("DATETIME2"):]
    if upper == "BIT":
        return "TINYINT(1)"
    return upper


def _column_definition(column) -> str:
    parts = [f"`{column.name}`", _type(column.sql_type)]
    if column.nullable:
        parts.append("NULL")
    else:
        parts.append("NOT NULL")
    if column.identity:
        parts.append("AUTO_INCREMENT")
    if column.default_sql is not None:
        default = re.sub(r"^N'", "'", column.default_sql)
        parts.append(f"DEFAULT {default}")
    return " ".join(parts)


def _check(expression: str) -> str:
    # SQL Server LEN/LIKE character class is not MySQL syntax.
    if expression.startswith("LEN([branch_no])=5"):
        return "`branch_no` REGEXP '^[0-9]{5}$'"
    return expression.replace("[", "`").replace("]", "`").replace("N'", "'")


def create_table_sql(table_name: str) -> str:
    columns = TABLE_COLUMNS[f"dbo.{table_name}"]
    primary = next(column.name for column in columns if column.identity)
    declarations = [_column_definition(col) for col in columns]
    declarations.append(f"PRIMARY KEY (`{primary}`)")
    if f"dbo.{table_name}" in FOREIGN_KEYS:
        fk_name, col, foreign_table, foreign_col = FOREIGN_KEYS[f"dbo.{table_name}"]
        declarations.append(
            f"CONSTRAINT `{fk_name}` FOREIGN KEY (`{col}`) "
            f"REFERENCES `{foreign_table.split('.', 1)[1]}` (`{foreign_col}`)"
        )
    for name, col in JSON_CHECKS.get(f"dbo.{table_name}", ()):
        declarations.append(
            f"CONSTRAINT `{name}` CHECK (`{col}` IS NULL OR JSON_VALID(`{col}`))"
        )
    for name, expression in EXTRA_CHECKS.get(f"dbo.{table_name}", ()):
        declarations.append(f"CONSTRAINT `{name}` CHECK ({_check(expression)})")
    return (
        f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(declarations)
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
    )


def _index_sql(table: str, name: str, unique: bool, columns: tuple[str, ...]) -> str:
    # SQL Server composite UNIQUE treats NULL as a scope value. MySQL does not.
    # Functional key expressions keep NULL scopes unique (no extra table columns).
    if table == "DBD_INDUSTRY" and name == "UQ_DBD_INDUSTRY_SCOPE":
        expressions = [
            f"`{col}`" if col in {"submit_objective_code", "fiscal_year"}
            else f"(COALESCE(`{col}`, ''))"
            for col in columns
        ]
    else:
        expressions = [f"`{col}`" for col in columns]
    unique_sql = "UNIQUE " if unique else ""
    return (
        f"CREATE {unique_sql}INDEX `{name}` ON `{table}` "
        f"({', '.join(expressions)})"
    )


def _tables(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE'"
        )
        return {str(row[0]).upper() for row in cur.fetchall()}


def _indexes(conn, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
            (table,),
        )
        return {str(row[0]) for row in cur.fetchall()}



def _matches_industry_scope_expression(expression: object, column: str) -> bool:
    """Accept MySQL catalog formatting, never different index semantics.

    Functional parts must remain COALESCE(the expected column, '').
    Parentheses/identifier quoting/recognized literal charset annotations
    are harmless; fallback values or a different column are not.
    """
    if expression is None:
        return False
    normalized = re.sub(r"\s+", "", str(expression).lower())
    normalized = normalized.replace("`", "")
    normalized = normalized.replace("_utf8mb4''", "''")
    normalized = normalized.replace("_utf8mb3''", "''")
    while normalized.startswith("(") and normalized.endswith(")"):
        depth = 0
        balanced = True
        for pos, ch in enumerate(normalized):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and pos != len(normalized) - 1:
                    balanced = False
                    break
        if not balanced or depth:
            break
        normalized = normalized[1:-1]
    return normalized == f"coalesce({column.lower()},'')"



def _verify_indexes(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME,NON_UNIQUE,SUB_PART,"
            "INDEX_TYPE,EXPRESSION FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s "
            "ORDER BY INDEX_NAME,SEQ_IN_INDEX",
            (table,),
        )
        rows = cur.fetchall()
    actual = {}
    for index, seq, col, nonunique, prefix, kind, expression in rows:
        actual.setdefault(str(index), []).append((
            int(seq), None if col is None else str(col),
            int(nonunique), prefix, str(kind), expression,
        ))
    pk = actual.get("PRIMARY")
    if pk is None or len(pk) != 1 or pk[0][1] != TABLE_COLUMNS[f"dbo.{table}"][0].name:
        raise RuntimeError(f"{table} primary key does not match production contract")
    for name, unique, columns in INDEXES[f"dbo.{table}"]:
        parts = actual.get(name)
        if parts is None or len(parts) != len(columns):
            raise RuntimeError(f"{table} missing or incompatible index {name}")
        for part, col in zip(parts, columns, strict=True):
            seq, stored_column, nonunique, prefix, kind, expression = part
            if nonunique != (0 if unique else 1) or prefix is not None or kind.upper() != "BTREE":
                raise RuntimeError(f"{table} incompatible index properties {name}")
            if table == "DBD_INDUSTRY" and name == "UQ_DBD_INDUSTRY_SCOPE" and col not in {
                "submit_objective_code", "fiscal_year",
            }:
                if not _matches_industry_scope_expression(expression, col):
                    raise RuntimeError(f"{table} nullable scope index mismatch {name}")
            elif str(stored_column or "").lower() != col.lower() or expression is not None:
                raise RuntimeError(f"{table} index column mismatch {name}")


def _columns(conn, table: str) -> dict[str, tuple[str, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, EXTRA "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
            (table,),
        )
        return {row[0]: (str(row[1]).lower(), str(row[2]), str(row[3]).lower())
                for row in cur.fetchall()}



def _normalize_check_expression(expression: object) -> str:
    value = re.sub(r"\s+", "", str(expression).lower().replace("`", ""))
    value = value.replace("_utf8mb4'", "'").replace("_utf8mb3'", "'")
    # MySQL exposes REGEXP as regexp_like() in CHECK_CONSTRAINTS.
    match = re.fullmatch(r"regexp_like\((\w+),('(?:[^']|'')*')\)", value)
    if match:
        value = match.group(1) + "regexp" + match.group(2)
    # SQL catalog adds redundant outer parentheses and parentheses around constants.
    value = re.sub(r"\((-?[0-9]+)\)", r"\1", value)
    while value.startswith("(") and value.endswith(")"):
        level = 0
        fully_wrapped = True
        for i, ch in enumerate(value):
            level += (ch == "(") - (ch == ")")
            if level == 0 and i != len(value) - 1:
                fully_wrapped = False
                break
        if not fully_wrapped or level:
            break
        value = value[1:-1]
    match = re.fullmatch(r"regexp_like\((\w+),('(?:[^']|'')*')\)", value)
    if match:
        value = match.group(1) + "regexp" + match.group(2)
    return value



def validate_schema(conn) -> dict[str, object]:
    existing = _tables(conn)
    for table in TABLES:
        if table.upper() not in existing:
            raise RuntimeError(f"Missing MySQL production table {table}")
        definitions = _columns(conn, table)
        for spec in TABLE_COLUMNS[f"dbo.{table}"]:
            current = definitions.get(spec.name)
            if current is None:
                raise RuntimeError(f"{table} missing required column {spec.name}")
            actual_type, actual_null, actual_extra = current
            expected = _type(spec.sql_type).lower()
            if actual_type != expected:
                raise RuntimeError(
                    f"{table}.{spec.name} type mismatch: {actual_type} != {expected}"
                )
            if (actual_null == "YES") != spec.nullable:
                raise RuntimeError(f"{table}.{spec.name} nullability mismatch")
            if spec.identity and "auto_increment" not in actual_extra:
                raise RuntimeError(f"{table}.{spec.name} missing AUTO_INCREMENT")
        _verify_indexes(conn, table)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS "
                "WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME=%s",
                (table,),
            )
            constraints = {str(row[0]) for row in cur.fetchall()}
        for name, _ in JSON_CHECKS.get(f"dbo.{table}", ()):
            if name not in constraints:
                raise RuntimeError(f"{table} missing JSON check {name}")
        for name, expression in EXTRA_CHECKS.get(f"dbo.{table}", ()):
            if name not in constraints:
                raise RuntimeError(f"{table} missing check {name}")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cc.CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS cc "
                    "WHERE cc.CONSTRAINT_SCHEMA=DATABASE() AND cc.CONSTRAINT_NAME=%s",
                    (name,),
                )
                actual = cur.fetchone()
            if not actual:
                raise RuntimeError(f"{table} missing check definition {name}")
            # MySQL normalizes quoting and parentheses in CHECK_CLAUSE.
            if _normalize_check_expression(actual[0]) != _normalize_check_expression(_check(expression)):
                raise RuntimeError(f"{table} incompatible check definition {name}")
        if f"dbo.{table}" in FOREIGN_KEYS:
            fk_name = FOREIGN_KEYS[f"dbo.{table}"][0]
            if fk_name not in constraints:
                raise RuntimeError(f"{table} missing foreign key {fk_name}")
    return {"status": "READY", "managed_tables": len(TABLES), "tables": TABLES}


def ensure_schema(conn) -> dict[str, object]:
    existing = _tables(conn)
    created = []
    # DDL is allowed only from explicit migrate, never normal collection.
    if "DBD_COMPANY" in existing and "branch_count" not in _columns(conn, "DBD_COMPANY"):
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE `DBD_COMPANY` ADD COLUMN `branch_count` INT NOT NULL DEFAULT 1")
    for table in TABLES:
        if table.upper() not in existing:
            with conn.cursor() as cur:
                cur.execute(create_table_sql(table))
            created.append(table)
        present = _indexes(conn, table)
        for name, unique, columns in INDEXES[f"dbo.{table}"]:
            if name not in present:
                with conn.cursor() as cur:
                    cur.execute(_index_sql(table, name, unique, columns))
        with conn.cursor() as cur:
            cur.execute(
                "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS "
                "WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME=%s", (table,),
            )
            constraints = {str(row[0]) for row in cur.fetchall()}
        planned_checks = [
            (name, f"`{column}` IS NULL OR JSON_VALID(`{column}`)")
            for name, column in JSON_CHECKS.get(f"dbo.{table}", ())
        ] + [
            (name, _check(expression))
            for name, expression in EXTRA_CHECKS.get(f"dbo.{table}", ())
        ]
        for name, predicate in planned_checks:
            if name in constraints:
                continue
            with conn.cursor() as cur:
                # CHECK rejects FALSE, but permits UNKNOWN / NULL.
                cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE NOT ({predicate})")
                bad = int(cur.fetchone()[0] or 0)
                if bad:
                    raise RuntimeError(
                        f"{table} has {bad} rows violating required {name}; refusing ALTER"
                    )
                cur.execute(
                    f"ALTER TABLE `{table}` ADD CONSTRAINT `{name}` CHECK ({predicate})"
                )
            constraints.add(name)
    return {**validate_schema(conn), "created": created}
