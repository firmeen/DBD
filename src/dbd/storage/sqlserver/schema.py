from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from ...contracts.financial_wide import AMOUNT_COLUMNS, YOY_COLUMNS

MANAGED_TABLES = (
    "dbo.DBD_COMPANY",
    "dbo.DBD_COMPANY_RELATION",
    "dbo.DBD_COMPANY_EVENT",
    "dbo.DBD_FINANCIAL",
    "dbo.DBD_INDUSTRY",
)


class SqlServerSchemaError(RuntimeError):
    pass


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sql_type: str
    nullable: bool = True
    identity: bool = False
    default_sql: str | None = None

    def ddl(self) -> str:
        parts = [f"[{self.name}]", self.sql_type]
        if self.identity:
            parts.append("IDENTITY(1,1)")
        parts.append("NULL" if self.nullable else "NOT NULL")
        if self.default_sql is not None:
            parts.append(f"DEFAULT ({self.default_sql})")
        return " ".join(parts)


COMPANY_COLUMNS = (
    ColumnSpec("company_id", "BIGINT", False, identity=True),
    ColumnSpec("juristic_type_code", "NVARCHAR(4)", False),
    ColumnSpec("registration_no", "NVARCHAR(20)", False),
    ColumnSpec("company_name_th", "NVARCHAR(500)"),
    ColumnSpec("company_name_en", "NVARCHAR(500)"),
    ColumnSpec("company_search_name", "NVARCHAR(500)"),
    ColumnSpec("status_code", "NVARCHAR(4)"),
    ColumnSpec("status_description", "NVARCHAR(255)"),
    ColumnSpec("juristic_type_description", "NVARCHAR(255)"),
    ColumnSpec("registration_date", "DATE"),
    ColumnSpec("status_change_date", "DATE"),
    ColumnSpec("dbd_update_date", "DATE"),
    ColumnSpec("address_th", "NVARCHAR(MAX)"),
    ColumnSpec("address_en", "NVARCHAR(MAX)"),
    ColumnSpec("province_code", "NVARCHAR(4)"),
    ColumnSpec("province_name", "NVARCHAR(255)"),
    ColumnSpec("district_code", "NVARCHAR(10)"),
    ColumnSpec("district_name", "NVARCHAR(191)"),
    ColumnSpec("subdistrict_code", "NVARCHAR(10)"),
    ColumnSpec("subdistrict_name", "NVARCHAR(191)"),
    ColumnSpec("postal_code", "NVARCHAR(10)"),
    ColumnSpec("phone_no", "NVARCHAR(50)"),
    ColumnSpec("email", "NVARCHAR(255)"),
    ColumnSpec("website_1", "NVARCHAR(255)"),
    ColumnSpec("website_2", "NVARCHAR(255)"),
    ColumnSpec("website_3", "NVARCHAR(255)"),
    ColumnSpec("website_4", "NVARCHAR(255)"),
    ColumnSpec("business_size_code", "NVARCHAR(4)"),
    ColumnSpec("business_type_code", "NVARCHAR(8)"),
    ColumnSpec("business_type_description", "NVARCHAR(191)"),
    ColumnSpec("setup_objective_code", "NVARCHAR(20)"),
    ColumnSpec("setup_objective_description", "NVARCHAR(500)"),
    ColumnSpec("setup_objective_group_code", "NVARCHAR(20)"),
    ColumnSpec("setup_objective_group_description", "NVARCHAR(500)"),
    ColumnSpec("setup_objective_keyin", "NVARCHAR(MAX)"),
    ColumnSpec("submit_objective_code", "NVARCHAR(20)"),
    ColumnSpec("submit_objective_description", "NVARCHAR(500)"),
    ColumnSpec("submit_objective_keyin", "NVARCHAR(MAX)"),
    ColumnSpec("registered_capital", "DECIMAL(20,2)"),
    ColumnSpec("paid_capital", "DECIMAL(20,2)"),
    ColumnSpec("share_quantity", "DECIMAL(20,2)"),
    ColumnSpec("share_volume", "DECIMAL(20,2)"),
    ColumnSpec("latest_fiscal_year", "NVARCHAR(8)"),
    ColumnSpec("total_assets_latest", "DECIMAL(20,2)"),
    ColumnSpec("total_equity_latest", "DECIMAL(20,2)"),
    ColumnSpec("total_income_latest", "DECIMAL(20,2)"),
    ColumnSpec("net_profit_latest", "DECIMAL(20,2)"),
    ColumnSpec("required_mask", "BIGINT", False, default_sql="0"),
    ColumnSpec("done_mask", "BIGINT", False, default_sql="0"),
    ColumnSpec("not_available_mask", "BIGINT", False, default_sql="0"),
    ColumnSpec("failed_mask", "BIGINT", False, default_sql="0"),
    ColumnSpec("collect_status", "NVARCHAR(16)", False, default_sql="N'DISCOVERED'"),
    ColumnSpec("last_endpoint", "NVARCHAR(64)"),
    ColumnSpec("last_http_status", "SMALLINT"),
    ColumnSpec("last_error", "NVARCHAR(MAX)"),
    ColumnSpec("last_error_at", "DATETIME2(6)"),
    ColumnSpec("last_request_at", "DATETIME2(6)"),
    ColumnSpec("collection_started_at", "DATETIME2(6)"),
    ColumnSpec("collection_completed_at", "DATETIME2(6)"),
    ColumnSpec("lease_owner", "NVARCHAR(191)"),
    ColumnSpec("lease_expires_at", "DATETIME2(6)"),
    ColumnSpec("retry_at", "DATETIME2(6)"),
    ColumnSpec("dataset_generation", "INT", False, default_sql="1"),
    ColumnSpec("first_seen_generation", "INT", False, default_sql="1"),
    ColumnSpec("last_seen_generation", "INT", False, default_sql="1"),
    ColumnSpec("master_payload", "NVARCHAR(MAX)"),
    ColumnSpec("profile_payload", "NVARCHAR(MAX)"),
    ColumnSpec("endpoint_meta", "NVARCHAR(MAX)"),
    ColumnSpec("source_updated_at", "DATETIME2(6)"),
    ColumnSpec("published_at", "DATETIME2(6)", False),
)

RELATION_COLUMNS = (
    ColumnSpec("relation_id", "BIGINT", False, identity=True),
    ColumnSpec("company_id", "BIGINT", False),
    ColumnSpec("relation_type", "NVARCHAR(32)", False),
    ColumnSpec("source_key", "NVARCHAR(191)", False),
    ColumnSpec("sequence_no", "INT"),
    ColumnSpec("committee_type_code", "NVARCHAR(20)"),
    ColumnSpec("committee_no", "NVARCHAR(50)"),
    ColumnSpec("title_code", "NVARCHAR(20)"),
    ColumnSpec("title_name_th", "NVARCHAR(255)"),
    ColumnSpec("title_name_en", "NVARCHAR(255)"),
    ColumnSpec("first_name_th", "NVARCHAR(255)"),
    ColumnSpec("middle_name_th", "NVARCHAR(255)"),
    ColumnSpec("last_name_th", "NVARCHAR(255)"),
    ColumnSpec("first_name_en", "NVARCHAR(255)"),
    ColumnSpec("middle_name_en", "NVARCHAR(255)"),
    ColumnSpec("last_name_en", "NVARCHAR(255)"),
    ColumnSpec("nationality_code", "NVARCHAR(20)"),
    ColumnSpec("nationality_name_th", "NVARCHAR(255)"),
    ColumnSpec("nationality_name_en", "NVARCHAR(255)"),
    ColumnSpec("investment_amount", "DECIMAL(20,2)"),
    ColumnSpec("investment_type_th", "NVARCHAR(255)"),
    ColumnSpec("investment_type_en", "NVARCHAR(255)"),
    ColumnSpec("share_amount", "DECIMAL(20,2)"),
    ColumnSpec("share_quantity", "DECIMAL(20,2)"),
    ColumnSpec("share_volume", "DECIMAL(20,2)"),
    ColumnSpec("share_pct_quantity", "DECIMAL(20,6)"),
    ColumnSpec("share_pct_volume", "DECIMAL(20,6)"),
    ColumnSpec("total_nation", "INT"),
    ColumnSpec("address", "NVARCHAR(MAX)"),
    ColumnSpec("province_code", "NVARCHAR(4)"),
    ColumnSpec("district_code", "NVARCHAR(10)"),
    ColumnSpec("subdistrict_code", "NVARCHAR(10)"),
    ColumnSpec("postal_code", "NVARCHAR(10)"),
    ColumnSpec("phone_no", "NVARCHAR(50)"),
    ColumnSpec("description_th", "NVARCHAR(MAX)"),
    ColumnSpec("description_en", "NVARCHAR(MAX)"),
    ColumnSpec("is_current", "BIT", False, default_sql="1"),
    ColumnSpec("first_seen_generation", "INT", False),
    ColumnSpec("last_seen_generation", "INT", False),
    ColumnSpec("source_updated_at", "DATETIME2(6)"),
    ColumnSpec("payload", "NVARCHAR(MAX)"),
    ColumnSpec("published_at", "DATETIME2(6)", False),
)

EVENT_COLUMNS = (
    ColumnSpec("event_id", "BIGINT", False, identity=True),
    ColumnSpec("company_id", "BIGINT", False),
    ColumnSpec("event_type", "NVARCHAR(32)", False),
    ColumnSpec("source_key", "NVARCHAR(191)", False),
    ColumnSpec("sequence_no", "INT"),
    ColumnSpec("event_date", "DATE"),
    ColumnSpec("fiscal_year", "NVARCHAR(8)"),
    ColumnSpec("nation_group", "NVARCHAR(16)"),
    ColumnSpec("company_name", "NVARCHAR(500)"),
    ColumnSpec("related_registration_no", "NVARCHAR(20)"),
    ColumnSpec("related_juristic_type_code", "NVARCHAR(4)"),
    ColumnSpec("related_company_name", "NVARCHAR(500)"),
    ColumnSpec("old_value", "NVARCHAR(MAX)"),
    ColumnSpec("new_value", "NVARCHAR(MAX)"),
    ColumnSpec("new_value_en", "NVARCHAR(MAX)"),
    ColumnSpec("change_flag", "NVARCHAR(20)"),
    ColumnSpec("change_amount", "DECIMAL(20,2)"),
    ColumnSpec("previous_amount", "DECIMAL(20,2)"),
    ColumnSpec("new_amount", "DECIMAL(20,2)"),
    ColumnSpec("share_amount", "DECIMAL(20,2)"),
    ColumnSpec("share_percentage", "DECIMAL(20,6)"),
    ColumnSpec("description_th", "NVARCHAR(MAX)"),
    ColumnSpec("description_en", "NVARCHAR(MAX)"),
    ColumnSpec("first_seen_generation", "INT", False),
    ColumnSpec("last_seen_generation", "INT", False),
    ColumnSpec("source_updated_at", "DATETIME2(6)"),
    ColumnSpec("payload", "NVARCHAR(MAX)"),
    ColumnSpec("published_at", "DATETIME2(6)", False),
)

FINANCIAL_COLUMNS = (
    ColumnSpec("financial_id", "BIGINT", False, identity=True),
    ColumnSpec("company_id", "BIGINT", False),
    ColumnSpec("juristic_type_code", "NVARCHAR(4)"),
    ColumnSpec("registration_no", "NVARCHAR(20)"),
    ColumnSpec("company_name_th", "NVARCHAR(500)"),
    ColumnSpec("company_name_en", "NVARCHAR(500)"),
    ColumnSpec("fiscal_year", "NVARCHAR(8)", False),
    ColumnSpec("fiscal_year_end_date", "DATE"),
    ColumnSpec("submit_objective_code", "NVARCHAR(20)"),
    ColumnSpec("business_size_code", "NVARCHAR(4)"),
    ColumnSpec("business_size_description", "NVARCHAR(255)"),
    ColumnSpec("accounting_date", "NVARCHAR(32)"),
    ColumnSpec("submission_date", "DATE"),
    ColumnSpec("accounting_code", "NVARCHAR(50)"),
    ColumnSpec("auditor_code", "NVARCHAR(50)"),
    ColumnSpec("submission_status_code", "NVARCHAR(20)"),
    *(ColumnSpec(column, "DECIMAL(28,2)") for column in AMOUNT_COLUMNS),
    ColumnSpec("return_on_cost", "DECIMAL(20,6)"),
    ColumnSpec("current_ratio", "DECIMAL(20,4)"),
    ColumnSpec("debt_to_asset", "DECIMAL(20,4)"),
    ColumnSpec("debt_to_equity", "DECIMAL(20,4)"),
    ColumnSpec("debt_to_capital", "DECIMAL(20,4)"),
    ColumnSpec("total_asset_to_equity", "DECIMAL(20,4)"),
    ColumnSpec("total_asset_turnover", "DECIMAL(20,4)"),
    ColumnSpec("inventory_turnover", "DECIMAL(20,4)"),
    ColumnSpec("account_receivable_turnover", "DECIMAL(20,4)"),
    ColumnSpec("account_payable_turnover", "DECIMAL(20,4)"),
    ColumnSpec("gross_profit_margin", "DECIMAL(20,4)"),
    ColumnSpec("operating_profit_margin", "DECIMAL(20,4)"),
    ColumnSpec("net_profit_margin", "DECIMAL(20,4)"),
    ColumnSpec("return_on_asset", "DECIMAL(20,4)"),
    ColumnSpec("return_on_equity", "DECIMAL(20,4)"),
    ColumnSpec("earning_per_share", "DECIMAL(20,4)"),
    ColumnSpec("cost_to_income", "DECIMAL(20,4)"),
    *(ColumnSpec(column, "DECIMAL(20,6)") for column in YOY_COLUMNS),
    ColumnSpec("statement_items", "NVARCHAR(MAX)"),
    ColumnSpec("submission_updated_at", "DATETIME2(6)"),
    ColumnSpec("statement_updated_at", "DATETIME2(6)"),
    ColumnSpec("submission_payload", "NVARCHAR(MAX)"),
    ColumnSpec("statement_payload", "NVARCHAR(MAX)"),
    ColumnSpec("first_seen_generation", "INT", False),
    ColumnSpec("last_seen_generation", "INT", False),
    ColumnSpec("published_at", "DATETIME2(6)", False),
)

INDUSTRY_COLUMNS = (
    ColumnSpec("industry_id", "BIGINT", False, identity=True),
    ColumnSpec("submit_objective_code", "NVARCHAR(20)", False),
    ColumnSpec("fiscal_year", "NVARCHAR(8)", False),
    ColumnSpec("business_size_code", "NVARCHAR(4)"),
    ColumnSpec("location_type", "NVARCHAR(20)"),
    ColumnSpec("location_no", "NVARCHAR(20)"),
    ColumnSpec("submit_financial_count", "INT"),
    ColumnSpec("profit_company_count", "INT"),
    ColumnSpec("loss_company_count", "INT"),
    ColumnSpec("benchmark_items", "NVARCHAR(MAX)"),
    ColumnSpec("payload", "NVARCHAR(MAX)"),
    ColumnSpec("first_seen_generation", "INT", False),
    ColumnSpec("last_seen_generation", "INT", False),
    ColumnSpec("source_updated_at", "DATETIME2(6)"),
    ColumnSpec("published_at", "DATETIME2(6)", False),
)

TABLE_COLUMNS = {
    "dbo.DBD_COMPANY": COMPANY_COLUMNS,
    "dbo.DBD_COMPANY_RELATION": RELATION_COLUMNS,
    "dbo.DBD_COMPANY_EVENT": EVENT_COLUMNS,
    "dbo.DBD_FINANCIAL": FINANCIAL_COLUMNS,
    "dbo.DBD_INDUSTRY": INDUSTRY_COLUMNS,
}

PRIMARY_KEYS = {
    "dbo.DBD_COMPANY": ("PK_DBD_COMPANY", "company_id"),
    "dbo.DBD_COMPANY_RELATION": ("PK_DBD_COMPANY_RELATION", "relation_id"),
    "dbo.DBD_COMPANY_EVENT": ("PK_DBD_COMPANY_EVENT", "event_id"),
    "dbo.DBD_FINANCIAL": ("PK_DBD_FINANCIAL", "financial_id"),
    "dbo.DBD_INDUSTRY": ("PK_DBD_INDUSTRY", "industry_id"),
}

INDEXES = {
    "dbo.DBD_COMPANY": (
        ("UQ_DBD_COMPANY_IDENTITY", True, ("juristic_type_code", "registration_no")),
        ("IX_DBD_COMPANY_COLLECT", False, ("collect_status", "retry_at", "lease_expires_at")),
        ("IX_DBD_COMPANY_PROVINCE", False, ("province_code",)),
        ("IX_DBD_COMPANY_BUSINESS_TYPE", False, ("business_type_code",)),
    ),
    "dbo.DBD_COMPANY_RELATION": (
        ("UQ_DBD_COMPANY_RELATION_SOURCE", True, ("company_id", "relation_type", "source_key")),
    ),
    "dbo.DBD_COMPANY_EVENT": (
        ("UQ_DBD_COMPANY_EVENT_SOURCE", True, ("company_id", "event_type", "source_key")),
    ),
    "dbo.DBD_FINANCIAL": (
        ("UQ_DBD_FINANCIAL_YEAR", True, ("company_id", "fiscal_year")),
    ),
    "dbo.DBD_INDUSTRY": (
        (
            "UQ_DBD_INDUSTRY_SCOPE",
            True,
            (
                "submit_objective_code",
                "fiscal_year",
                "business_size_code",
                "location_type",
                "location_no",
            ),
        ),
    ),
}

FOREIGN_KEYS = {
    "dbo.DBD_COMPANY_RELATION": (
        "FK_DBD_COMPANY_RELATION_COMPANY",
        "company_id",
        "dbo.DBD_COMPANY",
        "company_id",
    ),
    "dbo.DBD_COMPANY_EVENT": (
        "FK_DBD_COMPANY_EVENT_COMPANY",
        "company_id",
        "dbo.DBD_COMPANY",
        "company_id",
    ),
    "dbo.DBD_FINANCIAL": (
        "FK_DBD_FINANCIAL_COMPANY",
        "company_id",
        "dbo.DBD_COMPANY",
        "company_id",
    ),
}

JSON_CHECKS = {
    "dbo.DBD_COMPANY": (
        ("CK_DBD_COMPANY_MASTER_JSON", "master_payload"),
        ("CK_DBD_COMPANY_PROFILE_JSON", "profile_payload"),
        ("CK_DBD_COMPANY_ENDPOINT_META_JSON", "endpoint_meta"),
    ),
    "dbo.DBD_COMPANY_RELATION": (
        ("CK_DBD_COMPANY_RELATION_PAYLOAD_JSON", "payload"),
    ),
    "dbo.DBD_COMPANY_EVENT": (
        ("CK_DBD_COMPANY_EVENT_PAYLOAD_JSON", "payload"),
    ),
    "dbo.DBD_FINANCIAL": (
        ("CK_DBD_FINANCIAL_ITEMS_JSON", "statement_items"),
        ("CK_DBD_FINANCIAL_SUBMISSION_JSON", "submission_payload"),
        ("CK_DBD_FINANCIAL_STATEMENT_JSON", "statement_payload"),
    ),
    "dbo.DBD_INDUSTRY": (
        ("CK_DBD_INDUSTRY_ITEMS_JSON", "benchmark_items"),
        ("CK_DBD_INDUSTRY_PAYLOAD_JSON", "payload"),
    ),
}

EXTRA_CHECKS = {
    "dbo.DBD_COMPANY": (
        (
            "CK_DBD_COMPANY_MASKS_DISJOINT",
            "([done_mask] & [not_available_mask]) = 0 "
            "AND ([done_mask] & [failed_mask]) = 0 "
            "AND ([not_available_mask] & [failed_mask]) = 0",
        ),
        (
            "CK_DBD_COMPANY_GENERATION_POSITIVE",
            "[dataset_generation] >= 1 AND [first_seen_generation] >= 1 "
            "AND [last_seen_generation] >= [first_seen_generation]",
        ),
    ),
    "dbo.DBD_COMPANY_RELATION": (
        (
            "CK_DBD_COMPANY_RELATION_GENERATION",
            "[first_seen_generation] >= 1 AND [last_seen_generation] >= [first_seen_generation]",
        ),
    ),
    "dbo.DBD_COMPANY_EVENT": (
        (
            "CK_DBD_COMPANY_EVENT_GENERATION",
            "[first_seen_generation] >= 1 AND [last_seen_generation] >= [first_seen_generation]",
        ),
    ),
    "dbo.DBD_FINANCIAL": (
        (
            "CK_DBD_FINANCIAL_GENERATION",
            "[first_seen_generation] >= 1 AND [last_seen_generation] >= [first_seen_generation]",
        ),
    ),
    "dbo.DBD_INDUSTRY": (
        (
            "CK_DBD_INDUSTRY_GENERATION",
            "[first_seen_generation] >= 1 AND [last_seen_generation] >= [first_seen_generation]",
        ),
    ),
}


def _unqualified(table_name: str) -> str:
    return table_name.split(".", 1)[1]


def _create_table_sql(table_name: str) -> str:
    columns = [column.ddl() for column in TABLE_COLUMNS[table_name]]
    pk_name, pk_column = PRIMARY_KEYS[table_name]
    constraints = [f"CONSTRAINT [{pk_name}] PRIMARY KEY ([{pk_column}])"]
    if table_name in FOREIGN_KEYS:
        name, column, target, target_column = FOREIGN_KEYS[table_name]
        constraints.append(
            f"CONSTRAINT [{name}] FOREIGN KEY ([{column}]) "
            f"REFERENCES {target} ([{target_column}])"
        )
    for name, column in JSON_CHECKS.get(table_name, ()):
        constraints.append(
            f"CONSTRAINT [{name}] CHECK ([{column}] IS NULL OR ISJSON([{column}]) = 1)"
        )
    for name, expression in EXTRA_CHECKS.get(table_name, ()):
        constraints.append(f"CONSTRAINT [{name}] CHECK ({expression})")
    body = ",\n    ".join(columns + constraints)
    return f"CREATE TABLE {table_name} (\n    {body}\n)"


CREATE_TABLE_SQL = {table: _create_table_sql(table) for table in MANAGED_TABLES}


def _type_matches(expected: str, row: tuple[Any, ...]) -> bool:
    _name, data_type, char_length, precision, scale, datetime_precision, _nullable, _identity = row
    actual = str(data_type).upper()
    expected = expected.upper()
    if expected in {"BIGINT", "INT", "SMALLINT", "BIT", "DATE"}:
        return actual == expected
    if expected.startswith("DECIMAL("):
        p, s = expected.removeprefix("DECIMAL(").removesuffix(")").split(",")
        return (
            actual in {"DECIMAL", "NUMERIC"}
            and int(precision or 0) == int(p)
            and int(scale or 0) == int(s)
        )
    if expected.startswith("DATETIME2("):
        d = int(expected.removeprefix("DATETIME2(").removesuffix(")"))
        return actual == "DATETIME2" and int(datetime_precision or 0) == d
    if expected == "NVARCHAR(MAX)":
        return actual == "NVARCHAR" and int(char_length or 0) == -1
    if expected.startswith("NVARCHAR("):
        size = int(expected.removeprefix("NVARCHAR(").removesuffix(")"))
        return actual == "NVARCHAR" and int(char_length or 0) >= size
    return False


def _table_exists(cur, table_name: str) -> bool:
    cur.execute("SELECT CASE WHEN OBJECT_ID(%s, 'U') IS NULL THEN 0 ELSE 1 END", (table_name,))
    return bool(cur.fetchone()[0])


def _column_rows(cur, table_name: str) -> dict[str, tuple[Any, ...]]:
    schema_name, bare_name = table_name.split(".", 1)
    cur.execute(
        """
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.DATETIME_PRECISION,
            c.IS_NULLABLE,
            COLUMNPROPERTY(OBJECT_ID(%s), c.COLUMN_NAME, 'IsIdentity')
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA=%s AND c.TABLE_NAME=%s
        """,
        (table_name, schema_name, bare_name),
    )
    return {str(row[0]): row for row in cur.fetchall()}


def _index_rows(
    cur,
    table_name: str,
) -> dict[str, tuple[bool, tuple[str, ...], bool, bool, str | None]]:
    cur.execute(
        """
        SELECT
            i.name,
            i.is_unique,
            i.is_disabled,
            i.has_filter,
            i.filter_definition,
            ic.key_ordinal,
            c.name
        FROM sys.indexes i
        JOIN sys.index_columns ic
          ON ic.object_id=i.object_id AND ic.index_id=i.index_id
        JOIN sys.columns c
          ON c.object_id=ic.object_id AND c.column_id=ic.column_id
        WHERE i.object_id=OBJECT_ID(%s)
          AND ic.key_ordinal > 0
        ORDER BY i.name, ic.key_ordinal
        """,
        (table_name,),
    )
    grouped: dict[
        str,
        list[tuple[int, str, bool, bool, bool, str | None]],
    ] = {}
    for name, unique, disabled, has_filter, filter_definition, ordinal, column in cur.fetchall():
        grouped.setdefault(str(name), []).append(
            (
                int(ordinal),
                str(column),
                bool(unique),
                bool(disabled),
                bool(has_filter),
                None if filter_definition is None else str(filter_definition),
            )
        )
    return {
        name: (
            bool(entries[0][2]),
            tuple(entry[1] for entry in sorted(entries)),
            bool(entries[0][3]),
            bool(entries[0][4]),
            entries[0][5],
        )
        for name, entries in grouped.items()
    }


def _constraint_names(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT name
        FROM sys.objects
        WHERE parent_object_id=OBJECT_ID(%s)
          AND type IN ('F','C','PK','UQ')
        """,
        (table_name,),
    )
    return {str(row[0]) for row in cur.fetchall()}


def _normalize_check_definition(value: object) -> str:
    text = re.sub(
        r"\s+",
        "",
        str(value).upper().replace("[", "").replace("]", ""),
    )
    text = re.sub(r"\((-?\d+(?:\.\d+)?)\)", r"\1", text)
    while text.startswith("(") and text.endswith(")"):
        depth = 0
        encloses_all = True
        for index, char in enumerate(text):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and index != len(text) - 1:
                    encloses_all = False
                    break
        if not encloses_all or depth != 0:
            break
        text = text[1:-1]
    return text


def _check_constraint_rows(
    cur,
    table_name: str,
) -> dict[str, tuple[str, bool, bool]]:
    cur.execute(
        """
        SELECT name,definition,is_disabled,is_not_trusted
        FROM sys.check_constraints
        WHERE parent_object_id=OBJECT_ID(%s)
        """,
        (table_name,),
    )
    return {
        str(name): (str(definition), bool(disabled), bool(not_trusted))
        for name, definition, disabled, not_trusted in cur.fetchall()
    }


def _validate_columns(cur, table_name: str) -> None:
    actual = _column_rows(cur, table_name)
    missing = [column for column in TABLE_COLUMNS[table_name] if column.name not in actual]
    if missing:
        raise SqlServerSchemaError(
            f"{table_name} missing required column(s): "
            + ", ".join(column.name for column in missing)
        )
    mismatches = []
    for column in TABLE_COLUMNS[table_name]:
        row = actual[column.name]
        nullable = str(row[6]).upper() == "YES"
        identity = bool(row[7])
        if (
            not _type_matches(column.sql_type, row)
            or nullable != column.nullable
            or identity != column.identity
        ):
            mismatches.append(column.name)
    if mismatches:
        raise SqlServerSchemaError(
            f"{table_name} incompatible column definition(s): " + ", ".join(mismatches)
        )


def _validate_indexes(cur, table_name: str) -> None:
    actual = _index_rows(cur, table_name)
    pk_name, pk_column = PRIMARY_KEYS[table_name]
    pk = actual.get(pk_name)
    if (
        pk is None
        or not pk[0]
        or pk[1] != (pk_column,)
        or pk[2]
        or pk[3]
        or pk[4] is not None
    ):
        raise SqlServerSchemaError(
            f"{table_name} primary key {pk_name} does not match contract"
        )
    for name, unique, columns in INDEXES[table_name]:
        found = actual.get(name)
        if (
            found is None
            or found[0] != unique
            or found[1] != columns
            or found[2]
            or found[3]
            or found[4] is not None
        ):
            raise SqlServerSchemaError(
                f"{table_name} index {name} does not match contract"
            )


def _required_constraint_names(table_name: str) -> set[str]:
    required = {PRIMARY_KEYS[table_name][0]}
    if table_name in FOREIGN_KEYS:
        required.add(FOREIGN_KEYS[table_name][0])
    required.update(name for name, _column in JSON_CHECKS.get(table_name, ()))
    required.update(name for name, _expression in EXTRA_CHECKS.get(table_name, ()))
    return required


def _validate_constraints(cur, table_name: str) -> None:
    actual = _constraint_names(cur, table_name)
    missing = sorted(_required_constraint_names(table_name) - actual)
    if missing:
        raise SqlServerSchemaError(
            f"{table_name} missing required constraint(s): {', '.join(missing)}"
        )

    check_rows = _check_constraint_rows(cur, table_name)
    expected_checks = {
        name: f"([{column}] IS NULL OR ISJSON([{column}]) = 1)"
        for name, column in JSON_CHECKS.get(table_name, ())
    }
    expected_checks.update(
        {
            name: expression
            for name, expression in EXTRA_CHECKS.get(table_name, ())
        }
    )
    for name, expected_definition in expected_checks.items():
        found = check_rows.get(name)
        if found is None:
            raise SqlServerSchemaError(
                f"{table_name} check constraint {name} is missing"
            )
        definition, disabled, not_trusted = found
        if (
            disabled
            or not_trusted
            or _normalize_check_definition(definition)
            != _normalize_check_definition(expected_definition)
        ):
            raise SqlServerSchemaError(
                f"{table_name} check constraint {name} does not match contract"
            )

    if table_name in FOREIGN_KEYS:
        expected_name, expected_column, expected_target, expected_target_column = (
            FOREIGN_KEYS[table_name]
        )
        target_schema, target_table = expected_target.split(".", 1)
        cur.execute(
            """
            SELECT
                fk.name,
                pc.name,
                rs.name,
                rt.name,
                rc.name,
                fk.delete_referential_action_desc,
                fk.is_disabled,
                fk.is_not_trusted
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc
              ON fkc.constraint_object_id=fk.object_id
            JOIN sys.columns pc
              ON pc.object_id=fkc.parent_object_id
             AND pc.column_id=fkc.parent_column_id
            JOIN sys.tables rt
              ON rt.object_id=fkc.referenced_object_id
            JOIN sys.schemas rs
              ON rs.schema_id=rt.schema_id
            JOIN sys.columns rc
              ON rc.object_id=fkc.referenced_object_id
             AND rc.column_id=fkc.referenced_column_id
            WHERE fk.parent_object_id=OBJECT_ID(%s)
              AND fk.name=%s
            """,
            (table_name, expected_name),
        )
        row = cur.fetchone()
        expected = (
            expected_name,
            expected_column,
            target_schema,
            target_table,
            expected_target_column,
            "NO_ACTION",
        )
        if (
            row is None
            or tuple(str(value) for value in row[:6]) != expected
            or bool(row[6])
            or bool(row[7])
        ):
            raise SqlServerSchemaError(
                f"{table_name} foreign key {expected_name} does not match contract"
            )


def validate_schema(conn) -> dict[str, object]:
    cur = conn.cursor()
    try:
        for table_name in MANAGED_TABLES:
            if not _table_exists(cur, table_name):
                raise SqlServerSchemaError(f"Required production table {table_name} does not exist")
            _validate_columns(cur, table_name)
            _validate_indexes(cur, table_name)
            _validate_constraints(cur, table_name)
    finally:
        cur.close()
    return {"managed_tables": len(MANAGED_TABLES), "tables": MANAGED_TABLES, "status": "READY"}


def _ensure_indexes(cur, table_name: str) -> None:
    actual = _index_rows(cur, table_name)
    for name, unique, columns in INDEXES[table_name]:
        found = actual.get(name)
        if found is not None:
            if (
                found[0] != unique
                or found[1] != columns
                or found[2]
                or found[3]
                or found[4] is not None
            ):
                raise SqlServerSchemaError(
                    f"{table_name} index {name} exists with incompatible definition"
                )
            continue
        unique_sql = "UNIQUE " if unique else ""
        column_sql = ", ".join(f"[{column}]" for column in columns)
        cur.execute(f"CREATE {unique_sql}INDEX [{name}] ON {table_name} ({column_sql})")


def _ensure_constraints(cur, table_name: str) -> None:
    actual = _constraint_names(cur, table_name)
    if table_name in FOREIGN_KEYS:
        name, column, target, target_column = FOREIGN_KEYS[table_name]
        if name not in actual:
            cur.execute(
                f"ALTER TABLE {table_name} WITH CHECK ADD CONSTRAINT [{name}] "
                f"FOREIGN KEY ([{column}]) REFERENCES {target} ([{target_column}])"
            )
            cur.execute(f"ALTER TABLE {table_name} CHECK CONSTRAINT [{name}]")
    for name, column in JSON_CHECKS.get(table_name, ()):
        if name not in actual:
            cur.execute(
                f"ALTER TABLE {table_name} WITH CHECK ADD CONSTRAINT [{name}] "
                f"CHECK ([{column}] IS NULL OR ISJSON([{column}]) = 1)"
            )
    for name, expression in EXTRA_CHECKS.get(table_name, ()):
        if name not in actual:
            cur.execute(
                f"ALTER TABLE {table_name} WITH CHECK ADD CONSTRAINT [{name}] CHECK ({expression})"
            )


def ensure_schema(conn) -> dict[str, object]:
    cur = conn.cursor()
    created: list[str] = []
    altered: list[str] = []
    try:
        for table_name in MANAGED_TABLES:
            if not _table_exists(cur, table_name):
                cur.execute(CREATE_TABLE_SQL[table_name])
                created.append(table_name)
                _ensure_indexes(cur, table_name)
                continue

            actual = _column_rows(cur, table_name)
            for column in TABLE_COLUMNS[table_name]:
                if column.name in actual:
                    continue
                if not column.nullable:
                    raise SqlServerSchemaError(
                        f"{table_name} is missing non-null column {column.name}; "
                        "automatic backfill is intentionally refused"
                    )
                cur.execute(f"ALTER TABLE {table_name} ADD {column.ddl()}")
                altered.append(f"{table_name}.{column.name}")

            _validate_columns(cur, table_name)
            _ensure_indexes(cur, table_name)
            _ensure_constraints(cur, table_name)

        result = validate_schema(conn)
        return {**result, "created": created, "altered": altered}
    finally:
        cur.close()
