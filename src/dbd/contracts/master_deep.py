from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class ServingField:
    name: str
    mysql_type: str
    mssql_type: str
    nullable: bool = True


SERVING_FIELDS: tuple[ServingField, ...] = (
    ServingField("company_id", "BIGINT UNSIGNED", "BIGINT", False),
    ServingField("registration_no", "VARCHAR(20)", "NVARCHAR(20)", False),
    ServingField("juristic_type_code", "VARCHAR(4)", "NVARCHAR(4)", False),
    ServingField("company_name_th", "VARCHAR(500)", "NVARCHAR(500)"),
    ServingField("company_name_en", "VARCHAR(500)", "NVARCHAR(500)"),
    ServingField("juristic_type", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("status_code", "VARCHAR(4)", "NVARCHAR(4)"),
    ServingField("status", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("registration_date", "DATE", "DATE"),
    ServingField("status_change_date", "DATE", "DATE"),
    ServingField("last_dbd_update_date", "DATE", "DATE"),
    ServingField("address", "TEXT", "NVARCHAR(MAX)"),
    ServingField("province_code", "VARCHAR(4)", "NVARCHAR(4)"),
    ServingField("province", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("district_code", "VARCHAR(10)", "NVARCHAR(10)"),
    ServingField("district", "VARCHAR(191)", "NVARCHAR(191)"),
    ServingField("subdistrict_code", "VARCHAR(10)", "NVARCHAR(10)"),
    ServingField("subdistrict", "VARCHAR(191)", "NVARCHAR(191)"),
    ServingField("postal_code", "VARCHAR(10)", "NVARCHAR(10)"),
    ServingField("phone", "VARCHAR(50)", "NVARCHAR(50)"),
    ServingField("email", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("website_1", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("website_2", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("website_3", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("website_4", "VARCHAR(255)", "NVARCHAR(255)"),
    ServingField("business_size_code", "VARCHAR(4)", "NVARCHAR(4)"),
    ServingField("business_type_code", "VARCHAR(8)", "NVARCHAR(8)"),
    ServingField("business_type", "VARCHAR(191)", "NVARCHAR(191)"),
    ServingField("setup_objective_code", "VARCHAR(20)", "NVARCHAR(20)"),
    ServingField("setup_objective", "VARCHAR(500)", "NVARCHAR(500)"),
    ServingField("submit_objective_code", "VARCHAR(20)", "NVARCHAR(20)"),
    ServingField("submit_objective", "VARCHAR(500)", "NVARCHAR(500)"),
    ServingField("registered_capital", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("paid_capital", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("share_quantity", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("share_volume", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("latest_fiscal_year", "VARCHAR(8)", "NVARCHAR(8)"),
    ServingField("total_assets", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("total_equity", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("total_income", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("net_profit", "DECIMAL(20,2)", "DECIMAL(20,2)"),
    ServingField("current_ratio", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("debt_to_asset", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("debt_to_equity", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("debt_to_capital", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("total_asset_to_equity", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("total_asset_turnover", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("inventory_turnover", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("account_receivable_turnover", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("account_payable_turnover", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("gross_profit_margin", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("operating_profit_margin", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("net_profit_margin", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("return_on_asset", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("return_on_equity", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("earning_per_share", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("cost_to_income", "DECIMAL(20,4)", "DECIMAL(20,4)"),
    ServingField("profile_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("committees_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("committee_signs_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("transformations_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("financial_submissions_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("financial_statements_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("industry_benchmark_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("committee_count", "INT UNSIGNED", "INT", False),
    ServingField("committee_sign_count", "INT UNSIGNED", "INT", False),
    ServingField("transformation_count", "INT UNSIGNED", "INT", False),
    ServingField("financial_submission_count", "INT UNSIGNED", "INT", False),
    ServingField("financial_statement_year_count", "INT UNSIGNED", "INT", False),
    ServingField("industry_submit_fin_count", "INT", "INT"),
    ServingField("industry_profit_count", "INT", "INT"),
    ServingField("industry_loss_count", "INT", "INT"),
    ServingField("profile_requested_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("committees_requested_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("committee_signs_requested_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("transformations_requested_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("financial_submissions_requested_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("financial_statements_requested_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("industry_benchmark_requested_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("deep_status", "VARCHAR(16)", "NVARCHAR(16)", False),
    ServingField("deep_completed_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("last_request_at", "DATETIME(6)", "DATETIME2(6)"),
    ServingField("source_deep_updated_at", "DATETIME(6)", "DATETIME2(6)", False),
    ServingField("projected_at", "DATETIME(6)", "DATETIME2(6)", False),
    ServingField("row_hash", "CHAR(64)", "CHAR(64)", False),
)

SERVING_COLUMNS: tuple[str, ...] = tuple(field.name for field in SERVING_FIELDS)
HASH_COLUMNS: tuple[str, ...] = tuple(
    name for name in SERVING_COLUMNS if name not in {"projected_at", "row_hash"}
)
RATIO_COLUMNS: tuple[str, ...] = (
    "current_ratio",
    "debt_to_asset",
    "debt_to_equity",
    "debt_to_capital",
    "total_asset_to_equity",
    "total_asset_turnover",
    "inventory_turnover",
    "account_receivable_turnover",
    "account_payable_turnover",
    "gross_profit_margin",
    "operating_profit_margin",
    "net_profit_margin",
    "return_on_asset",
    "return_on_equity",
    "earning_per_share",
    "cost_to_income",
)


def _stable_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def sanitize_serving_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


def sanitize_serving_row(row: dict[str, Any]) -> dict[str, Any]:
    return {name: sanitize_serving_value(row.get(name)) for name in SERVING_COLUMNS}


def serving_row_hash(row: dict[str, Any]) -> str:
    payload = [_stable_value(row.get(name)) for name in HASH_COLUMNS]
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def ordered_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(name) for name in SERVING_COLUMNS)


def mysql_create_table_sql(table_name: str = "dbd_master_deep") -> str:
    definitions = []
    for field in SERVING_FIELDS:
        null_sql = "NULL" if field.nullable else "NOT NULL"
        definitions.append(f"    `{field.name}` {field.mysql_type} {null_sql}")
    definitions.extend(
        [
            "    PRIMARY KEY (`company_id`)",
            "    UNIQUE KEY `uq_dbd_master_deep_identity` "
            "(`juristic_type_code`, `registration_no`)",
            "    KEY `idx_dbd_master_deep_source` "
            "(`source_deep_updated_at`, `company_id`)",
        ]
    )
    return (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
        + ",\n".join(definitions)
        + "\n) ENGINE=InnoDB\n"
        "DEFAULT CHARACTER SET=utf8mb4\n"
        "COLLATE=utf8mb4_0900_ai_ci"
    )


def mssql_create_table_sql(
    table_name: str = "dbo.DBD_MASTER_DEEP",
) -> str:
    definitions = []
    for field in SERVING_FIELDS:
        null_sql = "NULL" if field.nullable else "NOT NULL"
        definitions.append(f"    [{field.name}] {field.mssql_type} {null_sql}")
    definitions.append(
        "    CONSTRAINT [PK_DBD_MASTER_DEEP] PRIMARY KEY CLUSTERED ([company_id])"
    )
    body = ",\n".join(definitions)
    return f"""
IF OBJECT_ID(N'{table_name}', N'U') IS NULL
BEGIN
CREATE TABLE {table_name} (
{body}
);
END;
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name=N'UQ_DBD_MASTER_DEEP_IDENTITY'
      AND object_id=OBJECT_ID(N'{table_name}')
)
CREATE UNIQUE INDEX [UQ_DBD_MASTER_DEEP_IDENTITY]
ON {table_name} ([juristic_type_code], [registration_no]);
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name=N'IX_DBD_MASTER_DEEP_ROW_HASH'
      AND object_id=OBJECT_ID(N'{table_name}')
)
CREATE INDEX [IX_DBD_MASTER_DEEP_ROW_HASH]
ON {table_name} ([row_hash]);
""".strip()
