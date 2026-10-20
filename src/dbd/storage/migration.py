"""Operator-gated v0.6 schema migration; inspection is SELECT-only.

Legacy PR100 counts are deliberately NOT converted by inference. Unsafe
historical state is reported before any DDL rather than silently rewritten.
"""
from __future__ import annotations

import re
from typing import Any

from ..core.settings import settings
from .production import production_repository


class MigrationBlocked(RuntimeError):
    """The observed database cannot be changed without loss of evidence."""


def _one(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return None if row is None else row[0]
    finally:
        cur.close()


def _many(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return list(cur.fetchall())
    finally:
        cur.close()


def _plan_base(backend: str, db: str, login: str) -> dict[str, Any]:
    return {
        "backend": backend, "database": db, "login": login,
        "contract": "v0.6-six-business-tables",
        "existing_tables": 0, "missing_tables": [], "missing_columns": [],
        "indexes": [], "constraints": [], "actions": [],
        "legacy_extension": False, "legacy_companies": 0,
        "legacy_rows": 0, "reconciliation": [], "data_conflicts": 0,
        "permissions": {}, "blockers": [], "warnings": [],
        "active_workload": False, "apply_permitted": False,
    }


def _final(plan: dict[str, Any]) -> dict[str, Any]:
    plan["blockers"] = list(dict.fromkeys(plan["blockers"]))
    plan["actions"] = list(dict.fromkeys(plan["actions"]))
    plan["apply_permitted"] = not plan["blockers"]
    plan["state"] = (
        "BLOCKED" if plan["blockers"]
        else "CHANGES_REQUIRED" if plan["actions"] else "READY"
    )
    return plan



def _sql_procedure_signature(body: object) -> str:
    text = re.sub(r"--[^\n]*", "", str(body).lower())
    text = re.sub(r"\s+", "", text)
    return text.replace("createoralterprocedure", "createprocedure")


def _sql_plan(conn: Any) -> dict[str, Any]:
    from .sqlserver import schema as s
    from .sqlserver.control_repository import ControlRepository
    from ..presentation.setup import local_node_id

    db, login = _many(conn, "SELECT DB_NAME(), ORIGINAL_LOGIN()")[0]
    p = _plan_base("sqlserver", str(db), str(login))
    if db != "TIPDataIntelligence":
        p["blockers"].append("Wrong SQL Server Production database")
    p["permissions"] = {
        "create_table": bool(_one(conn, "SELECT HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','CREATE TABLE')")),
        "alter_schema": bool(_one(conn, "SELECT HAS_PERMS_BY_NAME('dbo','SCHEMA','ALTER')")),
    }
    if not all(p["permissions"].values()):
        p["blockers"].append("Missing database CREATE TABLE or dbo ALTER permission")

    for table in s.MANAGED_TABLES:
        if not _one(conn, "SELECT OBJECT_ID(%s,'U')", (table,)):
            p["missing_tables"].append(table)
            p["actions"].append("CREATE " + table)
            continue
        p["existing_tables"] += 1
        cur = conn.cursor()
        try:
            columns = s._column_rows(cur, table)
            indexes = s._index_rows(cur, table)
            constraints = s._constraint_names(cur, table)
            checks = s._check_constraint_rows(cur, table)
        finally:
            cur.close()
        for spec in s.TABLE_COLUMNS[table]:
            row = columns.get(spec.name)
            if row is None:
                if spec.nullable or (table == "dbo.DBD_COMPANY" and spec.name == "branch_count"):
                    p["missing_columns"].append(table + "." + spec.name)
                    p["actions"].append("ADD COLUMN " + table + "." + spec.name)
                else:
                    p["blockers"].append("Missing required nonnull column: " + table + "." + spec.name)
                continue
            actual_nullable = str(row[6]).upper() == "YES"
            if not s._type_matches(spec.sql_type, row) or bool(row[7]) != spec.identity:
                p["blockers"].append("Incompatible column type/identity: " + table + "." + spec.name)
            elif actual_nullable != spec.nullable:
                if table == "dbo.DBD_COMPANY" and spec.name == "branch_count" and actual_nullable:
                    p["actions"].append("RECONCILE nullable dbo.DBD_COMPANY.branch_count")
                else:
                    p["blockers"].append("Incompatible nullability: " + table + "." + spec.name)
        expected_pk, pk_col = s.PRIMARY_KEYS[table]
        if expected_pk not in indexes:
            p["blockers"].append("Missing primary key: " + table + "." + expected_pk)
        elif indexes[expected_pk][1] != (pk_col,):
            p["blockers"].append("Incompatible primary key: " + table)
        for name, unique, fields in s.INDEXES[table]:
            found = indexes.get(name)
            if found is None:
                p["indexes"].append(table + "." + name)
                p["actions"].append("CREATE INDEX " + table + "." + name)
            elif found[0] != unique or found[1] != fields or found[2] or found[3] or found[4] is not None:
                p["blockers"].append("Incompatible index: " + table + "." + name)
        expected_checks = {
            name: f"([{col}] IS NULL OR ISJSON([{col}]) = 1)"
            for name, col in s.JSON_CHECKS.get(table, ())
        }
        expected_checks.update(dict(s.EXTRA_CHECKS.get(table, ())))
        for name, expr in expected_checks.items():
            existing = checks.get(name)
            if existing is None:
                p["constraints"].append(table + "." + name)
                p["actions"].append("ADD CHECK " + table + "." + name)
            elif existing[1] or existing[2] or s._normalize_check_definition(existing[0]) != s._normalize_check_definition(expr):
                p["blockers"].append("Incompatible CHECK: " + table + "." + name)
        if table in s.FOREIGN_KEYS:
            fk_name = s.FOREIGN_KEYS[table][0]
            if fk_name not in constraints:
                p["constraints"].append(table + "." + fk_name)
                p["actions"].append("ADD FOREIGN KEY " + table + "." + fk_name)
            else:
                cur = conn.cursor()
                try:
                    s._validate_constraints(cur, table)
                except Exception as exc:
                    # Missing additive CHECK constraints are handled above;
                    # an untrusted or wrong FK is never silently replaced.
                    if fk_name in str(exc) or "foreign key" in str(exc).lower():
                        p["blockers"].append("Incompatible FK: " + table + "." + fk_name)
                finally:
                    cur.close()

    control = bool(_one(conn, "SELECT OBJECT_ID(N'dbo.DBD_CONTROL',N'U')"))
    if not control:
        p["actions"].append("CREATE dbo.DBD_CONTROL")
    else:
        try:
            ControlRepository().validate_schema(conn)
        except Exception as exc:
            if "procedure is missing" not in str(exc):
                p["blockers"].append("Incompatible DBD_CONTROL: " + type(exc).__name__)
    proc = bool(_one(conn, "SELECT OBJECT_ID(N'dbo.DBD_CONTROL_ADMIT',N'P')"))
    if not proc:
        p["actions"].append("CREATE dbo.DBD_CONTROL_ADMIT")
    elif control:
        # Never silently replace a customized Worker admission procedure.
        from .sqlserver.control_repository import ADMIT_DDL
        definition = _one(conn, "SELECT OBJECT_DEFINITION(OBJECT_ID(N'dbo.DBD_CONTROL_ADMIT'))")
        if not definition:
            p["blockers"].append("Admission procedure definition cannot be inspected")
        elif _sql_procedure_signature(definition) != _sql_procedure_signature(ADMIT_DDL):
            p["blockers"].append("Existing DBD_CONTROL_ADMIT differs from approved contract")
    if control:
        admin_count = int(_one(conn,
            "SELECT COUNT(*) FROM dbo.DBD_CONTROL WHERE record_type='NODE' "
            "AND JSON_VALUE(payload,'$.role')='ADMIN' "
            "AND JSON_VALUE(payload,'$.approved')='true'") or 0)
        if admin_count:
            node = local_node_id()
            if node is None:
                p["blockers"].append("Admin Coordinator setup required for migration")
            else:
                try:
                    ControlRepository().require_admin(node)
                except Exception:
                    p["blockers"].append("SQL login/node is not the approved Admin Coordinator")
        if _one(conn,
            "SELECT TOP (1) 1 FROM dbo.DBD_CONTROL WHERE record_type='GENERATION' "
            "AND JSON_VALUE(payload,'$.state') NOT IN ('COMPLETED','COMPLETE_WITH_FAILURES')"):
            p["active_workload"] = True
            p["blockers"].append("Active SQL Server monthly Generation")
    if _one(conn, "SELECT OBJECT_ID(N'dbo.DBD_COMPANY',N'U')"):
        cols = {x[0] for x in _many(conn,
            "SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(N'dbo.DBD_COMPANY')")}
        if {"lease_owner", "lease_expires_at"} <= cols:
            leased = int(_one(conn, "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY "
                "WHERE lease_owner IS NOT NULL AND lease_expires_at>SYSUTCDATETIME()") or 0)
            if leased:
                p["active_workload"] = True
                p["blockers"].append(f"{leased} active company lease(s)")
        if "branch_count" in cols:
            bad = int(_one(conn, "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY "
                "WHERE branch_count IS NOT NULL AND branch_count < 0") or 0)
            if bad:
                p["data_conflicts"] += bad
                p["blockers"].append(f"{bad} negative historical branch_count value(s)")
            zero_count = int(_one(conn, "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY "
                "WHERE branch_count=0") or 0)
            nullable = bool(_one(conn,
                "SELECT is_nullable FROM sys.columns "
                "WHERE object_id=OBJECT_ID(N'dbo.DBD_COMPANY') AND name='branch_count'"))
            if zero_count and not nullable:
                p["data_conflicts"] += zero_count
                p["blockers"].append(f"{zero_count} invalid zero branch_count value(s)")
            if nullable:
                positive = int(_one(conn, "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY "
                    "WHERE branch_count IS NOT NULL AND branch_count<>0") or 0)
                if positive:
                    p["data_conflicts"] += positive
                    p["blockers"].append(
                        f"{positive} ambiguous nullable historical branch_count value(s); "
                        "review PR100 evidence before canonical conversion")
                else:
                    p["reconciliation"].append("Set unverified NULL/zero branch_count to HQ default 1")
        if "juristic_type_code" in cols and "registration_no" in cols:
            duplicates = int(_one(conn, "SELECT COUNT_BIG(*) FROM ("
                "SELECT juristic_type_code,registration_no FROM dbo.DBD_COMPANY "
                "GROUP BY juristic_type_code,registration_no HAVING COUNT_BIG(*)>1) d") or 0)
            if duplicates:
                p["data_conflicts"] += duplicates
                p["blockers"].append(f"{duplicates} duplicate legal identity group(s)")
        if "branch_status" in cols:
            histories = int(_one(conn, "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY "
                "WHERE branch_status IS NOT NULL") or 0)
            p["legacy_companies"] = histories
            if histories:
                p["blockers"].append(
                    f"{histories} legacy PR100 branch summary row(s); "
                    "run historical evidence review before schema convergence")
    if bool(_one(conn, "SELECT OBJECT_ID(N'dbo.DBD_COMPANY_BRANCH',N'U')")):
        p["legacy_extension"] = True
        count = int(_one(conn, "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY_BRANCH") or 0)
        p["legacy_rows"] = count
        if count:
            p["blockers"].append(
                f"{count} historical DBD_COMPANY_BRANCH row(s); "
                "no automatic reinterpretation of PR100 counts")
    work = bool(_one(conn, "SELECT OBJECT_ID(N'dbo.DBD_DISCOVERY_WORK',N'U')"))
    if not work:
        p["actions"].append("CREATE dbo.DBD_DISCOVERY_WORK")
    else:
        from .sqlserver.discovery_repository import validate_schema
        try:
            validate_schema(conn)
        except Exception as exc:
            p["blockers"].append("Incompatible DBD_DISCOVERY_WORK: " + type(exc).__name__)
    return _final(p)


def _mysql_plan(conn: Any, *, owning_manager_lock: bool = False) -> dict[str, Any]:
    from .mysql import production_schema as s
    from .mysql.discovery_repository import validate_schema as validate_work
    from .mysql.production_engine import _configuration
    _configuration()
    db, login = _many(conn, "SELECT DATABASE(), CURRENT_USER()")[0]
    p = _plan_base("mysql", str(db), str(login))
    if str(db) != settings.prod_mysql_database:
        p["blockers"].append("Wrong Production MySQL database")
    if int(_one(conn, "SELECT CAST(SUBSTRING_INDEX(VERSION(),'.',1) AS UNSIGNED)") or 0) < 8:
        p["blockers"].append("MySQL 8+ with functional-index and CHECK support required")
    p["permissions"] = {"account": str(login), "ddl": "verify CREATE/ALTER/INDEX/REFERENCES grants"}
    held = _one(conn, "SELECT IS_USED_LOCK('dbd:v06:production:cycle')")
    if held is not None and not owning_manager_lock:
        p["active_workload"] = True
        p["blockers"].append("MySQL production manager lock is held")
    existing = s._tables(conn)
    for table in s.TABLES:
        if table.upper() not in existing:
            p["missing_tables"].append(table)
            p["actions"].append("CREATE " + table)
            continue
        p["existing_tables"] += 1
        actual = s._columns(conn, table)
        for spec in s.TABLE_COLUMNS["dbo." + table]:
            if spec.name not in actual:
                if table == "DBD_COMPANY" and spec.name == "branch_count":
                    p["missing_columns"].append(table + "." + spec.name)
                    p["actions"].append("ADD COLUMN " + table + "." + spec.name)
                else:
                    p["blockers"].append("Missing existing MySQL column: " + table + "." + spec.name)
                continue
            sql_type, null_str, extra = actual[spec.name]
            if sql_type != s._type(spec.sql_type).lower() or (spec.identity and "auto_increment" not in extra):
                p["blockers"].append("Incompatible MySQL column: " + table + "." + spec.name)
            nullable = null_str == "YES"
            if nullable != spec.nullable:
                if table == "DBD_COMPANY" and spec.name == "branch_count" and nullable:
                    p["actions"].append("RECONCILE nullable DBD_COMPANY.branch_count")
                else:
                    p["blockers"].append("Incompatible MySQL nullability: " + table + "." + spec.name)
        indexes = s._indexes(conn, table)
        for name, unique, columns in s.INDEXES["dbo." + table]:
            if name not in indexes:
                p["indexes"].append(table + "." + name)
                p["actions"].append("CREATE INDEX " + table + "." + name)
        constraints = {str(r[0]) for r in _many(conn,
            "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME=%s", (table,))}
        desired_checks = [
            (name, f"`{col}` IS NULL OR JSON_VALID(`{col}`)")
            for name, col in s.JSON_CHECKS.get("dbo." + table, ())
        ] + [
            (name, s._check(expr))
            for name, expr in s.EXTRA_CHECKS.get("dbo." + table, ())
        ]
        for name, predicate in desired_checks:
            if name not in constraints:
                p["constraints"].append(table + "." + name)
                p["actions"].append("ADD CHECK " + table + "." + name)
                bad = int(_one(conn, f"SELECT COUNT(*) FROM `{table}` "
                    f"WHERE NOT ({predicate})") or 0)
                if bad:
                    p["data_conflicts"] += bad
                    p["blockers"].append(f"{bad} existing row(s) violate {table}.{name}")
            else:
                actual_expr = _one(conn,
                    "SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS "
                    "WHERE CONSTRAINT_SCHEMA=DATABASE() AND CONSTRAINT_NAME=%s",
                    (name,))
                if actual_expr is not None and (
                    s._normalize_check_expression(actual_expr)
                    != s._normalize_check_expression(predicate)
                ):
                    p["blockers"].append("Incompatible MySQL CHECK: " + table + "." + name)
        fk = s.FOREIGN_KEYS.get("dbo." + table)
        if fk and fk[0] not in constraints:
            p["blockers"].append("Missing MySQL FOREIGN KEY on existing table: " + table)
        # Validate each existing index even when another index is missing.
        meta = _many(conn,
            "SELECT INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME,NON_UNIQUE,SUB_PART,"
            "INDEX_TYPE,EXPRESSION FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s "
            "ORDER BY INDEX_NAME,SEQ_IN_INDEX", (table,))
        by_name = {}
        for name, seq, col, nonunique, prefix, kind, expression in meta:
            by_name.setdefault(str(name), []).append(
                (int(seq), None if col is None else str(col),
                 int(nonunique), prefix, str(kind), expression))
        pk = by_name.get("PRIMARY")
        if pk is None or len(pk) != 1 or pk[0][1] != s.TABLE_COLUMNS["dbo." + table][0].name:
            p["blockers"].append("Invalid MySQL primary key: " + table)
        for name, unique, columns in s.INDEXES["dbo." + table]:
            entries = by_name.get(name)
            if entries is None:
                continue
            if len(entries) != len(columns):
                p["blockers"].append("Incompatible MySQL index: " + table + "." + name)
                continue
            for entry, col in zip(entries, columns, strict=True):
                seq, physical, nonunique, prefix, kind, expression = entry
                if nonunique != (0 if unique else 1) or prefix is not None or kind.upper() != "BTREE":
                    p["blockers"].append("Incompatible MySQL index flags: " + table + "." + name)
                elif table == "DBD_INDUSTRY" and name == "UQ_DBD_INDUSTRY_SCOPE" and col not in {
                    "submit_objective_code", "fiscal_year"
                }:
                    if not s._matches_industry_scope_expression(expression, col):
                        p["blockers"].append("Incompatible MySQL functional index: " + table + "." + name)
                elif str(physical or "").lower() != col.lower() or expression is not None:
                    p["blockers"].append("Incompatible MySQL index order: " + table + "." + name)
        if "DBD_COMPANY" == table and "branch_count" in actual:
            invalid = int(_one(conn, "SELECT COUNT(*) FROM DBD_COMPANY "
                "WHERE branch_count IS NOT NULL AND branch_count < 0") or 0)
            if invalid:
                p["data_conflicts"] += invalid
                p["blockers"].append(f"{invalid} invalid MySQL branch_count value(s)")
            if actual["branch_count"][1] != "YES":
                zeros = int(_one(conn, "SELECT COUNT(*) FROM DBD_COMPANY "
                    "WHERE branch_count=0") or 0)
                if zeros:
                    p["data_conflicts"] += zeros
                    p["blockers"].append(f"{zeros} invalid MySQL HQ-inclusive zero count(s)")
            if actual["branch_count"][1] == "YES":
                positives = int(_one(conn, "SELECT COUNT(*) FROM DBD_COMPANY "
                    "WHERE branch_count IS NOT NULL AND branch_count > 0") or 0)
                if positives:
                    p["data_conflicts"] += positives
                    p["blockers"].append(
                        f"{positives} historical nullable counts; cannot infer HQ semantics")
                else:
                    p["reconciliation"].append("Set unverified NULL/zero count to HQ default 1")
        if "DBD_COMPANY" == table and {"lease_owner", "lease_expires_at"} <= actual.keys():
            leases = int(_one(conn, "SELECT COUNT(*) FROM DBD_COMPANY "
                "WHERE lease_owner IS NOT NULL AND lease_expires_at>UTC_TIMESTAMP(6)") or 0)
            if leases:
                p["active_workload"] = True
                p["blockers"].append(f"{leases} active MySQL company lease(s)")
    if "DBD_DISCOVERY_WORK" not in existing:
        p["actions"].append("CREATE DBD_DISCOVERY_WORK")
    else:
        try:
            validate_work(conn)
        except Exception:
            p["blockers"].append("Incompatible MySQL DBD_DISCOVERY_WORK")
    return _final(p)


def inspect_migration(
    backend: str | None = None, *, owning_manager_lock: bool = False
) -> dict[str, Any]:
    selected = backend or settings.dbd_production_backend
    if selected == "sqlserver":
        from .sqlserver.engine import connection
        with connection() as conn:
            return _sql_plan(conn)
    if selected == "mysql":
        from .mysql.production_engine import connection
        with connection() as conn:
            return _mysql_plan(conn, owning_manager_lock=owning_manager_lock)
    raise ValueError(f"Unsupported migration backend: {selected!r}")


def apply_migration(backend: str | None = None, *, expected: dict[str, Any]) -> dict[str, Any]:
    selected = backend or settings.dbd_production_backend
    repo = production_repository(selected)
    if expected.get("backend") != selected or not expected.get("apply_permitted"):
        raise MigrationBlocked("Explicit approved migration plan is missing or blocked")
    with repo.manager_lock() as lock:
        repo.assert_manager_lock(lock)
        # TOCTOU fence: a preview is never used as DDL authorization after
        # catalog state or legacy evidence changes.
        current = inspect_migration(selected, owning_manager_lock=True)
        if current != expected or not current["apply_permitted"]:
            raise MigrationBlocked("Migration plan changed since preview; rerun --check")
        if current["state"] == "READY":
            return {**current, "state": "READY", "created": [], "altered": []}
        if selected == "sqlserver":
            from .sqlserver.schema import ensure_schema
            from .sqlserver.control_repository import ControlRepository
            from .sqlserver import discovery_repository, branch_repository
            with repo.transaction() as conn:
                if "RECONCILE nullable dbo.DBD_COMPANY.branch_count" in current["actions"]:
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            "UPDATE dbo.DBD_COMPANY SET branch_count=1 "
                            "WHERE branch_count IS NULL OR branch_count=0"
                        )
                        cur.execute(
                            "ALTER TABLE dbo.DBD_COMPANY ALTER COLUMN branch_count INT NOT NULL"
                        )
                    finally:
                        cur.close()
                result = ensure_schema(conn)
                has_control = bool(_one(conn,
                    "SELECT OBJECT_ID(N'dbo.DBD_CONTROL',N'U')"))
                has_procedure = bool(_one(conn,
                    "SELECT OBJECT_ID(N'dbo.DBD_CONTROL_ADMIT',N'P')"))
                if not has_control or not has_procedure:
                    ControlRepository().ensure_schema(conn)
                discovery_repository.ensure_schema(conn)
                branch_repository.ensure_schema(conn)
                ControlRepository().validate_schema(conn)
                discovery_repository.validate_schema(conn)
                # Validate all canonical objects before committing the transaction.
                from .sqlserver.schema import validate_schema
                validate_schema(conn)
                repo.assert_manager_lock(lock)
            final = inspect_migration(selected)
        else:
            from .mysql.production_schema import ensure_schema
            from .mysql.discovery_repository import ensure_schema as ensure_work
            from .mysql.production_engine import connection
            with connection() as conn:
                if "RECONCILE nullable DBD_COMPANY.branch_count" in current["actions"]:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE DBD_COMPANY SET branch_count=1 "
                            "WHERE branch_count IS NULL OR branch_count=0"
                        )
                        conn.commit()
                        cur.execute(
                            "ALTER TABLE DBD_COMPANY MODIFY branch_count INT NOT NULL DEFAULT 1"
                        )
                result = ensure_schema(conn)
                ensure_work(conn)
                repo.validate_schema()
            final = inspect_migration(selected)
        if final["state"] != "READY":
            raise MigrationBlocked(
                "Schema DDL applied but final inspection not READY; recheck before rerun"
            )
        return {**final, "state": "APPLIED", "created": result.get("created", []),
                "altered": result.get("altered", [])}
