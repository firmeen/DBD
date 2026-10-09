"""MySQL implementation of the v0.6 five-table production repository.

Never reads or writes legacy DB_* tables. All state lives in PROD_MYSQL_DATABASE.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pymysql

from ..production import ProductionLeaseLost
from ..sqlserver.repository import (
    _COMPANY_STATE_COLUMNS,
    _DISCOVERY_COMPANY_COLUMNS,
    _PROFILE_COMPANY_COLUMNS,
    _STATEMENT_COLUMNS,
    _SUBMISSION_COLUMNS,
    _production_value,
)
from ..sqlserver.schema import TABLE_COLUMNS
from .production_engine import check_connection, connection, transaction
from .production_schema import TABLES, ensure_schema, validate_schema


class MySqlProductionWriteError(RuntimeError):
    pass


def _identity(value: Any, name: str) -> str:
    result = _production_value(value)
    if result is None:
        raise MySqlProductionWriteError(f"Missing natural key: {name}")
    return str(result)


def _utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _columns(table: str) -> tuple[str, ...]:
    return tuple(
        c.name for c in TABLE_COLUMNS[f"dbo.{table}"] if not c.identity
    )


def _insert(conn, table: str, row: dict[str, Any], *, preserve: tuple[str, ...] = ()):
    columns = _columns(table)
    unknown = set(row) - set(columns)
    if unknown:
        raise MySqlProductionWriteError(f"{table} unsupported columns: {sorted(unknown)}")
    values = [_production_value(row.get(column)) for column in columns]
    updates = [
        f"`{col}`=VALUES(`{col}`)"
        for col in columns if col not in preserve
    ]
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{table}` ("
            + ",".join(f"`{col}`" for col in columns)
            + ") VALUES (" + ",".join(["%s"] * len(columns)) + ") "
            + "ON DUPLICATE KEY UPDATE " + ",".join(updates),
            tuple(values),
        )


def _update(conn, table: str, values: dict[str, Any], where: str, params: tuple):
    if not values:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{table}` SET "
            + ",".join(f"`{col}`=%s" for col in values)
            + " WHERE " + where,
            tuple(_production_value(v) for v in values.values()) + params,
        )
        return cur.rowcount


def _status_sql(*, include_lease: bool = True) -> str:
    lease = (
        "WHEN lease_owner IS NOT NULL AND lease_expires_at > UTC_TIMESTAMP(6) "
        "THEN 'COLLECTING' "
        if include_lease else ""
    )
    return (
        "CASE " + lease
        + "WHEN (required_mask & ~(done_mask | not_available_mask | failed_mask)) <> 0 "
        "AND retry_at > UTC_TIMESTAMP(6) THEN 'RETRY_WAIT' "
        "WHEN (required_mask & ~(done_mask | not_available_mask | failed_mask)) <> 0 "
        "THEN 'PENDING' "
        "WHEN required_mask=0 THEN 'DISCOVERED' "
        "WHEN (failed_mask & required_mask)=0 THEN 'COMPLETE' "
        "WHEN (failed_mask & required_mask)=required_mask THEN 'FAILED' "
        "ELSE 'PARTIAL' END"
    )


class ProductionMySqlRepository:
    backend = "mysql"

    transaction = staticmethod(transaction)

    @staticmethod
    def is_transient_error(exc: BaseException) -> bool:
        return isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InterfaceError))

    def ensure_schema(self) -> dict[str, object]:
        # MySQL DDL is auto-committing. It is always an explicit migrate action.
        with connection() as conn:
            return ensure_schema(conn)

    def validate_schema(self) -> dict[str, object]:
        with connection() as conn:
            return validate_schema(conn)

    def check_database(self) -> dict[str, object]:
        info = check_connection()
        result = self.validate_schema()
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT CURRENT_USER()")
                login = cur.fetchone()[0]
                cur.execute("SHOW GRANTS FOR CURRENT_USER()")
                grants = [str(row[0]) for row in cur.fetchall()]
        # Actual privilege enforcement is performed by the target operations;
        # never incorrectly declare DML permission on SHOW GRANTS alone.
        return {**info, "login": login, "schema": result["status"],
                "managed_tables": len(TABLES), "grants": grants}

    @contextmanager
    def manager_lock(self):
        resource = "dbd:v06:production:cycle"
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT GET_LOCK(%s, 0)", (resource,))
                acquired = cur.fetchone()
            if not acquired or int(acquired[0] or 0) != 1:
                raise MySqlProductionWriteError(
                    "Another MySQL production manager owns the v0.6 lock"
                )
            try:
                yield conn
            finally:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT RELEASE_LOCK(%s)", (resource,))
                except Exception:
                    pass

    def assert_manager_lock(self, conn):
        conn.ping(reconnect=False)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT IS_USED_LOCK(%s)=CONNECTION_ID()",
                ("dbd:v06:production:cycle",),
            )
            row = cur.fetchone()
        if not row or int(row[0] or 0) != 1:
            raise MySqlProductionWriteError("MySQL production manager lock lost")

    def max_generation(self, conn=None):
        if conn is None:
            with connection() as owned:
                return self.max_generation(owned)
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(dataset_generation) FROM DBD_COMPANY")
            row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def generation_state(self, conn=None) -> dict[str, int | None]:
        if conn is None:
            with connection() as owned:
                return self.generation_state(owned)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(dataset_generation),0), "
                "COALESCE(MAX(last_seen_generation),0) FROM DBD_COMPANY"
            )
            finalized, seen = (int(v or 0) for v in cur.fetchone())
            cur.execute(
                "SELECT COUNT(*) FROM DBD_COMPANY WHERE "
                "dataset_generation > last_seen_generation "
                "OR first_seen_generation > last_seen_generation"
            )
            if int(cur.fetchone()[0] or 0):
                raise MySqlProductionWriteError("Impossible production generation rows")
            candidate = max(finalized, seen)
            unfinished = 0
            if candidate:
                cur.execute(
                    "SELECT COUNT(*) FROM DBD_COMPANY WHERE "
                    "last_seen_generation=%s AND dataset_generation<%s",
                    (candidate, candidate),
                )
                unfinished = int(cur.fetchone()[0] or 0)
        active = candidate if unfinished else None
        return {
            "finalized_generation": finalized or None,
            "last_seen_generation": seen or None,
            "active_generation": active,
            "unfinished_rows": unfinished,
            "next_generation": active or max(2, finalized + 1),
        }

    def company_ids(self, conn, identities):
        unique = list(dict.fromkeys(
            (_identity(a, "juristic_type_code"), _identity(b, "registration_no"))
            for a, b in identities
        ))
        result: dict[tuple[str, str], int] = {}
        with conn.cursor() as cur:
            for offset in range(0, len(unique), 300):
                chunk = unique[offset:offset + 300]
                if not chunk:
                    continue
                placeholders = ",".join(["(%s,%s)"] * len(chunk))
                flat = tuple(v for pair in chunk for v in pair)
                cur.execute(
                    "SELECT juristic_type_code,registration_no,company_id "
                    "FROM DBD_COMPANY WHERE (juristic_type_code,registration_no) "
                    f"IN ({placeholders})", flat,
                )
                result.update({(str(a), str(b)): int(pk) for a, b, pk in cur.fetchall()})
        if len(result) != len(unique):
            raise MySqlProductionWriteError("Unable to resolve MySQL company_id")
        return result

    def upsert_companies(self, conn, rows):
        for row in rows:
            _identity(row.get("juristic_type_code"), "juristic_type_code")
            _identity(row.get("registration_no"), "registration_no")
            _insert(conn, "DBD_COMPANY", row, preserve=(
                "company_id", "first_seen_generation",
            ))
        return self.company_ids(
            conn, [(x["juristic_type_code"], x["registration_no"]) for x in rows]
        )

    def upsert_relations(self, conn, rows):
        for row in rows:
            _insert(conn, "DBD_COMPANY_RELATION", row,
                    preserve=("first_seen_generation",))
        return len(rows)

    def upsert_events(self, conn, rows):
        for row in rows:
            _insert(conn, "DBD_COMPANY_EVENT", row,
                    preserve=("first_seen_generation",))
        return len(rows)

    def upsert_financials(self, conn, rows):
        for row in rows:
            _insert(conn, "DBD_FINANCIAL", row,
                    preserve=("first_seen_generation",))
        return len(rows)

    def upsert_industries(self, conn, rows, *, reject_conflicts=False):
        for row in rows:
            key = tuple(_production_value(row.get(k)) for k in (
                "submit_objective_code", "fiscal_year", "business_size_code",
                "location_type", "location_no",
            ))
            if key[0] is None or key[1] is None:
                raise MySqlProductionWriteError("Missing industry natural key")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT industry_id,source_updated_at,payload "
                    "FROM DBD_INDUSTRY WHERE submit_objective_code=%s "
                    "AND fiscal_year=%s AND business_size_code <=> %s "
                    "AND location_type <=> %s AND location_no <=> %s FOR UPDATE",
                    key,
                )
                existing = cur.fetchone()
            if existing:
                pk, old_date, old_payload = existing
                new_date = _production_value(row.get("source_updated_at"))
                new_payload = _production_value(row.get("payload"))
                if old_payload != new_payload:
                    if reject_conflicts:
                        raise MySqlProductionWriteError("Conflicting Generation 1 industry payload")
                    if old_date is not None and (
                        new_date is None or new_date < old_date
                    ):
                        continue
                    if new_date == old_date:
                        raise MySqlProductionWriteError("Conflicting industry source timestamp")
                elif old_date is not None and (new_date is None or new_date < old_date):
                    continue
                updates = {
                    col: _production_value(row.get(col))
                    for col in _columns("DBD_INDUSTRY")
                    if col not in {
                        "submit_objective_code", "fiscal_year", "business_size_code",
                        "location_type", "location_no", "first_seen_generation",
                    }
                }
                _update(conn, "DBD_INDUSTRY", updates, "industry_id=%s", (int(pk),))
            else:
                _insert(conn, "DBD_INDUSTRY", row,
                        preserve=("first_seen_generation",))
        return len(rows)

    def update_company_state(self, conn, company_id, values):
        unknown = set(values) - set(_COMPANY_STATE_COLUMNS)
        if unknown:
            raise MySqlProductionWriteError(f"Unsupported state fields: {sorted(unknown)}")
        if _update(conn, "DBD_COMPANY", values, "company_id=%s", (int(company_id),)) != 1:
            raise MySqlProductionWriteError(f"Missing company_id={company_id}")

    def update_company_profile(self, conn, company_id, values):
        unknown = set(values) - set(_PROFILE_COMPANY_COLUMNS)
        if unknown:
            raise MySqlProductionWriteError(f"Unsupported profile fields: {sorted(unknown)}")
        if values and _update(
            conn, "DBD_COMPANY", values, "company_id=%s", (int(company_id),)
        ) != 1:
            raise MySqlProductionWriteError(f"Missing company_id={company_id}")

    def upsert_discovered_companies(
        self, conn, rows, *, generation, financial_mask, refresh_mask=0
    ):
        if generation < 1:
            raise MySqlProductionWriteError("Generation must be positive")
        inserted = updated = refresh_reopened = 0
        with conn.cursor() as cur:
            for row in rows:
                juristic = _identity(row.get("juristic_type_code"), "juristic_type_code")
                number = _identity(row.get("registration_no"), "registration_no")
                unknown = set(row) - _DISCOVERY_COMPANY_COLUMNS - {
                    "juristic_type_code", "registration_no"
                }
                if unknown:
                    raise MySqlProductionWriteError(f"Unsupported discovery columns: {sorted(unknown)}")
                cur.execute(
                    "SELECT company_id,last_seen_generation,profile_payload,latest_fiscal_year "
                    "FROM DBD_COMPANY WHERE juristic_type_code=%s AND registration_no=%s FOR UPDATE",
                    (juristic, number),
                )
                previous = cur.fetchone()
                values = {k: _production_value(v) for k, v in row.items()
                          if k in _DISCOVERY_COMPANY_COLUMNS}
                if previous is None:
                    data = {
                        "juristic_type_code": juristic,
                        "registration_no": number,
                        **values,
                        "required_mask": 0,
                        "done_mask": 0,
                        "not_available_mask": 0,
                        "failed_mask": 0,
                        "collect_status": "DISCOVERED",
                        "dataset_generation": max(1, generation - 1),
                        "first_seen_generation": generation,
                        "last_seen_generation": generation,
                    }
                    cols = tuple(data)
                    cur.execute(
                        "INSERT INTO DBD_COMPANY (" + ",".join(f"`{c}`" for c in cols)
                        + ") VALUES (" + ",".join(["%s"] * len(cols)) + ")",
                        tuple(data.values()),
                    )
                    inserted += 1
                    continue
                pk, seen, has_profile, previous_fiscal = previous
                if int(seen or 0) > generation:
                    raise MySqlProductionWriteError("Discovery generation regression")
                first_observation = int(seen or 0) < generation
                fiscal_present = "latest_fiscal_year" in values
                changed_fiscal = fiscal_present and values["latest_fiscal_year"] != previous_fiscal
                if has_profile is not None:
                    values = {k: v for k, v in values.items() if k in {
                        "master_payload", "published_at", "latest_fiscal_year",
                    }}
                reopened = int(refresh_mask) if first_observation else 0
                if changed_fiscal:
                    reopened |= int(financial_mask)
                assignments = [f"`{k}`=%s" for k in values]
                params = list(values.values())
                if changed_fiscal:
                    if values["latest_fiscal_year"] is None:
                        assignments.append("required_mask=(required_mask & ~%s)")
                    else:
                        assignments.append("required_mask=(required_mask | %s)")
                    params.append(int(financial_mask))
                if reopened:
                    assignments.extend([
                        "done_mask=(done_mask & ~%s)",
                        "not_available_mask=(not_available_mask & ~%s)",
                        "failed_mask=(failed_mask & ~%s)",
                        "retry_at=NULL", "collection_completed_at=NULL",
                    ])
                    params.extend([reopened, reopened, reopened])
                    if first_observation and refresh_mask:
                        refresh_reopened += 1
                    assignments.append(
                        "collect_status=IF(lease_owner IS NOT NULL "
                        "AND lease_expires_at>UTC_TIMESTAMP(6), 'COLLECTING', 'PENDING')"
                    )
                assignments.append("last_seen_generation=%s")
                params.append(int(generation))
                cur.execute(
                    "UPDATE DBD_COMPANY SET " + ",".join(assignments)
                    + " WHERE company_id=%s",
                    tuple(params + [int(pk)]),
                )
                updated += 1
        return {"inserted": inserted, "updated": updated,
                "refresh_reopened": refresh_reopened}

    def prepare_companies(
        self, conn, *, generation, core_mask, base_mask, financial_mask,
        extended_mask, phase, company_id=None, limit=None, after_company_id=0,
    ):
        if phase not in ("core", "extended"):
            raise ValueError("phase must be core or extended")
        required_core = (
            f"({int(base_mask)} | IF(latest_fiscal_year IS NULL,"
            f" 0,{int(financial_mask)}))"
        )
        required = (
            f"((required_mask & ~{int(core_mask)}) | {required_core})"
            if phase == "core" else
            f"((IF(required_mask=0, {required_core}, required_mask)) "
            f"| {int(extended_mask)})"
        )
        where = "last_seen_generation=%s AND company_id > %s"
        params = [int(generation), int(after_company_id)]
        if company_id is not None:
            where += " AND company_id=%s"
            params.append(int(company_id))
        limit_sql = f" LIMIT {max(1, int(limit))}" if limit is not None else ""
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT company_id,required_mask,{required} AS new_required,"
                "done_mask,not_available_mask,failed_mask,lease_owner,lease_expires_at "
                f"FROM DBD_COMPANY WHERE {where} AND required_mask <> {required} "
                f"ORDER BY company_id{limit_sql} FOR UPDATE",
                tuple(params),
            )
            rows = cur.fetchall()
            for pk, old_req, new_req, done, na, failed, lease, lease_end in rows:
                current_lease = lease is not None and lease_end is not None and lease_end > _utc()
                outstanding = int(new_req) & ~(int(done) | int(na) | int(failed))
                new_status = (
                    "COLLECTING" if current_lease else
                    "PENDING" if outstanding else
                    "DISCOVERED" if int(new_req) == 0 else
                    "COMPLETE" if (int(failed) & int(new_req)) == 0 else
                    "FAILED" if (int(failed) & int(new_req)) == int(new_req) else "PARTIAL"
                )
                cur.execute(
                    "UPDATE DBD_COMPANY SET required_mask=%s, collect_status=%s,"
                    "collection_completed_at=IF(%s, NULL, collection_completed_at) "
                    "WHERE company_id=%s",
                    (int(new_req), new_status, bool(outstanding), int(pk)),
                )
        return len(rows), (int(rows[-1][0]) if rows else None)

    def claim_company(
        self, conn, *, generation, phase_mask, lease_seconds, worker_prefix, company_id=None,
    ):
        where = (
            "last_seen_generation=%s AND collect_status IN "
            "('DISCOVERED','PENDING','COLLECTING','RETRY_WAIT','PARTIAL') "
            "AND (required_mask & %s) <> 0 "
            "AND ((required_mask & %s) & "
            "~(done_mask | not_available_mask | failed_mask)) <> 0 "
            "AND (retry_at IS NULL OR retry_at<=UTC_TIMESTAMP(6)) "
            "AND (lease_owner IS NULL OR lease_expires_at<=UTC_TIMESTAMP(6))"
        )
        params = [int(generation), int(phase_mask), int(phase_mask)]
        if company_id is not None:
            where += " AND company_id=%s"
            params.append(int(company_id))
        fields = (
            "company_id", "juristic_type_code", "registration_no", "latest_fiscal_year",
            "required_mask", "done_mask", "not_available_mask", "failed_mask",
            "collect_status", "lease_owner", "lease_expires_at",
            "last_seen_generation", "endpoint_meta",
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT company_id FROM DBD_COMPANY WHERE " + where
                + " ORDER BY IF(retry_at IS NULL,0,1),retry_at,company_id "
                "LIMIT 1 FOR UPDATE SKIP LOCKED",
                tuple(params),
            )
            found = cur.fetchone()
            if not found:
                return None
            pk = int(found[0])
            token = f"{worker_prefix}:{uuid4().hex}"
            cur.execute(
                "UPDATE DBD_COMPANY SET lease_owner=%s,"
                "lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND), "
                "collect_status='COLLECTING',"
                "collection_started_at=COALESCE(collection_started_at,UTC_TIMESTAMP(6)),"
                "retry_at=NULL WHERE company_id=%s",
                (token, int(lease_seconds), pk),
            )
            cur.execute(
                "SELECT " + ",".join(f"`{x}`" for x in fields)
                + " FROM DBD_COMPANY WHERE company_id=%s", (pk,),
            )
            return dict(zip(fields, cur.fetchone(), strict=True))

    def claimed_company_state(self, conn, *, company_id, claim_token, claim_generation):
        fields = (
            "required_mask", "done_mask", "not_available_mask", "failed_mask",
            "latest_fiscal_year", "endpoint_meta", "collect_status",
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT " + ",".join(fields)
                + " FROM DBD_COMPANY WHERE company_id=%s AND lease_owner=%s "
                "AND last_seen_generation=%s AND lease_expires_at>UTC_TIMESTAMP(6) FOR UPDATE",
                (int(company_id), claim_token, int(claim_generation)),
            )
            row = cur.fetchone()
        if row is None:
            raise ProductionLeaseLost(f"Stale MySQL production claim company_id={company_id}")
        return dict(zip(fields, row, strict=True))

    def renew_company_lease(
        self, conn, *, company_id, claim_token, claim_generation, lease_seconds
    ):
        changed = _update(
            conn, "DBD_COMPANY",
            {"lease_expires_at": _utc() + timedelta(seconds=lease_seconds)},
            "company_id=%s AND lease_owner=%s AND last_seen_generation=%s "
            "AND lease_expires_at>UTC_TIMESTAMP(6)",
            (int(company_id), claim_token, int(claim_generation)),
        )
        if changed != 1:
            raise ProductionLeaseLost(f"MySQL production lease lost company_id={company_id}")

    def update_company_state_owned(
        self, conn, *, company_id, claim_token, claim_generation, values
    ):
        unknown = set(values) - set(_COMPANY_STATE_COLUMNS)
        if unknown:
            raise MySqlProductionWriteError(f"Unsupported state fields {sorted(unknown)}")
        if not values:
            values = {"collect_status": "COLLECTING"}
        changed = _update(
            conn, "DBD_COMPANY", values,
            "company_id=%s AND lease_owner=%s AND last_seen_generation=%s "
            "AND lease_expires_at>UTC_TIMESTAMP(6)",
            (int(company_id), claim_token, int(claim_generation)),
        )
        if changed != 1:
            raise ProductionLeaseLost(f"Stale MySQL production claim company_id={company_id}")

    def release_company_lease(
        self, conn, *, company_id, claim_token, claim_generation
    ):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE DBD_COMPANY SET lease_owner=NULL,lease_expires_at=NULL,"
                f"collect_status={_status_sql(include_lease=False)} "
                "WHERE company_id=%s AND lease_owner=%s AND last_seen_generation=%s",
                (int(company_id), claim_token, int(claim_generation)),
            )

    def recover_expired_leases(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE DBD_COMPANY SET lease_owner=NULL,lease_expires_at=NULL,"
                f"collect_status={_status_sql(include_lease=False)} "
                "WHERE lease_owner IS NOT NULL AND lease_expires_at<=UTC_TIMESTAMP(6)"
            )
            return int(cur.rowcount or 0)

    def reconcile_relations(
        self, conn, *, company_id, relation_type, rows, generation
    ):
        prepared = [
            {**row, "company_id": int(company_id),
             "relation_type": relation_type, "is_current": True,
             "last_seen_generation": generation,
             "first_seen_generation": row.get("first_seen_generation", generation)}
            for row in rows
        ]
        self.upsert_relations(conn, prepared)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE DBD_COMPANY_RELATION SET is_current=0 "
                "WHERE company_id=%s AND relation_type=%s AND is_current=1 "
                "AND last_seen_generation<%s",
                (int(company_id), relation_type, int(generation)),
            )
        return len(prepared)

    def _merge_financial_rows(
        self, conn, *, company_id, rows, generation, owned_columns
    ):
        with conn.cursor() as cur:
            for row in rows:
                year = _identity(row.get("fiscal_year"), "fiscal_year")
                values = {key: _production_value(row[key])
                          for key in owned_columns if key in row}
                cur.execute(
                    "SELECT financial_id FROM DBD_FINANCIAL WHERE "
                    "company_id=%s AND fiscal_year=%s FOR UPDATE",
                    (int(company_id), year),
                )
                previous = cur.fetchone()
                if previous:
                    values.update({
                        "last_seen_generation": int(generation),
                        "published_at": _production_value(row.get("published_at")),
                    })
                    _update(
                        conn, "DBD_FINANCIAL", values, "financial_id=%s",
                        (int(previous[0]),),
                    )
                else:
                    published = _production_value(row.get("published_at"))
                    if published is None:
                        raise MySqlProductionWriteError("DBD_FINANCIAL requires published_at")
                    data = {
                        "company_id": int(company_id), "fiscal_year": year,
                        **values, "first_seen_generation": int(generation),
                        "last_seen_generation": int(generation),
                        "published_at": published,
                    }
                    names = tuple(data)
                    cur.execute(
                        "INSERT INTO DBD_FINANCIAL ("
                        + ",".join(f"`{x}`" for x in names) + ") VALUES ("
                        + ",".join(["%s"] * len(names)) + ")",
                        tuple(data.values()),
                    )
        return len(rows)

    def merge_financial_submissions(self, conn, *, company_id, rows, generation):
        return self._merge_financial_rows(
            conn, company_id=company_id, rows=rows, generation=generation,
            owned_columns=_SUBMISSION_COLUMNS,
        )

    def merge_financial_statements(self, conn, *, company_id, rows, generation):
        return self._merge_financial_rows(
            conn, company_id=company_id, rows=rows, generation=generation,
            owned_columns=_STATEMENT_COLUMNS,
        )

    def retry_failed(
        self, conn, *, phase_mask, generation=None, limit=None, after_company_id=0
    ):
        where = (
            "(failed_mask & %s) <> 0 AND "
            "(lease_owner IS NULL OR lease_expires_at<=UTC_TIMESTAMP(6)) "
            "AND company_id>%s"
        )
        params = [int(phase_mask), int(after_company_id)]
        if generation is not None:
            where += " AND last_seen_generation=%s"
            params.append(int(generation))
        lim = f" LIMIT {max(1, int(limit))}" if limit is not None else ""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT company_id FROM DBD_COMPANY WHERE " + where
                + " ORDER BY company_id" + lim + " FOR UPDATE", tuple(params),
            )
            ids = [int(row[0]) for row in cur.fetchall()]
            for pk in ids:
                cur.execute(
                    "UPDATE DBD_COMPANY SET failed_mask=(failed_mask & ~%s), "
                    "retry_at=NULL,collection_completed_at=NULL,collect_status='PENDING' "
                    "WHERE company_id=%s", (int(phase_mask), pk),
                )
        return len(ids), (ids[-1] if ids else None)

    def status_summary(self, *, phase_mask=None, generation=None):
        where = ""
        params = ()
        if generation is not None:
            where = "WHERE last_seen_generation=%s"
            params = (int(generation),)
        required = "required_mask" if not phase_mask else f"(required_mask & {int(phase_mask)})"
        failed = "failed_mask" if not phase_mask else f"(failed_mask & {int(phase_mask)})"
        resolved = "(done_mask | not_available_mask | failed_mask)"
        unresolved = f"({required} & ~{resolved})"
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*), "
                    f"COALESCE(SUM({required}=0),0), "
                    f"COALESCE(SUM({required}<>0 AND {unresolved}<>0 AND "
                    f"(lease_owner IS NULL OR lease_expires_at<=UTC_TIMESTAMP(6)) "
                    f"AND (retry_at IS NULL OR retry_at<=UTC_TIMESTAMP(6))),0), "
                    f"COALESCE(SUM({unresolved}<>0 AND lease_owner IS NOT NULL "
                    f"AND lease_expires_at>UTC_TIMESTAMP(6)),0), "
                    f"COALESCE(SUM({unresolved}<>0 AND retry_at>UTC_TIMESTAMP(6)),0), "
                    f"COALESCE(SUM({required}<>0 AND {unresolved}=0 AND {failed}=0),0), "
                    f"COALESCE(SUM({required}<>0 AND {unresolved}=0 AND {failed}<>0 "
                    f"AND {failed}<>{required}),0), "
                    f"COALESCE(SUM({required}<>0 AND {unresolved}=0 AND {failed}={required}),0), "
                    f"COALESCE(SUM({unresolved}<>0),0), "
                    "COALESCE(SUM(lease_owner IS NOT NULL AND lease_expires_at>UTC_TIMESTAMP(6)),0), "
                    "COALESCE(SUM(lease_owner IS NOT NULL AND lease_expires_at<=UTC_TIMESTAMP(6)),0) "
                    f"FROM DBD_COMPANY {where}",
                    params,
                )
                row = cur.fetchone()
        keys = (
            "total", "discovered", "pending", "collecting", "retry_wait",
            "complete", "partial", "failed", "unresolved", "active_leases",
            "expired_leases",
        )
        return {
            **dict(zip(keys, (int(v or 0) for v in row), strict=True)),
            "generation": generation, "phase_mask": phase_mask,
        }

    def verify_generation(self, *, generation, phase_mask):
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT "
                    "COALESCE(SUM((done_mask & not_available_mask)<>0 OR "
                    "(done_mask & failed_mask)<>0 OR "
                    "(not_available_mask & failed_mask)<>0),0), "
                    "COALESCE(SUM(((required_mask & %s) & "
                    "~(done_mask | not_available_mask | failed_mask))<>0),0), "
                    "COALESCE(SUM((required_mask & "
                    "~(done_mask | not_available_mask | failed_mask))<>0),0), "
                    "COALESCE(SUM(retry_at>UTC_TIMESTAMP(6) AND "
                    "((required_mask & %s) & "
                    "~(done_mask | not_available_mask | failed_mask))<>0),0), "
                    "COALESCE(SUM(lease_owner IS NOT NULL "
                    "AND lease_expires_at>UTC_TIMESTAMP(6)),0), "
                    "COALESCE(SUM(lease_owner IS NOT NULL "
                    "AND lease_expires_at<=UTC_TIMESTAMP(6)),0), "
                    "COALESCE(SUM((failed_mask & %s)<>0),0), "
                    "COALESCE(SUM((failed_mask & required_mask)<>0),0), "
                    "COALESCE(SUM(last_seen_generation=%s AND "
                    "dataset_generation<%s),0) "
                    "FROM DBD_COMPANY WHERE last_seen_generation=%s",
                    (int(phase_mask), int(phase_mask), int(phase_mask),
                     int(generation), int(generation), int(generation)),
                )
                values = [int(x or 0) for x in cur.fetchone()]
                cur.execute(
                    "SELECT COUNT(*) FROM DBD_COMPANY WHERE "
                    "dataset_generation>last_seen_generation "
                    "OR first_seen_generation>last_seen_generation"
                )
                generation_errors = int(cur.fetchone()[0] or 0)
                orphans = {}
                for table in ("DBD_COMPANY_RELATION", "DBD_COMPANY_EVENT", "DBD_FINANCIAL"):
                    cur.execute(
                        f"SELECT COUNT(*) FROM {table} child "
                        "LEFT JOIN DBD_COMPANY parent ON parent.company_id=child.company_id "
                        "WHERE parent.company_id IS NULL"
                    )
                    orphans[table] = int(cur.fetchone()[0] or 0)
        keys = (
            "mask_overlap", "unresolved", "global_unresolved", "retry_wait",
            "active_leases", "expired_leases", "terminal_failures",
            "global_terminal_failures", "unfinished_rows",
        )
        details = dict(zip(keys, values, strict=True))
        ok = not any((
            *values[:6], generation_errors, *orphans.values(),
        ))
        return {
            "generation": generation, "phase_mask": phase_mask, **details,
            "generation_errors": generation_errors, "orphans": orphans,
            "ok": ok, "status": (
                "COMPLETE_WITH_FAILURES" if ok and details["global_terminal_failures"]
                else "COMPLETE" if ok else "FAILED"
            ),
        }

    def finalize_generation(
        self, conn, *, generation, batch_size, company_id=None, after_company_id=0
    ):
        where = (
            "last_seen_generation=%s AND dataset_generation<%s "
            "AND company_id>%s"
        )
        params = [int(generation), int(generation), int(after_company_id)]
        if company_id is not None:
            where += " AND company_id=%s"
            params.append(int(company_id))
        with conn.cursor() as cur:
            cur.execute(
                "SELECT company_id FROM DBD_COMPANY WHERE " + where
                + " ORDER BY company_id LIMIT %s FOR UPDATE",
                tuple(params + [max(1, int(batch_size))]),
            )
            ids = [int(x[0]) for x in cur.fetchall()]
            for pk in ids:
                cur.execute(
                    "UPDATE DBD_COMPANY SET dataset_generation=%s WHERE company_id=%s",
                    (int(generation), pk),
                )
        return len(ids), (ids[-1] if ids else None)

    def company_state(self, company_id):
        fields = (
            "company_id", "juristic_type_code", "registration_no",
            "latest_fiscal_year", "required_mask", "done_mask",
            "not_available_mask", "failed_mask", "collect_status",
            "lease_owner", "lease_expires_at", "retry_at",
            "dataset_generation", "first_seen_generation", "last_seen_generation",
            "last_endpoint", "last_http_status", "last_error",
        )
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT " + ",".join(fields)
                    + " FROM DBD_COMPANY WHERE company_id=%s", (int(company_id),),
                )
                row = cur.fetchone()
        return None if row is None else dict(zip(fields, row, strict=True))

    def coverage_summary(self, *, generation=None):
        where = " WHERE last_seen_generation=%s" if generation is not None else ""
        params = (int(generation),) if generation is not None else ()
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM DBD_COMPANY" + where, params)
                total = int(cur.fetchone()[0] or 0)
                data = {}
                for table, name in (
                    ("DBD_COMPANY_RELATION", "relations"),
                    ("DBD_COMPANY_EVENT", "events"),
                    ("DBD_FINANCIAL", "financials"),
                    ("DBD_INDUSTRY", "industries"),
                ):
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    data[name] = int(cur.fetchone()[0] or 0)
        return {"companies": total, **data}

    def count_generation_one(self):
        with connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM DBD_COMPANY WHERE dataset_generation=1"
                )
                return int(cur.fetchone()[0] or 0)
