from __future__ import annotations

import json
from datetime import datetime
from hashlib import sha256

from ....core.exceptions import IdentityConflict
from ....runtime.resources import normalize_resource
from ..engine import connect, use_connection
from .company_detail_repository import save_collection
from .company_repository import save_company_profile
from .financial_repository import save_benchmark, save_statements, save_submissions


class LeaseLost(RuntimeError):
    pass


def _company_key(task: dict, endpoint: str) -> str:
    return f"{endpoint}:company:{task['jp_type_code']}:{task['jp_no']}"


def _write_keys(task: dict, endpoint: str, normalized) -> list[str]:
    """Natural rows/collections mutated by one projection.

    These keys intentionally differ from request scope. One financial statement
    response can contain multiple fiscal years, submissions replaces the whole
    company collection, and industry benchmarks are shared across companies.
    """
    if endpoint == "financial_statements":
        return sorted(
            {
                f"financial_statement:{task['jp_type_code']}:{task['jp_no']}:{statement.fiscal_year}"
                for statement in normalized
            }
        )
    if endpoint == "industry_benchmark":
        if normalized is None:
            return []
        return [
            "industry_benchmark:"
            f"{normalized.submit_obj_code}:{normalized.fiscal_year}:"
            f"{normalized.business_size_code}:{normalized.location_type}:{normalized.location_no}"
        ]
    return [_company_key(task, endpoint)]


def _eligible_write_keys(cur, keys: list[str], endpoint: str, requested_at: datetime) -> set[str]:
    """Lock natural keys and return those not newer than this response."""
    if not keys:
        return set()
    for key in sorted(keys):
        cur.execute(
            """
            INSERT IGNORE INTO projection_write_state(write_key,requested_at,archive_id,endpoint)
            VALUES (%s,'1000-01-01 00:00:00.000000',0,%s)
            """,
            (key, endpoint),
        )
    placeholders = ",".join(["%s"] * len(keys))
    cur.execute(
        f"SELECT write_key,requested_at FROM projection_write_state "
        f"WHERE write_key IN ({placeholders}) ORDER BY write_key FOR UPDATE",
        tuple(sorted(keys)),
    )
    return {
        row["write_key"]
        for row in cur.fetchall()
        if row["requested_at"] <= requested_at
    }


def _record_write_state(cur, keys: set[str], endpoint: str, requested_at: datetime, archive_id: int):
    if not keys:
        return
    cur.executemany(
        """
        UPDATE projection_write_state
        SET requested_at=%s,archive_id=%s,endpoint=%s,updated_at=UTC_TIMESTAMP(6)
        WHERE write_key=%s
        """,
        [(requested_at, archive_id, endpoint, key) for key in sorted(keys)],
    )


def _update_request_state(
    cur,
    task: dict,
    endpoint: str,
    scope: str,
    requested_at: datetime,
    archive_id: int,
    status: str,
):
    """Keep request-scope state for observability without using it as a write fence."""
    cur.execute(
        """
        SELECT requested_at FROM company_endpoint_state
        WHERE company_id=%s AND endpoint=%s AND scope_key=%s FOR UPDATE
        """,
        (task["company_id"], endpoint, scope),
    )
    row = cur.fetchone()
    if row is not None and row["requested_at"] > requested_at:
        return
    cur.execute(
        """
        INSERT INTO company_endpoint_state(company_id,endpoint,scope_key,requested_at,archive_id,status)
        VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
          requested_at=VALUES(requested_at),archive_id=VALUES(archive_id),status=VALUES(status)
        """,
        (task["company_id"], endpoint, scope, requested_at, archive_id, status),
    )


def project(
    task: dict,
    archive_id: int,
    payload,
    requested_at: datetime,
    path: str,
    scope: str = "",
) -> str:
    """Fence projection and task acknowledgement in one MySQL transaction.

    V2 requires stage=project here. That invariant proves the response archive was
    durably committed before any relational projection can acknowledge the task.
    """
    endpoint = task["endpoint"]
    normalized = normalize_resource(endpoint, payload)
    if endpoint == "profile" and (normalized.jp_no, normalized.jp_type_code) != (
        task["jp_no"],
        task["jp_type_code"],
    ):
        raise IdentityConflict("Response identity does not match requested company")
    nominal_outcome = "not_available" if normalized is None or normalized == [] else "done"

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM endpoint_tasks WHERE id=%s AND stage='project' AND status='running'
                      AND lease_owner=%s AND lease_version=%s AND lease_expires_at>UTC_TIMESTAMP(6)
                    FOR UPDATE
                    """,
                    (task["id"], task["lease_owner"], task["lease_version"]),
                )
                if cur.fetchone() is None:
                    raise LeaseLost("Project-stage task lease is no longer owned")
                cur.execute(
                    "SELECT company_id FROM company_master WHERE company_id=%s FOR UPDATE",
                    (task["company_id"],),
                )
                cur.execute("SELECT jp_type_code FROM companies WHERE jp_no=%s", (task["jp_no"],))
                existing = cur.fetchone()
                if existing and existing["jp_type_code"] != task["jp_type_code"]:
                    raise IdentityConflict("Identity conflicts with existing company projection")

                keys = _write_keys(task, endpoint, normalized)
                eligible = _eligible_write_keys(cur, keys, endpoint, requested_at)

            should_write = not keys or bool(eligible)
            normalized_for_write = normalized
            if endpoint == "financial_statements" and keys:
                normalized_for_write = [
                    statement
                    for statement in normalized
                    if (
                        f"financial_statement:{task['jp_type_code']}:{task['jp_no']}:"
                        f"{statement.fiscal_year}"
                    )
                    in eligible
                ]
                should_write = bool(normalized_for_write)
            elif keys and len(eligible) != len(keys):
                should_write = False

            if should_write:
                common = dict(
                    jp_type_code=task["jp_type_code"],
                    jp_no=task["jp_no"],
                    raw_json=payload,
                    requested_at=requested_at,
                    request_path=path,
                    body_sha256=sha256(
                        json.dumps(payload, sort_keys=True, default=str).encode()
                    ).hexdigest(),
                )
                with use_connection(conn):
                    if endpoint == "profile":
                        save_company_profile(
                            profile=normalized_for_write,
                            http_status=200,
                            **{k: v for k, v in common.items() if k not in ("jp_type_code", "jp_no")},
                        )
                    elif endpoint == "financial_submissions":
                        save_submissions(rows=normalized_for_write, **common)
                    elif endpoint == "financial_statements":
                        save_statements(statements=normalized_for_write, **common)
                    elif endpoint == "industry_benchmark":
                        if normalized_for_write is not None:
                            save_benchmark(benchmark=normalized_for_write, **common)
                    else:
                        save_collection(resource=endpoint, rows=normalized_for_write, **common)

                with conn.cursor() as cur:
                    _record_write_state(cur, eligible, endpoint, requested_at, archive_id)
                    if endpoint == "profile":
                        cur.execute(
                            """
                            UPDATE company_master
                            SET identity_status='verified',
                                last_verified_at=%s,
                                observed_jp_stat_code=IF(
                                    %s IS NULL, observed_jp_stat_code, %s
                                ),
                                observed_status_at=IF(
                                    %s IS NULL, observed_status_at, %s
                                )
                            WHERE company_id=%s
                            """,
                            (
                                requested_at,
                                normalized_for_write.jp_stat_code,
                                normalized_for_write.jp_stat_code,
                                normalized_for_write.jp_stat_code,
                                requested_at,
                                task["company_id"],
                            ),
                        )

            outcome = nominal_outcome if should_write else "stale_skipped"
            with conn.cursor() as cur:
                _update_request_state(
                    cur,
                    task,
                    endpoint,
                    scope,
                    requested_at,
                    archive_id,
                    outcome,
                )
                task_status = "not_available" if outcome == "not_available" else "done"
                cur.execute(
                    """
                    UPDATE endpoint_tasks SET status=%s,raw_response_id=%s,finished_at=UTC_TIMESTAMP(6),
                      last_error=%s,lease_owner=NULL,lease_expires_at=NULL WHERE id=%s
                    """,
                    (
                        task_status,
                        archive_id,
                        "stale_replay_skipped" if outcome == "stale_skipped" else None,
                        task["id"],
                    ),
                )
                cur.execute(
                    "UPDATE response_archive SET processing_status=%s,error_code=%s WHERE id=%s",
                    (
                        "processed" if task_status == "done" else "not_available",
                        "stale_replay_skipped" if outcome == "stale_skipped" else None,
                        archive_id,
                    ),
                )
            conn.commit()
            return outcome
        except BaseException:
            conn.rollback()
            raise
