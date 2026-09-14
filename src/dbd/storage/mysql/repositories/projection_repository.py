from __future__ import annotations

import json
from datetime import datetime
from hashlib import sha256

from ....core.exceptions import ContractError
from ....runtime.resources import normalize_resource
from ..engine import connect, use_connection
from .company_detail_repository import save_collection
from .company_repository import save_company_profile
from .financial_repository import save_benchmark, save_statements, save_submissions


class LeaseLost(RuntimeError):
    pass


def project(task: dict, archive_id: int, payload, requested_at: datetime, path: str, scope: str = "") -> str:
    """Fence the projection and task completion in the same MySQL transaction.

    Reuse the mature endpoint SQL through a transaction-bound connection.
    Nested repositories cannot commit independently in this scope.
    """
    endpoint = task["endpoint"]
    normalized = normalize_resource(endpoint, payload)
    if endpoint == "profile" and (normalized.jp_no, normalized.jp_type_code) != (
        task["jp_no"],
        task["jp_type_code"],
    ):
        raise ContractError("Response identity does not match requested company")
    outcome = "not_available" if normalized is None or normalized == [] else "done"

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM endpoint_tasks WHERE id=%s AND status='running'
                      AND lease_owner=%s AND lease_version=%s AND lease_expires_at>UTC_TIMESTAMP(6)
                    FOR UPDATE
                """,
                    (task["id"], task["lease_owner"], task["lease_version"]),
                )
                if cur.fetchone() is None:
                    raise LeaseLost("Task lease is no longer owned")
                cur.execute(
                    "SELECT company_id FROM company_master WHERE company_id=%s FOR UPDATE",
                    (task["company_id"],),
                )
                cur.execute("SELECT jp_type_code FROM companies WHERE jp_no=%s", (task["jp_no"],))
                existing = cur.fetchone()
                if existing and existing["jp_type_code"] != task["jp_type_code"]:
                    raise ContractError("Identity conflicts with existing company projection")
                cur.execute(
                    """
                    SELECT requested_at FROM company_endpoint_state
                    WHERE company_id=%s AND endpoint=%s AND scope_key=%s
                """,
                    (task["company_id"], endpoint, scope),
                )
                state = cur.fetchone()
                stale = state is not None and state["requested_at"] > requested_at

            if not stale:
                common = dict(
                    jp_type_code=task["jp_type_code"],
                    jp_no=task["jp_no"],
                    raw_json=payload,
                    requested_at=requested_at,
                    request_path=path,
                    body_sha256=sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest(),
                )
                with use_connection(conn):
                    if endpoint == "profile":
                        save_company_profile(
                            profile=normalized,
                            http_status=200,
                            **{k: v for k, v in common.items() if k not in ("jp_type_code", "jp_no")},
                        )
                    elif endpoint == "financial_submissions":
                        save_submissions(rows=normalized, **common)
                    elif endpoint == "financial_statements":
                        save_statements(statements=normalized, **common)
                    elif endpoint == "industry_benchmark":
                        if normalized is not None:
                            save_benchmark(benchmark=normalized, **common)
                    else:
                        save_collection(resource=endpoint, rows=normalized, **common)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO company_endpoint_state(company_id,endpoint,scope_key,requested_at,archive_id,status)
                        VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                          requested_at=VALUES(requested_at),archive_id=VALUES(archive_id),status=VALUES(status)
                    """,
                        (task["company_id"], endpoint, scope, requested_at, archive_id, outcome),
                    )
                    if endpoint == "profile":
                        cur.execute(
                            "UPDATE company_master SET identity_status='verified',last_verified_at=%s WHERE company_id=%s",
                            (requested_at, task["company_id"]),
                        )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE endpoint_tasks SET status=%s,raw_response_id=%s,finished_at=UTC_TIMESTAMP(6),
                      last_error=NULL,lease_owner=NULL,lease_expires_at=NULL WHERE id=%s
                """,
                    (outcome, archive_id, task["id"]),
                )
                cur.execute(
                    "UPDATE response_archive SET processing_status=%s WHERE id=%s",
                    ("processed" if outcome == "done" else outcome, archive_id),
                )
            conn.commit()
            return outcome
        except BaseException:
            conn.rollback()
            raise
