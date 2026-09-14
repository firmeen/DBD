from __future__ import annotations

import json
from datetime import datetime


def insert_raw_response(
    cur,
    *,
    jp_type_code: str,
    jp_no: str,
    endpoint: str,
    request_path: str,
    requested_at: datetime,
    http_status: int,
    body_sha256: str,
    raw_json: object,
) -> int:
    """Append one fetch to the source-of-truth table. Takes an open cursor so
    the raw row and its derived rows commit in a single transaction.
    """
    cur.execute(
        """
        INSERT INTO raw_responses (
            jp_type_code, jp_no, endpoint, request_path,
            requested_at, http_status, response_body_sha256, raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            jp_type_code,
            jp_no,
            endpoint,
            request_path,
            requested_at,
            http_status,
            body_sha256,
            json.dumps(raw_json, ensure_ascii=False, default=str),
        ),
    )
    return int(cur.lastrowid)
