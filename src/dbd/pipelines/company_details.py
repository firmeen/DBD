from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256

from ..client import DbdClient
from ..client.endpoints.companies import collection_path, get_collection, split_company_code
from ..contracts.company import COLLECTION_NORMALIZERS
from ..storage.mysql.repositories.company_detail_repository import RESOURCES, save_collection


def run_one(client: DbdClient, company_code: str, resource: str) -> tuple[int, int]:
    """Fetch and persist one child resource. Returns (raw_response_id, rows)."""
    jp_type_code, jp_no = split_company_code(company_code)

    requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    raw_data = get_collection(client, resource, company_code)

    rows = COLLECTION_NORMALIZERS[resource](raw_data)
    body_sha256 = sha256(json.dumps(raw_data, sort_keys=True, default=str).encode("utf-8")).hexdigest()

    return save_collection(
        resource=resource,
        jp_type_code=jp_type_code,
        jp_no=jp_no,
        rows=rows,
        raw_json=raw_data,
        requested_at=requested_at,
        body_sha256=body_sha256,
        request_path=collection_path(resource, jp_type_code, jp_no),
    )


def run(client: DbdClient, company_code: str) -> dict[str, int]:
    """Fetch every child resource for one company. Returns rows saved per resource."""
    return {resource: run_one(client, company_code, resource)[1] for resource in RESOURCES}
