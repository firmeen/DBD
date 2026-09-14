from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256

from ..client import DbdClient
from ..client.endpoints.companies import get_company_profile, split_company_code
from ..contracts.company import normalize
from ..storage.mysql.repositories.company_repository import save_company_profile


@dataclass
class CompanyProfileResult:
    jp_no: str
    jp_type_code: str
    raw_response_id: int
    jp_name: str | None
    body_sha256: str


def run(client: DbdClient, company_code: str) -> CompanyProfileResult:
    """fetch -> validate/normalize -> persist, for one company code."""
    jp_type_code, jp_no = split_company_code(company_code)
    request_path = f"/api/v1/company-profiles/info/{jp_type_code}/{jp_no}"

    requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    raw_data = get_company_profile(client, company_code)

    profile = normalize(raw_data)

    body_sha256 = sha256(json.dumps(raw_data, sort_keys=True, default=str).encode("utf-8")).hexdigest()

    raw_response_id = save_company_profile(
        profile=profile,
        raw_json=raw_data,
        http_status=200,
        requested_at=requested_at,
        body_sha256=body_sha256,
        request_path=request_path,
    )

    return CompanyProfileResult(
        jp_no=profile.jp_no,
        jp_type_code=profile.jp_type_code,
        raw_response_id=raw_response_id,
        jp_name=profile.jp_name,
        body_sha256=body_sha256,
    )
