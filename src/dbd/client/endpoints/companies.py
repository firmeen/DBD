from __future__ import annotations

from ...contracts.identity import parse_code
from .. import DbdClient


def split_company_code(code: str) -> tuple[str, str]:
    """DBD profile URLs use one slug: /company/profile/{code}.

    The frontend splits it as jpTypeCode = code[0], jpNo = code[1:].
    e.g. "70107556000051" -> ("7", "0107556000051")
    """
    parsed = parse_code(code)
    return parsed.jp_type_code, parsed.jp_no


def profile_path(jp_type_code: str, jp_no: str) -> str:
    return f"/api/v1/company-profiles/info/{jp_type_code}/{jp_no}"


def get_company_profile(client: DbdClient, company_code: str) -> dict:
    jp_type_code, jp_no = split_company_code(company_code)
    return client.get(profile_path(jp_type_code, jp_no))


# Child resources of a company profile. Each returns a bare JSON array.
# `liquidators` is intentionally not included: it 404s for companies that are
# still operating, so it needs its own not-found handling before being useful.
_COLLECTIONS = {
    "committees": "committees",
    "committee_signs": "committee-signs",
    "nations": "nations",
    "name_history": "names",
    "capital_history": "capitals",
    "transformations": "mergers",
    "nation_history": "nations/charts",
}


def collection_path(resource: str, jp_type_code: str, jp_no: str) -> str:
    segment = _COLLECTIONS[resource]
    return f"/api/v1/company-profiles/{segment}/{jp_type_code}/{jp_no}"


def get_collection(client: DbdClient, resource: str, company_code: str) -> list:
    jp_type_code, jp_no = split_company_code(company_code)
    return client.get(collection_path(resource, jp_type_code, jp_no))
