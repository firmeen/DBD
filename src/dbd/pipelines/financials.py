from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256

from ..client import DbdClient
from ..client.endpoints.companies import get_company_profile, split_company_code
from ..client.endpoints.financials import (
    benchmark_path,
    get_financial_statements,
    get_financial_submissions,
    get_industry_benchmark,
    statements_path,
    submissions_path,
)
from ..contracts.financial import normalize_statements, normalize_submissions
from ..contracts.strict import normalize_industry_benchmark
from ..core.exceptions import ContractError
from ..storage.mysql.repositories.company_repository import find_submit_fin_year
from ..storage.mysql.repositories.financial_repository import (
    save_benchmark,
    save_statements,
    save_submissions,
)


@dataclass
class FinancialResult:
    fiscal_year: str
    submissions: int
    statements: int
    items: int
    benchmark_items: int = 0


def resolve_fiscal_year(client: DbdClient, company_code: str) -> str:
    """The API rejects a request without fiscalYear (400) but then returns every
    submitted year, so any valid year works. Prefer the latest one the company
    has actually submitted: from MySQL if the profile was already ingested,
    otherwise from a fresh profile call.
    """
    _, jp_no = split_company_code(company_code)

    submitted = find_submit_fin_year(jp_no)
    if not submitted:
        profile = get_company_profile(client, company_code)
        submitted = profile.get("submitFinYear") if isinstance(profile, dict) else None

    years = sorted((submitted or "").split(), reverse=True)
    if not years:
        raise ContractError(f"No submitted fiscal years found for {company_code}")

    return years[0]


def run(client: DbdClient, company_code: str, fiscal_year: str | None = None) -> FinancialResult:
    jp_type_code, jp_no = split_company_code(company_code)
    fiscal_year = fiscal_year or resolve_fiscal_year(client, company_code)

    requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    raw_submissions = get_financial_submissions(client, company_code, fiscal_year)
    submission_rows = normalize_submissions(raw_submissions)

    _, submissions_saved = save_submissions(
        jp_type_code=jp_type_code,
        jp_no=jp_no,
        rows=submission_rows,
        raw_json=raw_submissions,
        requested_at=requested_at,
        body_sha256=sha256(
            json.dumps(raw_submissions, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest(),
        request_path=submissions_path(jp_type_code, jp_no),
    )

    requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    raw_statements = get_financial_statements(client, company_code, fiscal_year)
    statements = normalize_statements(raw_statements)

    _, statement_count, item_count = save_statements(
        jp_type_code=jp_type_code,
        jp_no=jp_no,
        statements=statements,
        raw_json=raw_statements,
        requested_at=requested_at,
        body_sha256=sha256(
            json.dumps(raw_statements, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest(),
        request_path=statements_path(jp_type_code, jp_no),
    )

    requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    raw_benchmark = get_industry_benchmark(client, company_code, fiscal_year)
    benchmark = normalize_industry_benchmark(raw_benchmark)

    benchmark_items = 0
    if benchmark is not None:
        _, benchmark_items = save_benchmark(
            jp_type_code=jp_type_code,
            jp_no=jp_no,
            benchmark=benchmark,
            raw_json=raw_benchmark,
            requested_at=requested_at,
            body_sha256=sha256(
                json.dumps(raw_benchmark, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest(),
            request_path=benchmark_path(jp_type_code, jp_no),
        )

    return FinancialResult(
        fiscal_year=fiscal_year,
        submissions=submissions_saved,
        statements=statement_count,
        items=item_count,
        benchmark_items=benchmark_items,
    )
