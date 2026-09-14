from __future__ import annotations

from .. import DbdClient
from .companies import split_company_code

# The frontend swaps this path segment when you switch between
# งบแสดงฐานะการเงิน / งบกำไรขาดทุน / อัตราส่วนทางการเงิน, but all four values
# return the same payload — the UI just renders a different subset of it.
# So one call fetches every statement line and ratio for every fiscal year.
STATEMENT_PAGE = "balancesheet"


def statements_path(jp_type_code: str, jp_no: str, page: str = STATEMENT_PAGE) -> str:
    return f"/api/v1/fin/{page}/year/{jp_type_code}/{jp_no}"


def submissions_path(jp_type_code: str, jp_no: str) -> str:
    return f"/api/v1/fin/submit/{jp_type_code}/{jp_no}"


def get_financial_statements(client: DbdClient, company_code: str, fiscal_year: str) -> dict:
    jp_type_code, jp_no = split_company_code(company_code)
    return client.get(statements_path(jp_type_code, jp_no), {"fiscalYear": fiscal_year})


def get_financial_submissions(client: DbdClient, company_code: str, fiscal_year: str) -> list:
    jp_type_code, jp_no = split_company_code(company_code)
    return client.get(submissions_path(jp_type_code, jp_no), {"fiscalYear": fiscal_year})


def benchmark_path(jp_type_code: str, jp_no: str) -> str:
    return f"/api/v1/fin/basics/{jp_type_code}/{jp_no}"


def get_industry_benchmark(client: DbdClient, company_code: str, fiscal_year: str) -> dict:
    """ค่าเฉลี่ยในประเภทธุรกิจ.

    Addressed by company but answers about the company's whole TSIC group —
    do not mistake this for the company's own figures.
    """
    jp_type_code, jp_no = split_company_code(company_code)
    return client.get(benchmark_path(jp_type_code, jp_no), {"fiscalYear": fiscal_year})
