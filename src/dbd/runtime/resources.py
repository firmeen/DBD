from __future__ import annotations

from ..client.endpoints.companies import collection_path, profile_path
from ..client.endpoints.financials import (
    benchmark_path,
    statements_path,
    submissions_path,
)
from ..contracts.company import COLLECTION_NORMALIZERS, normalize
from ..contracts.financial import normalize_statements, normalize_submissions
from ..contracts.strict import normalize_collection, normalize_industry_benchmark

# Grouped by the tab each endpoint appears on in the DBD web UI, left to right:
#   1 ข้อมูลนิติบุคคล            profile + details
#   2 ข้อมูลงบการเงิน            financials
#   3 การลงทุนจำแนกตามสัญชาติ    nationality   (not collected by default)
#   4 ประวัติการเปลี่ยนแปลง       history       (not collected by default)
#
# Tabs 3 and 4 are intentionally still implemented - normalizers, repositories
# and tables are untouched - but are left out of DEFAULT_PARTS. Skipping them
# removes four requests per company, which is about four million requests and
# roughly four days at a million companies. Ask for them explicitly with
# `--only nationality,history` if they are ever wanted again.
PARTS = {
    "profile": ("profile",),
    "details": ("committees", "committee_signs", "transformations"),
    "financials": ("financial_submissions", "financial_statements", "industry_benchmark"),
    "nationality": ("nations", "nation_history"),
    "history": ("name_history", "capital_history"),
}

DEFAULT_PARTS = ("profile", "details", "financials")

FINANCIAL = set(PARTS["financials"])

# Every endpoint still has a normalizer, whether or not it is collected by
# default. A mismatch means a part list drifted away from the contracts.
assert {e for group in PARTS.values() for e in group} - FINANCIAL - {"profile"} == set(
    COLLECTION_NORMALIZERS
), "PARTS and COLLECTION_NORMALIZERS disagree"


def endpoints(parts: list[str]) -> tuple[str, ...]:
    if not parts or any(p not in PARTS for p in parts):
        raise ValueError(f"Choose at least one of {','.join(PARTS)}")
    # The profile is a durable prerequisite for automatic fiscal-year selection.
    selected = [e for p in parts for e in PARTS[p]]
    if "financials" in parts and "profile" not in selected:
        selected.insert(0, "profile")
    return tuple(dict.fromkeys(selected))


def request_path(endpoint: str, kind: str, number: str) -> str:
    if endpoint == "profile":
        return profile_path(kind, number)
    if endpoint == "financial_submissions":
        return submissions_path(kind, number)
    if endpoint == "financial_statements":
        return statements_path(kind, number)
    if endpoint == "industry_benchmark":
        return benchmark_path(kind, number)
    return collection_path(endpoint, kind, number)


def normalize_resource(endpoint: str, payload):
    if endpoint == "profile":
        return normalize(payload)
    if endpoint == "financial_submissions":
        return normalize_submissions(payload)
    if endpoint == "financial_statements":
        return normalize_statements(payload)
    if endpoint == "industry_benchmark":
        return normalize_industry_benchmark(payload)
    return normalize_collection(endpoint, payload)
