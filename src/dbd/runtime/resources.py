from __future__ import annotations

from ..client.endpoints.companies import collection_path, profile_path
from ..client.endpoints.financials import (
    benchmark_path,
    statements_path,
    submissions_path,
)
from ..contracts.company import COLLECTION_NORMALIZERS, normalize
from ..contracts.financial import (
    normalize_benchmark,
    normalize_statements,
    normalize_submissions,
)

PARTS = {
    "profile": ("profile",),
    "details": tuple(COLLECTION_NORMALIZERS),
    "financials": ("financial_submissions", "financial_statements", "industry_benchmark"),
}
FINANCIAL = set(PARTS["financials"])


def endpoints(parts: list[str]) -> tuple[str, ...]:
    if not parts or any(p not in PARTS for p in parts):
        raise ValueError("Choose at least one of profile,details,financials")
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
        return normalize_benchmark(payload)
    return COLLECTION_NORMALIZERS[endpoint](payload)
