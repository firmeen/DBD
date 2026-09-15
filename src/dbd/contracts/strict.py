from __future__ import annotations

from .company import COLLECTION_NORMALIZERS
from .financial import normalize_benchmark
from ..core.exceptions import ContractError

_TRANSFORMATION_KEYS = ("transformFroms", "transformTos", "mergerFroms", "mergerTos")
_NATION_HISTORY_KEYS = ("shareHistTHDps", "shareHistOTDps")


def normalize_collection(resource: str, data: object) -> list[dict]:
    """Apply endpoint-level root-shape contracts before collection normalization.

    Several legacy normalizers intentionally treated missing nested values as an
    empty list. That is useful inside a valid response, but unsafe at the root:
    an upstream schema rename could otherwise look like a legitimate empty
    collection and replace a previously populated projection.
    """
    if resource == "transformations":
        if not isinstance(data, dict) or not any(key in data for key in _TRANSFORMATION_KEYS):
            raise ContractError(
                "Transformation response is missing all expected direction collections"
            )
    elif resource == "nation_history":
        if not isinstance(data, dict) or "customChart" not in data:
            raise ContractError("Nation-history response is missing customChart")
        chart = data["customChart"]
        if not isinstance(chart, dict) or not any(key in chart for key in _NATION_HISTORY_KEYS):
            raise ContractError("Nation-history chart is missing expected history series")

    return COLLECTION_NORMALIZERS[resource](data)


def normalize_industry_benchmark(data: object):
    """Only an explicit null means no published benchmark.

    A JSON object missing its natural identity is a contract failure, not
    `not_available`; treating it as absent could silently hide an upstream schema
    change and allow an older shared benchmark row to survive unchallenged.
    """
    if data is None:
        return None
    if not isinstance(data, dict):
        raise ContractError(
            f"Expected a JSON object or null for industry benchmark, got {type(data).__name__}"
        )
    missing = [key for key in ("submitObjCode", "fiscalYear") if not data.get(key)]
    if missing:
        raise ContractError("Industry benchmark is missing " + ", ".join(missing))
    return normalize_benchmark(data)
