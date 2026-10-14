from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from ...contracts.common import as_list
from ...core.exceptions import ContractError

SEARCH_PATH = "/api/v1/company-profiles/infos"
SEARCH_REFERER = "https://datawarehouse.dbd.go.th/juristic/searchInfo"

# Proven backend limits (see scripts/dbd_master_reference.py):
#   itemsPerPage is locked to 10 no matter what is requested,
#   currentPage above 1000 is rejected with COMPANY_SEARCH_PAGE_LIMIT_EXCEEDED.
# So one query can never yield more than 10,000 rows, and any bucket larger
# than that must be partitioned before it can be fully enumerated.
ITEMS_PER_PAGE = 10
MAX_PAGE = 1000
MAX_ROWS_PER_QUERY = ITEMS_PER_PAGE * MAX_PAGE

# A keyword is mandatory: a filters-only request returns HTTP 400.
MIN_KEYWORD_LENGTH = 3


@dataclass
class SearchFilters:
    keyword: str
    search_type: str = ""
    pv_codes: list[str] = field(default_factory=list)
    jp_statuses: list[str] = field(default_factory=list)
    jp_types: list[str] = field(default_factory=list)
    business_sizes: list[str] = field(default_factory=list)
    cap_amt_min: Decimal | None = None
    cap_amt_max: Decimal | None = None
    total_asset_min: Decimal | None = None
    total_asset_max: Decimal | None = None
    total_income_min: Decimal | None = None
    total_income_max: Decimal | None = None
    net_profit_min: Decimal | None = None
    net_profit_max: Decimal | None = None

    def __post_init__(self) -> None:
        """Canonicalize decimal ranges without changing legacy bucket identity."""
        for name in (
            "cap_amt_min", "cap_amt_max",
            "total_asset_min", "total_asset_max",
            "total_income_min", "total_income_max",
            "net_profit_min", "net_profit_max",
        ):
            raw = getattr(self, name)
            if raw is None:
                continue
            if isinstance(raw, bool):
                raise ValueError(f"{name} must be a finite decimal")
            try:
                value = Decimal(str(raw))
            except (InvalidOperation, ValueError):
                raise ValueError(f"{name} must be a finite decimal") from None
            if not value.is_finite() or (value and not -12 <= value.adjusted() <= 25):
                raise ValueError(f"{name} is outside supported numeric precision")
            if value == 0:
                value = Decimal(0)
            normalized = format(value.normalize(), "f")
            if len(normalized) > 60:
                raise ValueError(f"{name} is too long to persist safely")
            setattr(self, name, Decimal(normalized))
        for field in ("cap_amt", "total_asset", "total_income", "net_profit"):
            minimum = getattr(self, field + "_min")
            maximum = getattr(self, field + "_max")
            if minimum is not None and maximum is not None and minimum > maximum:
                raise ValueError(f"{field}_min exceeds {field}_max")

    def numeric_range(self) -> dict[str, str] | None:
        """Persist only specified bounds, preserving legacy capital-only keys."""
        bounds = (
            ("capAmtMin", self.cap_amt_min), ("capAmtMax", self.cap_amt_max),
            ("totalAssetMin", self.total_asset_min), ("totalAssetMax", self.total_asset_max),
            ("totalIncomeMin", self.total_income_min), ("totalIncomeMax", self.total_income_max),
            ("netProfitMin", self.net_profit_min), ("netProfitMax", self.net_profit_max),
        )
        result = {key: format(value, "f") for key, value in bounds if value is not None}
        return result or None

    def payload(self, page: int = 1, sort_by: str = "jpName") -> dict:
        if len(self.keyword) < MIN_KEYWORD_LENGTH:
            raise ValueError(f"keyword must be at least {MIN_KEYWORD_LENGTH} characters: {self.keyword!r}")

        return {
            "keyword": self.keyword,
            "type": self.search_type,
            "sortBy": sort_by,
            "currentPage": page,
            "pvCodeList": self.pv_codes,
            "jpStatusList": self.jp_statuses,
            "jpTypeList": self.jp_types,
            "businessSizeList": self.business_sizes,
            "capAmtMin": self._api_amount(self.cap_amt_min),
            "capAmtMax": self._api_amount(self.cap_amt_max),
            "totalIncomeMin": self._api_amount(self.total_income_min),
            "totalIncomeMax": self._api_amount(self.total_income_max),
            "netProfitMin": self._api_amount(self.net_profit_min),
            "netProfitMax": self._api_amount(self.net_profit_max),
            "totalAssetMin": self._api_amount(self.total_asset_min),
            "totalAssetMax": self._api_amount(self.total_asset_max),
        }

    @staticmethod
    def _api_amount(value: Decimal | None) -> int | str | None:
        if value is None:
            return None
        # Avoid binary float rounding. Integral bounds remain JSON numbers;
        # fractional bounds stay exact strings until their API semantics pass
        # the read-only probe gate.
        if value == value.to_integral_value():
            return int(value)
        return format(value, "f")

    def key(self) -> str:
        """Stable identity for a bucket, used to resume without duplicating."""
        parts = [
            f"k={self.keyword}",
            f"t={self.search_type}",
            f"pv={','.join(sorted(self.pv_codes))}",
            f"st={','.join(sorted(self.jp_statuses))}",
            f"jt={','.join(sorted(self.jp_types))}",
            f"sz={','.join(sorted(self.business_sizes))}",
        ]
        numeric = self.numeric_range()
        if numeric is not None:
            # Preserve the key of every existing capital-only child unchanged.
            if "capAmtMin" in numeric or "capAmtMax" in numeric:
                parts.append(
                    "cap=" + ",".join(f"{key}:{numeric.get(key, '')}"
                                     for key in ("capAmtMin", "capAmtMax"))
                )
            for label, fields in (
                ("asset", ("totalAssetMin", "totalAssetMax")),
                ("income", ("totalIncomeMin", "totalIncomeMax")),
                ("profit", ("netProfitMin", "netProfitMax")),
            ):
                if any(key in numeric for key in fields):
                    parts.append(label + "=" + ",".join(
                        f"{key}:{numeric.get(key, '')}" for key in fields
                    ))
        key = "|".join(parts)
        if len(key) > 500:
            raise ValueError("Discovery bucket key exceeds VARCHAR(500)")
        return key


@dataclass
class SearchPage:
    total_items: int
    total_pages: int
    current_page: int
    items_per_page: int
    rows: list[dict]


def parse_search_response(data: object) -> SearchPage:
    if not isinstance(data, dict):
        raise TypeError(f"Expected a JSON object from search, got {type(data).__name__}")

    meta = data.get("meta")
    if (
        not isinstance(meta, dict)
        or any(k not in meta for k in ("totalItems", "totalPages", "currentPage"))
        or "contents" not in data
    ):
        raise ContractError("Search response is missing pagination metadata or contents")
    for key in ("totalItems", "totalPages", "currentPage"):
        if isinstance(meta[key], bool) or not isinstance(meta[key], int) or meta[key] < 0:
            raise ContractError(f"Invalid search metadata: {key}")
    if meta["totalItems"] > 0 and meta["totalPages"] < 1:
        raise ContractError("Nonempty search has no pages")
    rows = as_list(data["contents"])

    return SearchPage(
        total_items=int(meta.get("totalItems") or 0),
        total_pages=int(meta.get("totalPages") or 0),
        current_page=int(meta.get("currentPage") or 0),
        items_per_page=int(meta.get("itemsPerPage") or ITEMS_PER_PAGE),
        rows=[r for r in rows if isinstance(r, dict)],
    )
