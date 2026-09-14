from __future__ import annotations

from dataclasses import dataclass, field

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
            "capAmtMin": None,
            "capAmtMax": None,
            "totalIncomeMin": None,
            "totalIncomeMax": None,
            "netProfitMin": None,
            "netProfitMax": None,
            "totalAssetMin": None,
            "totalAssetMax": None,
        }

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
        return "|".join(parts)


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

    meta = data.get("meta") or {}
    rows = data.get("contents") or []

    return SearchPage(
        total_items=int(meta.get("totalItems") or 0),
        total_pages=int(meta.get("totalPages") or 0),
        current_page=int(meta.get("currentPage") or 0),
        items_per_page=int(meta.get("itemsPerPage") or ITEMS_PER_PAGE),
        rows=[r for r in rows if isinstance(r, dict)],
    )
