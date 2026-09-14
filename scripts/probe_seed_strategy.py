"""Determine how DBD's company search matches a keyword.

Why this exists
---------------
Enumerating companies is only possible through POST /company-profiles/infos,
and that endpoint requires a keyword of at least three characters: a
filters-only request returns HTTP 400. The seed keyword list therefore decides
whether full coverage is even reachable, and the answer depends entirely on how
the keyword is matched:

  PREFIX     jpNo starts with the keyword
             -> seeds "000".."999" partition the registration space with no
                overlap, and completeness is provable by construction.

  SUBSTRING  jpNo merely contains the keyword
             -> one company appears under many seeds, so the same rows are
                fetched repeatedly and no seed set can be shown to be complete.

  NAME       the keyword also matches jpName
             -> numeric seeds alone can never be exhaustive, because coverage
                depends on text as well.

Observed but unexplained: keyword "010" reports ~962,862 rows, roughly half of
all Thai juristic persons, which is implausible for a single prefix. This script
replaces that guess with evidence.

Cost: about ten requests. It reads only and writes nothing.

    python scripts\\probe_seed_strategy.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dbd.client.async_client import AsyncDbdClient  # noqa: E402
from dbd.client.endpoints.search import (  # noqa: E402
    SEARCH_PATH,
    SEARCH_REFERER,
    SearchFilters,
    parse_search_response,
)

# "010" is the keyword whose count motivated this probe. "556" is a sequence that
# appears inside many registration numbers but should start almost none of them,
# so a non-empty result is by itself strong evidence of substring matching.
NUMERIC_SEEDS = ["010", "556", "700"]
TEXT_SEED = "ประกัน"


async def look(client: AsyncDbdClient, keyword: str) -> tuple[int, list[str], list[str]]:
    filters = SearchFilters(keyword)
    data = await client.post(
        SEARCH_PATH, filters.payload(page=1), referer=SEARCH_REFERER, timeout=120
    )
    page = parse_search_response(data)
    numbers = [str(row.get("jpNo") or "") for row in page.rows]
    names = [str(row.get("jpName") or "") for row in page.rows]
    return page.total_items, numbers, names


def classify(keyword: str, numbers: list[str]) -> str:
    if not numbers:
        return "no rows"
    starts = sum(n.startswith(keyword) for n in numbers)
    contains = sum(keyword in n for n in numbers)
    if starts == len(numbers):
        return f"all {len(numbers)} start with it -> PREFIX"
    if contains == len(numbers):
        return f"all contain it, only {starts} start with it -> SUBSTRING"
    return f"{contains}/{len(numbers)} contain it, {starts} start with it -> matches something else too"


async def main() -> None:
    # One request at a time: this is a diagnostic, not a workload.
    async with AsyncDbdClient(concurrency=1, rate_per_second=2) as client:
        print("=" * 78)
        print("NUMERIC KEYWORDS")
        print("=" * 78)

        verdicts = {}
        for seed in NUMERIC_SEEDS:
            total, numbers, _ = await look(client, seed)
            verdicts[seed] = classify(seed, numbers)
            print(f"\nkeyword {seed!r}: totalItems={total:,}")
            for n in numbers[:5]:
                marker = "start" if n.startswith(seed) else ("inside" if seed in n else "NOT FOUND")
                print(f"    {n}   [{marker}]")
            print(f"  -> {verdicts[seed]}")

        print()
        print("=" * 78)
        print("TEXT KEYWORD")
        print("=" * 78)
        total, numbers, names = await look(client, TEXT_SEED)
        print(f"\nkeyword {TEXT_SEED!r}: totalItems={total:,}")
        for name in names[:5]:
            print(f"    {name}")
        name_match = total > 0

        print()
        print("=" * 78)
        print("CONCLUSION")
        print("=" * 78)
        substring = any("SUBSTRING" in v or "something else" in v for v in verdicts.values())

        if name_match:
            print("  Keyword matches company NAMES as well as numbers.")
            print("  -> A purely numeric seed list can never be proven complete.")
        if substring:
            print("  Numbers are matched as a SUBSTRING, not a prefix.")
            print("  -> Seeds overlap. Dedup on (jp_type_code, jp_no) is required, and")
            print("     coverage must be reconciled against an authoritative DBD total")
            print("     rather than claimed from the seed list.")
        elif not substring and not name_match:
            print("  Numbers are matched as a PREFIX and text does not match.")
            print('  -> Seeds "000".."999" partition the space cleanly.')
            print("     Every bucket over 10,000 rows still needs splitting.")

        print()
        print(f"  requests used: {client.requests_made}")


if __name__ == "__main__":
    asyncio.run(main())
