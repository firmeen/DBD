from dbd.client.endpoints.search import SearchFilters
from dbd.pipelines import discovery
from dbd.storage.mysql.repositories import discovery_repository as repository


async def test_failed_middle_page_is_retried_without_redoing_completed_pages(monkeypatch):
    states = {}
    archives = []
    updates = []
    requested = []
    fail = True

    class Client:
        concurrency = 3

        async def post(self, path, payload, **kwargs):
            page = payload["currentPage"]
            requested.append(page)
            if page == 2 and fail:
                raise TimeoutError()
            return {
                "meta": {"currentPage": page, "totalItems": 3, "totalPages": 3},
                "contents": [{"jpTypeCode": "7", "jpNo": str(page).zfill(13)}],
            }

    def archive(*args):
        archives.append(args)
        return len(archives)

    def finish(bucket, page, *args):
        states[page] = "done"
        return 1

    monkeypatch.setattr(discovery, "update_bucket", lambda *a, **kw: updates.append(kw))
    monkeypatch.setattr(
        repository, "completed_pages", lambda b: {p for p, s in states.items() if s == "done"}
    )
    monkeypatch.setattr(repository, "archive_search", archive)
    monkeypatch.setattr(repository, "finish_page", finish)
    monkeypatch.setattr(repository, "fail_page", lambda b, p, e: states.update({p: "failed"}))
    monkeypatch.setattr(
        repository,
        "page_progress",
        lambda b: {
            "done": list(states.values()).count("done"),
            "failed": list(states.values()).count("failed"),
            "rows": list(states.values()).count("done"),
        },
    )
    filters = SearchFilters("010")
    await discovery._drain_bucket(Client(), 1, filters, 3, 1, verbose=False)
    assert updates[-1]["status"] == "failed"
    assert updates[-1]["pages_done"] == 2
    fail = False
    requested.clear()
    await discovery._drain_bucket(Client(), 1, filters, 3, 3, verbose=False)
    assert requested == [2]
    assert updates[-1]["status"] == "done"
    assert updates[-1]["rows_seen"] == 3


def test_province_seed_cover_is_complete_by_construction():
    """Seeds must cover every possible company-number prefix.

    A Thai juristic person number is 13 digits: "0" + two-digit registration
    province + serial. Search matches numbers as a substring, so searching
    "0"+province necessarily returns everything whose number starts with it.
    Seeding one keyword per province therefore covers the registration space
    without needing an external total to confirm it.
    """
    import asyncio

    from dbd.pipelines.discovery import province_seeds

    class Client:
        async def get(self, path, params=None, **kw):
            assert path == "/api/v1/commons/provinces"
            # Deliberately messy: unsorted, duplicated, and a single-digit code.
            return [
                {"pvCode": "10"},
                {"pvCode": "96"},
                {"pvCode": "10"},
                {"pvCode": "5"},
                {"pvCode": ""},
                {"nope": 1},
            ]

    seeds = asyncio.run(province_seeds(Client()))

    assert seeds == ["005", "010", "096"], seeds
    assert all(len(s) == 3 for s in seeds), "search requires a keyword of at least 3 characters"
    assert len(seeds) == len(set(seeds)), "a duplicated seed would refetch a whole province"

    # Every real number beginning with a seeded province is reachable from a seed.
    for number, expected in [
        ("0107556000051", "010"),
        ("0965568000123", "096"),
        ("0055560000001", "005"),
    ]:
        assert number[:3] in seeds, number
        assert number.startswith(expected)
