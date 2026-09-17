import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.cli import legacy
from dbd.core.exceptions import ContractError
from dbd.pipelines import discovery
from dbd.storage.mysql.repositories import discovery_batch_repository as batch_repository
from dbd.storage.mysql.repositories import discovery_repository as repository


async def test_failed_middle_page_is_retried_without_redoing_completed_pages(monkeypatch):
    states = {}
    persisted = []
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

    def persist_batch(records):
        outcomes = []
        for record in records:
            page = record["page_no"]
            persisted.append(page)
            states[page] = "done"
            outcomes.append(
                {
                    "page_no": page,
                    "archive_id": page,
                    "status": "done",
                    "written": len(record["profiles"]),
                    "rejected": 0,
                    "projected": len(record["projection_profiles"]),
                }
            )
        return outcomes

    monkeypatch.setattr(discovery, "update_bucket", lambda *a, **kw: updates.append(kw))
    monkeypatch.setattr(
        repository, "completed_pages", lambda b: {p for p, s in states.items() if s == "done"}
    )
    monkeypatch.setattr(batch_repository, "persist_page_batch", persist_batch)
    monkeypatch.setattr(repository, "fail_page", lambda b, p, e: states.update({p: "failed"}))
    monkeypatch.setattr(
        repository,
        "page_progress",
        lambda b: {
            "done": list(states.values()).count("done"),
            "failed": list(states.values()).count("failed"),
            "partial": list(states.values()).count("partial"),
            "rejected": 0,
            "rows": list(states.values()).count("done"),
        },
    )
    filters = SearchFilters("010")
    await discovery._drain_bucket(Client(), 1, filters, 3, 1, verbose=False)
    assert updates[-1]["status"] == "failed"
    assert updates[-1]["pages_done"] == 2
    assert sorted(persisted) == [1, 3]

    fail = False
    requested.clear()
    persisted.clear()
    await discovery._drain_bucket(Client(), 1, filters, 3, 3, verbose=False)
    assert requested == [2]
    assert persisted == [2]
    assert updates[-1]["status"] == "done"
    assert updates[-1]["rows_seen"] == 3


def test_invalid_row_is_quarantined_without_losing_valid_neighbor():
    profiles, rejected = discovery._normalize_rows(
        [
            {"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "valid"},
            {"jpTypeCode": "7", "jpNo": "short", "jpName": "invalid"},
        ]
    )
    assert [(p.jp_type_code, p.jp_no) for p in profiles] == [("7", "0107556000051")]
    assert len(rejected) == 1
    assert rejected[0]["row_index"] == 1
    assert rejected[0]["error_code"] == "ContractError"


def test_observed_type_a_short_identity_is_not_quarantined():
    profiles, rejected = discovery._normalize_rows(
        [{"jpTypeCode": "A", "jpNo": "12345678901", "jpName": "observed type A"}]
    )
    assert profiles[0].jp_no == "12345678901"
    assert rejected == []


def test_discover_status_uses_current_discovery_summary(monkeypatch, capsys):
    monkeypatch.setattr(
        discovery,
        "discovery_summary",
        lambda: {
            "unique_companies": 12,
            "rows_fetched": 34,
            "buckets": 5,
            "buckets_done": 2,
            "buckets_split": 1,
            "buckets_open": 2,
            "buckets_failed": 0,
            "buckets_uncounted": 1,
            "remaining_counted_pages": 3,
            "pages_done": 9,
            "pages_failed": 1,
            "pages_partial": 0,
            "unresolved_rejections": 0,
            "partitions_lossy": 0,
        },
    )
    monkeypatch.setattr(discovery, "coverage_gaps", lambda: [])

    legacy.command_discover_status()

    output = capsys.readouterr().out
    assert "unique companies : 12" in output
    assert "page checkpoints  : 9 done, 1 failed, 0 partial" in output


async def test_province_seed_cover_includes_audited_exception():
    from dbd.pipelines.discovery import province_seeds

    class Client:
        async def get(self, path, params=None, **kw):
            assert path == "/api/v1/commons/provinces"
            return [
                {"pvCode": "10"},
                {"pvCode": "96"},
                {"pvCode": "10"},
                {"pvCode": "5"},
                {"pvCode": ""},
                {"nope": 1},
            ]

    seeds = await province_seeds(Client())

    assert seeds == ["005", "010", "096", "099"], seeds
    assert all(len(s) == 3 for s in seeds)
    assert len(seeds) == len(set(seeds))
    for number, expected in [
        ("0107556000051", "010"),
        ("0965568000123", "096"),
        ("0055560000001", "005"),
        ("0991234567890", "099"),
    ]:
        assert number[:3] in seeds, number
        assert number.startswith(expected)


@pytest.mark.parametrize("payload", [None, {}, {"unexpected": []}, []])
async def test_invalid_or_empty_province_response_fails_loudly(payload):
    class Client:
        async def get(self, path, **kwargs):
            return payload

    with pytest.raises(ContractError):
        await discovery.fetch_provinces(Client())
