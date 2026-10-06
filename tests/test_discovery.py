from types import SimpleNamespace

import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.core.exceptions import ContractError
from dbd.pipelines import discovery
from dbd.storage.mysql.repositories import discovery_batch_repository as batch_repository
from dbd.storage.mysql.repositories import discovery_repository as repository


async def test_failed_middle_page_is_retried_without_redoing_completed_pages(monkeypatch):
    states = {}
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

    def persist(records):
        outcomes = []
        for record in records:
            states[record["page_no"]] = "done"
            outcomes.append(
                {
                    "page_no": record["page_no"],
                    "archive_id": record["page_no"],
                    "status": "done",
                    "written": len(record["profiles"]),
                    "rejected": 0,
                    "projected": len(record["projection_profiles"]),
                }
            )
        return outcomes

    monkeypatch.setattr(discovery, "update_bucket", lambda *a, **kw: updates.append(kw))
    monkeypatch.setattr(
        repository, "completed_pages", lambda b: {p for p, state in states.items() if state == "done"}
    )
    monkeypatch.setattr(
        repository, "fail_page", lambda b, page, error: states.update({page: "failed"})
    )
    monkeypatch.setattr(batch_repository, "persist_page_batch", persist)
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

    fail = False
    requested.clear()
    await discovery._drain_bucket(Client(), 1, filters, 3, 3, verbose=False)
    assert requested == [2]
    assert updates[-1]["status"] == "done"
    assert updates[-1]["rows_seen"] == 3


def test_invalid_row_is_quarantined_without_losing_valid_neighbor():
    profiles, rejected = discovery._normalize_rows(
        [
            {"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "valid"},
            {"jpTypeCode": "7", "jpNo": "short", "jpName": "invalid"},
        ]
    )
    assert [(profile.jp_type_code, profile.jp_no) for profile in profiles] == [
        ("7", "0107556000051")
    ]
    assert len(rejected) == 1
    assert rejected[0]["row_index"] == 1
    assert rejected[0]["error_code"] == "ContractError"


def test_master_cache_promotes_only_after_successful_persistence():
    profiles, rejected = discovery._normalize_rows(
        [{"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "stable"}]
    )
    assert rejected == []
    cache = {}

    first = {"profiles": list(profiles)}
    first_staged = discovery._stage_projection_cache([first], cache)
    assert len(first["projection_profiles"]) == 1
    assert first_staged
    assert cache == {}

    # A failed DB transaction must leave the committed cache untouched, so retrying
    # the same profile still projects it instead of treating rolled-back state as real.
    retry = {"profiles": list(profiles)}
    retry_staged = discovery._stage_projection_cache([retry], cache)
    assert len(retry["projection_profiles"]) == 1
    assert cache == {}

    cache.update(retry_staged)
    after_commit = {"profiles": list(profiles)}
    assert discovery._stage_projection_cache([after_commit], cache) == {}
    assert after_commit["projection_profiles"] == []


def test_master_cache_suppresses_duplicates_inside_one_microbatch():
    profiles, _ = discovery._normalize_rows(
        [{"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "stable"}]
    )
    first = {"profiles": list(profiles)}
    second = {"profiles": list(profiles)}

    staged = discovery._stage_projection_cache([first, second], {})

    assert staged
    assert len(first["projection_profiles"]) == 1
    assert second["projection_profiles"] == []


def test_observed_type_a_short_identity_is_not_quarantined():
    profiles, rejected = discovery._normalize_rows(
        [{"jpTypeCode": "A", "jpNo": "12345678901", "jpName": "observed type A"}]
    )
    assert profiles[0].jp_no == "12345678901"
    assert rejected == []


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
    assert all(len(seed) == 3 for seed in seeds)
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


class _DirectClient:
    def __init__(self, *args, **kwargs) -> None:
        self.concurrency = int(kwargs.get("concurrency") or 4)
        self.requests_made = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def fetch(self, method, path, **_kwargs):
        assert method == "GET"
        assert path == "/api/v1/commons/provinces"
        self.requests_made += 1
        return SimpleNamespace(
            data=[{"pvCode": "10"}],
            requested_at=discovery._now(),
        )


@pytest.mark.asyncio
async def test_direct_discovery_persists_valid_rows_without_mysql_runtime(monkeypatch):
    persisted = []
    legacy_called = []

    async def direct_page(_client, filters, page_no):
        assert filters.keyword == "010"
        assert page_no == 1
        data = {
            "meta": {"currentPage": 1, "totalItems": 2, "totalPages": 1},
            "contents": [
                {"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "valid"},
                {"jpTypeCode": "7", "jpNo": "short", "jpName": "invalid"},
            ],
        }
        return data, discovery._now()

    def persist(_repo, rows, *, generation, refresh_mask):
        persisted.extend(rows)
        assert generation == 2
        assert refresh_mask == 0
        return {"inserted": len(rows), "updated": 0, "refresh_reopened": 0}

    monkeypatch.setattr(discovery, "FixedRateDbdClient", _DirectClient)
    monkeypatch.setattr(discovery, "_direct_page", direct_page)
    monkeypatch.setattr(discovery, "_persist_direct_batch", persist)
    monkeypatch.setattr(
        discovery,
        "upsert_bucket",
        lambda *_args, **_kwargs: legacy_called.append("upsert_bucket"),
    )

    with pytest.raises(ContractError, match="contract-invalid"):
        await discovery.run(
            ["010"],
            generation=2,
            repository=object(),
            verbose=False,
        )

    assert legacy_called == []
    assert len(persisted) == 1
    assert persisted[0]["juristic_type_code"] == "7"
    assert persisted[0]["registration_no"] == "0107556000051"
    assert '"jpNo":"0107556000051"' in persisted[0]["master_payload"]


@pytest.mark.asyncio
async def test_direct_discovery_splits_oversized_partition_and_reconciles_counts(monkeypatch):
    persisted = []
    child_calls = []

    async def direct_page(_client, filters, page_no):
        assert page_no == 1
        if not filters.jp_types:
            total = 16000
            row = {"jpTypeCode": "7", "jpNo": "0107556000051"}
        else:
            child_calls.append(filters.jp_types[0])
            total = 2000
            index = len(child_calls)
            row = {
                "jpTypeCode": "A",
                "jpNo": f"{index:011d}",
                "jpName": f"child-{index}",
            }
        data = {
            "meta": {"currentPage": 1, "totalItems": total, "totalPages": 1},
            "contents": [row],
        }
        return data, discovery._now()

    def persist(_repo, rows, *, generation, refresh_mask):
        persisted.extend(rows)
        return {"inserted": len(rows), "updated": 0, "refresh_reopened": 0}

    monkeypatch.setattr(discovery, "FixedRateDbdClient", _DirectClient)
    monkeypatch.setattr(discovery, "_direct_page", direct_page)
    monkeypatch.setattr(discovery, "_persist_direct_batch", persist)

    result = await discovery.run(
        ["010"],
        generation=2,
        repository=object(),
        verbose=False,
    )

    assert set(child_calls) == set(discovery.JP_TYPES)
    assert result.buckets_processed == 1 + len(discovery.JP_TYPES)
    assert result.pages_fetched == 1 + len(discovery.JP_TYPES)
    assert result.invalid_rows == 0
    assert len(persisted) == len(discovery.JP_TYPES)



@pytest.mark.asyncio
async def test_bounded_direct_discovery_reports_incomplete_coverage(monkeypatch) -> None:
    persisted = []

    async def direct_page(_client, filters, page_no):
        assert page_no == 1
        data = {
            "meta": {"currentPage": 1, "totalItems": 1, "totalPages": 1},
            "contents": [
                {
                    "jpTypeCode": "7",
                    "jpNo": "0107556000051",
                    "jpName": filters.keyword,
                }
            ],
        }
        return data, discovery._now()

    def persist(_repo, rows, *, generation, refresh_mask):
        persisted.extend(rows)
        return {"inserted": len(rows), "updated": 0, "refresh_reopened": 0}

    monkeypatch.setattr(discovery, "FixedRateDbdClient", _DirectClient)
    monkeypatch.setattr(discovery, "_direct_page", direct_page)
    monkeypatch.setattr(discovery, "_persist_direct_batch", persist)

    with pytest.raises(ContractError, match="coverage is incomplete"):
        await discovery.run(
            ["010", "011"],
            generation=2,
            max_buckets=1,
            repository=object(),
            verbose=False,
        )

    assert len(persisted) == 1

def test_fully_partitioned_search_has_no_silent_fallback_split() -> None:
    filters = SearchFilters(
        keyword="010",
        jp_types=["7"],
        pv_codes=["10"],
        business_sizes=["S"],
    )
    assert discovery._children_for(filters, ["10"]) == []


@pytest.mark.asyncio
async def test_v06_direct_discovery_rejects_legacy_distributed_mode() -> None:
    with pytest.raises(ValueError, match="legacy MySQL discovery mode"):
        await discovery.run(
            ["010"],
            generation=2,
            distributed=True,
            repository=object(),
            verbose=False,
        )
