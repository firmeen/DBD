from decimal import Decimal

import pytest

from dbd.client.endpoints.search import parse_search_response
from dbd.contracts.company import (
    normalize,
    normalize_committees,
    normalize_nation_history,
    normalize_nations,
)
from dbd.contracts.financial import (
    normalize_submissions,
    submission_collision_evidence,
    validate_deep_submissions,
)
from dbd.contracts.identity import parse_code
from dbd.contracts.strict import normalize_collection, normalize_industry_benchmark
from dbd.core.exceptions import ContractError
from dbd.runtime.worker import code_batches


@pytest.mark.parametrize("bad", [123, "", "123", None])
def test_identity_never_coerces_or_loses_leading_zero(bad):
    with pytest.raises(ContractError):
        normalize({"jpNo": bad, "jpTypeCode": "7"})


def test_type_letter_and_full_url():
    assert parse_code("A0107556000051").jp_type_code == "A"
    assert (
        parse_code("https://datawarehouse.dbd.go.th/company/profile/70107556000051").jp_no
        == "0107556000051"
    )
    with pytest.raises(ContractError):
        parse_code("17010755600005199")


def test_observed_type_a_short_identities_are_preserved_exactly():
    for jp_no in ("12345678901", "123456789012", "1234567890123"):
        parsed = parse_code("A" + jp_no)
        assert parsed.jp_type_code == "A"
        assert parsed.jp_no == jp_no
        assert parsed.code == "A" + jp_no

    # The exception is evidence-based and type-specific; ordinary juristic
    # types remain strict 13-digit identifiers.
    with pytest.raises(ContractError):
        parse_code("7" + "1" * 12)


@pytest.mark.parametrize("payload", [["bad"], [{"unexpected": "shape"}], {"contents": []}])
def test_malformed_collection_cannot_clear_projection(payload):
    with pytest.raises(ContractError):
        normalize_committees(payload)


def test_empty_collection_remains_valid():
    assert normalize_committees([]) == []


def test_strict_root_contracts_distinguish_empty_from_malformed():
    assert normalize_collection(
        "transformations",
        {"transformFroms": [], "transformTos": [], "mergerFroms": [], "mergerTos": []},
    ) == []
    with pytest.raises(ContractError):
        normalize_collection("transformations", {"renamedCollections": []})

    assert normalize_collection(
        "nation_history", {"customChart": {"shareHistTHDps": [], "shareHistOTDps": []}}
    ) == []
    with pytest.raises(ContractError):
        normalize_collection("nation_history", {"customChart": {"renamed": []}})

    assert normalize_industry_benchmark(None) is None
    with pytest.raises(ContractError):
        normalize_industry_benchmark({})
    with pytest.raises(ContractError):
        normalize_industry_benchmark({"submitObjCode": "65110"})


def test_nationality_summary_is_not_a_malformed_country():
    data = [{"ntCode": "TH"}, {"ntCode": None, "nationality": {"ntCode": "WORLD2"}}]
    assert len(normalize_nations(data)) == 1
    with pytest.raises(ContractError):
        normalize_nations([{"ntCode": None, "nationality": {"ntCode": "UNKNOWN"}}])


def test_dates_nested_shape_and_precision():
    base = {"jpNo": "0107556000051", "jpTypeCode": "7"}
    with pytest.raises(ContractError):
        normalize(base | {"regDate": [2026, 2, 31]})
    with pytest.raises(ContractError):
        normalize(base | {"jpStatus": ["active"]})
    p = normalize(base | {"capAmt": "9999999999999999.99", "newField": {"a": 1}})
    assert p.cap_amt == Decimal("9999999999999999.99")
    assert p.facts["newField"] == {"a": 1}


def test_zero_percentage_is_not_replaced():
    rows = normalize_nation_history(
        {"customChart": {"shareHistTHDps": [{"label": "2568", "sharePctVol": "0", "y": 8}]}}
    )
    assert rows[0]["share_pct"] == 0


def test_missing_metadata_is_not_an_empty_search():
    with pytest.raises(ContractError):
        parse_search_response({"error": "schema changed"})


def test_code_import_is_streamed_and_strict(tmp_path):
    p = tmp_path / "codes.txt"
    p.write_text("\n".join("7" + str(i).zfill(13) for i in range(1001)))
    iterator = code_batches(str(p), 100)
    assert len(next(iterator)) == 100
    assert sum(map(len, iterator)) == 901
    p.write_text("7010755600005199")
    with pytest.raises(ContractError):
        list(code_batches(str(p), 100))


def test_default_parts_cover_tabs_one_and_two_only():
    """Tabs 3-4 stay implemented but are not collected by default."""
    from dbd.runtime.resources import DEFAULT_PARTS, PARTS, endpoints

    default = endpoints(list(DEFAULT_PARTS))

    assert default == (
        "profile",
        "committees",
        "committee_signs",
        "transformations",
        "financial_submissions",
        "financial_statements",
        "industry_benchmark",
    )

    dropped = {"nations", "nation_history", "name_history", "capital_history"}
    assert dropped.isdisjoint(default)
    assert set(endpoints(["nationality", "history"])) == dropped
    assert dropped <= set(endpoints(list(PARTS)))



def test_financial_submission_exact_duplicates_are_deduplicated_losslessly():
    payload = [
        {
            "submitYear": "2566",
            "acDate": "3112",
            "submitDate": [2024, 5, 31],
            "acCode": "A",
            "auCode": "U",
            "statusCode": "1",
            "updateDate": [2024, 6, 1],
        },
        {
            "submitYear": "2566",
            "acDate": "3112",
            "submitDate": [2024, 5, 31],
            "acCode": "A",
            "auCode": "U",
            "statusCode": "1",
            "updateDate": [2024, 6, 1],
        },
    ]

    rows = normalize_submissions(payload)

    assert len(rows) == 1
    assert rows[0]["submit_year"] == "2566"
    assert rows[0]["status_code"] == "1"


def test_financial_submission_conflicting_same_year_requires_revision_identity():
    payload = [
        {"submitYear": "2566", "statusCode": "1"},
        {"submitYear": "2566", "statusCode": "2"},
    ]

    with pytest.raises(ContractError, match="source revision identity is required"):
        normalize_submissions(payload)



def test_financial_submission_different_raw_rows_are_not_collapsed():
    payload = [
        {"submitYear": "2566", "statusCode": "1", "sourceRevision": "A"},
        {"submitYear": "2566", "statusCode": "1", "sourceRevision": "B"},
    ]

    evidence = submission_collision_evidence(payload)
    assert evidence["exact_duplicate_years"] == []
    assert evidence["conflicting_years"] == ["2566"]

    with pytest.raises(ContractError, match="source revision identity is required"):
        normalize_submissions(payload)


def test_deep_submission_accepts_distinct_same_year_records_without_losing_revisions():
    payload = [
        {
            "submitYear": "2568",
            "statusCode": "1",
            "submitDate": [2025, 5, 31],
            "updateDate": [2025, 6, 1],
            "sourceOnlyField": {"revision": "A"},
        },
        {
            "submitYear": "2568",
            "statusCode": "2",
            "submitDate": [2025, 7, 1],
            "updateDate": [2025, 7, 2],
            "sourceOnlyField": {"revision": "B"},
        },
        {
            "submitYear": "2567",
            "statusCode": "1",
            "submitDate": None,
            "updateDate": None,
        },
    ]

    deep = validate_deep_submissions(payload)
    assert len(deep) == 3
    assert [row["submit_year"] for row in deep] == ["2568", "2568", "2567"]
    assert [row["status_code"] for row in deep] == ["1", "2", "1"]
    assert payload[0]["sourceOnlyField"] == {"revision": "A"}
    assert payload[1]["sourceOnlyField"] == {"revision": "B"}

    # The relational legacy projection deliberately remains strict.
    with pytest.raises(ContractError, match="source revision identity"):
        normalize_submissions(payload)


def test_deep_submission_keeps_unknown_only_differences_but_counts_exact_duplicates_once():
    first = {"submitYear": "2568", "statusCode": "1", "revisionHint": "A"}
    second = {"submitYear": "2568", "statusCode": "1", "revisionHint": "B"}
    payload = [first, dict(first), second]
    deep = validate_deep_submissions(payload)

    assert len(payload) == 3  # Deep JSON persistence keeps the full source array.
    assert len(deep) == 2  # Derived serving count excludes exact duplicates only.
    assert [r["submit_year"] for r in deep] == ["2568", "2568"]
    assert [r["status_code"] for r in deep] == ["1", "1"]


@pytest.mark.parametrize(
    "payload",
    [
        {"submitYear": "2568"},
        ["invalid"],
        [{"statusCode": "1"}],
        [{"submitYear": ""}],
        [{"submitYear": "2568", "submitDate": "2025-05-31"}],
        [{"submitYear": "2568", "updateDate": [2025, 13, 1]}],
    ],
)
def test_deep_submission_still_rejects_invalid_source_contract(payload):
    with pytest.raises(ContractError):
        validate_deep_submissions(payload)


def test_deep_submission_empty_array_remains_valid():
    assert validate_deep_submissions([]) == []
