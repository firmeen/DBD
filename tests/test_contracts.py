from decimal import Decimal

import pytest

from dbd.client.endpoints.search import parse_search_response
from dbd.contracts.company import (
    normalize,
    normalize_committees,
    normalize_nation_history,
    normalize_nations,
)
from dbd.contracts.identity import parse_code
from dbd.core.exceptions import ContractError
from dbd.runtime.worker import code_batches


@pytest.mark.parametrize("bad", [123, "", "123", None])
def test_identity_never_coerces_or_loses_leading_zero(bad):
    with pytest.raises(ContractError):
        normalize({"jpNo": bad, "jpTypeCode": "7"})


def test_type_letter_and_full_url():
    assert parse_code("A0107556000051").jp_type_code == "A"
    assert (
        parse_code("https://datawarehouse.dbd.go.th/company/profile/70107556000051").jp_no == "0107556000051"
    )
    with pytest.raises(ContractError):
        parse_code("17010755600005199")


@pytest.mark.parametrize("payload", [["bad"], [{"unexpected": "shape"}], {"contents": []}])
def test_malformed_collection_cannot_clear_projection(payload):
    with pytest.raises(ContractError):
        normalize_committees(payload)


def test_empty_collection_remains_valid():
    assert normalize_committees([]) == []


def test_nationality_summary_is_not_a_malformed_country():
    data = [{'ntCode':'TH'}, {'ntCode':None,'nationality':{'ntCode':'WORLD2'}}]
    assert len(normalize_nations(data)) == 1
    with pytest.raises(ContractError):
        normalize_nations([{'ntCode':None,'nationality':{'ntCode':'UNKNOWN'}}])


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
