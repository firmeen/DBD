from decimal import Decimal

import pytest

from dbd.core.exceptions import IdentityConflict
from dbd.deep.catalog import (
    ACTIVE_ENDPOINT_MASK,
    BASE_REQUIRED_MASK,
    ENDPOINT_BITS,
    FINANCIAL_MASK,
    EndpointSpec,
    deep_status,
    latest_fiscal_year,
    required_mask,
)
from dbd.deep.collector import DeepCollector, _json_payload


def _spec(key: str) -> EndpointSpec:
    return EndpointSpec(
        key=key,
        sequence_no=10,
        bit=ENDPOINT_BITS[key],
        method="GET",
        path_template="/api/{jp_type_code}/{jp_no}",
        referer_template=None,
        destination_column={
            "profile": "profile_data",
            "financial_submissions": "financial_submissions_data",
        }[key],
        request_schema={},
        response_schema={},
        required_rule={"type": "always"},
    )


def test_active_scope_masks_and_latest_fiscal_year():
    assert ACTIVE_ENDPOINT_MASK == 127
    assert BASE_REQUIRED_MASK == 113
    assert FINANCIAL_MASK == 14

    assert latest_fiscal_year(None) is None
    assert latest_fiscal_year("") is None
    assert latest_fiscal_year("2567 2568") == "2568"
    assert latest_fiscal_year("2568 2567") == "2568"
    assert latest_fiscal_year("  2568\t2567  ") == "2568"

    assert required_mask(None) == BASE_REQUIRED_MASK
    assert required_mask("") == BASE_REQUIRED_MASK
    assert required_mask("2567 2568") == ACTIVE_ENDPOINT_MASK


def test_financial_request_uses_one_latest_submitted_year():
    path, params = DeepCollector._request_for(
        _spec("financial_submissions"),
        {
            "jp_type_code": "7",
            "jp_no": "0107556000051",
            "submit_fin_year": "2567 2568",
        },
    )

    assert path == "/api/7/0107556000051"
    assert params == {"fiscalYear": "2568"}


def test_profile_distinguishes_absent_from_explicit_null_fiscal_year():
    company = {
        "jp_type_code": "7",
        "jp_no": "0107556000051",
    }

    outcome, fiscal_year, present = DeepCollector._validate_payload(
        _spec("profile"),
        company,
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
        },
    )
    assert (outcome, fiscal_year, present) == ("done", None, False)

    outcome, fiscal_year, present = DeepCollector._validate_payload(
        _spec("profile"),
        company,
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "submitFinYear": None,
        },
    )
    assert (outcome, fiscal_year, present) == ("done", None, True)


def test_profile_identity_mismatch_is_terminal_identity_conflict():
    with pytest.raises(IdentityConflict):
        DeepCollector._validate_payload(
            _spec("profile"),
            {
                "jp_type_code": "7",
                "jp_no": "0107556000051",
            },
            {
                "jpNo": "0107556000052",
                "jpTypeCode": "7",
            },
        )


def test_deep_status_resolves_only_required_active_bits():
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, 0) == "PENDING"
    assert deep_status(BASE_REQUIRED_MASK, ENDPOINT_BITS["profile"], 0, 0) == "COLLECTING"
    assert deep_status(BASE_REQUIRED_MASK, BASE_REQUIRED_MASK, 0, 0) == "COMPLETE"
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, BASE_REQUIRED_MASK) == "FAILED"

    one_failed = ENDPOINT_BITS["committees"]
    done = BASE_REQUIRED_MASK & ~one_failed
    assert deep_status(BASE_REQUIRED_MASK, done, 0, one_failed) == "PARTIAL"


def test_deep_payload_preserves_exact_json_numeric_type():
    value = Decimal("12345678901234567890.123456789")
    encoded = _json_payload({"amount": value})

    assert encoded == '{"amount":12345678901234567890.123456789}'
    assert '"12345678901234567890.123456789"' not in encoded
