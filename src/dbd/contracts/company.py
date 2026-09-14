from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from ..core.exceptions import ContractError
from .common import as_list, to_date

# Top-level scalar fields promoted to dedicated `companies` columns because
# they're used for identity, search, or business calculations. Everything
# else observed in the response still survives in raw_responses (full JSON)
# and gets a generic row in company_facts, so a field DBD adds/renames later
# doesn't get silently dropped.
_REQUIRED_FIELDS = ("jpNo", "jpTypeCode")

_PROMOTED_FIELDS = (
    "jpNo",
    "jpTypeCode",
    "jpName",
    "jpNameE",
    "jpSearchName",
    "jpStatCode",
    "address",
    "addressE",
    "ampurCode",
    "tumbonCode",
    "pvCode",
    "zipCode",
    "phoneNo",
    "email",
    "webSite1",
    "webSite2",
    "webSite3",
    "webSite4",
    "regDate",
    "statChangeDate",
    "updateDate",
    "capAmt",
    "paidAmt",
    "shareQty",
    "shareVol",
    "businessSizeCode",
    "setupObjCode",
    "setupObjNameKeyin",
    "totalAsset",
    "totalEquity",
    "totalIncome",
    "netProfit",
    "submitFinYear",
)


@dataclass
class CompanyProfile:
    jp_no: str
    jp_type_code: str
    jp_name: str | None = None
    jp_name_e: str | None = None
    jp_search_name: str | None = None
    jp_stat_code: str | None = None
    jp_stat_desc: str | None = None
    jp_type_desc: str | None = None
    address: str | None = None
    address_e: str | None = None
    ampur_code: str | None = None
    tumbon_code: str | None = None
    pv_code: str | None = None
    pv_desc: str | None = None
    zip_code: str | None = None
    phone_no: str | None = None
    email: str | None = None
    website_1: str | None = None
    website_2: str | None = None
    website_3: str | None = None
    website_4: str | None = None
    reg_date: date | None = None
    stat_change_date: date | None = None
    update_date: date | None = None
    cap_amt: float | None = None
    paid_amt: float | None = None
    share_qty: float | None = None
    share_vol: float | None = None
    business_size_code: str | None = None
    setup_obj_code: str | None = None
    setup_obj_name_keyin: str | None = None
    total_asset: float | None = None
    total_equity: float | None = None
    total_income: float | None = None
    net_profit: float | None = None
    submit_fin_year: str | None = None
    facts: dict = field(default_factory=dict)


_to_date = to_date


def validate(data: object) -> dict:
    """Fail loudly if DBD's response is missing what this codebase depends on,
    instead of silently writing a half-empty row to MySQL.
    """
    if not isinstance(data, dict):
        raise ContractError(f"Expected a JSON object for company profile, got {type(data).__name__}")

    missing = [f for f in _REQUIRED_FIELDS if not data.get(f)]
    if missing:
        raise ContractError(f"Company profile response is missing required field(s): {missing}")

    return data


def normalize(data: dict) -> CompanyProfile:
    """Split the response into (a) the fields we've promoted to typed columns
    and (b) everything else, kept as a generic key/value dict for company_facts.
    """
    data = validate(data)

    location_province = data.get("locationProvince") or {}
    jp_status = data.get("jpStatus") or {}
    jp_type = data.get("jpType") or {}

    profile = CompanyProfile(
        jp_no=data["jpNo"],
        jp_type_code=data["jpTypeCode"],
        jp_name=data.get("jpName"),
        jp_name_e=data.get("jpNameE"),
        jp_search_name=data.get("jpSearchName"),
        jp_stat_code=data.get("jpStatCode"),
        jp_stat_desc=jp_status.get("jpStatDesc"),
        jp_type_desc=jp_type.get("jpTypeDesc"),
        address=data.get("address"),
        address_e=data.get("addressE"),
        ampur_code=data.get("ampurCode"),
        tumbon_code=data.get("tumbonCode"),
        pv_code=data.get("pvCode"),
        pv_desc=location_province.get("pvDesc"),
        zip_code=data.get("zipCode"),
        phone_no=data.get("phoneNo"),
        email=data.get("email"),
        website_1=data.get("webSite1"),
        website_2=data.get("webSite2"),
        website_3=data.get("webSite3"),
        website_4=data.get("webSite4"),
        reg_date=_to_date(data.get("regDate")),
        stat_change_date=_to_date(data.get("statChangeDate")),
        update_date=_to_date(data.get("updateDate")),
        cap_amt=data.get("capAmt"),
        paid_amt=data.get("paidAmt"),
        share_qty=data.get("shareQty"),
        share_vol=data.get("shareVol"),
        business_size_code=data.get("businessSizeCode"),
        setup_obj_code=data.get("setupObjCode"),
        setup_obj_name_keyin=data.get("setupObjNameKeyin"),
        total_asset=data.get("totalAsset"),
        total_equity=data.get("totalEquity"),
        total_income=data.get("totalIncome"),
        net_profit=data.get("netProfit"),
        submit_fin_year=data.get("submitFinYear"),
    )

    profile.facts = {
        key: value
        for key, value in data.items()
        if key not in _PROMOTED_FIELDS and not isinstance(value, (dict, list))
    }

    return profile


def _blank(value: object) -> object:
    """DBD uses "" rather than null in several text fields."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


def normalize_committees(data: object) -> list[dict]:
    return [
        {
            "cmt_seq": row.get("cmtSeq"),
            "cmt_type_code": _blank(row.get("cmtTypeCode")),
            "cmt_no": _blank(row.get("cmtNo")),
            "title_code": _blank(row.get("titleCode")),
            "title_name": _blank(row.get("titleName")),
            "title_name_e": _blank(row.get("titleNameE")),
            "first_name": _blank(row.get("firstName")),
            "middle_name": _blank(row.get("middleName")),
            "last_name": _blank(row.get("lastName")),
            "first_name_e": _blank(row.get("firstNameE")),
            "middle_name_e": _blank(row.get("middleNameE")),
            "last_name_e": _blank(row.get("lastNameE")),
            "nt_code": _blank(row.get("ntCode")),
            "invest_amt": row.get("investAmt"),
            "invest_type": _blank(row.get("investType")),
            "invest_type_e": _blank(row.get("investTypeE")),
            "address": _blank(row.get("address")),
            "pv_code": _blank(row.get("pvCode")),
            "ampur_code": _blank(row.get("ampurCode")),
            "tumbon_code": _blank(row.get("tumbonCode")),
            "zip_code": _blank(row.get("zipCode")),
            "phone_no": _blank(row.get("phoneNo")),
            "update_date": to_date(row.get("updateDate")),
        }
        for row in as_list(data)
        if row.get("cmtSeq") is not None
    ]


def normalize_committee_signs(data: object) -> list[dict]:
    return [
        {
            "sign_seq": row.get("signSeq"),
            "sign_description": _blank(row.get("signDescription")),
            "sign_description_e": _blank(row.get("signDescriptionE")),
        }
        for row in as_list(data)
        if row.get("signSeq") is not None
    ]


def normalize_nations(data: object) -> list[dict]:
    rows = []
    for row in as_list(data):
        nt_code = row.get("ntCode")
        if not nt_code:
            continue
        nationality = row.get("nationality") or {}
        rows.append(
            {
                "nt_code": nt_code,
                "country_fullname": _blank(nationality.get("countryFullname")),
                "country_fullname_e": _blank(nationality.get("countryFullnameE")),
                "share_amt": row.get("shareAmt"),
                "share_qty": row.get("shareQty"),
                "share_vol": row.get("shareVol"),
                "share_pct_qty": row.get("sharePctQty"),
                "share_pct_vol": row.get("sharePctVol"),
                "total_nation": row.get("totalNation"),
                "update_date": to_date(row.get("updateDate")),
            }
        )
    return rows


def normalize_name_history(data: object) -> list[dict]:
    return [
        {
            "seq_no": row.get("seqNo"),
            "change_date": to_date(row.get("changeDate")),
            "jp_new_name": _blank(row.get("jpNewName")),
            "jp_new_name_e": _blank(row.get("jpNewNameE")),
            "seq_desc": _blank(row.get("seqDesc")),
            "seq_desc_e": _blank(row.get("seqDescE")),
            "update_date": to_date(row.get("updateDate")),
        }
        for row in as_list(data)
        if row.get("seqNo") is not None
    ]


def normalize_capital_history(data: object) -> list[dict]:
    return [
        {
            "seq_no": row.get("seqNo"),
            "change_date": to_date(row.get("changeDate")),
            "inc_dec_flag": _blank(row.get("incDecFlag")),
            "inc_dec_amt": row.get("incDecAmt"),
            "previous_cap_amt": row.get("previousCapAmt"),
            "new_cap_amt": row.get("newCapAmt"),
            "seq_desc": _blank(row.get("seqDesc")),
            "seq_desc_e": _blank(row.get("seqDescE")),
            "update_date": to_date(row.get("updateDate")),
        }
        for row in as_list(data)
        if row.get("seqNo") is not None
    ]


COLLECTION_NORMALIZERS = {
    "committees": normalize_committees,
    "committee_signs": normalize_committee_signs,
    "nations": normalize_nations,
    "name_history": normalize_name_history,
    "capital_history": normalize_capital_history,
}
