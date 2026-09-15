from __future__ import annotations

import re
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from ..core.exceptions import ContractError


class CompanyIdentity(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    # DBD search observations include type A identifiers with 11/12 digits.
    # Keep the exact upstream string: never left-pad or coerce an identity.
    jp_no: str = Field(pattern=r"^[0-9]{11,13}$")
    jp_type_code: str = Field(pattern=r"^[0-9A-Z]$")

    @model_validator(mode="after")
    def validate_length_for_type(self):
        if self.jp_type_code != "A" and len(self.jp_no) != 13:
            raise ValueError("Only type A may use an 11- or 12-digit jpNo")
        return self

    @property
    def code(self) -> str:
        return self.jp_type_code + self.jp_no


def identity(jp_no: object, jp_type_code: object) -> CompanyIdentity:
    try:
        return CompanyIdentity(jp_no=jp_no, jp_type_code=jp_type_code)
    except ValidationError:
        raise ContractError(
            "Expected an exact DBD jpNo string (13 digits, or observed 11-13 digits for type A) "
            "and one-character jpTypeCode"
        ) from None


def parse_code(value: str) -> CompanyIdentity:
    code = value.strip()
    if "://" in code:
        url = urlsplit(code)
        if url.hostname != "datawarehouse.dbd.go.th" or not url.path.startswith("/company/profile/"):
            raise ContractError("Expected a DBD company profile URL")
        code = url.path.removeprefix("/company/profile/").rstrip("/")
    match = re.fullmatch(r"([0-9A-Z])([0-9]{11,13})", code)
    if not match:
        raise ContractError("Company code must be one type character followed by an accepted jpNo")
    return identity(match.group(2), match.group(1))
