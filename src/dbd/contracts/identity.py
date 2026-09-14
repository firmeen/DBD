from __future__ import annotations

import re
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..core.exceptions import ContractError


class CompanyIdentity(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)
    jp_no: str = Field(pattern=r"^[0-9]{13}$")
    jp_type_code: str = Field(pattern=r"^[0-9A-Z]$")

    @property
    def code(self) -> str:
        return self.jp_type_code + self.jp_no


def identity(jp_no: object, jp_type_code: object) -> CompanyIdentity:
    try:
        return CompanyIdentity(jp_no=jp_no, jp_type_code=jp_type_code)
    except ValidationError:
        raise ContractError("Expected a 13-digit string jpNo and one-character jpTypeCode") from None


def parse_code(value: str) -> CompanyIdentity:
    code = value.strip()
    if "://" in code:
        url = urlsplit(code)
        if url.hostname != "datawarehouse.dbd.go.th" or not url.path.startswith("/company/profile/"):
            raise ContractError("Expected a DBD company profile URL")
        code = url.path.removeprefix("/company/profile/").rstrip("/")
    if not re.fullmatch(r"[0-9A-Z][0-9]{13}", code):
        raise ContractError("Company code must be one type character followed by 13 digits")
    return identity(code[1:], code[0])
