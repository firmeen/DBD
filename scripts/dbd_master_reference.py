"""
================================================================================
DBD DATAWAREHOUSE - MASTER REFERENCE / AI HANDOFF GUIDE
================================================================================

ไฟล์นี้ตั้งใจทำหน้าที่ 2 อย่างพร้อมกัน:

1) เป็น Python reference implementation ที่สามารถเอาไปทดลองกับ DBD DataWarehouse ได้จริง
2) เป็น "คู่มือในรูปแบบ source code" สำหรับให้ AI / Developer คนถัดไปอ่านแล้วเข้าใจว่า
   ระบบที่ reverse-engineer มาแล้วทำงานอย่างไร, อะไรพิสูจน์แล้ว, อะไรยังไม่ควร assume

--------------------------------------------------------------------------------
IMPORTANT STATUS / PROVEN FACTS
--------------------------------------------------------------------------------

[PROVEN] Public website:
    https://datawarehouse.dbd.go.th/

[PROVEN] Public API base:
    https://datawarehouse.dbd.go.th/api

[PROVEN] Session:
    - requests ธรรมดาเคยเจอ 403
    - curl_cffi.requests.Session(impersonate="chrome") ใช้งานได้
    - ควรเปิด HOME/PAGE ก่อนเพื่อให้ session cookies ถูกสร้าง

[PROVEN] Anonymous auth:
    POST /api/refresh

    response มี:
        idToken

    JWT payload มี:
        provider = ANONYMOUS
        encKey   = key material สำหรับ decrypt API response

[PROVEN] API response encryption:
    envelope:
        {
            "kid": ...,
            "salt": "...",
            "iv": "...",
            "ct": "..."
        }

    headers:
        x-encrypted: true

    AAD:
        b"bdw|v{kid}|{PATH}"

    IMPORTANT:
        PATH = pathname เท่านั้น
        เช่น:
            /api/v1/company-profiles/infos

        ห้ามใส่ query string ลง AAD

    Key derivation:
        HKDF-SHA256
        IKM  = JWT encKey
        salt = envelope.salt
        info = AAD
        len  = 32 bytes

    Cipher:
        AES-GCM

    decrypted bytes:
        มักเป็น gzip
        แต่ code รองรับ zlib / raw deflate ด้วย

[PROVEN] Company exact profile:
    GET /api/v1/company-profiles/info/{jpTypeCode}/{jpNo}

    example:
        company code: 70107556000051

        jpTypeCode = "7"
        jpNo       = "0107556000051"

[PROVEN] Company search/list:
    POST /api/v1/company-profiles/infos

[PROVEN] NORMAL SEARCH:
    payload ตัวอย่าง:
        {
            "keyword": "64202",
            "type": "",
            "sortBy": "jpName",
            "currentPage": 1
        }

    type="" หมายถึง normal search
    keyword อาจ match ชื่อ / เลขทะเบียน / text อื่น
    จึงไม่ใช่ canonical business-type enumeration

[PROVEN] BUSINESS / TSIC SEARCH:
    payload:
        {
            "keyword": "64202",
            "type": "biz",
            "sortBy": "jpName",
            "currentPage": 1
        }

    ผลทดสอบจริง:
        totalItems   = 8519
        totalPages   = 852
        itemsPerPage = 10

    ตัวอย่าง records ที่ตรวจ:
        submitObjCode == "64202"

    ดังนั้น:
        type="biz"
        keyword="<TSIC>"

    คือวิธีค้นนิติบุคคลตามประเภทธุรกิจจริง

[PROVEN] Search limits:
    keyword minimum length = 3

    itemsPerPage ถูก backend ล็อก:
        10

    แม้ส่ง:
        50
        100
        500
        1000

    search API ก็ยังคืน:
        itemsPerPage = 10

    page:
        1..1000    = allowed
        1001+      = rejected

    error:
        COMPANY_SEARCH_PAGE_LIMIT_EXCEEDED

    ดังนั้นหนึ่ง query ดึงได้สูงสุด:
        1000 pages * 10 rows = 10,000 rows

[PROVEN] Numeric registration prefix:
    ไม่สามารถใช้เป็น lossless partition ได้

    ตัวอย่าง:
        count("010") != SUM(count("0100") ... count("0109"))

    ดังนั้น:
        ห้ามใช้ numeric-prefix tree เป็น canonical all-company discovery

[PROVEN] jpType partition:
    ในการทดสอบ keyword "010"

    parent:
        397,885

    sum ของ:
        type 0
        type 2
        type 3
        type 5
        type 7
        type 8
        type 9
        type A

    = 397,885

    จึงเป็น partition dimension ที่ดีจากผลทดสอบนั้น

[PROVEN FROM FRONTEND SOURCE] /juristic/searchInfo:
    route receives:

        ?keyword=<value>&v=<mode>

    แล้วสร้าง:

        {
            keyword: route.query.keyword,
            type: route.query.v,
            sortBy: "jpName",
            currentPage: 1
        }

    BizType page ใช้:

        /juristic/searchInfo?keyword=<objCode>&v=biz

[PROVEN FROM FRONTEND SOURCE] Current advanced search filters include:

    pvCodeList
    jpStatusList
    jpTypeList
    businessSizeList

    capAmtMin
    capAmtMax

    totalIncomeMin
    totalIncomeMax

    netProfitMin
    netProfitMax

    totalAssetMin
    totalAssetMax

[IMPORTANT]
    การ enumerate บริษัท ~1 ล้านรายผ่าน TSIC เป็นไปได้ในเชิง architecture
    แต่ห้าม claim ว่า TSIC union = บริษัททั้งหมด 100%
    จนกว่าจะ:

        1. enumerate TSIC ทุก code
        2. dedupe ด้วย (jpTypeCode, jpNo)
        3. เทียบ unique total กับ authoritative DBD total
        4. ตรวจ distribution / missing populations

    เพราะอาจมีนิติบุคคลบางประเภทที่:
        - ไม่มี submitObjCode
        - ไม่มีงบล่าสุด
        - มี classification พิเศษ
        - ไม่อยู่ใน TSIC listing แบบเดียวกัน

--------------------------------------------------------------------------------
MASTER DISCOVERY IDEA
--------------------------------------------------------------------------------

Business Types / TSIC
        |
        v
POST /company-profiles/infos
type="biz"
keyword=TSIC
        |
        +---- totalItems <= 10,000
        |          |
        |          +--> page 1..N
        |
        +---- totalItems > 10,000
                   |
                   +--> partition using filters
                             |
                             +--> province
                             +--> jpType
                             +--> status
                             +--> business size
                             +--> possibly finer valid filters
                                      |
                                      v
                              every leaf <= 10,000

EVERY PARTITION MUST BE VALIDATED:

    SUM(child.totalItems) == parent.totalItems

ก่อนถือว่า partition นั้น lossless

--------------------------------------------------------------------------------
INSTALL
--------------------------------------------------------------------------------

pip install curl-cffi cryptography

--------------------------------------------------------------------------------
THIS SCRIPT
--------------------------------------------------------------------------------

- ไม่เขียน output file
- ไม่เก็บ token
- ไม่เก็บ cookie
- ไม่ print encKey
- ไม่เก็บ Authorization header
- ใช้เพื่อ reference / smoke-test / foundation สำหรับ production client

Production ต่อไปควรแยกเป็น:

    client/session/auth/crypto/search/discovery/storage/runtime

แต่ไฟล์นี้จงใจรวมไว้ไฟล์เดียวเพื่อเป็น MASTER REFERENCE
================================================================================
"""

from __future__ import annotations

import base64
import gzip
import json
import math
import sys
import time
import zlib

from dataclasses import dataclass
from typing import Any, Iterable, Optional

from curl_cffi import requests

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


# =============================================================================
# GLOBAL CONSTANTS
# =============================================================================

BASE_URL = "https://datawarehouse.dbd.go.th"

HOME_URL = f"{BASE_URL}/"

REFRESH_PATH = "/api/refresh"

COMMON_PARAMS_PATH = "/api/v1/commons/params"

COMMON_BUSINESS_TYPES_PATH = "/api/v1/commons/business-types"

COMMON_PROVINCES_PATH = "/api/v1/commons/provinces"

SEARCH_PATH = "/api/v1/company-profiles/infos"

COMPANY_INFO_PATH_TEMPLATE = (
    "/api/v1/company-profiles/info/{jp_type_code}/{jp_no}"
)

SEARCH_PAGE = (
    f"{BASE_URL}/juristic/searchInfo"
)

# Backend / frontend hard limit ที่พิสูจน์แล้ว
MAX_SEARCH_PAGE = 1000

# Search API ล็อกไว้ที่ 10 records/page
SEARCH_PAGE_SIZE = 10

# หนึ่ง query จึงเข้าถึงได้สูงสุด 10,000 rows
MAX_ROWS_PER_SEARCH_BUCKET = (
    MAX_SEARCH_PAGE
    * SEARCH_PAGE_SIZE
)

TIMEOUT_SECONDS = 30

# เว้น request เพื่อไม่ยิง public API หนาแน่นเกินไป
DEFAULT_REQUEST_DELAY = 1.0

# jpType ที่เคยเห็นจาก frontend / search
KNOWN_JP_TYPES = [
    "0",
    "2",
    "3",
    "5",
    "7",
    "8",
    "9",
    "A",
]

# status ที่ frontend รุ่นก่อนใช้เป็น default
KNOWN_JP_STATUS = [
    "1",
    "A",
    "B",
]

KNOWN_BUSINESS_SIZES = [
    "S",
    "M",
    "L",
]


# Windows PowerShell บางครั้งแสดงภาษาไทยผิด
# ทำให้ stdout เป็น UTF-8
try:
    sys.stdout.reconfigure(
        encoding="utf-8",
        errors="backslashreplace",
    )
except AttributeError:
    pass


# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================


class DBDException(Exception):
    """
    Base exception สำหรับ client นี้
    """


class DBDAuthError(DBDException):
    """
    ใช้เมื่อ anonymous auth ไม่สำเร็จ
    """


class DBDRateLimitError(DBDException):
    """
    API ตอบ HTTP 429
    """

    def __init__(
        self,
        message: str,
        retry_after: float = 5.0,
    ):
        super().__init__(message)

        self.retry_after = retry_after


class DBDPageLimitError(DBDException):
    """
    currentPage > 1000
    """


class DBDBucketTooLarge(DBDException):
    """
    Discovery bucket มี > 10,000 records
    จึงห้าม paginate ตรงๆ เพราะจะตกข้อมูล
    """

    def __init__(
        self,
        total_items: int,
        payload: dict[str, Any],
    ):
        super().__init__(
            f"Bucket contains {total_items:,} records; "
            f"maximum safely accessible per query is "
            f"{MAX_ROWS_PER_SEARCH_BUCKET:,}."
        )

        self.total_items = total_items
        self.payload = payload


# =============================================================================
# SMALL DATA OBJECTS
# =============================================================================


@dataclass
class SearchMeta:
    """
    Normalize meta จาก search response
    """

    current_page: int
    total_items: int
    total_pages: int
    items_per_page: int

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
    ) -> "SearchMeta":
        return cls(
            current_page=int(
                data.get(
                    "currentPage",
                    0,
                )
                or 0
            ),
            total_items=int(
                data.get(
                    "totalItems",
                    0,
                )
                or 0
            ),
            total_pages=int(
                data.get(
                    "totalPages",
                    0,
                )
                or 0
            ),
            items_per_page=int(
                data.get(
                    "itemsPerPage",
                    SEARCH_PAGE_SIZE,
                )
                or SEARCH_PAGE_SIZE
            ),
        )


@dataclass
class SearchResult:
    """
    Normalized company search result
    """

    meta: SearchMeta
    contents: list[dict[str, Any]]
    raw: dict[str, Any]


# =============================================================================
# BASE64 / JWT HELPERS
# =============================================================================


def b64url_decode(
    value: str,
) -> bytes:
    """
    JWT และ response envelope ใช้ base64/base64url style

    Python ต้องการ padding ให้ครบ multiple-of-4
    จึงเติม '=' ก่อน decode
    """

    value = value.strip()

    value += "=" * (
        (
            4
            - len(value) % 4
        )
        % 4
    )

    return base64.urlsafe_b64decode(
        value
    )


def decode_jwt_payload(
    token: str,
) -> dict[str, Any]:
    """
    อ่าน JWT payload แบบไม่ verify signature

    จุดประสงค์:
        - อ่าน exp
        - อ่าน provider
        - อ่าน encKey

    ไม่ได้ใช้ function นี้เพื่อ trust identity
    แต่ใช้ token จาก /api/refresh ที่ server เพิ่งออกให้
    """

    parts = token.split(".")

    if len(parts) < 2:
        raise DBDAuthError(
            "Invalid JWT format"
        )

    raw = b64url_decode(
        parts[1]
    )

    return json.loads(
        raw.decode(
            "utf-8"
        )
    )


# =============================================================================
# RESPONSE DECOMPRESSION
# =============================================================================


def decompress_payload(
    data: bytes,
) -> bytes:
    """
    หลัง AES-GCM decrypt แล้ว response ที่เห็นจริงเป็น compressed bytes

    observed:
        gzip magic:
            1f 8b

    แต่เพื่อ robustness รองรับ:
        gzip
        zlib
        raw deflate
        uncompressed
    """

    # -------------------------------------------------------------------------
    # GZIP
    # -------------------------------------------------------------------------

    if data.startswith(
        b"\x1f\x8b"
    ):
        return gzip.decompress(
            data
        )

    # -------------------------------------------------------------------------
    # Standard zlib
    # -------------------------------------------------------------------------

    try:
        return zlib.decompress(
            data
        )
    except zlib.error:
        pass

    # -------------------------------------------------------------------------
    # Raw DEFLATE
    # -------------------------------------------------------------------------

    try:
        return zlib.decompress(
            data,
            -zlib.MAX_WBITS,
        )
    except zlib.error:
        pass

    # -------------------------------------------------------------------------
    # No compression
    # -------------------------------------------------------------------------

    return data


# =============================================================================
# RESPONSE DECRYPTION
# =============================================================================


def decrypt_dbd_envelope(
    envelope: dict[str, Any],
    request_path: str,
    request_token: str,
) -> Any:
    """
    ถอด encrypted DBD API response

    CRITICAL RULE:
        ต้องใช้ token ตัวเดียวกับที่ใช้ยิง request นั้น

    เหตุผล:
        encKey อยู่ใน JWT
        ถ้า token refresh ระหว่าง request/response แล้วเอา token ใหม่มา decrypt
        AES-GCM จะ fail

    AAD:
        bdw|v{kid}|{requestPath}

    requestPath:
        มีเฉพาะ pathname
        ห้ามรวม query string

    ตัวอย่าง:
        CORRECT:
            /api/v1/commons/params

        WRONG:
            /api/v1/commons/params?foo=bar
    """

    claims = decode_jwt_payload(
        request_token
    )

    enc_key_value = claims.get(
        "encKey"
    )

    if not enc_key_value:
        raise DBDException(
            "JWT does not contain encKey"
        )

    # -------------------------------------------------------------------------
    # Input key material
    # -------------------------------------------------------------------------

    input_key_material = (
        b64url_decode(
            enc_key_value
        )
    )

    # -------------------------------------------------------------------------
    # Response fields
    # -------------------------------------------------------------------------

    kid = envelope[
        "kid"
    ]

    salt = b64url_decode(
        envelope[
            "salt"
        ]
    )

    iv = b64url_decode(
        envelope[
            "iv"
        ]
    )

    ciphertext = b64url_decode(
        envelope[
            "ct"
        ]
    )

    # -------------------------------------------------------------------------
    # AAD
    # -------------------------------------------------------------------------

    aad = (
        f"bdw|v{kid}|{request_path}"
    ).encode(
        "utf-8"
    )

    # -------------------------------------------------------------------------
    # HKDF
    # -------------------------------------------------------------------------

    aes_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=aad,
    ).derive(
        input_key_material
    )

    # -------------------------------------------------------------------------
    # AES-GCM
    # -------------------------------------------------------------------------

    plaintext = AESGCM(
        aes_key
    ).decrypt(
        iv,
        ciphertext,
        aad,
    )

    # -------------------------------------------------------------------------
    # Decompress
    # -------------------------------------------------------------------------

    plaintext = decompress_payload(
        plaintext
    )

    # -------------------------------------------------------------------------
    # JSON decode
    # -------------------------------------------------------------------------

    return json.loads(
        plaintext.decode(
            "utf-8"
        )
    )


# =============================================================================
# SEARCH PAYLOAD BUILDERS
# =============================================================================


def build_minimal_search_payload(
    keyword: str,
    *,
    search_type: str = "",
    current_page: int = 1,
    sort_by: str = "jpName",
) -> dict[str, Any]:
    """
    Payload แบบ minimal ที่พิสูจน์แล้วว่าใช้งานได้

    NORMAL:
        search_type=""

    BUSINESS TYPE / TSIC:
        search_type="biz"

    Example:

        build_minimal_search_payload(
            "64202",
            search_type="biz",
        )

    จะสร้าง:

        {
            "keyword": "64202",
            "type": "biz",
            "sortBy": "jpName",
            "currentPage": 1
        }
    """

    return {
        "keyword":
            keyword,

        "type":
            search_type,

        "sortBy":
            sort_by,

        "currentPage":
            current_page,
    }


def build_current_advanced_search_payload(
    keyword: str,
    *,
    search_type: str = "",
    current_page: int = 1,
    sort_by: str = "jpName",
    pv_code_list: Optional[list[str]] = None,
    jp_status_list: Optional[list[str]] = None,
    jp_type_list: Optional[list[str]] = None,
    business_size_list: Optional[list[str]] = None,
    cap_amt_min: Optional[float] = None,
    cap_amt_max: Optional[float] = None,
    total_income_min: Optional[float] = None,
    total_income_max: Optional[float] = None,
    net_profit_min: Optional[float] = None,
    net_profit_max: Optional[float] = None,
    total_asset_min: Optional[float] = None,
    total_asset_max: Optional[float] = None,
) -> dict[str, Any]:
    """
    Current /juristic/searchInfo frontend schema

    ใช้ schema นี้สำหรับ future partition experiments

    NOTE:
        อย่าใส่ filter ที่ไม่ต้องการแบบเดาสุ่ม
        list ว่างหมายถึง no filter

    IMPORTANT:
        ก่อนใช้ filter dimension เป็น canonical partition ต้องพิสูจน์:

            SUM(children.totalItems)
            ==
            parent.totalItems
    """

    return {
        "keyword":
            keyword,

        "type":
            search_type,

        "sortBy":
            sort_by,

        "currentPage":
            current_page,

        "pvCodeList":
            pv_code_list or [],

        "jpStatusList":
            jp_status_list or [],

        "jpTypeList":
            jp_type_list or [],

        "businessSizeList":
            business_size_list or [],

        "capAmtMin":
            cap_amt_min,

        "capAmtMax":
            cap_amt_max,

        "totalIncomeMin":
            total_income_min,

        "totalIncomeMax":
            total_income_max,

        "netProfitMin":
            net_profit_min,

        "netProfitMax":
            net_profit_max,

        "totalAssetMin":
            total_asset_min,

        "totalAssetMax":
            total_asset_max,
    }


def build_verified_old_search_payload(
    keyword: str,
    *,
    current_page: int = 1,
    jp_types: Optional[list[str]] = None,
    statuses: Optional[list[str]] = None,
    sizes: Optional[list[str]] = None,
    zone_code: str = "",
    pv_code: str = "",
    ampur_code: str = "",
) -> dict[str, Any]:
    """
    Payload schema ที่ reverse-engineer จาก searchJpBizInfo รุ่นที่ทดสอบก่อนหน้า
    และใช้สำเร็จจริงกับ exact company search

    Example verified:
        keyword = 0107556000051

    NOTE:
        Current /juristic/searchInfo ใช้ pvCodeList
        แต่ older page ใช้ pvCode singular

    เก็บ function นี้ไว้ใน MASTER เพื่อไม่สูญ context
    แต่สำหรับ production ใหม่ควรยึด current schema เป็นหลัก
    หลัง validation backend semantics แล้ว
    """

    return {
        "keyword":
            keyword,

        "type":
            "",

        "zoneCode":
            zone_code,

        "pvCode":
            pv_code,

        "ampurCode":
            ampur_code,

        "jpStatusList":
            KNOWN_JP_STATUS
            if statuses is None
            else statuses,

        "jpTypeList":
            KNOWN_JP_TYPES
            if jp_types is None
            else jp_types,

        "businessSizeList":
            KNOWN_BUSINESS_SIZES
            if sizes is None
            else sizes,

        "sortBy":
            "jpName",

        "currentPage":
            current_page,
    }


# =============================================================================
# MAIN DBD CLIENT
# =============================================================================


class DBDClient:
    """
    Single-file reference implementation ของ DBD client

    Responsibilities:
        - Browser-like session
        - Initial cookies
        - Anonymous auth
        - Token lifetime
        - API requests
        - Response decrypt
        - Rate-limit handling
        - Search
        - Company profile
        - Commons

    Production:
        ควรแตก class นี้เป็นหลาย module

    แต่ MASTER นี้ตั้งใจรวมไว้ที่เดียว
    """

    def __init__(
        self,
        *,
        timeout: int = TIMEOUT_SECONDS,
        request_delay: float = DEFAULT_REQUEST_DELAY,
    ):
        self.timeout = timeout

        self.request_delay = (
            request_delay
        )

        self.token: Optional[str] = (
            None
        )

        self.token_claims: dict[
            str,
            Any,
        ] = {}

        # ---------------------------------------------------------------------
        # curl_cffi impersonating Chrome
        # ---------------------------------------------------------------------

        self.session = (
            requests.Session(
                impersonate="chrome"
            )
        )

        self.session.headers.update({
            "Accept-Language":
                (
                    "th-TH,th;q=0.9,"
                    "en-US;q=0.8,en;q=0.7"
                ),

            "Cache-Control":
                "no-cache",

            "Pragma":
                "no-cache",
        })

    # =========================================================================
    # SESSION BOOTSTRAP
    # =========================================================================

    def initialize(
        self,
        page_url: Optional[str] = None,
    ) -> None:
        """
        เปิด HOME ก่อน แล้วค่อยเปิด target page

        จุดประสงค์:
            ให้ session/cookies/browser context ถูก initialize
            เหมือน flow ของหน้าเว็บจริง
        """

        response = self.session.get(
            HOME_URL,
            timeout=self.timeout,
        )

        response.raise_for_status()

        if page_url:
            response = (
                self.session.get(
                    page_url,
                    headers={
                        "Referer":
                            HOME_URL,
                    },
                    timeout=self.timeout,
                )
            )

            response.raise_for_status()

    # =========================================================================
    # AUTH
    # =========================================================================

    def authenticate(
        self,
        *,
        referer: str = SEARCH_PAGE,
        max_attempts: int = 5,
    ) -> str:
        """
        Anonymous auth

        POST:
            /api/refresh

        response:
            idToken

        409:
            respect Retry-After
        """

        for attempt in range(
            1,
            max_attempts + 1,
        ):
            response = (
                self.session.post(
                    f"{BASE_URL}{REFRESH_PATH}",
                    headers={
                        "Accept":
                            "application/json",

                        "Origin":
                            BASE_URL,

                        "Referer":
                            referer,
                    },
                    timeout=self.timeout,
                )
            )

            # -----------------------------------------------------------------
            # Server asks to retry
            # -----------------------------------------------------------------

            if (
                response.status_code
                == 409
            ):
                retry_after = (
                    self._parse_retry_after(
                        response,
                        default=2.0,
                    )
                )

                time.sleep(
                    retry_after
                )

                continue

            response.raise_for_status()

            body = response.json()

            token = body.get(
                "idToken"
            )

            if not token:
                continue

            claims = decode_jwt_payload(
                token
            )

            if not claims.get(
                "encKey"
            ):
                raise DBDAuthError(
                    "Authenticated JWT has no encKey"
                )

            self.token = token

            self.token_claims = (
                claims
            )

            return token

        raise DBDAuthError(
            "Unable to obtain DBD anonymous token"
        )

    def token_needs_refresh(
        self,
        margin_seconds: int = 30,
    ) -> bool:
        """
        ตรวจ JWT exp ก่อน request

        ถ้าไม่มี token:
            True

        ถ้าใกล้หมดอายุ:
            True
        """

        if not self.token:
            return True

        exp = self.token_claims.get(
            "exp"
        )

        if not exp:
            return False

        try:
            exp_value = float(
                exp
            )
        except (
            TypeError,
            ValueError,
        ):
            return False

        return (
            time.time()
            >= exp_value
            - margin_seconds
        )

    def ensure_authenticated(
        self,
        referer: str,
    ) -> None:
        """
        Refresh token ถ้าจำเป็น
        """

        if self.token_needs_refresh():
            self.authenticate(
                referer=referer
            )

    # =========================================================================
    # INTERNAL RESPONSE HELPERS
    # =========================================================================

    @staticmethod
    def _parse_retry_after(
        response,
        *,
        default: float = 5.0,
        body: Any = None,
    ) -> float:
        """
        Retry-After อาจมาจาก:

            HTTP header:
                Retry-After

            JSON:
                retryAfterSeconds
        """

        raw = response.headers.get(
            "retry-after"
        )

        if raw:
            try:
                return max(
                    1.0,
                    float(raw),
                )
            except ValueError:
                pass

        if isinstance(
            body,
            dict,
        ):
            raw_body = body.get(
                "retryAfterSeconds"
            )

            if raw_body is not None:
                try:
                    return max(
                        1.0,
                        float(
                            raw_body
                        ),
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    pass

        return default

    def _decode_response(
        self,
        response,
        *,
        request_path: str,
        request_token: Optional[str],
    ) -> Any:
        """
        Decode HTTP response

        ถ้า encrypted:
            decrypt ด้วย token ที่ใช้กับ request นั้น

        ถ้าไม่ encrypted:
            return normal JSON/text
        """

        if not response.content:
            return None

        try:
            body = response.json()
        except Exception:
            return response.text

        encrypted = (
            response.headers.get(
                "x-encrypted",
                "",
            ).lower()
            == "true"
        )

        if (
            encrypted
            and isinstance(
                body,
                dict,
            )
            and "ct" in body
        ):
            if not request_token:
                raise DBDException(
                    "Encrypted response "
                    "but request token unavailable"
                )

            body = (
                decrypt_dbd_envelope(
                    body,
                    request_path,
                    request_token,
                )
            )

        # ---------------------------------------------------------------------
        # Some APIs may use wrapped response
        # ---------------------------------------------------------------------

        wrapped = (
            response.headers.get(
                "x-wrapped",
                "",
            ).lower()
            == "true"
        )

        if (
            wrapped
            and isinstance(
                body,
                dict,
            )
            and "data" in body
        ):
            return body[
                "data"
            ]

        return body

    # =========================================================================
    # GENERIC API REQUEST
    # =========================================================================

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[
            dict[str, Any]
        ] = None,
        params: Optional[
            dict[str, Any]
        ] = None,
        referer: str = SEARCH_PAGE,
        authenticated: bool = True,
        max_retries: int = 3,
    ) -> Any:
        """
        Generic request wrapper

        Handles:
            - auth
            - 401 refresh
            - 429 Retry-After
            - decrypt
            - 5xx retry

        IMPORTANT:
            request_token ถูก snapshot ก่อนยิง request
            แล้วใช้ token ตัวนั้น decrypt response
        """

        method = method.upper()

        for attempt in range(
            1,
            max_retries + 1,
        ):
            if authenticated:
                self.ensure_authenticated(
                    referer
                )

            # -----------------------------------------------------------------
            # CRITICAL:
            # capture exact token used for this request
            # -----------------------------------------------------------------

            request_token = (
                self.token
                if authenticated
                else None
            )

            headers = {
                "Accept":
                    "application/json",

                "Origin":
                    BASE_URL,

                "Referer":
                    referer,
            }

            if authenticated:
                headers[
                    "Authorization"
                ] = (
                    f"Bearer "
                    f"{request_token}"
                )

            if json_body is not None:
                headers[
                    "Content-Type"
                ] = (
                    "application/json"
                )

            url = (
                f"{BASE_URL}{path}"
            )

            response = (
                self.session.request(
                    method,
                    url,
                    json=json_body,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
            )

            # -----------------------------------------------------------------
            # 401
            # -----------------------------------------------------------------

            if (
                response.status_code
                == 401
            ):
                self.token = None
                self.token_claims = {}

                if (
                    attempt
                    < max_retries
                ):
                    continue

            # -----------------------------------------------------------------
            # Decode before rate limit handling
            # because server may return useful JSON body
            # -----------------------------------------------------------------

            body = (
                self._decode_response(
                    response,
                    request_path=path,
                    request_token=request_token,
                )
            )

            # -----------------------------------------------------------------
            # 429 RATE LIMIT
            # -----------------------------------------------------------------

            if (
                response.status_code
                == 429
            ):
                retry_after = (
                    self._parse_retry_after(
                        response,
                        body=body,
                        default=5.0,
                    )
                )

                if (
                    attempt
                    < max_retries
                ):
                    time.sleep(
                        retry_after
                    )

                    continue

                raise DBDRateLimitError(
                    "DBD rate limited request",
                    retry_after=retry_after,
                )

            # -----------------------------------------------------------------
            # 5xx
            # -----------------------------------------------------------------

            if (
                500
                <= response.status_code
                < 600
            ):
                if (
                    attempt
                    < max_retries
                ):
                    time.sleep(
                        min(
                            2 ** attempt,
                            8,
                        )
                    )

                    continue

            # -----------------------------------------------------------------
            # Other error
            # -----------------------------------------------------------------

            if (
                response.status_code
                >= 400
            ):
                raise DBDException(
                    json.dumps(
                        {
                            "httpStatus":
                                response.status_code,

                            "body":
                                body,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )

            if (
                self.request_delay
                > 0
            ):
                time.sleep(
                    self.request_delay
                )

            return body

        raise DBDException(
            "Request failed after retries"
        )

    # =========================================================================
    # COMMONS
    # =========================================================================

    def fetch_params(
        self,
    ) -> Any:
        """
        ตัวนี้ทดสอบแล้ว

        GET:
            /api/v1/commons/params

        ใช้หาค่าต่างๆ เช่น:
            MONTHLY_DATA_MONTH
            TODAY_DATA_DATE
            DAILY_FISCAL_YEAR
            ...
        """

        return self.request(
            "GET",
            COMMON_PARAMS_PATH,
            referer=HOME_URL,
        )

    def fetch_business_types(
        self,
    ) -> Any:
        """
        Endpoint นี้ยืนยันจาก frontend source:

            GET /api/v1/commons/business-types

        Response schema ยังต้อง inspect ก่อน production assumption
        จึง return raw object
        """

        return self.request(
            "GET",
            COMMON_BUSINESS_TYPES_PATH,
            referer=SEARCH_PAGE,
        )

    def fetch_provinces(
        self,
    ) -> Any:
        """
        Source-confirmed commons endpoint

            GET /api/v1/commons/provinces

        ใช้เป็น master province list
        สำหรับ partition ในอนาคต
        """

        return self.request(
            "GET",
            COMMON_PROVINCES_PATH,
            referer=SEARCH_PAGE,
        )

    # =========================================================================
    # COMPANY SEARCH
    # =========================================================================

    def search_companies(
        self,
        payload: dict[str, Any],
    ) -> SearchResult:
        """
        POST company search/list

            /api/v1/company-profiles/infos

        Expected response:

            {
                "contents": [...],
                "meta": {
                    "currentPage": ...,
                    "totalItems": ...,
                    "totalPages": ...,
                    "itemsPerPage": 10
                }
            }
        """

        current_page = int(
            payload.get(
                "currentPage",
                1,
            )
            or 1
        )

        # ---------------------------------------------------------------------
        # Do not knowingly send impossible page
        # ---------------------------------------------------------------------

        if (
            current_page
            > MAX_SEARCH_PAGE
        ):
            raise DBDPageLimitError(
                f"DBD search page limit "
                f"is {MAX_SEARCH_PAGE}"
            )

        body = self.request(
            "POST",
            SEARCH_PATH,
            json_body=payload,
            referer=SEARCH_PAGE,
        )

        if not isinstance(
            body,
            dict,
        ):
            raise DBDException(
                "Unexpected search response type"
            )

        if (
            "contents" not in body
            or "meta" not in body
        ):
            raise DBDException(
                "Search response has no contents/meta:\n"
                + json.dumps(
                    body,
                    ensure_ascii=False,
                    indent=2,
                )
            )

        meta = SearchMeta.from_dict(
            body.get(
                "meta"
            )
            or {}
        )

        contents = body.get(
            "contents"
        )

        if not isinstance(
            contents,
            list,
        ):
            contents = []

        return SearchResult(
            meta=meta,
            contents=contents,
            raw=body,
        )

    # =========================================================================
    # BUSINESS / TSIC SEARCH
    # =========================================================================

    def search_by_business_type(
        self,
        obj_code: str,
        *,
        current_page: int = 1,
        extra_filters: Optional[
            dict[str, Any]
        ] = None,
    ) -> SearchResult:
        """
        CANONICAL VERIFIED TSIC SEARCH

        DBD frontend route:

            /juristic/searchInfo
                ?keyword=<objCode>
                &v=biz

        ส่ง backend เป็น:

            {
                "keyword": "<objCode>",
                "type": "biz",
                "sortBy": "jpName",
                "currentPage": N
            }

        Verified example:

            objCode = 64202

            totalItems   = 8519
            totalPages   = 852
            itemsPerPage = 10

            rows checked:
                submitObjCode == 64202
        """

        payload = (
            build_minimal_search_payload(
                obj_code,
                search_type="biz",
                current_page=current_page,
            )
        )

        if extra_filters:
            payload.update(
                extra_filters
            )

        return self.search_companies(
            payload
        )

    def count_business_type(
        self,
        obj_code: str,
        *,
        extra_filters: Optional[
            dict[str, Any]
        ] = None,
    ) -> int:
        """
        COUNT-ONLY logic

        Search API ไม่มี endpoint count แยกที่เราพิสูจน์
        ดังนั้นใช้ page 1 แล้วอ่าน meta.totalItems

        ถึง API จะคืน 10 rows แต่เราสนใจแค่ count

        ใช้สำหรับ:
            - scan TSIC ทั้งหมด
            - หา bucket >10,000
            - วาง partition strategy
        """

        result = (
            self.search_by_business_type(
                obj_code,
                current_page=1,
                extra_filters=extra_filters,
            )
        )

        return (
            result.meta.total_items
        )

    def fetch_complete_small_business_bucket(
        self,
        obj_code: str,
        *,
        extra_filters: Optional[
            dict[str, Any]
        ] = None,
    ) -> list[dict[str, Any]]:
        """
        ดึง TSIC bucket ให้ครบเฉพาะกรณี total <= 10,000

        SAFETY:
            ถ้า >10,000 จะ raise DBDBucketTooLarge

        เหตุผล:
            การปล่อยให้ loop ถึง page 1000 แล้วหยุดเงียบๆ
            จะทำให้ data incomplete โดยไม่รู้ตัว

        Production ต้อง fail loudly ถ้า bucket ใหญ่เกิน cap
        """

        first = (
            self.search_by_business_type(
                obj_code,
                current_page=1,
                extra_filters=extra_filters,
            )
        )

        total_items = (
            first.meta.total_items
        )

        if (
            total_items
            > MAX_ROWS_PER_SEARCH_BUCKET
        ):
            payload = (
                build_minimal_search_payload(
                    obj_code,
                    search_type="biz",
                    current_page=1,
                )
            )

            if extra_filters:
                payload.update(
                    extra_filters
                )

            raise DBDBucketTooLarge(
                total_items,
                payload,
            )

        rows = list(
            first.contents
        )

        total_pages = (
            first.meta.total_pages
        )

        for page in range(
            2,
            total_pages + 1,
        ):
            result = (
                self.search_by_business_type(
                    obj_code,
                    current_page=page,
                    extra_filters=extra_filters,
                )
            )

            rows.extend(
                result.contents
            )

        return rows

    # =========================================================================
    # COMPANY PROFILE
    # =========================================================================

    def fetch_company_profile(
        self,
        jp_type_code: str,
        jp_no: str,
    ) -> Any:
        """
        Verified company detail endpoint

            GET
            /api/v1/company-profiles/info/{jpTypeCode}/{jpNo}

        Example:
            jp_type_code = "7"
            jp_no         = "0107556000051"
        """

        path = (
            COMPANY_INFO_PATH_TEMPLATE
            .format(
                jp_type_code=
                    jp_type_code,

                jp_no=
                    jp_no,
            )
        )

        company_page = (
            f"{BASE_URL}/company/profile/"
            f"{jp_type_code}{jp_no}"
        )

        return self.request(
            "GET",
            path,
            referer=company_page,
        )


# =============================================================================
# COMPANY ID HELPERS
# =============================================================================


def company_identity(
    row: dict[str, Any],
) -> tuple[str, str]:
    """
    Canonical deduplication key

    DO NOT dedupe by company name

    ใช้:
        (jpTypeCode, jpNo)

    เพราะ company profile route ก็ประกอบจากสองค่านี้
    """

    jp_type_code = str(
        row.get(
            "jpTypeCode"
        )
        or ""
    )

    jp_no = str(
        row.get(
            "jpNo"
        )
        or ""
    )

    return (
        jp_type_code,
        jp_no,
    )


def company_code(
    jp_type_code: str,
    jp_no: str,
) -> str:
    """
    URL company code

    example:
        jpTypeCode = "7"
        jpNo       = "0107556000051"

    result:
        70107556000051
    """

    return (
        f"{jp_type_code}"
        f"{jp_no}"
    )


def deduplicate_companies(
    rows: Iterable[
        dict[str, Any]
    ],
) -> list[dict[str, Any]]:
    """
    Deduplicate company search results

    TSIC overlap เป็นสิ่งที่ต้องคาดไว้:
        บริษัทหนึ่งอาจ appear ในหลาย business-related contexts
        หรือ discovery sources หลายตัว

    canonical key:
        jpTypeCode + jpNo
    """

    unique: dict[
        tuple[str, str],
        dict[str, Any],
    ] = {}

    for row in rows:
        key = company_identity(
            row
        )

        if not all(
            key
        ):
            continue

        unique[
            key
        ] = row

    return list(
        unique.values()
    )


# =============================================================================
# BUSINESS TYPE CODE EXTRACTION
# =============================================================================


def extract_obj_codes(
    data: Any,
) -> set[str]:
    """
    Defensive recursive extractor

    Intended use:
        response = client.fetch_business_types()
        codes = extract_obj_codes(response)

    เพราะ schema ของ /commons/business-types ยังไม่ควรถูก assume
    ก่อน inspect runtime response

    Function นี้จะเดิน recursive JSON แล้วเก็บ field:
        objCode

    NOTE:
        หลังเห็น response schema จริงแล้ว production ควรเขียน parser แบบ explicit
        ไม่ควรพึ่ง recursive generic extractor ตลอดไป
    """

    result: set[str] = set()

    def walk(
        value: Any,
    ) -> None:
        if isinstance(
            value,
            dict,
        ):
            for key, child in (
                value.items()
            ):
                if (
                    key == "objCode"
                    and child is not None
                ):
                    result.add(
                        str(
                            child
                        ).strip()
                    )

                walk(
                    child
                )

        elif isinstance(
            value,
            list,
        ):
            for child in value:
                walk(
                    child
                )

    walk(
        data
    )

    return {
        code
        for code in result
        if code
    }


# =============================================================================
# PARTITION VALIDATION
# =============================================================================


def validate_partition_counts(
    parent_count: int,
    child_counts: Iterable[int],
) -> bool:
    """
    MOST IMPORTANT RULE สำหรับ recursive discovery

    partition จะถือว่า valid/lossless ได้เมื่อ:

        SUM(children) == parent

    Example:

        parent:
            TSIC 12345 = 50,000

        partition by province:

            Bangkok    = 20,000
            Chonburi   = 10,000
            ...
            total      = 50,000

        ถ้า sum = 50,000:
            ผ่านในเชิง count

        ถ้า sum != 50,000:
            ห้ามใช้ partition นั้นเป็น canonical partition
    """

    child_total = sum(
        child_counts
    )

    return (
        child_total
        == parent_count
    )


# =============================================================================
# COUNT-ONLY TSIC INVENTORY
# =============================================================================


def scan_business_type_counts(
    client: DBDClient,
    obj_codes: Iterable[str],
) -> list[dict[str, Any]]:
    """
    PHASE BEFORE FULL 1-MILLION RECORD CRAWL

    ทำเพียง:
        TSIC -> totalItems

    ไม่ paginate ทุก page

    เป้าหมาย:
        - จำนวน TSIC ทั้งหมด
        - TSIC ไหน 0
        - TSIC ไหน <= 10,000
        - TSIC ไหน > 10,000
        - bucket ใหญ่สุด
        - วาง partition plan ก่อนยิงข้อมูลจริง

    IMPORTANT:
        SUM ของ TSIC totalItems ไม่ใช่ unique companies
        เพราะบริษัทอาจ overlap ระหว่าง TSIC ได้

    unique count ต้องทำหลัง dedup:
        (jpTypeCode, jpNo)
    """

    stats: list[
        dict[str, Any]
    ] = []

    codes = sorted(
        {
            str(code).strip()
            for code in obj_codes
            if str(
                code
            ).strip()
        }
    )

    for index, obj_code in enumerate(
        codes,
        start=1,
    ):
        try:
            total_items = (
                client.count_business_type(
                    obj_code
                )
            )

            stats.append({
                "objCode":
                    obj_code,

                "totalItems":
                    total_items,

                "requiresPartition":
                    (
                        total_items
                        >
                        MAX_ROWS_PER_SEARCH_BUCKET
                    ),
            })

            print(
                f"[{index:>5}/{len(codes):>5}] "
                f"TSIC={obj_code:<10} "
                f"items={total_items:>10,} "
                f"partition="
                f"{total_items > MAX_ROWS_PER_SEARCH_BUCKET}"
            )

        except DBDRateLimitError as exc:
            print(
                f"[RATE LIMIT] "
                f"TSIC={obj_code} "
                f"wait={exc.retry_after}s"
            )

            time.sleep(
                exc.retry_after
            )

            # ใน production:
            # อย่า skip เงียบๆ
            # enqueue retry/checkpoint
            raise

    return stats


# =============================================================================
# VERIFIED SMOKE TESTS
# =============================================================================


def run_verified_smoke_test(
    client: DBDClient,
) -> None:
    """
    Smoke test ที่อิงจากสิ่งที่เคยพิสูจน์สำเร็จ

    ใช้เพื่อให้ AI/Developer ตรวจ client หลัง refactor
    """

    print()
    print(
        "=" * 100
    )

    print(
        "DBD MASTER SMOKE TEST"
    )

    print(
        "=" * 100
    )

    # -------------------------------------------------------------------------
    # Initialize browser-like session
    # -------------------------------------------------------------------------

    page = (
        f"{SEARCH_PAGE}"
        "?keyword=64202"
        "&v=biz"
    )

    client.initialize(
        page
    )

    print(
        "[SESSION] initialized"
    )

    # -------------------------------------------------------------------------
    # Authenticate
    # -------------------------------------------------------------------------

    client.authenticate(
        referer=page
    )

    print(
        "[AUTH] success"
    )

    print(
        "[AUTH] provider:",
        client.token_claims.get(
            "provider"
        ),
    )

    print(
        "[AUTH] encKey:",
        bool(
            client.token_claims.get(
                "encKey"
            )
        ),
    )

    # NEVER print:
    #     client.token
    #     encKey actual value

    # -------------------------------------------------------------------------
    # Commons params
    # -------------------------------------------------------------------------

    params = (
        client.fetch_params()
    )

    print()
    print(
        "[COMMON PARAMS TYPE]",
        type(
            params
        ).__name__,
    )

    if isinstance(
        params,
        dict,
    ):
        print(
            "[COMMON PARAM KEYS]",
            sorted(
                params.keys()
            )[:30],
        )

    # -------------------------------------------------------------------------
    # Known exact company
    # -------------------------------------------------------------------------

    print()
    print(
        "-" * 100
    )

    print(
        "KNOWN COMPANY PROFILE"
    )

    print(
        "-" * 100
    )

    profile = (
        client.fetch_company_profile(
            "7",
            "0107556000051",
        )
    )

    if isinstance(
        profile,
        dict,
    ):
        print(
            "jpNo    :",
            profile.get(
                "jpNo"
            ),
        )

        print(
            "jpName  :",
            profile.get(
                "jpName"
            ),
        )

        print(
            "paidAmt :",
            profile.get(
                "paidAmt"
            ),
        )

    # -------------------------------------------------------------------------
    # Normal 64202
    # -------------------------------------------------------------------------

    print()
    print(
        "-" * 100
    )

    print(
        "NORMAL SEARCH 64202"
    )

    print(
        "-" * 100
    )

    normal = (
        client.search_companies(
            build_minimal_search_payload(
                "64202",
                search_type="",
            )
        )
    )

    print(
        "totalItems:",
        normal.meta.total_items,
    )

    print(
        "totalPages:",
        normal.meta.total_pages,
    )

    print(
        "itemsPerPage:",
        normal.meta.items_per_page,
    )

    # Previous verified:
    #     totalItems = 9
    #
    # แต่ public dataset เปลี่ยนได้ตามเวลา
    # จึงไม่ assert ว่าต้องเท่ากับ 9 ตลอดไป

    # -------------------------------------------------------------------------
    # Business TSIC 64202
    # -------------------------------------------------------------------------

    print()
    print(
        "-" * 100
    )

    print(
        "BUSINESS TYPE SEARCH 64202"
    )

    print(
        "-" * 100
    )

    biz = (
        client.search_by_business_type(
            "64202"
        )
    )

    print(
        "totalItems:",
        biz.meta.total_items,
    )

    print(
        "totalPages:",
        biz.meta.total_pages,
    )

    print(
        "itemsPerPage:",
        biz.meta.items_per_page,
    )

    print(
        "rows:",
        len(
            biz.contents
        ),
    )

    print()

    for index, row in enumerate(
        biz.contents,
        start=1,
    ):
        print(
            f"{index:>2}. "
            f"{row.get('jpTypeCode')} "
            f"{row.get('jpNo')} "
            f"| {row.get('jpName')} "
            f"| submitObjCode="
            f"{row.get('submitObjCode')}"
        )

    # -------------------------------------------------------------------------
    # Validate observed TSIC semantics on returned page
    # -------------------------------------------------------------------------

    matching_submit_obj = [
        row
        for row in biz.contents
        if str(
            row.get(
                "submitObjCode"
            )
        )
        == "64202"
    ]

    print()

    print(
        "[64202 submitObjCode match]",
        len(
            matching_submit_obj
        ),
        "/",
        len(
            biz.contents
        ),
    )

    print()
    print(
        "=" * 100
    )

    print(
        "SMOKE TEST COMPLETE"
    )

    print(
        "=" * 100
    )


# =============================================================================
# DISCOVERY ARCHITECTURE NOTES FOR FUTURE AI
# =============================================================================

"""
================================================================================
HOW TO BUILD COMPANY MASTER
================================================================================

DO NOT start by requesting 1 million company profiles.

Correct order:

PHASE 1
-------
Fetch business-type master:

    GET /api/v1/commons/business-types

Extract all TSIC/objCode.

Inspect response schema manually first.


PHASE 2
-------
For every TSIC:

    POST /api/v1/company-profiles/infos

    {
        "keyword": "<TSIC>",
        "type": "biz",
        "sortBy": "jpName",
        "currentPage": 1
    }

Read:
    meta.totalItems

Build count inventory only.


PHASE 3
-------
Classify buckets:

A)
    totalItems == 0

    no work


B)
    1 <= totalItems <= 10000

    safe to paginate directly


C)
    totalItems > 10000

    MUST partition


PHASE 4
-------
Experiment with valid partition dimensions.

Recommended order to investigate:

    province
        pvCodeList

    jpType
        jpTypeList

    status
        jpStatusList

    businessSize
        businessSizeList

Potentially financial ranges:
    capAmt
    totalIncome
    netProfit
    totalAsset

But financial ranges are harder to make guaranteed exhaustive,
therefore categorical dimensions should be preferred.


PHASE 5
-------
For every candidate partition:

    parent_count = count(parent)

    child1 = count(parent + filter A)
    child2 = count(parent + filter B)
    ...

REQUIRE:

    SUM(children) == parent_count

If NOT equal:
    partition semantics are not lossless
    DO NOT use it for canonical discovery.


PHASE 6
-------
Every final leaf must satisfy:

    totalItems <= 10,000

Then paginate:

    page = 1..totalPages

Never request:
    page > 1000


PHASE 7
-------
For every row keep at minimum:

    jpTypeCode
    jpNo
    jpName
    jpStatCode
    submitObjCode
    setupObjCode
    pvCode

Potential fields visible from search:
    capAmt
    totalIncome
    netProfit
    totalAsset
    totalEquity
    locationProvince
    locationAmpur
    locationTumbon
    address


PHASE 8
-------
Deduplicate globally using:

    PRIMARY DISCOVERY IDENTITY:
        (jpTypeCode, jpNo)

NOT:
    jpName

NOT:
    submitObjCode


Suggested MySQL company_master:

    CREATE TABLE company_master (
        jp_type_code VARCHAR(4) NOT NULL,
        jp_no        VARCHAR(32) NOT NULL,

        jp_name      TEXT NULL,
        jp_stat_code VARCHAR(16) NULL,

        submit_obj_code VARCHAR(32) NULL,
        setup_obj_code  VARCHAR(32) NULL,

        pv_code VARCHAR(16) NULL,

        discovered_at DATETIME NOT NULL,
        last_seen_at  DATETIME NOT NULL,

        PRIMARY KEY (
            jp_type_code,
            jp_no
        )
    );


IMPORTANT:
    company_master should represent UNIQUE companies

Separate relation table should preserve discovery source:

    company_business_discovery

        jp_type_code
        jp_no
        obj_code
        discovery_mode
        first_seen_at
        last_seen_at

Because one company may have relationships to multiple business classifications.


PHASE 9
-------
Only after company_master exists:

queue company profile enrichment:

    GET
    /api/v1/company-profiles/info/{jpTypeCode}/{jpNo}

Then optional related endpoints:

    committees
    committee-signs
    descriptions
    partners
    mergers
    liquidators
    names
    capitals
    nations
    nations/charts


PHASE 10
--------
RECONCILIATION

This is mandatory before claiming "all companies".

Compute:

    UNIQUE COUNT(company_master)

Compare against authoritative DBD population.

Example logic:

    DBD_TOTAL       = authoritative active/all scope count
    DISCOVERED      = unique companies from TSIC union
    MISSING         = DBD_TOTAL - DISCOVERED

If:
    MISSING == 0

still validate:
    - status distribution
    - jpType distribution
    - province distribution

If:
    MISSING > 0

investigate missing population:
    - companies with no submitObjCode
    - companies without financial filing
    - special juristic types
    - inactive / historical statuses
    - entities outside normal TSIC classification


================================================================================
RUNTIME / PRODUCTION REQUIREMENTS
================================================================================

For ~1 million records DO NOT build one giant synchronous script.

Production should include:

    run_id

    checkpoints

    work queue

    retry queue

    persistent state

    graceful shutdown

    resume

    rate-limit awareness

    bounded concurrency

    batch MySQL inserts


Suggested statuses:

    PENDING
    RUNNING
    SUCCESS
    FAILED
    RETRY
    SKIPPED


A few failed companies must NOT terminate entire run.

Final run state could be:

    COMPLETED
    COMPLETED_WITH_ERRORS
    FAILED


================================================================================
DO NOT STORE
================================================================================

Never persist:

    JWT
    encKey
    Authorization header
    cookies
    browser challenge data

Only persist business/public data and operational metadata.


================================================================================
KNOWN ERROR / CONTROL CASES
================================================================================

HTTP 429
--------
Respect Retry-After.

Do not aggressively retry.


HTTP 401
--------
Refresh anonymous token then retry.


HTTP 409 on /api/refresh
------------------------
Respect Retry-After.


HTTP 400:
COMPANY_SEARCH_PAGE_LIMIT_EXCEEDED
----------------------------------
Page > 1000.


HTTP 400:
INVALID_COMPANY_SEARCH_REQUEST
--------------------------------
Example:
    keyword shorter than 3 characters


COMPANY_SEARCH_KEYWORD_TOO_BROAD
--------------------------------
Frontend recognizes this case.
Do not blindly retry.


DECRYPT FAILURE
---------------
Possible reasons:

    wrong request token
    token rotation
    wrong request path in AAD
    including query string by mistake
    malformed envelope

First check:
    exact token used for request
    exact pathname


================================================================================
ANTI-PATTERNS
================================================================================

DO NOT brute force 13-digit juristic numbers.

DO NOT assume numeric prefix is a prefix partition.

DO NOT assume:
    totalPages > 1000
means you can read pages >1000.

DO NOT silently stop at page 1000.

DO NOT assume:
    SUM(TSIC totalItems)
is total unique companies.

DO NOT claim:
    TSIC union == all DBD companies
before reconciliation.

DO NOT fire huge concurrency at public API.

DO NOT save raw authentication secrets.

================================================================================
"""


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    """
    Default behavior:
        run only verified smoke test

    ไม่เริ่ม full crawl อัตโนมัติ

    เหตุผล:
        MASTER reference ไม่ควรเผลอยิง request หลักแสนครั้ง
        แค่เพราะมีคน run ไฟล์

    Full discovery ควรเป็น explicit job/CLI ใน production
    """

    client = DBDClient(
        timeout=30,

        # ถ้าทดลอง local สามารถปรับได้
        # แต่ public API ไม่ควรถูกยิงแบบ aggressive
        request_delay=1.0,
    )

    run_verified_smoke_test(
        client
    )


if __name__ == "__main__":
    main()