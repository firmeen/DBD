# Manual smoke-test script. Not part of the production package (src/dbd_data).
# Use this to poke at new DBD endpoints / verify auth+decryption still works
# before promoting anything into src/dbd_data/resources.
#
# pip install curl_cffi cryptography truststore browser-cookie3

import base64
import gzip
import json
import time
import zlib

from curl_cffi import requests

try:
    import truststore
    truststore.inject_into_ssl()
except Exception:
    pass

try:
    import browser_cookie3
except ImportError:
    browser_cookie3 = None

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

BASE_URL = "https://datawarehouse.dbd.go.th"
HOME_URL = f"{BASE_URL}/"
PAGE_URL = f"{BASE_URL}/biztype/overview"

TIMEOUT = 30


def b64url_decode(value: str) -> bytes:
    value = value.strip()
    value += "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value)


def decode_jwt_payload(token: str) -> dict:
    parts = token.split(".")

    if len(parts) < 2:
        raise ValueError("Invalid JWT")

    return json.loads(
        b64url_decode(parts[1]).decode("utf-8")
    )


def decompress_payload(data: bytes) -> bytes:
    if data.startswith(b"\x1f\x8b"):
        return gzip.decompress(data)

    try:
        return zlib.decompress(data)
    except zlib.error:
        pass

    try:
        return zlib.decompress(
            data,
            -zlib.MAX_WBITS,
        )
    except zlib.error:
        pass

    return data


def decrypt_envelope(
    envelope: dict,
    request_path: str,
    access_token: str,
):
    claims = decode_jwt_payload(access_token)

    enc_key_string = claims.get("encKey")

    if not enc_key_string:
        raise ValueError("JWT does not contain encKey")

    exp = claims.get("exp")

    if exp and time.time() >= exp:
        raise ValueError("Access token expired")

    kid = envelope["kid"]

    enc_key = b64url_decode(enc_key_string)
    salt = b64url_decode(envelope["salt"])
    iv = b64url_decode(envelope["iv"])
    ciphertext = b64url_decode(envelope["ct"])

    aad = (
        f"bdw|v{kid}|{request_path}"
    ).encode("utf-8")

    key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=aad,
    ).derive(enc_key)

    plaintext = AESGCM(key).decrypt(
        iv,
        ciphertext,
        aad,
    )

    plaintext = decompress_payload(
        plaintext
    )

    return json.loads(
        plaintext.decode("utf-8")
    )


def create_session():
    session = requests.Session(
        impersonate="chrome"
    )

    session.headers.update(
        {
            "Accept-Language": (
                "th-TH,th;q=0.9,"
                "en-US;q=0.8,en;q=0.7"
            ),
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )

    return session


def import_chrome_cookies(session):
    if browser_cookie3 is None:
        return False

    try:
        chrome_cookies = browser_cookie3.chrome(
            domain_name="datawarehouse.dbd.go.th"
        )

        count = 0

        for cookie in chrome_cookies:
            session.cookies.set(
                cookie.name,
                cookie.value,
                domain=cookie.domain,
                path=cookie.path,
            )
            count += 1

        print(
            f"[COOKIE] Imported {count} Chrome cookies"
        )

        return count > 0

    except Exception as exc:
        print(
            "[COOKIE] Chrome cookie import failed:",
            exc,
        )

        return False


def document_headers():
    return {
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,"
            "image/avif,image/webp,"
            "image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def initialize_session(session):
    print("=" * 100)
    print("INITIALIZE SESSION")
    print("=" * 100)

    response = session.get(
        HOME_URL,
        headers=document_headers(),
        timeout=TIMEOUT,
    )

    print(
        "[HOME]",
        response.status_code,
        response.url,
    )

    if response.status_code == 403:
        print(
            "[HOME] 403 - importing Chrome cookies"
        )

        if import_chrome_cookies(session):
            response = session.get(
                HOME_URL,
                headers=document_headers(),
                timeout=TIMEOUT,
            )

            print(
                "[HOME RETRY]",
                response.status_code,
                response.url,
            )

    page_headers = document_headers()

    page_headers.update(
        {
            "Referer": HOME_URL,
            "Sec-Fetch-Site": "same-origin",
        }
    )

    response = session.get(
        PAGE_URL,
        headers=page_headers,
        timeout=TIMEOUT,
    )

    print(
        "[PAGE]",
        response.status_code,
        response.url,
    )

    if response.status_code == 403:
        print(
            "[PAGE] 403 - retrying with Chrome cookies"
        )

        import_chrome_cookies(session)

        response = session.get(
            PAGE_URL,
            headers=page_headers,
            timeout=TIMEOUT,
        )

        print(
            "[PAGE RETRY]",
            response.status_code,
            response.url,
        )

    print(
        "[COOKIE COUNT]",
        len(session.cookies),
    )

    return response


def get_access_token(session):
    url = f"{BASE_URL}/api/refresh"

    headers = {
        "Accept": "application/json",
        "Origin": BASE_URL,
        "Referer": PAGE_URL,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    for attempt in range(1, 5):
        print(
            f"[AUTH] Attempt {attempt}"
        )

        response = session.post(
            url,
            headers=headers,
            timeout=TIMEOUT,
        )

        print(
            "[AUTH] Status:",
            response.status_code,
        )

        if response.status_code == 403:
            print(
                "[AUTH] 403 - importing Chrome cookies"
            )

            import_chrome_cookies(session)

            time.sleep(1)
            continue

        if response.status_code == 409:
            retry_after = (
                response.headers.get(
                    "retry-after",
                    "1",
                )
            )

            try:
                delay = max(
                    1,
                    int(retry_after),
                )
            except ValueError:
                delay = 1

            print(
                f"[AUTH] Waiting {delay}s"
            )

            time.sleep(delay)
            continue

        try:
            data = response.json()
        except Exception:
            data = {}

        if (
            response.ok
            and data.get("idToken")
        ):
            token = data["idToken"]

            claims = decode_jwt_payload(
                token
            )

            print("[AUTH] SUCCESS")
            print(
                "[AUTH] Provider:",
                claims.get("provider"),
            )
            print(
                "[AUTH] encKey:",
                bool(claims.get("encKey")),
            )

            return token

        print(
            "[AUTH] Response:",
            response.text[:1000],
        )

        if response.status_code == 401:
            time.sleep(1)
            continue

        raise RuntimeError(
            f"Authentication failed: "
            f"{response.status_code}"
        )

    raise RuntimeError(
        "Unable to obtain DBD access token"
    )


def api_get(
    session,
    token,
    path,
    params=None,
):
    url = BASE_URL + path

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "Origin": BASE_URL,
        "Referer": PAGE_URL,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    response = session.get(
        url,
        params=params,
        headers=headers,
        timeout=TIMEOUT,
    )

    print()
    print("-" * 100)
    print("GET:", response.url)
    print("STATUS:", response.status_code)
    print(
        "X-ENCRYPTED:",
        response.headers.get(
            "x-encrypted"
        ),
    )
    print(
        "X-WRAPPED:",
        response.headers.get(
            "x-wrapped"
        ),
    )

    if response.status_code == 401:
        raise PermissionError(
            "Access token expired"
        )

    if response.status_code == 403:
        raise PermissionError(
            f"403 Forbidden: {response.url}"
        )

    response.raise_for_status()

    data = response.json()

    encrypted = (
        response.headers.get(
            "x-encrypted",
            "",
        ).lower()
        == "true"
    )

    if (
        encrypted
        and isinstance(data, dict)
        and "ct" in data
    ):
        data = decrypt_envelope(
            data,
            path,
            token,
        )

    wrapped = (
        response.headers.get(
            "x-wrapped",
            "",
        ).lower()
        == "true"
    )

    if (
        wrapped
        and isinstance(data, dict)
        and "data" in data
    ):
        data = data["data"]

    return data


def split_company_code(code):
    code = code.strip()

    if len(code) < 2:
        raise ValueError(
            f"Invalid company code: {code!r}"
        )

    return code[0], code[1:]


def get_company_profile(
    session,
    token,
    company_code,
):
    jp_type_code, jp_no = split_company_code(
        company_code
    )

    return api_get(
        session,
        token,
        f"/api/v1/company-profiles/info/{jp_type_code}/{jp_no}",
    )


def get_common_params(
    session,
    token,
):
    data = api_get(
        session,
        token,
        "/api/v1/commons/params",
    )

    result = {}

    for item in data:
        if item.get("paraStatus") != "A":
            continue

        code = item.get("paraCode")

        if code:
            result[code] = item.get(
                "paraValue"
            )

    return result


def main():
    session = create_session()

    initialize_session(
        session
    )

    token = get_access_token(
        session
    )

    common_params = get_common_params(
        session,
        token,
    )

    month_no = common_params.get(
        "MONTHLY_DATA_MONTH"
    )

    print()
    print("=" * 100)
    print("COMMON PARAMS")
    print("=" * 100)

    print(
        "MONTHLY_DATA_MONTH:",
        month_no,
    )

    company_code = "70107556000051"

    profile = get_company_profile(
        session,
        token,
        company_code,
    )

    print()
    print("=" * 100)
    print(f"COMPANY PROFILE ({company_code})")
    print("=" * 100)

    with open(
        f"company_profile_{company_code}.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            profile,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(
        f"Saved to company_profile_{company_code}.json"
    )

    if isinstance(profile, dict):
        print("jpName   :", profile.get("jpName"))
        print("jpNo     :", profile.get("jpNo"))
        print("paidAmt  :", profile.get("paidAmt"))
        print("shareQty :", profile.get("shareQty"))


if __name__ == "__main__":
    main()
