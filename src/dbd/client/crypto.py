from __future__ import annotations

import base64
import gzip
import json
import time
import zlib
from decimal import Decimal

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from ..core.exceptions import DbdTokenExpiredError


def b64url_decode(value: str) -> bytes:
    value = value.strip()
    value += "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value)


def decode_jwt_payload(token: str) -> dict:
    parts = token.split(".")
    if len(parts) < 2:
        raise ValueError("Invalid JWT")
    return json.loads(b64url_decode(parts[1]).decode("utf-8"))


def _decompress(data: bytes) -> bytes:
    if data.startswith(b"\x1f\x8b"):
        return gzip.decompress(data)

    try:
        return zlib.decompress(data)
    except zlib.error:
        pass

    try:
        return zlib.decompress(data, -zlib.MAX_WBITS)
    except zlib.error:
        pass

    return data


def decrypt_envelope(envelope: dict, request_path: str, access_token: str) -> dict:
    """Reverse DBD's response encryption: HKDF-SHA256 derives an AES-GCM key
    from the JWT's `encKey` claim, salted per-request and bound (as AAD) to
    the envelope's key id and the exact request path.
    """
    claims = decode_jwt_payload(access_token)

    enc_key_string = claims.get("encKey")
    if not enc_key_string:
        raise ValueError("JWT does not contain encKey")

    exp = claims.get("exp")
    if exp and time.time() >= exp:
        raise DbdTokenExpiredError("Access token expired")

    kid = envelope["kid"]
    enc_key = b64url_decode(enc_key_string)
    salt = b64url_decode(envelope["salt"])
    iv = b64url_decode(envelope["iv"])
    ciphertext = b64url_decode(envelope["ct"])

    aad = f"bdw|v{kid}|{request_path}".encode()

    key = HKDF(algorithm=hashes.SHA256(), length=32, salt=salt, info=aad).derive(enc_key)
    plaintext = AESGCM(key).decrypt(iv, ciphertext, aad)
    plaintext = _decompress(plaintext)

    return json.loads(plaintext.decode("utf-8"), parse_float=Decimal)
