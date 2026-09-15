from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

import orjson
import zstandard as zstd


@dataclass(frozen=True)
class EncodedPayload:
    json_text: str
    compressed: bytes | None
    codec: str
    sha256_hex: str
    original_bytes: int
    stored_bytes: int


def _default(value):
    return str(value)


def encode_payload(payload, *, compress: bool = True, threshold: int = 4096) -> EncodedPayload:
    raw = orjson.dumps(payload, default=_default)
    digest = sha256(raw).hexdigest()
    if compress and len(raw) >= threshold:
        compressed = zstd.ZstdCompressor(level=3).compress(raw)
        if len(compressed) < len(raw):
            return EncodedPayload(
                json_text="",
                compressed=compressed,
                codec="zstd",
                sha256_hex=digest,
                original_bytes=len(raw),
                stored_bytes=len(compressed),
            )
    return EncodedPayload(
        json_text=raw.decode("utf-8"),
        compressed=None,
        codec="json",
        sha256_hex=digest,
        original_bytes=len(raw),
        stored_bytes=len(raw),
    )


def encoded_bytes(encoded: EncodedPayload) -> bytes:
    if encoded.codec == "zstd":
        if encoded.compressed is None:
            raise ValueError("zstd payload has no compressed bytes")
        return bytes(encoded.compressed)
    if encoded.codec == "json":
        return encoded.json_text.encode("utf-8")
    raise ValueError(f"Unsupported archive codec {encoded.codec!r}")


def restored_json_bytes(raw: bytes, codec: str) -> bytes:
    """Return the canonical serialized JSON bytes represented by an archive codec.

    `payload_sha256` is intentionally the digest of these uncompressed bytes for
    both inline and external archives. Compression/encryption therefore does not
    change the content-integrity identity used across storage backends.
    """
    if codec == "zstd":
        return zstd.ZstdDecompressor().decompress(raw)
    if codec == "json":
        return raw
    raise ValueError(f"Unsupported archive codec {codec!r}")


def decode_bytes(raw: bytes, codec: str):
    return orjson.loads(restored_json_bytes(raw, codec))


def decode_payload(*, payload: str | None, payload_zstd: bytes | None, codec: str | None):
    if codec == "zstd":
        if not payload_zstd:
            raise ValueError("Compressed archive row has no payload_zstd")
        return decode_bytes(bytes(payload_zstd), "zstd")
    if codec not in (None, "json"):
        raise ValueError(f"Unsupported inline archive codec {codec!r}")
    if payload is None:
        raise ValueError("JSON archive row has no payload")
    return decode_bytes(payload.encode("utf-8"), "json")
