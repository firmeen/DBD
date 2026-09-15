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


def decode_payload(*, payload: str | None, payload_zstd: bytes | None, codec: str | None):
    if codec == "zstd":
        if not payload_zstd:
            raise ValueError("Compressed archive row has no payload_zstd")
        raw = zstd.ZstdDecompressor().decompress(bytes(payload_zstd))
        return orjson.loads(raw)
    if payload is None:
        raise ValueError("JSON archive row has no payload")
    return orjson.loads(payload)
