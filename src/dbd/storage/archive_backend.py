from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from hashlib import sha256
from typing import Mapping
from uuid import uuid4

import boto3
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..core.settings import Settings, settings
from .archive_codec import EncodedPayload, encoded_bytes, restored_json_bytes


@dataclass(frozen=True)
class ExternalArchivePayload:
    backend: str
    object_key: str
    codec: str
    nonce: bytes
    key_id: str
    cipher_sha256: str
    payload_sha256: str
    original_bytes: int
    stored_bytes: int


def _decode_key(value: str) -> bytes:
    try:
        padded = value + "=" * (-len(value) % 4)
        key = base64.urlsafe_b64decode(padded.encode("ascii"))
    except Exception as exc:
        raise ValueError("DBD archive encryption key is not valid base64") from exc
    if len(key) != 32:
        raise ValueError("DBD archive encryption key must decode to exactly 32 bytes")
    return key


class S3ArchiveBackend:
    """Client-side AES-256-GCM encrypted archive blobs in S3-compatible storage.

    MySQL remains the metadata/source-of-truth boundary. The blob is written first;
    only after a successful PUT may the caller commit the archive metadata and move
    the endpoint task to project stage. The active key encrypts new objects; a
    read-only decryption key ring allows older key ids to remain replayable during
    rotation without using an old key for new writes.
    """

    backend_name = "s3"

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        encryption_key_b64: str,
        key_id: str,
        decryption_keys: Mapping[str, str] | None = None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        client=None,
    ):
        if not bucket:
            raise ValueError("DBD_ARCHIVE_S3_BUCKET is required for the S3 archive backend")
        if not key_id:
            raise ValueError("DBD_ARCHIVE_ENCRYPTION_KEY_ID must not be empty")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.key_id = key_id
        self._key = _decode_key(encryption_key_b64)
        self._keys = {
            str(old_key_id): _decode_key(old_key)
            for old_key_id, old_key in (decryption_keys or {}).items()
        }
        self._keys[self.key_id] = self._key
        self.client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

    def _object_key(self, endpoint: str, task_id: int) -> str:
        suffix = f"{endpoint}/{task_id}/{uuid4().hex}.bin"
        return f"{self.prefix}/{suffix}" if self.prefix else suffix

    def store(self, encoded: EncodedPayload, *, endpoint: str, task_id: int) -> ExternalArchivePayload:
        plaintext = encoded_bytes(encoded)
        object_key = self._object_key(endpoint, task_id)
        nonce = os.urandom(12)
        ciphertext = AESGCM(self._key).encrypt(nonce, plaintext, object_key.encode("utf-8"))
        cipher_digest = sha256(ciphertext).hexdigest()
        self.client.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=ciphertext,
            ContentType="application/octet-stream",
            Metadata={
                "sha256": cipher_digest,
                "key-id": self.key_id,
                "codec": encoded.codec,
            },
        )
        return ExternalArchivePayload(
            backend=self.backend_name,
            object_key=object_key,
            codec=f"aesgcm+{encoded.codec}",
            nonce=nonce,
            key_id=self.key_id,
            cipher_sha256=cipher_digest,
            payload_sha256=encoded.sha256_hex,
            original_bytes=encoded.original_bytes,
            stored_bytes=len(ciphertext),
        )

    def delete(self, object_key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=object_key)

    def load(self, row: Mapping[str, object]):
        object_key = str(row.get("payload_object_key") or "")
        if not object_key:
            raise OSError("External archive row has no object key")
        stored_key_id = str(row.get("payload_key_id") or "")
        key = self._keys.get(stored_key_id)
        if key is None:
            raise OSError(
                f"Archive key id {stored_key_id!r} is not available in this process"
            )
        nonce = row.get("payload_nonce")
        if not nonce:
            raise OSError("External archive row has no encryption nonce")
        response = self.client.get_object(Bucket=self.bucket, Key=object_key)
        ciphertext = response["Body"].read()
        expected_digest = str(row.get("payload_cipher_sha256") or "")
        actual_digest = sha256(ciphertext).hexdigest()
        if not expected_digest or actual_digest != expected_digest:
            raise OSError("External archive ciphertext checksum mismatch")
        try:
            encoded_plaintext = AESGCM(key).decrypt(
                bytes(nonce), ciphertext, object_key.encode("utf-8")
            )
        except Exception as exc:
            raise OSError("External archive authentication/decryption failed") from exc
        codec = str(row.get("payload_codec") or "")
        if not codec.startswith("aesgcm+"):
            raise OSError(f"Unsupported external archive codec {codec!r}")
        inner_codec = codec.split("+", 1)[1]
        try:
            serialized_json = restored_json_bytes(encoded_plaintext, inner_codec)
        except Exception as exc:
            raise OSError("External archive payload decode failed") from exc
        expected_payload_digest = str(row.get("payload_sha256") or "")
        if not expected_payload_digest or sha256(serialized_json).hexdigest() != expected_payload_digest:
            raise OSError("External archive plaintext checksum mismatch")
        try:
            import orjson

            return orjson.loads(serialized_json)
        except Exception as exc:
            raise OSError("External archive payload decode failed") from exc


def build_archive_backend(config: Settings = settings):
    if config.dbd_archive_backend == "mysql":
        return None
    if config.dbd_archive_backend != "s3":
        raise ValueError(f"Unsupported archive backend {config.dbd_archive_backend!r}")
    if not config.dbd_archive_s3_bucket:
        raise ValueError("DBD_ARCHIVE_S3_BUCKET is required when DBD_ARCHIVE_BACKEND=s3")
    if not config.dbd_archive_encryption_key_b64:
        raise ValueError(
            "DBD_ARCHIVE_ENCRYPTION_KEY_B64 is required when DBD_ARCHIVE_BACKEND=s3"
        )
    return S3ArchiveBackend(
        bucket=config.dbd_archive_s3_bucket,
        prefix=config.dbd_archive_s3_prefix,
        encryption_key_b64=config.dbd_archive_encryption_key_b64,
        key_id=config.dbd_archive_encryption_key_id,
        decryption_keys=config.dbd_archive_decryption_keys,
        endpoint_url=config.dbd_archive_s3_endpoint_url,
        region_name=config.dbd_archive_s3_region,
    )
