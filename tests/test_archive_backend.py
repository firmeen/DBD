import base64
from io import BytesIO

import pytest

from dbd.storage.archive_backend import S3ArchiveBackend
from dbd.storage.archive_codec import encode_payload


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = {
            "body": bytes(kwargs["Body"]),
            "metadata": dict(kwargs.get("Metadata") or {}),
        }
        return {"ETag": "fake"}

    def get_object(self, *, Bucket, Key):
        item = self.objects[(Bucket, Key)]
        return {"Body": BytesIO(item["body"]), "Metadata": item["metadata"]}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {}


def key_b64(byte=7):
    return base64.urlsafe_b64encode(bytes([byte]) * 32).decode("ascii")


def backend(client, key=7, key_id="v1", decryption_keys=None):
    return S3ArchiveBackend(
        bucket="archive-test",
        prefix="dbd/test",
        encryption_key_b64=key_b64(key),
        key_id=key_id,
        decryption_keys=decryption_keys,
        client=client,
    )


def row_from_external(external):
    return {
        "payload_backend": external.backend,
        "payload_object_key": external.object_key,
        "payload_nonce": external.nonce,
        "payload_key_id": external.key_id,
        "payload_cipher_sha256": external.cipher_sha256,
        "payload_sha256": external.payload_sha256,
        "payload_codec": external.codec,
    }


def test_s3_archive_round_trip_is_client_side_encrypted():
    client = FakeS3()
    store = backend(client)
    payload = {"company": "ACME", "secret": "not-plaintext-in-object"}
    encoded = encode_payload(payload, compress=False)

    external = store.store(encoded, endpoint="profile", task_id=123)
    object_bytes = client.objects[("archive-test", external.object_key)]["body"]

    assert b"not-plaintext-in-object" not in object_bytes
    assert external.codec == "aesgcm+json"
    assert len(external.nonce) == 12
    assert store.load(row_from_external(external)) == payload


def test_s3_archive_round_trip_preserves_zstd_codec():
    client = FakeS3()
    store = backend(client)
    payload = {"rows": ["compress-me" * 100] * 50}
    encoded = encode_payload(payload, compress=True, threshold=1)
    assert encoded.codec == "zstd"

    external = store.store(encoded, endpoint="financial_statements", task_id=456)

    assert external.codec == "aesgcm+zstd"
    assert store.load(row_from_external(external)) == payload


def test_ciphertext_tamper_is_rejected_before_decryption():
    client = FakeS3()
    store = backend(client)
    external = store.store(encode_payload({"ok": True}), endpoint="profile", task_id=1)
    key = ("archive-test", external.object_key)
    client.objects[key]["body"] += b"tampered"

    with pytest.raises(OSError, match="ciphertext checksum mismatch"):
        store.load(row_from_external(external))


def test_plaintext_checksum_mismatch_is_rejected_after_authentication():
    client = FakeS3()
    store = backend(client)
    external = store.store(encode_payload({"ok": True}), endpoint="profile", task_id=1)
    row = row_from_external(external) | {"payload_sha256": "0" * 64}

    with pytest.raises(OSError, match="plaintext checksum mismatch"):
        store.load(row)


def test_wrong_key_id_is_rejected_without_exposing_key_material():
    client = FakeS3()
    store = backend(client)
    external = store.store(encode_payload({"ok": True}), endpoint="profile", task_id=1)
    row = row_from_external(external) | {"payload_key_id": "v2"}

    with pytest.raises(OSError, match="not available"):
        store.load(row)


def test_new_active_key_can_read_archives_from_previous_key_ring():
    client = FakeS3()
    old_store = backend(client, key=7, key_id="v1")
    payload = {"rotation": "keeps-old-archives-readable"}
    external = old_store.store(encode_payload(payload), endpoint="profile", task_id=8)

    new_store = backend(
        client,
        key=8,
        key_id="v2",
        decryption_keys={"v1": key_b64(7)},
    )

    assert new_store.load(row_from_external(external)) == payload
    new_external = new_store.store(encode_payload({"new": True}), endpoint="profile", task_id=9)
    assert new_external.key_id == "v2"


def test_encryption_key_must_be_exactly_256_bits():
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        S3ArchiveBackend(
            bucket="archive-test",
            prefix="dbd/test",
            encryption_key_b64=base64.urlsafe_b64encode(b"too-short").decode("ascii"),
            key_id="v1",
            client=FakeS3(),
        )
