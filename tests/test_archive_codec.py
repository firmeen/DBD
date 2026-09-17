from dbd.storage.archive_codec import decode_payload, encode_payload


def test_small_archive_stays_json_and_round_trips():
    payload = {"jpNo": "0100000000000", "rows": [1, 2, 3], "ok": True}
    encoded = encode_payload(payload, threshold=4096)

    assert encoded.codec == "json"
    assert encoded.compressed is None
    assert encoded.original_bytes == encoded.stored_bytes
    assert decode_payload(
        payload=encoded.json_text,
        payload_zstd=encoded.compressed,
        codec=encoded.codec,
    ) == payload


def test_large_archive_uses_zstd_when_smaller_and_round_trips():
    payload = {"rows": [{"name": "synthetic-" * 80, "value": index} for index in range(200)]}
    encoded = encode_payload(payload, threshold=1)

    assert encoded.codec == "zstd"
    assert encoded.compressed is not None
    assert encoded.stored_bytes < encoded.original_bytes
    assert len(encoded.sha256_hex) == 64
    assert decode_payload(
        payload=encoded.json_text,
        payload_zstd=encoded.compressed,
        codec=encoded.codec,
    ) == payload
