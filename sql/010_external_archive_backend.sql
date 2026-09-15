-- Optional encrypted external archive metadata. MySQL remains the source of truth.
ALTER TABLE response_archive
  ADD COLUMN payload_backend VARCHAR(16) NOT NULL DEFAULT 'mysql' AFTER payload_codec,
  ADD COLUMN payload_object_key VARCHAR(512) NULL AFTER payload_backend,
  ADD COLUMN payload_nonce VARBINARY(12) NULL AFTER payload_object_key,
  ADD COLUMN payload_key_id VARCHAR(64) NULL AFTER payload_nonce,
  ADD COLUMN payload_cipher_sha256 CHAR(64) NULL AFTER payload_key_id;
