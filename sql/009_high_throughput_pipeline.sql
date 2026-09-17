-- High-throughput V2: durable fetch/project staging and compressed raw archives.
ALTER TABLE endpoint_tasks
  ADD COLUMN stage ENUM('fetch','project') NOT NULL DEFAULT 'fetch' AFTER status,
  ADD COLUMN fetched_at DATETIME(6) NULL AFTER raw_response_id;

CREATE INDEX idx_task_stage_claim
  ON endpoint_tasks(run_id, stage, status, available_at, id);

ALTER TABLE response_archive
  ADD COLUMN payload_codec VARCHAR(16) NOT NULL DEFAULT 'json' AFTER payload,
  ADD COLUMN payload_zstd LONGBLOB NULL AFTER payload_codec,
  ADD COLUMN payload_original_bytes BIGINT UNSIGNED NULL AFTER payload_zstd,
  ADD COLUMN payload_stored_bytes BIGINT UNSIGNED NULL AFTER payload_original_bytes;
