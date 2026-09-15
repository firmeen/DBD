-- Correctness guards added after the durable worker/discovery schema.
-- Forward-only and data-preserving: no existing projection rows are deleted.

ALTER TABLE discovery_pages
  MODIFY COLUMN status ENUM('pending','done','failed','partial') NOT NULL DEFAULT 'pending',
  ADD COLUMN rejected_rows INT UNSIGNED NOT NULL DEFAULT 0 AFTER rows_written;

CREATE TABLE IF NOT EXISTS discovery_rejections (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  bucket_id BIGINT UNSIGNED NOT NULL,
  page_no INT UNSIGNED NOT NULL,
  row_index INT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NULL,
  jp_type_code VARCHAR(4) NULL,
  error_code VARCHAR(100) NOT NULL,
  error_message VARCHAR(500) NOT NULL,
  payload LONGTEXT NOT NULL,
  payload_sha256 CHAR(64) NOT NULL,
  first_seen_at DATETIME(6) NOT NULL,
  last_seen_at DATETIME(6) NOT NULL,
  resolved_at DATETIME(6) NULL,
  UNIQUE KEY uq_discovery_rejection (bucket_id, page_no, row_index, payload_sha256),
  KEY idx_discovery_rejection_open (resolved_at, bucket_id, page_no),
  KEY idx_discovery_rejection_identity (jp_type_code, jp_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS projection_write_state (
  write_key VARCHAR(512) NOT NULL PRIMARY KEY,
  requested_at DATETIME(6) NOT NULL,
  archive_id BIGINT UNSIGNED NOT NULL,
  endpoint VARCHAR(64) NOT NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_projection_write_state_endpoint (endpoint, requested_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
