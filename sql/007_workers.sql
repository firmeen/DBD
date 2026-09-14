-- Applied through Alembic. Existing projections are preserved.
CREATE TABLE IF NOT EXISTS company_master (
  company_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  identity_status ENUM('discovered','verified','conflict') NOT NULL DEFAULT 'discovered',
  first_seen_at DATETIME(6) NOT NULL,
  last_seen_at DATETIME(6) NOT NULL,
  last_verified_at DATETIME(6) NULL,
  UNIQUE KEY uq_master_identity (jp_type_code, jp_no),
  KEY idx_master_number (jp_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS worker_runs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  source_label VARCHAR(255) NOT NULL,
  parts VARCHAR(100) NOT NULL,
  fiscal_year VARCHAR(8) NULL,
  status ENUM('building','ready','finished','failed') NOT NULL DEFAULT 'building',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  finished_at DATETIME(6) NULL,
  KEY idx_worker_runs_status (status, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS endpoint_tasks (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  run_id BIGINT UNSIGNED NOT NULL,
  company_id BIGINT UNSIGNED NOT NULL,
  endpoint VARCHAR(64) NOT NULL,
  status ENUM('pending','running','retry_wait','done','failed','not_available') NOT NULL DEFAULT 'pending',
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  lease_owner VARCHAR(64) NULL,
  lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  lease_expires_at DATETIME(6) NULL,
  raw_response_id BIGINT UNSIGNED NULL,
  last_error VARCHAR(500) NULL,
  finished_at DATETIME(6) NULL,
  UNIQUE KEY uq_endpoint_task (run_id, company_id, endpoint),
  KEY idx_task_claim (run_id, status, available_at, id),
  KEY idx_task_lease (run_id, status, lease_expires_at),
  FOREIGN KEY (run_id) REFERENCES worker_runs(id),
  FOREIGN KEY (company_id) REFERENCES company_master(company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS response_archive (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  task_id BIGINT UNSIGNED NULL,
  company_id BIGINT UNSIGNED NULL,
  endpoint VARCHAR(64) NOT NULL,
  request_path VARCHAR(255) NOT NULL,
  request_context JSON NOT NULL,
  http_status SMALLINT UNSIGNED NOT NULL,
  requested_at DATETIME(6) NOT NULL,
  received_at DATETIME(6) NOT NULL,
  payload LONGTEXT NOT NULL,
  payload_sha256 CHAR(64) NOT NULL,
  processing_status ENUM('pending','processed','invalid','failed','not_available') NOT NULL DEFAULT 'pending',
  error_code VARCHAR(100) NULL,
  KEY idx_archive_task (task_id, id),
  KEY idx_archive_processing (processing_status, id),
  KEY idx_archive_company (company_id, endpoint, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS company_endpoint_state (
  company_id BIGINT UNSIGNED NOT NULL,
  endpoint VARCHAR(64) NOT NULL,
  scope_key VARCHAR(32) NOT NULL DEFAULT '',
  requested_at DATETIME(6) NOT NULL,
  archive_id BIGINT UNSIGNED NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (company_id, endpoint, scope_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS discovery_pages (
  bucket_id BIGINT UNSIGNED NOT NULL,
  page_no INT UNSIGNED NOT NULL,
  status ENUM('pending','done','failed') NOT NULL DEFAULT 'pending',
  rows_written INT UNSIGNED NOT NULL DEFAULT 0,
  archive_id BIGINT UNSIGNED NULL,
  last_error VARCHAR(255) NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (bucket_id, page_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Existing companies remain keyed as before. The master provides a stable
-- surrogate identity for new tasks, and conflicts are quarantined at projection.
INSERT INTO company_master (jp_no, jp_type_code, first_seen_at, last_seen_at)
SELECT jp_no, jp_type_code, first_seen_at, last_seen_at FROM companies
ON DUPLICATE KEY UPDATE last_seen_at=GREATEST(company_master.last_seen_at, VALUES(last_seen_at));
