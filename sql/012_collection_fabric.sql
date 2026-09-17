-- Applied through Alembic 0008_collection_fabric.
-- Kept as an inspectable schema reference for operators and review.

ALTER TABLE worker_runs
  ADD COLUMN scheduler_mode VARCHAR(16) NOT NULL DEFAULT 'legacy' AFTER fiscal_year;

ALTER TABLE company_master
  ADD COLUMN master_hash CHAR(64) NULL AFTER observed_status_at,
  ADD COLUMN last_master_changed_at DATETIME(6) NULL AFTER master_hash,
  ADD COLUMN last_seen_campaign BIGINT UNSIGNED NULL AFTER last_master_changed_at,
  ADD COLUMN latest_search_archive_id BIGINT UNSIGNED NULL AFTER last_seen_campaign;

ALTER TABLE companies
  ADD COLUMN latest_archive_id BIGINT UNSIGNED NULL AFTER latest_raw_response_id;

ALTER TABLE response_archive
  ADD COLUMN run_id BIGINT UNSIGNED NULL AFTER task_id;

CREATE TABLE IF NOT EXISTS company_collection_state (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  run_id BIGINT UNSIGNED NOT NULL,
  company_id BIGINT UNSIGNED NOT NULL,
  required_mask BIGINT UNSIGNED NOT NULL,
  done_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
  not_available_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
  failed_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
  dirty_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
  next_endpoint VARCHAR(64) NULL,
  fiscal_year VARCHAR(8) NULL,
  master_hash CHAR(64) NULL,
  stage ENUM('fetch','project') NOT NULL DEFAULT 'fetch',
  status ENUM('pending','running','retry_wait','done','failed') NOT NULL DEFAULT 'pending',
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  project_attempts INT UNSIGNED NOT NULL DEFAULT 0,
  available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  lease_owner VARCHAR(64) NULL,
  lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  lease_expires_at DATETIME(6) NULL,
  raw_response_id BIGINT UNSIGNED NULL,
  last_error VARCHAR(500) NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  finished_at DATETIME(6) NULL,
  UNIQUE KEY uq_collection_state (run_id, company_id),
  KEY idx_collection_claim (run_id, stage, status, available_at, id),
  KEY idx_collection_endpoint (run_id, next_endpoint, status, available_at, id),
  KEY idx_collection_lease (lease_owner, status, lease_expires_at),
  CONSTRAINT fk_collection_run FOREIGN KEY (run_id) REFERENCES worker_runs(id),
  CONSTRAINT fk_collection_company FOREIGN KEY (company_id) REFERENCES company_master(company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_archive_run_processing
  ON response_archive(run_id,processing_status,id);
CREATE INDEX idx_master_hash ON company_master(master_hash);
