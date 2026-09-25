-- Reference DDL for resilient fabric upstream recovery.
-- Alembic revision 0011_fabric_recovery is authoritative and adoption-safe.

ALTER TABLE worker_runs
  ADD COLUMN parent_run_id BIGINT UNSIGNED NULL AFTER force_refresh_mask,
  ADD COLUMN recovery_kind VARCHAR(32) NULL AFTER parent_run_id;

CREATE INDEX idx_worker_runs_parent
  ON worker_runs(parent_run_id,id);

CREATE TABLE collection_failure_events (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  run_id BIGINT UNSIGNED NOT NULL,
  company_id BIGINT UNSIGNED NOT NULL,
  endpoint VARCHAR(64) NOT NULL,
  stage VARCHAR(16) NOT NULL,
  outcome VARCHAR(32) NOT NULL,
  error_code VARCHAR(100) NOT NULL,
  error_message VARCHAR(500) NOT NULL,
  http_status SMALLINT UNSIGNED NULL,
  attempt INT UNSIGNED NOT NULL DEFAULT 0,
  archive_id BIGINT UNSIGNED NULL,
  occurred_at DATETIME(6) NOT NULL,
  KEY idx_failure_event_run (run_id,outcome,id),
  KEY idx_failure_event_company (company_id,endpoint,id),
  KEY idx_failure_event_error (run_id,error_code,http_status,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
