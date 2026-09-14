USE dbd;

-- ----------------------------------------------------------------------------
-- Resumable bulk ingestion state.
--
-- A run is a batch of company codes to ingest. Each code is one task whose
-- status is committed as it changes, so killing the process (Ctrl+C, crash,
-- lost network) loses at most the company in flight — restarting picks up
-- from the remaining work rather than starting over.
--
-- These are the only tables in this schema that describe *our* processing
-- rather than DBD's data.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_runs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  source_label VARCHAR(255) NOT NULL,
  parts VARCHAR(100) NOT NULL,
  total_tasks INT UNSIGNED NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL,
  finished_at DATETIME(6) NULL,
  PRIMARY KEY (id),
  KEY idx_ingestion_runs_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS ingestion_tasks (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  run_id BIGINT UNSIGNED NOT NULL,
  company_code VARCHAR(20) NOT NULL,
  status ENUM('pending','running','done','failed') NOT NULL DEFAULT 'pending',
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  started_at DATETIME(6) NULL,
  finished_at DATETIME(6) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_ingestion_tasks (run_id, company_code),
  KEY idx_ingestion_tasks_status (run_id, status),
  CONSTRAINT fk_ingestion_tasks_run
    FOREIGN KEY (run_id) REFERENCES ingestion_runs(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
