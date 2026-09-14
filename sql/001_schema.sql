CREATE DATABASE IF NOT EXISTS dbd
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE dbd;

-- Append-only source of truth. Every fetch is stored verbatim (decrypted,
-- pre-normalization) so parsing/mapping logic can change later without
-- ever needing to re-hit DBD's servers.
CREATE TABLE IF NOT EXISTS raw_responses (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  jp_type_code VARCHAR(4) NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  endpoint VARCHAR(64) NOT NULL,
  request_path VARCHAR(255) NOT NULL,
  requested_at DATETIME(6) NOT NULL,
  http_status SMALLINT UNSIGNED NOT NULL,
  response_body_sha256 CHAR(64) NOT NULL,
  raw_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_raw_responses_jp (jp_type_code, jp_no, endpoint),
  KEY idx_raw_responses_sha (response_body_sha256)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Latest normalized projection, one row per juristic person.
CREATE TABLE IF NOT EXISTS companies (
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  jp_name VARCHAR(500) NULL,
  jp_name_e VARCHAR(500) NULL,
  jp_search_name VARCHAR(500) NULL,
  jp_stat_code VARCHAR(4) NULL,
  jp_stat_desc VARCHAR(255) NULL,
  jp_type_desc VARCHAR(255) NULL,
  address TEXT NULL,
  address_e TEXT NULL,
  ampur_code VARCHAR(10) NULL,
  tumbon_code VARCHAR(10) NULL,
  pv_code VARCHAR(4) NULL,
  pv_desc VARCHAR(255) NULL,
  zip_code VARCHAR(10) NULL,
  phone_no VARCHAR(50) NULL,
  email VARCHAR(255) NULL,
  website_1 VARCHAR(255) NULL,
  website_2 VARCHAR(255) NULL,
  website_3 VARCHAR(255) NULL,
  website_4 VARCHAR(255) NULL,
  reg_date DATE NULL,
  stat_change_date DATE NULL,
  update_date DATE NULL,
  cap_amt DECIMAL(20,2) NULL,
  paid_amt DECIMAL(20,2) NULL,
  share_qty DECIMAL(20,2) NULL,
  share_vol DECIMAL(20,2) NULL,
  business_size_code VARCHAR(4) NULL,
  setup_obj_code VARCHAR(20) NULL,
  setup_obj_name_keyin VARCHAR(500) NULL,
  total_asset DECIMAL(20,2) NULL,
  total_equity DECIMAL(20,2) NULL,
  total_income DECIMAL(20,2) NULL,
  net_profit DECIMAL(20,2) NULL,
  submit_fin_year VARCHAR(50) NULL,
  latest_raw_response_id BIGINT UNSIGNED NOT NULL,
  first_seen_at DATETIME(6) NOT NULL,
  last_seen_at DATETIME(6) NOT NULL,
  PRIMARY KEY (jp_no),
  KEY idx_companies_jp_name (jp_name(191)),
  KEY idx_companies_jp_name_e (jp_name_e(191)),
  CONSTRAINT fk_companies_latest_raw
    FOREIGN KEY (latest_raw_response_id) REFERENCES raw_responses(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Catch-all for every response field not promoted to a `companies` column.
-- Scoped to the raw_response that produced it (like creden's company_facts),
-- so a field DBD adds/renames later shows up here instead of being dropped.
CREATE TABLE IF NOT EXISTS company_facts (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  fact_key VARCHAR(191) NOT NULL,
  fact_value LONGTEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_company_facts_jp (jp_no),
  KEY idx_company_facts_raw (raw_response_id),
  KEY idx_company_facts_key (fact_key),
  CONSTRAINT fk_company_facts_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
