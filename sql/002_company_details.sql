USE dbd;

-- Child resources of a company profile. All are replace-on-refresh per
-- company: DBD is authoritative for current state, while raw_responses keeps
-- the append-only history of every fetch.

-- รายชื่อกรรมการ
CREATE TABLE IF NOT EXISTS company_committees (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  cmt_seq INT UNSIGNED NOT NULL,
  cmt_type_code VARCHAR(8) NULL,
  cmt_no VARCHAR(32) NULL,
  title_code VARCHAR(8) NULL,
  title_name VARCHAR(191) NULL,
  title_name_e VARCHAR(191) NULL,
  first_name VARCHAR(191) NULL,
  middle_name VARCHAR(191) NULL,
  last_name VARCHAR(191) NULL,
  first_name_e VARCHAR(191) NULL,
  middle_name_e VARCHAR(191) NULL,
  last_name_e VARCHAR(191) NULL,
  nt_code VARCHAR(8) NULL,
  invest_amt DECIMAL(20,2) NULL,
  invest_type VARCHAR(64) NULL,
  invest_type_e VARCHAR(64) NULL,
  address TEXT NULL,
  pv_code VARCHAR(8) NULL,
  ampur_code VARCHAR(10) NULL,
  tumbon_code VARCHAR(10) NULL,
  zip_code VARCHAR(10) NULL,
  phone_no VARCHAR(50) NULL,
  update_date DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_company_committees (jp_no, cmt_seq),
  KEY idx_company_committees_raw (raw_response_id),
  CONSTRAINT fk_company_committees_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- กรรมการลงชื่อผูกพัน
CREATE TABLE IF NOT EXISTS company_committee_signs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  sign_seq INT UNSIGNED NOT NULL,
  sign_description TEXT NULL,
  sign_description_e TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_company_committee_signs (jp_no, sign_seq),
  KEY idx_company_committee_signs_raw (raw_response_id),
  CONSTRAINT fk_company_committee_signs_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- การลงทุนจำแนกตามสัญชาติ
CREATE TABLE IF NOT EXISTS company_nations (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  nt_code VARCHAR(8) NOT NULL,
  country_fullname VARCHAR(191) NULL,
  country_fullname_e VARCHAR(191) NULL,
  share_amt DECIMAL(20,2) NULL,
  share_qty DECIMAL(20,2) NULL,
  share_vol DECIMAL(20,2) NULL,
  share_pct_qty DECIMAL(10,4) NULL,
  share_pct_vol DECIMAL(10,4) NULL,
  total_nation INT NULL,
  update_date DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_company_nations (jp_no, nt_code),
  KEY idx_company_nations_raw (raw_response_id),
  CONSTRAINT fk_company_nations_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ประวัติการเปลี่ยนชื่อนิติบุคคล
CREATE TABLE IF NOT EXISTS company_name_history (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  seq_no INT NOT NULL,
  change_date DATE NULL,
  jp_new_name VARCHAR(500) NULL,
  jp_new_name_e VARCHAR(500) NULL,
  seq_desc VARCHAR(191) NULL,
  seq_desc_e VARCHAR(191) NULL,
  update_date DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_company_name_history (jp_no, seq_no),
  KEY idx_company_name_history_raw (raw_response_id),
  CONSTRAINT fk_company_name_history_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ประวัติการเปลี่ยนแปลงทุนจดทะเบียน
CREATE TABLE IF NOT EXISTS company_capital_history (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  seq_no INT NOT NULL,
  change_date DATE NULL,
  inc_dec_flag VARCHAR(4) NULL,
  inc_dec_amt DECIMAL(20,2) NULL,
  previous_cap_amt DECIMAL(20,2) NULL,
  new_cap_amt DECIMAL(20,2) NULL,
  seq_desc VARCHAR(191) NULL,
  seq_desc_e VARCHAR(191) NULL,
  update_date DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_company_capital_history (jp_no, seq_no),
  KEY idx_company_capital_history_raw (raw_response_id),
  CONSTRAINT fk_company_capital_history_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
