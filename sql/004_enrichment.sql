USE dbd;

-- ----------------------------------------------------------------------------
-- 1. Decoded labels that were already in the company-profile payload.
--
-- The profile response nests these inside objects (businessType, setupObjType,
-- submitObjType, locationAmpur, locationTumbon) which the first version skipped
-- because it only promoted top-level scalars. No lookup tables or extra API
-- calls are needed: "65110" -> "การประกันชีวิต" and "D" -> "บริการ" ship with
-- every profile response.
-- ----------------------------------------------------------------------------
ALTER TABLE companies
  ADD COLUMN business_type_code   VARCHAR(8)   NULL AFTER business_size_code,
  ADD COLUMN business_type_desc   VARCHAR(191) NULL AFTER business_type_code,
  ADD COLUMN setup_obj_desc       VARCHAR(500) NULL AFTER setup_obj_code,
  ADD COLUMN setup_obj_grp_code   VARCHAR(8)   NULL AFTER setup_obj_desc,
  ADD COLUMN setup_obj_grp_desc   VARCHAR(191) NULL AFTER setup_obj_grp_code,
  ADD COLUMN submit_obj_code      VARCHAR(20)  NULL AFTER setup_obj_name_keyin,
  ADD COLUMN submit_obj_desc      VARCHAR(500) NULL AFTER submit_obj_code,
  ADD COLUMN submit_obj_name_keyin VARCHAR(500) NULL AFTER submit_obj_desc,
  ADD COLUMN ampur_desc           VARCHAR(191) NULL AFTER ampur_code,
  ADD COLUMN tumbon_desc          VARCHAR(191) NULL AFTER tumbon_code;

-- ----------------------------------------------------------------------------
-- 2. ข้อมูลการเปลี่ยนแปลงสถานะ — แปรสภาพ / ควบรวม
--    Web page tab 1, bottom card ("นิติบุคคลนี้แปรสภาพมาจาก ...").
--    One response carries four directions, so `direction` distinguishes them
--    rather than splitting into four tables.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_transformations (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  direction ENUM('transform_from','transform_to','merger_from','merger_to') NOT NULL,
  seq_no INT NOT NULL,
  jp_name VARCHAR(500) NULL,
  related_jp_no VARCHAR(20) NULL,
  related_jp_type_code VARCHAR(4) NULL,
  related_jp_name VARCHAR(500) NULL,
  change_date DATE NULL,
  update_date DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_company_transformations (jp_no, direction, seq_no),
  KEY idx_company_transformations_related (related_jp_no),
  KEY idx_company_transformations_raw (raw_response_id),
  CONSTRAINT fk_company_transformations_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------------------------------------------------------
-- 3. กราฟสัดส่วนสัญชาติย้อนหลัง 5 ปี
--    Web page tab 3, the bar chart. company_nations holds only the current
--    snapshot, while this holds the historical series behind the chart.
--    nation_group: TH = ไทย, OTHER = ต่างชาติ.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_nation_history (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  fiscal_year VARCHAR(8) NOT NULL,
  nation_group ENUM('TH','OTHER') NOT NULL,
  share_amt DECIMAL(24,2) NULL,
  share_pct DECIMAL(10,4) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_company_nation_history (jp_no, fiscal_year, nation_group),
  KEY idx_company_nation_history_raw (raw_response_id),
  CONSTRAINT fk_company_nation_history_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------------------------------------------------------
-- 4. ค่าเฉลี่ยเปรียบเทียบในประเภทธุรกิจ (industry benchmark)
--    Web page tab 2, toggle "เปรียบเทียบในประเภทธุรกิจ".
--
--    Keyed by INDUSTRY, not by company: the request is made per company but the
--    answer describes the whole TSIC group, so many companies collapse onto one
--    row instead of storing duplicates per company.
--
--    Suffix meanings on the source: A = average, T = industry total,
--    Ly = last year's average, Pc = % change.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS industry_benchmarks (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  submit_obj_code VARCHAR(20) NOT NULL,
  fiscal_year VARCHAR(8) NOT NULL,
  business_size_code VARCHAR(4) NOT NULL,
  location_type VARCHAR(8) NOT NULL,
  location_no VARCHAR(8) NOT NULL,
  submit_fin_cnt INT NULL,
  profit_cnt INT NULL,
  loss_cnt INT NULL,
  update_date DATE NULL,
  first_seen_at DATETIME(6) NOT NULL,
  last_seen_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_industry_benchmarks (submit_obj_code, fiscal_year, business_size_code, location_type, location_no),
  KEY idx_industry_benchmarks_raw (raw_response_id),
  CONSTRAINT fk_industry_benchmarks_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS industry_benchmark_items (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  benchmark_id BIGINT UNSIGNED NOT NULL,
  item_code VARCHAR(64) NOT NULL,
  avg_amount DECIMAL(24,4) NULL,
  avg_amount_ly DECIMAL(24,4) NULL,
  pct_change DECIMAL(20,4) NULL,
  industry_total DECIMAL(28,4) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_industry_benchmark_items (benchmark_id, item_code),
  KEY idx_industry_benchmark_items_code (item_code),
  CONSTRAINT fk_industry_benchmark_items_bm
    FOREIGN KEY (benchmark_id) REFERENCES industry_benchmarks(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
