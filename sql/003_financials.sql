USE dbd;

-- งบการเงิน. The source returns every submitted fiscal year in one response,
-- so both tables are keyed by (jp_no, fiscal_year) and replaced on refresh.

-- ประวัติการนำส่งงบการเงิน
CREATE TABLE IF NOT EXISTS financial_submissions (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  submit_year VARCHAR(8) NOT NULL,
  ac_date VARCHAR(8) NULL,
  submit_date DATE NULL,
  ac_code VARCHAR(32) NULL,
  au_code VARCHAR(32) NULL,
  status_code VARCHAR(8) NULL,
  update_date DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_financial_submissions (jp_no, submit_year),
  KEY idx_financial_submissions_raw (raw_response_id),
  CONSTRAINT fk_financial_submissions_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- One row per fiscal year: the header plus the single-valued ratio metrics.
CREATE TABLE IF NOT EXISTS financial_statements (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  fiscal_year VARCHAR(8) NOT NULL,
  fiscal_year_end_date DATE NULL,
  submit_obj_code VARCHAR(20) NULL,
  business_size_code VARCHAR(4) NULL,
  business_size_desc VARCHAR(191) NULL,
  current_ratio DECIMAL(20,4) NULL,
  debt_to_asset DECIMAL(20,4) NULL,
  debt_to_equity DECIMAL(20,4) NULL,
  debt_to_capital DECIMAL(20,4) NULL,
  total_asset_to_equity DECIMAL(20,4) NULL,
  total_asset_turnover DECIMAL(20,4) NULL,
  inventory_turnover DECIMAL(20,4) NULL,
  account_receivable_turnover DECIMAL(20,4) NULL,
  account_payable_turnover DECIMAL(20,4) NULL,
  gross_profit_margin DECIMAL(20,4) NULL,
  operating_profit_margin DECIMAL(20,4) NULL,
  net_profit_margin DECIMAL(20,4) NULL,
  return_on_asset DECIMAL(20,4) NULL,
  return_on_equity DECIMAL(20,4) NULL,
  earning_per_share DECIMAL(20,4) NULL,
  cost_to_income DECIMAL(20,4) NULL,
  update_date DATE NULL,
  first_seen_at DATETIME(6) NOT NULL,
  last_seen_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_financial_statements (jp_no, fiscal_year),
  KEY idx_financial_statements_raw (raw_response_id),
  CONSTRAINT fk_financial_statements_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- The repeating statement line items. The source encodes each as a
-- base/base+Ly/base+Pc triplet (e.g. assets / assetsLy / assetsPc), so they
-- are stored tall rather than as ~150 columns. A line item DBD adds later
-- lands here automatically with no schema change.
CREATE TABLE IF NOT EXISTS financial_statement_items (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_response_id BIGINT UNSIGNED NOT NULL,
  jp_no VARCHAR(20) NOT NULL,
  fiscal_year VARCHAR(8) NOT NULL,
  item_code VARCHAR(64) NOT NULL,
  amount DECIMAL(24,4) NULL,
  amount_ly DECIMAL(24,4) NULL,
  pct_change DECIMAL(20,4) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_financial_statement_items (jp_no, fiscal_year, item_code),
  KEY idx_financial_statement_items_raw (raw_response_id),
  KEY idx_financial_statement_items_code (item_code),
  CONSTRAINT fk_financial_statement_items_raw
    FOREIGN KEY (raw_response_id) REFERENCES raw_responses(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
