USE dbd;

-- ----------------------------------------------------------------------------
-- Bulk discovery state.
--
-- A "bucket" is one search query (keyword + filters). Because the backend
-- caps a query at 10,000 rows (1000 pages x 10), any bucket whose totalItems
-- exceeds that must be split by a filter dimension before it can be fully
-- read. A parent bucket is only trusted as lossless when
-- SUM(children.total_items) == parent.total_items, which is recorded here
-- rather than assumed.
--
-- Enumeration completeness is deliberately NOT claimed: search matches
-- broadly, so buckets can overlap. Companies are deduplicated on
-- (jp_type_code, jp_no) and coverage is reported from what was actually seen.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS discovery_buckets (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  run_id BIGINT UNSIGNED NULL,
  parent_id BIGINT UNSIGNED NULL,
  bucket_key VARCHAR(500) NOT NULL,
  keyword VARCHAR(191) NOT NULL,
  search_type VARCHAR(16) NOT NULL DEFAULT '',
  pv_codes VARCHAR(500) NOT NULL DEFAULT '',
  jp_statuses VARCHAR(255) NOT NULL DEFAULT '',
  jp_types VARCHAR(255) NOT NULL DEFAULT '',
  business_sizes VARCHAR(255) NOT NULL DEFAULT '',
  depth INT UNSIGNED NOT NULL DEFAULT 0,
  total_items INT UNSIGNED NULL,
  pages_total INT UNSIGNED NULL,
  pages_done INT UNSIGNED NOT NULL DEFAULT 0,
  rows_seen INT UNSIGNED NOT NULL DEFAULT 0,
  -- counted: size known. split: too big, replaced by children.
  -- draining: pages being read. done: fully read. failed: gave up.
  status ENUM('pending','counted','split','draining','done','failed') NOT NULL DEFAULT 'pending',
  -- NULL until a split is validated; 0 means SUM(children) != parent.
  partition_lossless TINYINT(1) NULL,
  last_error TEXT NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_discovery_buckets (bucket_key),
  KEY idx_discovery_buckets_status (status),
  KEY idx_discovery_buckets_parent (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Which bucket first produced each company. Kept separate from `companies`
-- so re-running discovery never rewrites the company projection itself.
CREATE TABLE IF NOT EXISTS discovery_sightings (
  jp_no VARCHAR(20) NOT NULL,
  jp_type_code VARCHAR(4) NOT NULL,
  first_bucket_id BIGINT UNSIGNED NULL,
  times_seen INT UNSIGNED NOT NULL DEFAULT 1,
  first_seen_at DATETIME(6) NOT NULL,
  last_seen_at DATETIME(6) NOT NULL,
  PRIMARY KEY (jp_no),
  KEY idx_discovery_sightings_bucket (first_bucket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
