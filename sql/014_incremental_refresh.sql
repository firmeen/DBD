-- Reference DDL for exact delta + stale-aware fabric refresh planning.
-- Alembic revision 0010_incremental_refresh is authoritative and adoption-safe.

ALTER TABLE worker_runs
  ADD COLUMN force_refresh_mask BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER scheduler_mode;

ALTER TABLE company_master
  ADD COLUMN master_components_json JSON NULL AFTER master_hash_version,
  ADD COLUMN master_observed_at DATETIME(6) NULL AFTER master_components_json,
  ADD COLUMN pending_dirty_mask BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER change_mask;

ALTER TABLE company_collection_state
  ADD COLUMN change_dirty_mask BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER dirty_mask,
  ADD COLUMN stale_dirty_mask BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER change_dirty_mask,
  ADD COLUMN forced_dirty_mask BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER stale_dirty_mask;
