-- DBD campaign acceptance report (read-only apart from session variables).
-- MySQL 8+. Run after migrations. Set @run_id to a specific worker run or
-- leave NULL to use the latest run.

SET @run_id := NULL;
SET @selected_run := COALESCE(@run_id, (SELECT MAX(id) FROM worker_runs));

-- 0. Schema / selected campaign ------------------------------------------------
SELECT DATABASE() AS database_name,
       (SELECT version_num FROM alembic_version LIMIT 1) AS alembic_revision,
       @selected_run AS selected_run_id,
       UTC_TIMESTAMP(6) AS reported_at_utc;

SELECT id, source_label, parts, fiscal_year, status, created_at, started_at, finished_at
FROM worker_runs
WHERE id=@selected_run;

-- 1. Master denominator ---------------------------------------------------------
SELECT
  COUNT(*) AS master_total,
  SUM(identity_status='conflict') AS identity_conflicts,
  SUM(identity_status<>'conflict') AS eligible_all_statuses,
  SUM(identity_status<>'conflict' AND observed_jp_stat_code='1') AS eligible_operating,
  SUM(identity_status<>'conflict' AND observed_jp_stat_code IS NULL) AS unknown_status,
  MIN(first_seen_at) AS first_identity_seen,
  MAX(last_seen_at) AS last_identity_seen,
  MAX(observed_status_at) AS latest_status_observation
FROM company_master;

SELECT identity_status, COUNT(*) AS identities
FROM company_master
GROUP BY identity_status
ORDER BY identities DESC;

-- 2. Discovery blockers ---------------------------------------------------------
SELECT
  (SELECT COUNT(*) FROM discovery_sightings) AS unique_discovered,
  (SELECT COUNT(*) FROM discovery_buckets) AS buckets_total,
  (SELECT COUNT(*) FROM discovery_buckets WHERE status='done') AS buckets_done,
  (SELECT COUNT(*) FROM discovery_buckets WHERE status='split') AS buckets_split,
  (SELECT COUNT(*) FROM discovery_buckets WHERE status='failed') AS buckets_failed,
  (SELECT COUNT(*) FROM discovery_buckets
     WHERE status IN ('pending','counted','draining')) AS buckets_open,
  (SELECT COUNT(*) FROM discovery_buckets
     WHERE total_items IS NULL AND status IN ('pending','counted','draining')) AS buckets_uncounted,
  (SELECT COUNT(*) FROM discovery_buckets WHERE partition_lossless=0) AS lossy_partitions,
  (SELECT COUNT(*) FROM discovery_pages WHERE status='done') AS pages_done,
  (SELECT COUNT(*) FROM discovery_pages WHERE status='failed') AS pages_failed,
  (SELECT COUNT(*) FROM discovery_pages WHERE status='partial') AS pages_partial,
  (SELECT COUNT(*) FROM discovery_rejections WHERE resolved_at IS NULL) AS unresolved_rejections,
  (SELECT COALESCE(SUM(GREATEST(
      CAST(pages_total AS SIGNED)-CAST(pages_done AS SIGNED),0)),0)
     FROM discovery_buckets
     WHERE pages_total IS NOT NULL AND status IN ('counted','draining','failed'))
     AS known_pages_remaining;

SELECT id, parent_id, keyword, depth, total_items, pages_total, pages_done,
       rows_seen, status, partition_lossless, LEFT(last_error,300) AS last_error
FROM discovery_buckets
WHERE status='failed' OR partition_lossless=0
ORDER BY id;

SELECT bucket_id, page_no, row_index, jp_type_code, jp_no,
       error_code, error_message, first_seen_at, last_seen_at
FROM discovery_rejections
WHERE resolved_at IS NULL
ORDER BY bucket_id, page_no, row_index;

-- 3. Observed-prefix reconciliation aid ----------------------------------------
SELECT LEFT(s.jp_no,3) AS observed_prefix,
       COUNT(*) AS identities,
       MAX(EXISTS(
         SELECT 1 FROM discovery_buckets b WHERE b.keyword=LEFT(s.jp_no,3)
       )) AS has_matching_seed
FROM discovery_sightings s
GROUP BY LEFT(s.jp_no,3)
ORDER BY observed_prefix;

-- This section is only an observed-set check. It cannot reveal a company the
-- upstream search never returned. Reconcile unique_discovered against an
-- authoritative external DBD population before claiming full coverage.

-- 4. Endpoint queue completion --------------------------------------------------
SELECT endpoint, status, COUNT(*) AS tasks
FROM endpoint_tasks
WHERE run_id=@selected_run
GROUP BY endpoint, status
ORDER BY endpoint, status;

SELECT endpoint, last_error, COUNT(*) AS tasks
FROM endpoint_tasks
WHERE run_id=@selected_run AND status='failed'
GROUP BY endpoint, last_error
ORDER BY tasks DESC, endpoint;

SELECT COUNT(*) AS unfinished_tasks
FROM endpoint_tasks
WHERE run_id=@selected_run AND status IN ('pending','running','retry_wait');

SELECT COUNT(*) AS identity_conflict_tasks
FROM endpoint_tasks
WHERE run_id=@selected_run AND status='failed' AND last_error='IdentityConflict';

-- 5. Per-company seven-endpoint completeness -----------------------------------
-- A terminal task is done or source-confirmed not_available. Failed/conflict is
-- deliberately not counted complete.
SELECT
  COUNT(*) AS companies_in_run,
  SUM(terminal_count=7) AS companies_terminal_all_7,
  SUM(failed_count>0) AS companies_with_failure,
  SUM(unfinished_count>0) AS companies_unfinished
FROM (
  SELECT company_id,
         SUM(status IN ('done','not_available')) AS terminal_count,
         SUM(status='failed') AS failed_count,
         SUM(status IN ('pending','running','retry_wait')) AS unfinished_count
  FROM endpoint_tasks
  WHERE run_id=@selected_run
  GROUP BY company_id
) q;

-- If a non-default endpoint selection was used, compare against worker_runs.parts
-- instead of interpreting the fixed seven-endpoint count above as acceptance.

-- 6. Archive / replay health ----------------------------------------------------
SELECT processing_status, error_code, endpoint, COUNT(*) AS archives
FROM response_archive
GROUP BY processing_status, error_code, endpoint
ORDER BY processing_status, archives DESC;

SELECT endpoint, status, COUNT(*) AS request_scope_states,
       MIN(requested_at) AS oldest, MAX(requested_at) AS newest
FROM company_endpoint_state
GROUP BY endpoint, status
ORDER BY endpoint, status;

SELECT endpoint, COUNT(*) AS natural_write_keys,
       MIN(requested_at) AS oldest_write, MAX(requested_at) AS newest_write
FROM projection_write_state
GROUP BY endpoint
ORDER BY endpoint;

-- 7. Identity conflicts ---------------------------------------------------------
SELECT m.company_id, m.jp_type_code, m.jp_no, m.identity_status,
       m.first_seen_at, m.last_seen_at, m.last_verified_at,
       m.observed_jp_stat_code, m.observed_status_at,
       SUM(t.status='failed' AND t.last_error='IdentityConflict') AS conflict_tasks
FROM company_master m
LEFT JOIN endpoint_tasks t ON t.company_id=m.company_id
WHERE m.identity_status='conflict'
GROUP BY m.company_id, m.jp_type_code, m.jp_no, m.identity_status,
         m.first_seen_at, m.last_seen_at, m.last_verified_at,
         m.observed_jp_stat_code, m.observed_status_at
ORDER BY m.company_id;

-- 8. Storage footprint ----------------------------------------------------------
SELECT table_name,
       table_rows,
       ROUND(data_length/1024/1024,2) AS data_mb,
       ROUND(index_length/1024/1024,2) AS index_mb,
       ROUND((data_length+index_length)/1024/1024,2) AS total_mb
FROM information_schema.tables
WHERE table_schema=DATABASE()
ORDER BY data_length+index_length DESC;

SELECT
  (SELECT COUNT(*) FROM response_archive) AS archive_rows,
  (SELECT COALESCE(SUM(OCTET_LENGTH(payload)),0) FROM response_archive) AS archive_payload_bytes,
  (SELECT COUNT(*) FROM raw_responses) AS legacy_raw_rows,
  (SELECT COALESCE(SUM(OCTET_LENGTH(CAST(raw_json AS CHAR))),0) FROM raw_responses)
    AS legacy_raw_json_bytes;

-- 9. Final blockers -------------------------------------------------------------
SELECT 'discovery_failed_buckets' AS blocker, COUNT(*) AS n
FROM discovery_buckets WHERE status='failed'
UNION ALL
SELECT 'discovery_partial_pages', COUNT(*) FROM discovery_pages WHERE status='partial'
UNION ALL
SELECT 'unresolved_discovery_rejections', COUNT(*)
FROM discovery_rejections WHERE resolved_at IS NULL
UNION ALL
SELECT 'lossy_partitions', COUNT(*) FROM discovery_buckets WHERE partition_lossless=0
UNION ALL
SELECT 'run_unfinished_tasks', COUNT(*) FROM endpoint_tasks
WHERE run_id=@selected_run AND status IN ('pending','running','retry_wait')
UNION ALL
SELECT 'run_failed_non_conflict_tasks', COUNT(*) FROM endpoint_tasks
WHERE run_id=@selected_run AND status='failed' AND COALESCE(last_error,'')<>'IdentityConflict'
UNION ALL
SELECT 'run_identity_conflicts', COUNT(*) FROM endpoint_tasks
WHERE run_id=@selected_run AND status='failed' AND last_error='IdentityConflict';

-- A database-only report cannot validate the external population denominator or
-- the 48-hour wall-clock SLA. Record those separately in the campaign manifest.