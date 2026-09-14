# Operations

The real deployment uses MySQL port 3307. Port 3317 was an isolated local test
server and is not an application setting. `.env` is private and remains untracked.

## Configuration

| Setting | Purpose |
| --- | --- |
| DBD_CONCURRENCY | HTTP concurrency; start at 16, maximum supported override 256 |
| DBD_RATE_PER_SECOND | Process-wide paced admission rate |
| DB_POOL_SIZE | Bound each database pool and projection writers |
| DBD_QUEUE_SIZE | Maximum number of simultaneous endpoint tasks |
| DBD_WRITE_BATCH_SIZE | Identity/task import batch size |
| DBD_LEASE_SECONDS | Lease duration; heartbeat at most every 10 seconds |
| DBD_HTTP_RETRIES | Retries after the initial HTTP attempt |
| DBD_MAX_TASK_ATTEMPTS | Total attempts before failed |
| DBD_METRICS_PORT | 0 disables; nonzero binds localhost |
| DBD_ENV_FILE | Absolute alternative configuration path |

If DBD starts returning 429/503, the shared limiter reduces rate. Watch successful
request rate, HTTP latency, endpoint outcomes and queue backlog before increasing
concurrency. A large concurrency value cannot remove the source server's limits.

## Failure inspection

```sql
SELECT t.id,t.endpoint,t.status,t.attempts,t.last_error,t.raw_response_id,
       m.jp_no,m.jp_type_code
FROM endpoint_tasks t JOIN company_master m USING(company_id)
WHERE t.run_id=1 AND t.status='failed';

SELECT id,endpoint,processing_status,error_code,requested_at
FROM response_archive WHERE processing_status IN ('invalid','failed');
```

`endpoint_tasks.raw_response_id` refers to `response_archive.id`; projection table
raw IDs still refer to legacy `raw_responses.id`. A failed task may have an archived
payload even when its raw_response_id is null; join archive by task_id in that case.

Run `bulk-retry RUN_ID` then `bulk --run-id RUN_ID`, or `replay ARCHIVE_ID` after
fixing a normalizer. Raw replay does not request the source again. An old snapshot
cannot overwrite a newer endpoint state. Interruption of an import leaves its run
building; create a new run from the input and retain the incomplete run for audit.

## Growth

Keep active and historical queue volume under observation. The heartbeat index is
on `(lease_owner,status)`, and claim uses `(run_id,status,available_at,id)`.
Do not turn on millions of labels in metrics: company IDs stay in logs/SQL.
Archive retention and partitioning should be chosen from measured payload volume
and recovery requirements; this release does not automatically delete raw data.

Back up MySQL before schema maintenance. Migrations are additive and forward-only;
restoring a backup is the supported rollback path. Avoid editing historic migration
files after release. Install from requirements.lock constraints for reproducibility.
