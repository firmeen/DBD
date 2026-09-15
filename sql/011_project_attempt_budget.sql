-- Keep durable project retries independent from upstream fetch attempts.
ALTER TABLE endpoint_tasks
  ADD COLUMN project_attempts INT UNSIGNED NOT NULL DEFAULT 0 AFTER attempts;
