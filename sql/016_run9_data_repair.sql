-- Run 9 data repair reference migration.
-- Existing databases are changed by Alembic revision 0012_run9_data_repair,
-- which verifies the live column type and refuses to proceed when the column
-- participates in an index or generated-column dependency.
ALTER TABLE companies
  MODIFY COLUMN setup_obj_name_keyin TEXT NULL;
