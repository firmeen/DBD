-- Reference DDL for fabric_v2 scheduler activation.
-- Alembic revision 0009_fabric_scheduler is authoritative and adoption-safe.

CREATE INDEX idx_collection_active
  ON company_collection_state(run_id,status,id);
