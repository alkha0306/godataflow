ALTER TABLE table_metadata
ADD COLUMN IF NOT EXISTS mapping_json JSONB;