-- Create table_metadata with full ETL support
CREATE TABLE IF NOT EXISTS table_metadata (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    table_type TEXT NOT NULL,          -- "normal" or "time_series"
    refresh_interval INT,              -- seconds, null for normal tables
    data_source_url TEXT,              -- API endpoint for auto-refresh

    -- ETL tracking fields
    last_refresh_success TIMESTAMP,
    last_refresh_error TEXT,
    status TEXT DEFAULT 'OK',          -- "OK" or "ERROR"

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
