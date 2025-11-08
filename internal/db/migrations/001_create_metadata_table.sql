-- db/migrations/001_create_metadata_table.sql

CREATE TABLE IF NOT EXISTS table_metadata (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    table_type TEXT NOT NULL,      -- e.g., "normal", "time_series"
    refresh_interval INT,          -- in seconds, nullable for normal tables
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
