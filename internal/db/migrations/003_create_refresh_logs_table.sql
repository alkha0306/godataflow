CREATE TABLE IF NOT EXISTS refresh_logs (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,        -- OK or ERROR
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
