CREATE TABLE IF NOT EXISTS saved_queries (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,          -- descriptive name for the query
    sql_text TEXT NOT NULL,             -- the actual SQL query to run
    description TEXT,                   -- optional description
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
