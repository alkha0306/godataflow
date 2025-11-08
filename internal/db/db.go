package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Connect initializes a PostgreSQL connection
func Connect(dbURL string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("db connect failed: %w", err)
	}

	// connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)

	return db, nil
}
