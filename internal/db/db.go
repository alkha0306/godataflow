package db

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

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

// RunMigrations reads all SQL files in migrations folder and executes them
func RunMigrations(db *sqlx.DB) error {
	migrationsPath := "./db/migrations"
	files, err := os.ReadDir(migrationsPath)
	if err != nil {
		return fmt.Errorf("failed to read migrations folder: %w", err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sql" {
			path := filepath.Join(migrationsPath, file.Name())
			sqlBytes, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read migration %s: %w", file.Name(), err)
			}

			if _, err := db.Exec(string(sqlBytes)); err != nil {
				return fmt.Errorf("failed to execute migration %s: %w", file.Name(), err)
			}

			log.Printf("Applied migration: %s", file.Name())
		}
	}
	return nil
}
