package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port        string
	DatabaseURL string
}

// Load reads .env file (if present) and returns config values
func Load() (*Config, error) {
	_ = godotenv.Load() // ignore error if no .env found

	port := os.Getenv("PORT")
	dbURL := os.Getenv("DATABASE_URL")

	if port == "" || dbURL == "" {
		return nil, errors.New("PORT or DATABASE_URL missing")
	}

	return &Config{
		Port:        port,
		DatabaseURL: dbURL,
	}, nil
}
