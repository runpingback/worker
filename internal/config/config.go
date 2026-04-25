package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL     string
	PollInterval    time.Duration
	PollBatchSize   int
	WorkerPoolSize  int
	LogLevel        string
	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}

	return Config{
		DatabaseURL:     dbURL,
		PollInterval:    durationEnv("POLL_INTERVAL", 500*time.Millisecond),
		PollBatchSize:   intEnv("POLL_BATCH_SIZE", 20),
		WorkerPoolSize:  intEnv("WORKER_POOL_SIZE", 20),
		LogLevel:        stringEnv("LOG_LEVEL", "info"),
		ShutdownTimeout: durationEnv("SHUTDOWN_TIMEOUT", 30*time.Second),
	}, nil
}

func stringEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
