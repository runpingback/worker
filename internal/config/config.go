package config

import "time"

type Config struct {
	DatabaseURL     string        `env:"DATABASE_URL,required"`
	PollInterval    time.Duration `env:"POLL_INTERVAL" envDefault:"500ms"`
	PollBatchSize   int           `env:"POLL_BATCH_SIZE" envDefault:"20"`
	WorkerPoolSize  int           `env:"WORKER_POOL_SIZE" envDefault:"20"`
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"30s"`
}
