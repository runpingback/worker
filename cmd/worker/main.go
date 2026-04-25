package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/runpingback/worker/internal/config"
	"github.com/runpingback/worker/internal/db"
	"github.com/runpingback/worker/internal/pgboss"
	"github.com/runpingback/worker/internal/pipeline"
	"github.com/runpingback/worker/internal/processor"
)

func main() {
	_ = godotenv.Load() // .env is optional — env vars can be set directly
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Setup logging
	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	slog.Info("worker starting",
		"pool_size", cfg.WorkerPoolSize,
		"poll_interval", cfg.PollInterval,
		"batch_size", cfg.PollBatchSize,
	)

	// Context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Postgres connection pool
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to database")

	// Initialize components
	pgbossClient := pgboss.New(pool)
	store := db.NewStore(pool)
	dispatcher := &processor.Dispatcher{Client: &http.Client{}}
	proc := processor.New(pgbossClient, store, dispatcher)

	// Handler bridges pipeline.Job -> processor.Process
	handler := func(ctx context.Context, pgbossJobID string, data []byte) {
		var msg processor.QueueMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Error("failed to unmarshal job data", "pgboss_job_id", pgbossJobID, "error", err)
			_ = pgbossClient.Complete(ctx, "pingback-execution", pgbossJobID, nil)
			return
		}

		if err := proc.Process(ctx, pgbossJobID, msg); err != nil {
			slog.Error("process error", "execution_id", msg.ExecutionID, "error", err)
		}
	}

	// Pipeline
	jobs := make(chan pipeline.Job, cfg.WorkerPoolSize)
	adapter := pipeline.NewPgBossAdapter(pgbossClient)
	poller := pipeline.NewPoller(adapter, "pingback-execution", cfg.PollBatchSize, cfg.PollInterval, jobs)
	workerPool := pipeline.NewPool(cfg.WorkerPoolSize, jobs, handler)

	// Start pool first, then poller
	workerPool.Start(ctx)
	go poller.Start(ctx)

	slog.Info("worker running")

	// Wait for shutdown signal
	<-sigCh
	slog.Info("shutting down")

	// Cancel context to stop poller
	cancel()

	// Close jobs channel so pool drains
	close(jobs)

	// Wait for pool to finish with timeout
	done := make(chan struct{})
	go func() {
		workerPool.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("shutdown complete")
	case <-time.After(cfg.ShutdownTimeout):
		slog.Warn("shutdown timed out, forcing exit")
	}
}
