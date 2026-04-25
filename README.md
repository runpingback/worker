# Pingback Worker

Standalone Go binary that processes Pingback job executions. Replaces the NestJS worker.

Polls the pgboss queue directly via PostgreSQL, dispatches HMAC-signed HTTP requests to user endpoints, handles retries with exponential backoff, fan-out task dispatch, and plan limit enforcement.

## Setup

```bash
cp .env.example .env
# edit DATABASE_URL in .env
```

## Run

```bash
go run ./cmd/worker/
```

The worker auto-loads `.env` on startup.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *required* | PostgreSQL connection string |
| `POLL_INTERVAL` | `500ms` | How often to poll pgboss for new jobs |
| `POLL_BATCH_SIZE` | `20` | Max jobs to claim per poll |
| `WORKER_POOL_SIZE` | `20` | Number of concurrent worker goroutines |
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | `30s` | Max time to wait for in-flight jobs on shutdown |

## Architecture

```
Poller (500ms) ──→ chan Job ──→ Worker Pool (N goroutines)
     │                              │
     └────────── PostgreSQL ────────┘
```

**Poller** claims jobs from `pgboss.job` via `SELECT ... FOR UPDATE SKIP LOCKED`. **Worker pool** goroutines process jobs concurrently: HMAC-signed HTTP dispatch, execution tracking, retries, fan-out, and alert events.

Multiple instances can run safely — `SKIP LOCKED` prevents duplicate processing.

## How It Works

1. **Scheduler** (NestJS, unchanged) creates pending executions and enqueues them to pgboss
2. **Worker** (this binary) claims jobs, POSTs to user endpoints with HMAC-SHA256 signatures
3. On **success**: marks execution complete, dispatches fan-out child tasks
4. On **failure**: retries with exponential backoff (`2^attempt` seconds, max 60s)
5. On **permanent failure**: marks failed, enqueues alert evaluation event for NestJS to process

## Tests

```bash
go test ./... -v
```

## Project Structure

```
cmd/worker/main.go           entrypoint, wiring, graceful shutdown
internal/
  config/config.go            env-based configuration
  pgboss/pgboss.go            pgboss SQL: claim, complete, insert
  pipeline/
    pool.go                   goroutine worker pool
    poller.go                 pgboss poller + adapter
  processor/
    processor.go              job processing orchestration
    dispatch.go               HMAC signing + HTTP dispatch
    fanout.go                 child task dispatch
  db/
    executions.go             execution CRUD + types
    users.go                  quota management
    projects.go               project lookup
  limits/
    limits.go                 plan limits + enforcement
```
