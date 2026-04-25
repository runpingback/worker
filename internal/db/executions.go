package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pgx connection pool for database operations.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Types shared across db queries.

type ProjectUser struct {
	ProjectID           string
	EndpointURL         string
	CronSecret          string
	UserID              string
	Plan                string
	ExecutionsThisMonth int
	ExecutionsResetAt   *time.Time
}

type JobRecord struct {
	ID             string
	ProjectID      string
	Name           string
	Retries        int
	TimeoutSeconds int
}

type LogEntry struct {
	Timestamp int64           `json:"timestamp"`
	Level     string          `json:"level"`
	Message   string          `json:"message"`
	Meta      json.RawMessage `json:"meta,omitempty"`
}

type AttemptRecord struct {
	Attempt      int        `json:"attempt"`
	Status       string     `json:"status"`
	StartedAt    *string    `json:"startedAt"`
	CompletedAt  *string    `json:"completedAt"`
	DurationMs   *int64     `json:"durationMs"`
	HttpStatus   *int       `json:"httpStatus"`
	ErrorMessage *string    `json:"errorMessage"`
	Logs         []LogEntry `json:"logs"`
}

type SuccessResult struct {
	HttpStatus   int
	ResponseBody string
	Logs         []LogEntry
	DurationMs   int64
}

type FailResult struct {
	HttpStatus   *int
	ErrorMessage string
	Logs         []LogEntry
	DurationMs   int64
}

type CreateExecParams struct {
	JobID       string
	ScheduledAt time.Time
	ParentID    *string
	Payload     json.RawMessage
}

// Execution queries.

func (s *Store) MarkRunning(ctx context.Context, id string) error {
	query := `UPDATE executions SET status = 'running', started_at = now() WHERE id = $1`
	_, err := s.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("mark running: %w", err)
	}
	return nil
}

func (s *Store) MarkSuccess(ctx context.Context, id string, r SuccessResult) error {
	logsJSON, err := json.Marshal(r.Logs)
	if err != nil {
		return fmt.Errorf("marshal logs: %w", err)
	}

	responseBody := r.ResponseBody
	if len(responseBody) > 10240 {
		responseBody = responseBody[:10240]
	}

	query := `
		UPDATE executions
		SET status = 'success',
			started_at = now() - ($2 || ' milliseconds')::interval,
			completed_at = now(),
			duration_ms = $2,
			http_status = $3,
			response_body = $4,
			logs = $5
		WHERE id = $1`
	_, err = s.pool.Exec(ctx, query, id, r.DurationMs, r.HttpStatus, responseBody, logsJSON)
	if err != nil {
		return fmt.Errorf("mark success: %w", err)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, id string, r FailResult) error {
	logsJSON, err := json.Marshal(r.Logs)
	if err != nil {
		return fmt.Errorf("marshal logs: %w", err)
	}

	query := `
		UPDATE executions
		SET status = 'failed',
			started_at = now() - ($2 || ' milliseconds')::interval,
			completed_at = now(),
			duration_ms = $2,
			http_status = $3,
			error_message = $4,
			logs = $5
		WHERE id = $1`
	_, err = s.pool.Exec(ctx, query, id, r.DurationMs, r.HttpStatus, r.ErrorMessage, logsJSON)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}

func (s *Store) SaveAttemptAndRetry(ctx context.Context, id string, attempt AttemptRecord) error {
	attemptJSON, err := json.Marshal(attempt)
	if err != nil {
		return fmt.Errorf("marshal attempt: %w", err)
	}

	query := `
		UPDATE executions
		SET attempts = attempts || $2::jsonb,
			attempt = attempt + 1,
			status = 'pending',
			started_at = NULL,
			completed_at = NULL,
			duration_ms = NULL,
			http_status = NULL,
			response_body = NULL,
			error_message = NULL,
			logs = '[]'::jsonb
		WHERE id = $1`
	_, err = s.pool.Exec(ctx, query, id, attemptJSON)
	if err != nil {
		return fmt.Errorf("save attempt and retry: %w", err)
	}
	return nil
}

func (s *Store) CreatePendingExecution(ctx context.Context, p CreateExecParams) (string, error) {
	var payloadJSON []byte
	if p.Payload != nil {
		payloadJSON = p.Payload
	}

	query := `
		INSERT INTO executions (job_id, status, scheduled_at, attempt, parent_id, payload)
		VALUES ($1, 'pending', $2, 1, $3, $4)
		RETURNING id`

	var id string
	err := s.pool.QueryRow(ctx, query, p.JobID, p.ScheduledAt, p.ParentID, payloadJSON).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("create pending execution: %w", err)
	}
	return id, nil
}

func (s *Store) FindJobByName(ctx context.Context, projectID string, name string) (*JobRecord, error) {
	query := `
		SELECT id, project_id, name, retries, timeout_seconds
		FROM jobs
		WHERE project_id = $1 AND name = $2 AND status = 'active'`

	var j JobRecord
	err := s.pool.QueryRow(ctx, query, projectID, name).Scan(
		&j.ID, &j.ProjectID, &j.Name, &j.Retries, &j.TimeoutSeconds,
	)
	if err != nil {
		return nil, fmt.Errorf("find job by name: %w", err)
	}
	return &j, nil
}
