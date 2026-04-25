package processor

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/runpingback/worker/internal/db"
	"github.com/runpingback/worker/internal/limits"
)

const (
	queueExecution = "pingback-execution"
	queueAlert     = "pingback-alert-evaluation"
)

// Interfaces the processor depends on.

type QueueClient interface {
	Complete(ctx context.Context, queue string, id string, output any) error
	Insert(ctx context.Context, queue string, data any, startAfter time.Duration) error
}

type Store interface {
	MarkRunning(ctx context.Context, id string) error
	MarkSuccess(ctx context.Context, id string, r db.SuccessResult) error
	MarkFailed(ctx context.Context, id string, r db.FailResult) error
	SaveAttemptAndRetry(ctx context.Context, id string, attempt db.AttemptRecord) error
	LoadProjectUser(ctx context.Context, projectID string) (*db.ProjectUser, error)
	ResetMonthlyQuota(ctx context.Context, userID string) error
	IncrementExecutions(ctx context.Context, userID string) error
	FindJobByName(ctx context.Context, projectID string, name string) (*db.JobRecord, error)
	CreatePendingExecution(ctx context.Context, p db.CreateExecParams) (string, error)
}

// QueueMessage is the JSON payload stored in pgboss.job.data by the scheduler.

type QueueMessage struct {
	ExecutionID    string          `json:"executionId"`
	JobID          string          `json:"jobId"`
	ProjectID      string          `json:"projectId"`
	FunctionName   string          `json:"functionName"`
	EndpointURL    string          `json:"endpointUrl"`
	CronSecret     string          `json:"cronSecret"`
	Attempt        int             `json:"attempt"`
	MaxRetries     int             `json:"maxRetries"`
	TimeoutSeconds int             `json:"timeoutSeconds"`
	ScheduledAt    string          `json:"scheduledAt"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

type Processor struct {
	Queue      QueueClient
	Store      Store
	Dispatcher *Dispatcher
}

func New(queue QueueClient, store Store, dispatcher *Dispatcher) *Processor {
	return &Processor{
		Queue:      queue,
		Store:      store,
		Dispatcher: dispatcher,
	}
}

func (p *Processor) Process(ctx context.Context, pgbossJobID string, msg QueueMessage) error {
	log := slog.With("execution_id", msg.ExecutionID, "function", msg.FunctionName)

	t0 := time.Now()

	// 1-2. Mark running + load project/user in parallel
	var (
		pu      *db.ProjectUser
		markErr error
		loadErr error
		wg      sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		markErr = p.Store.MarkRunning(ctx, msg.ExecutionID)
	}()
	go func() {
		defer wg.Done()
		pu, loadErr = p.Store.LoadProjectUser(ctx, msg.ProjectID)
	}()
	wg.Wait()

	t1 := time.Now()

	if markErr != nil {
		log.Error("failed to mark running", "error", markErr)
		_ = p.Queue.Complete(ctx, queueExecution, pgbossJobID, nil)
		return markErr
	}
	if loadErr != nil {
		log.Error("failed to load project/user", "error", loadErr)
		_ = p.Store.MarkFailed(ctx, msg.ExecutionID, db.FailResult{
			ErrorMessage: "failed to load project",
			DurationMs:   time.Since(t1).Milliseconds(),
		})
		_ = p.Queue.Complete(ctx, queueExecution, pgbossJobID, nil)
		return loadErr
	}

	// 3. Plan limit check
	if db.NeedsQuotaReset(pu.ExecutionsResetAt) {
		if err := p.Store.ResetMonthlyQuota(ctx, pu.UserID); err != nil {
			log.Error("failed to reset quota", "error", err)
		} else {
			pu.ExecutionsThisMonth = 0
		}
	}

	if !limits.CanExecute(pu.Plan, pu.ExecutionsThisMonth) {
		log.Warn("plan limit reached", "user_id", pu.UserID, "plan", pu.Plan)
		_ = p.Store.MarkFailed(ctx, msg.ExecutionID, db.FailResult{
			ErrorMessage: "Monthly execution limit reached",
			DurationMs:   time.Since(t1).Milliseconds(),
		})
		_ = p.Queue.Complete(ctx, queueExecution, pgbossJobID, nil)
		return nil
	}

	if err := p.Store.IncrementExecutions(ctx, pu.UserID); err != nil {
		log.Error("failed to increment executions", "error", err)
	}

	t2 := time.Now()

	// Cap retries to plan limit
	msg.MaxRetries = limits.CapRetries(pu.Plan, msg.MaxRetries)

	// 4-5. Dispatch HTTP request
	result, err := p.Dispatcher.Dispatch(ctx, DispatchRequest{
		EndpointURL:    pu.EndpointURL,
		CronSecret:     pu.CronSecret,
		FunctionName:   msg.FunctionName,
		ExecutionID:    msg.ExecutionID,
		Attempt:        msg.Attempt,
		ScheduledAt:    msg.ScheduledAt,
		TimeoutSeconds: msg.TimeoutSeconds,
		Payload:        msg.Payload,
	})

	t3 := time.Now()
	durationMs := t3.Sub(t1).Milliseconds()

	log.Info("timing",
		"setup_ms", t1.Sub(t0).Milliseconds(),
		"limits_ms", t2.Sub(t1).Milliseconds(),
		"http_ms", t3.Sub(t2).Milliseconds(),
		"total_ms", t3.Sub(t0).Milliseconds(),
	)

	// Network error / timeout
	if err != nil {
		log.Warn("dispatch error", "error", err, "attempt", msg.Attempt)
		p.handleFailure(ctx, pgbossJobID, msg, db.FailResult{
			ErrorMessage: err.Error(),
			DurationMs:   durationMs,
		})
		return nil
	}

	// 6a. Success
	if result.Success {
		log.Info("job completed", "status", "success", "duration_ms", durationMs, "http_status", result.HttpStatus)

		// Mark success + complete pgboss in parallel
		var successWg sync.WaitGroup
		successWg.Add(2)
		go func() {
			defer successWg.Done()
			_ = p.Store.MarkSuccess(ctx, msg.ExecutionID, db.SuccessResult{
				HttpStatus:   result.HttpStatus,
				ResponseBody: result.ResponseBody,
				Logs:         result.Logs,
				DurationMs:   durationMs,
			})
		}()
		go func() {
			defer successWg.Done()
			_ = p.Queue.Complete(ctx, queueExecution, pgbossJobID, nil)
		}()

		// Fan-out while DB writes happen
		if len(result.Tasks) > 0 {
			capped := limits.CapFanOut(pu.Plan, result.Tasks)
			if len(capped) < len(result.Tasks) {
				log.Warn("fan-out capped", "requested", len(result.Tasks), "allowed", len(capped), "plan", pu.Plan)
			}
			p.dispatchFanOut(ctx, msg, capped)
		}

		successWg.Wait()
		return nil
	}

	// 6b. Failure (non-2xx)
	log.Warn("job failed", "http_status", result.HttpStatus, "attempt", msg.Attempt)
	httpStatus := result.HttpStatus
	p.handleFailure(ctx, pgbossJobID, msg, db.FailResult{
		HttpStatus:   &httpStatus,
		ErrorMessage: result.ErrorMessage,
		Logs:         result.Logs,
		DurationMs:   durationMs,
	})
	return nil
}

func (p *Processor) handleFailure(ctx context.Context, pgbossJobID string, msg QueueMessage, fail db.FailResult) {
	log := slog.With("execution_id", msg.ExecutionID)

	if msg.Attempt <= msg.MaxRetries {
		now := time.Now().UTC()
		nowStr := now.Format(time.RFC3339Nano)
		startedStr := now.Add(-time.Duration(fail.DurationMs) * time.Millisecond).Format(time.RFC3339Nano)

		attempt := db.AttemptRecord{
			Attempt:      msg.Attempt,
			Status:       "failed",
			StartedAt:    &startedStr,
			CompletedAt:  &nowStr,
			DurationMs:   &fail.DurationMs,
			HttpStatus:   fail.HttpStatus,
			ErrorMessage: &fail.ErrorMessage,
			Logs:         fail.Logs,
		}

		if err := p.Store.SaveAttemptAndRetry(ctx, msg.ExecutionID, attempt); err != nil {
			log.Error("failed to save attempt", "error", err)
		}

		_ = p.Queue.Complete(ctx, queueExecution, pgbossJobID, nil)

		backoff := limits.BackoffSeconds(msg.Attempt)
		retryMsg := buildRetryMessage(msg)
		log.Warn("retrying", "next_attempt", retryMsg.Attempt, "backoff_s", backoff)

		if err := p.Queue.Insert(ctx, queueExecution, retryMsg, time.Duration(backoff)*time.Second); err != nil {
			log.Error("failed to enqueue retry", "error", err)
		}
		return
	}

	// Permanent failure — run all three in parallel
	log.Error("permanently failed", "attempts", msg.Attempt)
	var failWg sync.WaitGroup
	failWg.Add(3)
	go func() {
		defer failWg.Done()
		_ = p.Store.MarkFailed(ctx, msg.ExecutionID, fail)
	}()
	go func() {
		defer failWg.Done()
		_ = p.Queue.Complete(ctx, queueExecution, pgbossJobID, nil)
	}()
	go func() {
		defer failWg.Done()
		alertData := map[string]string{
			"jobId":       msg.JobID,
			"executionId": msg.ExecutionID,
		}
		if err := p.Queue.Insert(ctx, queueAlert, alertData, 0); err != nil {
			log.Error("failed to enqueue alert", "error", err)
		}
	}()
	failWg.Wait()
}

func buildRetryMessage(msg QueueMessage) QueueMessage {
	retry := msg
	retry.Attempt = msg.Attempt + 1
	return retry
}
