package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/runpingback/worker/internal/db"
)

type mockQueue struct {
	completeCalls []string
	insertCalls   []struct {
		queue string
		data  any
	}
}

func (m *mockQueue) Complete(ctx context.Context, queue string, id string, output any) error {
	m.completeCalls = append(m.completeCalls, id)
	return nil
}

func (m *mockQueue) Insert(ctx context.Context, queue string, data any, startAfter time.Duration) error {
	m.insertCalls = append(m.insertCalls, struct {
		queue string
		data  any
	}{queue, data})
	return nil
}

type mockStore struct {
	markRunningCalled bool
	markSuccessCalled bool
	markFailedCalled  bool
	saveAttemptCalled bool
	incrementCalled   bool
	projectUser       *db.ProjectUser
}

func (m *mockStore) MarkRunning(ctx context.Context, id string) error {
	m.markRunningCalled = true
	return nil
}
func (m *mockStore) MarkSuccess(ctx context.Context, id string, r db.SuccessResult) error {
	m.markSuccessCalled = true
	return nil
}
func (m *mockStore) MarkFailed(ctx context.Context, id string, r db.FailResult) error {
	m.markFailedCalled = true
	return nil
}
func (m *mockStore) SaveAttemptAndRetry(ctx context.Context, id string, a db.AttemptRecord) error {
	m.saveAttemptCalled = true
	return nil
}
func (m *mockStore) LoadProjectUser(ctx context.Context, projectID string) (*db.ProjectUser, error) {
	return m.projectUser, nil
}
func (m *mockStore) ResetMonthlyQuota(ctx context.Context, userID string) error { return nil }
func (m *mockStore) IncrementExecutions(ctx context.Context, userID string) error {
	m.incrementCalled = true
	return nil
}
func (m *mockStore) FindJobByName(ctx context.Context, projectID string, name string) (*db.JobRecord, error) {
	return nil, nil
}
func (m *mockStore) CreatePendingExecution(ctx context.Context, p db.CreateExecParams) (string, error) {
	return "child-exec-1", nil
}

func newTestMsg() QueueMessage {
	return QueueMessage{
		ExecutionID:    "exec-1",
		JobID:          "job-1",
		ProjectID:      "proj-1",
		FunctionName:   "test-fn",
		EndpointURL:    "",
		CronSecret:     "secret",
		Attempt:        1,
		MaxRetries:     3,
		TimeoutSeconds: 30,
		ScheduledAt:    "2026-04-25T12:00:00.000Z",
	}
}

func defaultProjectUser() *db.ProjectUser {
	future := time.Now().Add(24 * time.Hour)
	return &db.ProjectUser{
		ProjectID:           "proj-1",
		EndpointURL:         "",
		CronSecret:          "secret",
		UserID:              "user-1",
		Plan:                "pro",
		ExecutionsThisMonth: 100,
		ExecutionsResetAt:   &future,
	}
}

func TestProcess_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{
			"logs":  []map[string]any{{"timestamp": 123, "level": "info", "message": "ok"}},
			"tasks": []any{},
		})
	}))
	defer server.Close()

	pu := defaultProjectUser()
	pu.EndpointURL = server.URL
	store := &mockStore{projectUser: pu}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{Client: server.Client()}}

	msg := newTestMsg()
	err := p.Process(context.Background(), "pgboss-job-1", msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !store.markRunningCalled {
		t.Error("expected execution marked running")
	}
	if !store.markSuccessCalled {
		t.Error("expected execution marked success")
	}
	if !store.incrementCalled {
		t.Error("expected executions incremented")
	}
	if len(queue.completeCalls) != 1 {
		t.Error("expected pgboss job completed")
	}
}

func TestProcess_PlanLimitExceeded(t *testing.T) {
	store := &mockStore{
		projectUser: func() *db.ProjectUser {
			pu := defaultProjectUser()
			pu.Plan = "free"
			pu.ExecutionsThisMonth = 1000
			return pu
		}(),
	}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	msg := newTestMsg()
	err := p.Process(context.Background(), "pgboss-job-1", msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !store.markFailedCalled {
		t.Error("expected execution to be marked failed")
	}
	if store.incrementCalled {
		t.Error("should not increment counter when limit exceeded")
	}
	if len(queue.completeCalls) != 1 {
		t.Error("expected pgboss job to be completed")
	}
}

func TestProcess_RetryOnFailure(t *testing.T) {
	store := &mockStore{projectUser: defaultProjectUser()}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	msg := newTestMsg()
	msg.Attempt = 1
	msg.MaxRetries = 3

	p.handleFailure(context.Background(), "pgboss-1", msg, db.FailResult{
		ErrorMessage: "HTTP 500",
		DurationMs:   100,
	})

	if !store.saveAttemptCalled {
		t.Error("expected attempt to be saved")
	}
	if len(queue.completeCalls) != 1 {
		t.Error("expected original pgboss job completed")
	}
	if len(queue.insertCalls) != 1 {
		t.Error("expected retry job inserted")
	}
	if queue.insertCalls[0].queue != "pingback-execution" {
		t.Errorf("expected queue pingback-execution, got %s", queue.insertCalls[0].queue)
	}
}

func TestProcess_PermanentFailure(t *testing.T) {
	store := &mockStore{projectUser: defaultProjectUser()}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	msg := newTestMsg()
	msg.Attempt = 4
	msg.MaxRetries = 3

	p.handleFailure(context.Background(), "pgboss-1", msg, db.FailResult{
		ErrorMessage: "HTTP 500",
		DurationMs:   100,
	})

	if !store.markFailedCalled {
		t.Error("expected execution marked failed")
	}
	if len(queue.completeCalls) != 1 {
		t.Error("expected pgboss job completed")
	}
	hasAlertInsert := false
	for _, call := range queue.insertCalls {
		if call.queue == "pingback-alert-evaluation" {
			hasAlertInsert = true
		}
	}
	if !hasAlertInsert {
		t.Error("expected alert evaluation event to be enqueued")
	}
}

// errorStore embeds mockStore and overrides specific methods to return errors.
type errorStore struct {
	mockStore
	markRunningErr    error
	loadProjectErr    error
	resetQuotaCalled  bool
}

func (m *errorStore) MarkRunning(ctx context.Context, id string) error {
	if m.markRunningErr != nil {
		return m.markRunningErr
	}
	return m.mockStore.MarkRunning(ctx, id)
}

func (m *errorStore) LoadProjectUser(ctx context.Context, projectID string) (*db.ProjectUser, error) {
	if m.loadProjectErr != nil {
		return nil, m.loadProjectErr
	}
	return m.mockStore.LoadProjectUser(ctx, projectID)
}

func (m *errorStore) ResetMonthlyQuota(ctx context.Context, userID string) error {
	m.resetQuotaCalled = true
	return nil
}

func TestProcess_QuotaReset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"logs": []any{}, "tasks": []any{}})
	}))
	defer server.Close()

	past := time.Now().Add(-24 * time.Hour)
	pu := defaultProjectUser()
	pu.ExecutionsResetAt = &past
	pu.EndpointURL = server.URL

	store := &errorStore{mockStore: mockStore{projectUser: pu}}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{Client: server.Client()}}

	msg := newTestMsg()
	err := p.Process(context.Background(), "pgboss-1", msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !store.resetQuotaCalled {
		t.Error("expected ResetMonthlyQuota to be called when ExecutionsResetAt is in the past")
	}
	if !store.markSuccessCalled {
		t.Error("expected execution to succeed after quota reset")
	}
}

func TestProcess_CapRetriesEnforced(t *testing.T) {
	// free plan: MaxRetries capped to 1
	pu := defaultProjectUser()
	pu.Plan = "free"
	pu.ExecutionsThisMonth = 0

	store := &mockStore{projectUser: pu}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	msg := newTestMsg()
	msg.Attempt = 1
	msg.MaxRetries = 10

	// Trigger failure directly via handleFailure after cap is applied by Process.
	// We need the full Process path so the cap happens. Use an unreachable URL.
	pu.EndpointURL = "http://127.0.0.1:1"
	msg.TimeoutSeconds = 5

	err := p.Process(context.Background(), "pgboss-1", msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have a retry insert because attempt(1) <= capped maxRetries(1).
	var retryInsert *QueueMessage
	for i, call := range queue.insertCalls {
		if call.queue == queueExecution {
			// Type-assert or marshal/unmarshal to get the QueueMessage.
			raw, err := json.Marshal(call.data)
			if err != nil {
				t.Fatalf("failed to marshal insert data at index %d: %v", i, err)
			}
			var qm QueueMessage
			if err := json.Unmarshal(raw, &qm); err != nil {
				t.Fatalf("failed to unmarshal QueueMessage at index %d: %v", i, err)
			}
			retryInsert = &qm
			break
		}
	}
	if retryInsert == nil {
		t.Fatal("expected a retry message to be enqueued")
	}
	if retryInsert.MaxRetries != 1 {
		t.Errorf("expected MaxRetries capped to 1 (free plan), got %d", retryInsert.MaxRetries)
	}
}

// fanoutSuccessStore embeds mockStore and returns a real job from FindJobByName.
type fanoutSuccessStore struct {
	mockStore
	createCount int
}

func (m *fanoutSuccessStore) FindJobByName(ctx context.Context, projectID string, name string) (*db.JobRecord, error) {
	return &db.JobRecord{
		ID:             "task-job-42",
		ProjectID:      projectID,
		Name:           name,
		Retries:        2,
		TimeoutSeconds: 30,
	}, nil
}

func (m *fanoutSuccessStore) CreatePendingExecution(ctx context.Context, p db.CreateExecParams) (string, error) {
	m.createCount++
	return "child-exec-fanout", nil
}

func TestProcess_SuccessWithFanOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{
			"logs":  []any{},
			"tasks": []map[string]any{{"name": "child-task", "payload": map[string]any{"key": "val"}}},
		})
	}))
	defer server.Close()

	pu := defaultProjectUser()
	pu.EndpointURL = server.URL

	store := &fanoutSuccessStore{mockStore: mockStore{projectUser: pu}}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{Client: server.Client()}}

	msg := newTestMsg()
	err := p.Process(context.Background(), "pgboss-1", msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !store.markSuccessCalled {
		t.Error("expected execution marked success")
	}
	if store.createCount != 1 {
		t.Errorf("expected 1 child execution created, got %d", store.createCount)
	}
	fanoutInserts := 0
	for _, call := range queue.insertCalls {
		if call.queue == queueExecution {
			fanoutInserts++
		}
	}
	if fanoutInserts < 1 {
		t.Errorf("expected at least 1 fan-out queue insert, got %d", fanoutInserts)
	}
}

func TestProcess_DispatchNetworkError(t *testing.T) {
	pu := defaultProjectUser()
	pu.EndpointURL = "http://127.0.0.1:1" // port 1 — connection refused

	store := &mockStore{projectUser: pu}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	msg := newTestMsg()
	msg.Attempt = 1
	msg.MaxRetries = 3
	msg.TimeoutSeconds = 5

	err := p.Process(context.Background(), "pgboss-1", msg)
	if err != nil {
		t.Fatalf("unexpected error from Process: %v", err)
	}

	// Attempt 1 <= MaxRetries 3, so it should retry (saveAttemptCalled).
	if !store.saveAttemptCalled {
		t.Error("expected SaveAttemptAndRetry to be called for retryable failure")
	}
	if len(queue.completeCalls) == 0 {
		t.Error("expected pgboss job to be completed")
	}
}

func TestProcess_MarkRunningFails(t *testing.T) {
	store := &errorStore{
		mockStore:      mockStore{projectUser: defaultProjectUser()},
		markRunningErr: fmt.Errorf("db connection lost"),
	}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	msg := newTestMsg()
	err := p.Process(context.Background(), "pgboss-1", msg)
	if err == nil {
		t.Fatal("expected error when MarkRunning fails")
	}
	if len(queue.completeCalls) != 1 {
		t.Errorf("expected pgboss job completed even on MarkRunning failure, got %d complete calls", len(queue.completeCalls))
	}
}

func TestProcess_LoadProjectUserFails(t *testing.T) {
	store := &errorStore{
		mockStore:      mockStore{projectUser: defaultProjectUser()},
		loadProjectErr: fmt.Errorf("project not found"),
	}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	msg := newTestMsg()
	err := p.Process(context.Background(), "pgboss-1", msg)
	if err == nil {
		t.Fatal("expected error when LoadProjectUser fails")
	}
	if !store.markFailedCalled {
		t.Error("expected execution to be marked failed when LoadProjectUser fails")
	}
	if len(queue.completeCalls) != 1 {
		t.Errorf("expected pgboss job completed, got %d complete calls", len(queue.completeCalls))
	}
}

func TestBuildRetryMessage(t *testing.T) {
	msg := newTestMsg()
	msg.Attempt = 2

	retry := buildRetryMessage(msg)
	if retry.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", retry.Attempt)
	}
	if retry.ExecutionID != msg.ExecutionID {
		t.Error("execution ID should be preserved")
	}

	_, err := json.Marshal(retry)
	if err != nil {
		t.Fatalf("failed to marshal retry message: %v", err)
	}
}
