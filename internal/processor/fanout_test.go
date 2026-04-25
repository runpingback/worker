package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/runpingback/worker/internal/db"
	"github.com/runpingback/worker/internal/limits"
)

type fanoutMockStore struct {
	mockStore
	foundJob       *db.JobRecord
	createdExecIDs []string
	createCount    int
}

func (m *fanoutMockStore) FindJobByName(ctx context.Context, projectID string, name string) (*db.JobRecord, error) {
	return m.foundJob, nil
}

func (m *fanoutMockStore) CreatePendingExecution(ctx context.Context, p db.CreateExecParams) (string, error) {
	m.createCount++
	id := "child-" + string(rune('0'+m.createCount))
	m.createdExecIDs = append(m.createdExecIDs, id)
	return id, nil
}

func TestDispatchFanOut(t *testing.T) {
	store := &fanoutMockStore{
		mockStore: mockStore{projectUser: defaultProjectUser()},
		foundJob: &db.JobRecord{
			ID:             "task-job-1",
			ProjectID:      "proj-1",
			Name:           "send-email",
			Retries:        3,
			TimeoutSeconds: 30,
		},
	}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	parent := newTestMsg()
	tasks := []limits.Task{
		{Name: "send-email", Payload: json.RawMessage(`{"id":"1"}`)},
		{Name: "send-email", Payload: json.RawMessage(`{"id":"2"}`)},
	}

	p.dispatchFanOut(context.Background(), parent, tasks)

	if store.createCount != 2 {
		t.Errorf("expected 2 child executions, got %d", store.createCount)
	}
	if len(queue.insertCalls) != 2 {
		t.Errorf("expected 2 queue inserts, got %d", len(queue.insertCalls))
	}
	for _, call := range queue.insertCalls {
		if call.queue != "pingback-execution" {
			t.Errorf("expected queue pingback-execution, got %s", call.queue)
		}
	}
}

// fanoutCreateErrStore returns errors from CreatePendingExecution.
type fanoutCreateErrStore struct {
	fanoutMockStore
}

func (m *fanoutCreateErrStore) CreatePendingExecution(ctx context.Context, p db.CreateExecParams) (string, error) {
	return "", fmt.Errorf("db error creating execution")
}

// fanoutQueueErrQueue returns error from Insert.
type fanoutQueueErrQueue struct {
	mockQueue
	insertErrCalled bool
}

func (m *fanoutQueueErrQueue) Insert(ctx context.Context, queue string, data any, startAfter time.Duration) error {
	m.insertErrCalled = true
	return fmt.Errorf("queue insert failed")
}

func TestDispatchFanOut_CreateExecFails(t *testing.T) {
	store := &fanoutCreateErrStore{
		fanoutMockStore: fanoutMockStore{
			mockStore: mockStore{projectUser: defaultProjectUser()},
			foundJob: &db.JobRecord{
				ID:             "task-job-99",
				ProjectID:      "proj-1",
				Name:           "task-a",
				Retries:        1,
				TimeoutSeconds: 10,
			},
		},
	}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	parent := newTestMsg()
	tasks := []limits.Task{
		{Name: "task-a", Payload: json.RawMessage(`{"x":1}`)},
		{Name: "task-a", Payload: json.RawMessage(`{"x":2}`)},
	}

	// Should not panic; should skip enqueue for failed creations.
	p.dispatchFanOut(context.Background(), parent, tasks)

	if len(queue.insertCalls) != 0 {
		t.Errorf("expected 0 queue inserts when CreatePendingExecution fails, got %d", len(queue.insertCalls))
	}
}

func TestDispatchFanOut_QueueInsertFails(t *testing.T) {
	store := &fanoutMockStore{
		mockStore: mockStore{projectUser: defaultProjectUser()},
		foundJob: &db.JobRecord{
			ID:             "task-job-77",
			ProjectID:      "proj-1",
			Name:           "task-b",
			Retries:        2,
			TimeoutSeconds: 15,
		},
	}
	queue := &fanoutQueueErrQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	parent := newTestMsg()
	tasks := []limits.Task{
		{Name: "task-b", Payload: json.RawMessage(`{"y":1}`)},
		{Name: "task-b", Payload: json.RawMessage(`{"y":2}`)},
	}

	// Should not panic or return error; logs the failure and continues.
	p.dispatchFanOut(context.Background(), parent, tasks)

	if store.createCount != 2 {
		t.Errorf("expected 2 child executions created, got %d", store.createCount)
	}
	if !queue.insertErrCalled {
		t.Error("expected queue Insert to be called (and fail)")
	}
}

func TestDispatchFanOut_JobNotFound(t *testing.T) {
	store := &fanoutMockStore{
		mockStore: mockStore{projectUser: defaultProjectUser()},
		foundJob:  nil,
	}
	queue := &mockQueue{}
	p := &Processor{Queue: queue, Store: store, Dispatcher: &Dispatcher{}}

	parent := newTestMsg()
	tasks := []limits.Task{
		{Name: "nonexistent", Payload: json.RawMessage(`{}`)},
	}

	p.dispatchFanOut(context.Background(), parent, tasks)

	if store.createCount != 0 {
		t.Errorf("expected 0 child executions, got %d", store.createCount)
	}
	if len(queue.insertCalls) != 0 {
		t.Errorf("expected 0 queue inserts, got %d", len(queue.insertCalls))
	}
}
