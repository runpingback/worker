package processor

import (
	"context"
	"encoding/json"
	"testing"

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
