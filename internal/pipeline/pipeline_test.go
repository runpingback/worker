package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_ProcessesAllJobs(t *testing.T) {
	var processed atomic.Int32

	handler := func(ctx context.Context, id string, data []byte) {
		processed.Add(1)
	}

	jobs := make(chan Job, 10)
	pool := NewPool(4, jobs, handler)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	for i := 0; i < 10; i++ {
		jobs <- Job{ID: "id", Data: []byte(`{}`)}
	}
	close(jobs)

	pool.Wait()
	cancel()

	if processed.Load() != 10 {
		t.Errorf("expected 10 processed, got %d", processed.Load())
	}
}

func TestPool_GracefulShutdown(t *testing.T) {
	var processed atomic.Int32

	handler := func(ctx context.Context, id string, data []byte) {
		time.Sleep(50 * time.Millisecond)
		processed.Add(1)
	}

	jobs := make(chan Job, 5)
	pool := NewPool(2, jobs, handler)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	for i := 0; i < 5; i++ {
		jobs <- Job{ID: "id", Data: []byte(`{}`)}
	}
	close(jobs)
	cancel()

	pool.Wait()

	if processed.Load() != 5 {
		t.Errorf("expected 5 processed, got %d", processed.Load())
	}
}

func TestPool_PanicRecovery(t *testing.T) {
	var processed atomic.Int32

	handler := func(ctx context.Context, id string, data []byte) {
		if processed.Add(1) == 1 {
			panic("test panic")
		}
	}

	jobs := make(chan Job, 3)
	pool := NewPool(1, jobs, handler)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	for i := 0; i < 3; i++ {
		jobs <- Job{ID: "id", Data: []byte(`{}`)}
	}
	close(jobs)

	pool.Wait()
	cancel()

	if processed.Load() != 3 {
		t.Errorf("expected 3 processed (including panic), got %d", processed.Load())
	}
}

type mockClaimer struct {
	batches [][]Job
	callNum int
	done    chan struct{}
}

func (m *mockClaimer) Claim(ctx context.Context, queue string, batchSize int) ([]Job, error) {
	if m.callNum >= len(m.batches) {
		if m.done != nil {
			select {
			case <-m.done:
			default:
				close(m.done)
			}
		}
		return nil, nil
	}
	batch := m.batches[m.callNum]
	m.callNum++
	return batch, nil
}

func TestPoller_PushesJobsToChannel(t *testing.T) {
	done := make(chan struct{})
	claimer := &mockClaimer{
		batches: [][]Job{
			{{ID: "1", Data: []byte(`{}`)}},
			{{ID: "2", Data: []byte(`{}`)}},
		},
		done: done,
	}

	jobs := make(chan Job, 10)
	poller := NewPoller(claimer, "pingback-execution", 5, 10*time.Millisecond, jobs)

	ctx, cancel := context.WithCancel(context.Background())
	go poller.Start(ctx)

	// Wait until claimer has exhausted all batches
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for poller to claim all batches")
	}

	cancel()
	close(jobs)

	var received []Job
	for j := range jobs {
		received = append(received, j)
	}

	if len(received) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(received))
	}
}
