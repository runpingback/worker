package pipeline

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
)

type Job struct {
	ID   string
	Data []byte
}

type Handler func(ctx context.Context, id string, data []byte)

type Pool struct {
	size    int
	jobs    <-chan Job
	handler Handler
	wg      sync.WaitGroup
}

func NewPool(size int, jobs <-chan Job, handler Handler) *Pool {
	return &Pool{
		size:    size,
		jobs:    jobs,
		handler: handler,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	for job := range p.jobs {
		p.safeHandle(ctx, job)
	}
}

func (p *Pool) safeHandle(ctx context.Context, job Job) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in worker", "job_id", job.ID, "panic", r, "stack", string(debug.Stack()))
		}
	}()

	p.handler(ctx, job.ID, job.Data)
}
