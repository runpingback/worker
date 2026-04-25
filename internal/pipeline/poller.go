package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/runpingback/worker/internal/pgboss"
)

type Claimer interface {
	Claim(ctx context.Context, queue string, batchSize int) ([]Job, error)
}

type Poller struct {
	claimer   Claimer
	queue     string
	batchSize int
	interval  time.Duration
	jobs      chan<- Job
}

func NewPoller(claimer Claimer, queue string, batchSize int, interval time.Duration, jobs chan<- Job) *Poller {
	return &Poller{
		claimer:   claimer,
		queue:     queue,
		batchSize: batchSize,
		interval:  interval,
		jobs:      jobs,
	}
}

func (p *Poller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *Poller) poll(ctx context.Context) {
	jobs, err := p.claimer.Claim(ctx, p.queue, p.batchSize)
	if err != nil {
		slog.Error("poll failed", "error", err)
		return
	}

	for _, job := range jobs {
		select {
		case p.jobs <- job:
		case <-ctx.Done():
			return
		}
	}

	if len(jobs) > 0 {
		slog.Debug("claimed jobs", "count", len(jobs))
	}
}

// PgBossAdapter bridges pgboss.Client to the Claimer interface.

type PgBossAdapter struct {
	client *pgboss.Client
}

func NewPgBossAdapter(client *pgboss.Client) *PgBossAdapter {
	return &PgBossAdapter{client: client}
}

func (a *PgBossAdapter) Claim(ctx context.Context, queue string, batchSize int) ([]Job, error) {
	pgJobs, err := a.client.Claim(ctx, queue, batchSize)
	if err != nil {
		return nil, err
	}

	jobs := make([]Job, len(pgJobs))
	for i, j := range pgJobs {
		jobs[i] = Job{ID: j.ID, Data: j.Data}
	}
	return jobs, nil
}
