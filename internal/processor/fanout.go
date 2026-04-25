package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/runpingback/worker/internal/db"
	"github.com/runpingback/worker/internal/limits"
)

func (p *Processor) dispatchFanOut(ctx context.Context, parent QueueMessage, tasks []limits.Task) {
	log := slog.With("parent_id", parent.ExecutionID)

	for _, task := range tasks {
		job, err := p.Store.FindJobByName(ctx, parent.ProjectID, task.Name)
		if err != nil || job == nil {
			log.Warn("fan-out task job not found", "task", task.Name)
			continue
		}

		parentID := parent.ExecutionID
		execID, err := p.Store.CreatePendingExecution(ctx, db.CreateExecParams{
			JobID:       job.ID,
			ScheduledAt: time.Now().UTC(),
			ParentID:    &parentID,
			Payload:     task.Payload,
		})
		if err != nil {
			log.Error("failed to create child execution", "task", task.Name, "error", err)
			continue
		}

		childMsg := QueueMessage{
			ExecutionID:    execID,
			JobID:          job.ID,
			ProjectID:      parent.ProjectID,
			FunctionName:   task.Name,
			EndpointURL:    parent.EndpointURL,
			CronSecret:     parent.CronSecret,
			Attempt:        1,
			MaxRetries:     job.Retries,
			TimeoutSeconds: job.TimeoutSeconds,
			ScheduledAt:    time.Now().UTC().Format(time.RFC3339Nano),
		}
		if task.Payload != nil {
			childMsg.Payload = task.Payload
		}

		if err := p.Queue.Insert(ctx, queueExecution, childMsg, 0); err != nil {
			log.Error("failed to enqueue child", "task", task.Name, "error", err)
		}
	}

	log.Info("fan-out dispatched", "children", len(tasks))
}
