package db

import (
	"context"
	"fmt"
	"time"
)

func nextMonthReset() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

func NeedsQuotaReset(resetAt *time.Time) bool {
	if resetAt == nil {
		return true
	}
	return time.Now().UTC().After(*resetAt)
}

func (s *Store) ResetMonthlyQuota(ctx context.Context, userID string) error {
	query := `
		UPDATE users
		SET executions_this_month = 0,
			executions_reset_at = $2
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, query, userID, nextMonthReset())
	if err != nil {
		return fmt.Errorf("reset monthly quota: %w", err)
	}
	return nil
}

func (s *Store) IncrementExecutions(ctx context.Context, userID string) error {
	query := `UPDATE users SET executions_this_month = executions_this_month + 1 WHERE id = $1`
	_, err := s.pool.Exec(ctx, query, userID)
	if err != nil {
		return fmt.Errorf("increment executions: %w", err)
	}
	return nil
}
