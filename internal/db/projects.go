package db

import (
	"context"
	"fmt"
)

func (s *Store) LoadProjectUser(ctx context.Context, projectID string) (*ProjectUser, error) {
	query := `
		SELECT p.id, p.endpoint_url, p.cron_secret,
			   u.id, u.plan, u.executions_this_month, u.executions_reset_at
		FROM projects p
		JOIN users u ON p.user_id = u.id
		WHERE p.id = $1`

	var pu ProjectUser
	err := s.pool.QueryRow(ctx, query, projectID).Scan(
		&pu.ProjectID, &pu.EndpointURL, &pu.CronSecret,
		&pu.UserID, &pu.Plan, &pu.ExecutionsThisMonth, &pu.ExecutionsResetAt,
	)
	if err != nil {
		return nil, fmt.Errorf("load project user: %w", err)
	}
	return &pu, nil
}
