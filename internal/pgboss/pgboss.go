package pgboss

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Job struct {
	ID   string
	Data json.RawMessage
}

type Client struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

func (c *Client) Claim(ctx context.Context, queue string, batchSize int) ([]Job, error) {
	query := `
		WITH next AS (
			SELECT id
			FROM pgboss.job
			WHERE name = $1
				AND state < 'active'
				AND start_after < now()
			ORDER BY priority DESC, created_on, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE pgboss.job j
		SET state = 'active', started_on = now()
		FROM next
		WHERE j.name = $1 AND j.id = next.id
		RETURNING j.id, j.data`

	rows, err := c.pool.Query(ctx, query, queue, batchSize)
	if err != nil {
		return nil, fmt.Errorf("pgboss claim: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		var data []byte
		if err := rows.Scan(&j.ID, &data); err != nil {
			return nil, fmt.Errorf("pgboss claim scan: %w", err)
		}
		j.Data = data
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (c *Client) Complete(ctx context.Context, queue string, id string, output any) error {
	var outputJSON []byte
	if output != nil {
		var err error
		outputJSON, err = json.Marshal(output)
		if err != nil {
			return fmt.Errorf("pgboss complete marshal: %w", err)
		}
	}

	query := `
		UPDATE pgboss.job
		SET state = 'completed', completed_on = now(), output = $3
		WHERE name = $1 AND id = $2 AND state = 'active'`

	_, err := c.pool.Exec(ctx, query, queue, id, outputJSON)
	if err != nil {
		return fmt.Errorf("pgboss complete: %w", err)
	}
	return nil
}

func (c *Client) Insert(ctx context.Context, queue string, data any, startAfter time.Duration) error {
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("pgboss insert marshal: %w", err)
	}

	query := `
		INSERT INTO pgboss.job (name, data, state, start_after, created_on)
		VALUES ($1, $2, 'created', now() + $3::interval, now())`

	_, err = c.pool.Exec(ctx, query, queue, dataJSON, fmt.Sprintf("%d seconds", int(startAfter.Seconds())))
	if err != nil {
		return fmt.Errorf("pgboss insert: %w", err)
	}
	return nil
}
