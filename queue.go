package queuelite

import (
	"context"
	"database/sql"
	"time"
)

type SqlQueue struct {
	db *sql.DB
}

func NewSQLiteQueue(db *sql.DB) (*SqlQueue, error) {
	ctx := context.Background()
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	err := createTables(timeoutCtx, db)
	if err != nil {
		return nil, err
	}

	return &SqlQueue{
		db: db,
	}, nil
}

func (q *SqlQueue) Enqueue(ctx context.Context, job Job) error {
	_, err := q.db.ExecContext(
		ctx,
		"INSERT INTO queuelite_job (id, status, data) VALUES (?, ?, ?)",
		job.ID,
		job.Status,
		job.Data,
	)
	if err != nil {
		return nil
	}

	return nil
}
