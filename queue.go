package queuelite

import (
	"context"
	"database/sql"
	"runtime"
	"time"
)

type SqlQueue struct {
	db      *sql.DB
	writeDB *sql.DB
	readDB  *sql.DB
}

func NewSQLiteQueue(db string) (*SqlQueue, error) {
	ctx := context.Background()
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	writeDB, err := sql.Open("sqlite", db+"")
	if err != nil {
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)

	readDB, err := sql.Open("sqlite", db+"")
	if err != nil {
		return nil, err
	}
	readDB.SetMaxOpenConns(max(4, runtime.NumCPU()))

	if err := writeDB.Ping(); err != nil {
		return nil, err
	}
	if err := readDB.Ping(); err != nil {
		return nil, err
	}

	err = createTables(timeoutCtx, writeDB)
	if err != nil {
		return nil, err
	}

	return &SqlQueue{
		writeDB: writeDB,
		readDB:  readDB,
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

func (q *SqlQueue) BatchEnqueue(ctx context.Context, jobs []Job) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		_, err := tx.ExecContext(
			ctx,
			"INSERT INTO queuelite_job (id, status, data) VALUES (?, ?, ?)",
			job.ID,
			job.Status,
			job.Data,
		)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
