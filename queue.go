package queuelite

import (
	"context"
	"database/sql"
	"runtime"
	"time"

	_ "modernc.org/sqlite"
)

type SqlQueue struct {
	writeDB *sql.DB
	readDB  *sql.DB
}

func NewSQLiteQueue(db string) (*SqlQueue, error) {
	ctx := context.Background()
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	writeDB, err := sql.Open("sqlite", db)
	if err != nil {
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)

	readDB, err := sql.Open("sqlite", db)
	if err != nil {
		return nil, err
	}
	readDB.SetMaxOpenConns(max(4, runtime.NumCPU()))

	if err := writeDB.Ping(); err != nil {
		return nil, err
	}
	if err := setupDB(writeDB); err != nil {
		return nil, err
	}

	if err := readDB.Ping(); err != nil {
		return nil, err
	}
	if err := setupDB(readDB); err != nil {
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

func (q *SqlQueue) Close() error {
	if err := q.writeDB.Close(); err != nil {
		return err
	}
	if err := q.readDB.Close(); err != nil {
		return err
	}
	return nil
}

func (q *SqlQueue) Enqueue(ctx context.Context, job Job) error {
	if _, err := q.writeDB.ExecContext(
		ctx,
		"INSERT INTO queuelite_job (id, status, data, added_at) VALUES (?, ?, ?, ?)",
		job.ID,
		job.Status,
		job.Data,
		time.Now().UnixMilli(),
	); err != nil {
		return err
	}

	return nil
}

func (q *SqlQueue) BatchEnqueue(ctx context.Context, jobs []Job) error {
	tx, err := q.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO queuelite_job (id, status, data, added_at) VALUES (?, ?, ?, ?)",
			job.ID,
			job.Status,
			job.Data,
			time.Now().UnixMilli(),
		); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func setupDB(db *sql.DB) error {
	pragmas := []string{
		"journal_mode = WAL",
		"busy_timeout = 5000",
		"synchronous = NORMAL",
		"cache_size = 500000000", // 500MB
		"foreign_keys = true",
		"temp_store = memory",
		"mmap_size = 3000000000",
	}

	for _, pragma := range pragmas {
		_, err := db.Exec("PRAGMA " + pragma)
		if err != nil {
			return err
		}
	}

	return nil
}
