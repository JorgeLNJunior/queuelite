package queuelite

import (
	"context"
	"database/sql"
	"runtime"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteQueue struct {
	writeDB *sql.DB
	readDB  *sql.DB
}

// NewSQLiteQueue return an instance of [SQLiteQueue].
func NewSQLiteQueue(db string) (*SQLiteQueue, error) {
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

	return &SQLiteQueue{
		writeDB: writeDB,
		readDB:  readDB,
	}, nil
}

// Close closes the queue and it's underhood database.
func (q *SQLiteQueue) Close() error {
	if err := q.writeDB.Close(); err != nil {
		return err
	}
	if err := q.readDB.Close(); err != nil {
		return err
	}
	return nil
}

// Enqueue adds a new job to the queue with [JobStatusPending] status.
func (q *SQLiteQueue) Enqueue(ctx context.Context, job Job) error {
	if _, err := q.writeDB.ExecContext(
		ctx,
		"INSERT INTO queuelite_job (id, status, data, added_at) VALUES (?, ?, ?, ?)",
		job.ID,
		JobStatusPending,
		job.Data,
		time.Now().UnixMilli(),
	); err != nil {
		return err
	}

	return nil
}

// BatchEnqueue adds a list of jobs to the queue at once.
// If inserting a task fails, the previous ones are rolled back and the error is returned.
func (q *SQLiteQueue) BatchEnqueue(ctx context.Context, jobs []Job) error {
	tx, err := q.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for _, job := range jobs {
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO queuelite_job (id, status, data, added_at) VALUES (?, ?, ?, ?)",
			job.ID,
			JobStatusPending,
			job.Data,
			time.Now().UnixMilli(),
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Dequeue returns the oldest job in the queue and set it's status to [JobStatusRunning].
func (q *SQLiteQueue) Dequeue(ctx context.Context) (*Job, error) {
	job := new(Job)

	row := q.readDB.QueryRowContext(
		ctx,
		`SELECT id, status, data, added_at, error_reason, retry_count from queuelite_job 
		WHERE added_at = (SELECT MIN(added_at) FROM queuelite_job WHERE status IN (?, ?))`,
		JobStatusPending,
		JobStatusRetry,
	)
	if err := row.Scan(
		&job.ID,
		&job.Status,
		&job.Data,
		&job.AddedAt,
		&job.ErrorReason,
		&job.RetryCount,
	); err != nil {
		return nil, err
	}

	if _, err := q.writeDB.ExecContext(
		ctx,
		"UPDATE queuelite_job SET status = ?",
		JobStatusRunning,
	); err != nil {
		return nil, err
	}

	return job, nil
}

// IsEmpty returns true if the queue has no jobs with the status [JobStatusPending] otherwise returns false.
func (q *SQLiteQueue) IsEmpty(ctx context.Context) (bool, error) {
	var jobsCount int

	row := q.readDB.QueryRowContext(
		ctx,
		"SELECT COUNT() FROM queuelite_job WHERE status = ?",
		JobStatusPending,
	)
	if err := row.Scan(&jobsCount); err != nil {
		return false, err
	}

	return (jobsCount < 1), nil
}

// Retry re-adds a [Job] in the queue with [JobStatusRetry].
// If the job is not in the queue returns [JobNotFoundErr].
func (q *SQLiteQueue) Retry(ctx context.Context, job Job) error {
	row := q.readDB.QueryRowContext(
		ctx,
		"SELECT EXISTS(SELECT id FROM queuelite_job WHERE id = ?)",
		job.ID,
	)

	var jobExists bool
	if err := row.Scan(&jobExists); err != nil {
		return err
	}
	if !jobExists {
		return JobNotFoundErr
	}

	tx, err := q.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(
		ctx,
		"UPDATE queuelite_job SET status = ?, retry_count = (retry_count + 1) WHERE id = ?",
		JobStatusRetry,
		job.ID,
	); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Count returns how many jobs are in the queue.
func (q *SQLiteQueue) Count(ctx context.Context) (*JobCount, error) {
	row := q.readDB.QueryRowContext(
		ctx,
		`SELECT COUNT(id) AS total, 
    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as pending,
    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as running,
    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as retry,
    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as error
    from queuelite_job`,
		JobStatusPending,
		JobStatusRunning,
		JobStatusRetry,
		JobStatusError,
	)

	count := new(JobCount)
	if err := row.Scan(
		&count.Total,
		&count.Pending,
		&count.Running,
		&count.Retry,
		&count.Error,
	); err != nil {
		return nil, err
	}

	return count, nil
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
