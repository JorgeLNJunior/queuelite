package queuelite

import (
	"context"
	"database/sql"
	"errors"
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
	ctx := context.Background()

	runningJobs, err := q.ListRunning(ctx)
	if err != nil {
		return err
	}

	for _, job := range runningJobs {
		if _, err := q.readDB.ExecContext(
			ctx,
			"UPDATE queuelite_job SET state = ? WHERE rowid = ?",
			JobStatePending,
			job.ID,
		); err != nil {
			return err
		}
	}

	if err := q.writeDB.Close(); err != nil {
		return err
	}
	if err := q.readDB.Close(); err != nil {
		return err
	}
	return nil
}

// Enqueue adds a new job to the queue with [JobStatePending] state.
func (q *SQLiteQueue) Enqueue(ctx context.Context, job Job) error {
	if _, err := q.writeDB.ExecContext(
		ctx,
		"INSERT INTO queuelite_job (state, data, added_at) VALUES (?, ?, ?)",
		JobStatePending,
		job.Data,
		time.Now().Unix(),
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
			"INSERT INTO queuelite_job (state, data, added_at) VALUES (?, ?, ?)",
			JobStatePending,
			job.Data,
			time.Now().Unix(),
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Dequeue returns the oldest job in the queue and set it's state to [JobStateRunning].
func (q *SQLiteQueue) Dequeue(ctx context.Context) (*Job, error) {
	job := new(Job)

	row := q.readDB.QueryRowContext(
		ctx,
		`SELECT rowid, state, data, added_at, failure_reason, retry_count FROM queuelite_job
		WHERE rowid = (SELECT MIN(rowid) FROM queuelite_job WHERE state IN (?, ?))`,
		JobStatePending,
		JobStateRetry,
	)
	if err := row.Scan(
		&job.ID,
		&job.State,
		&job.Data,
		&job.AddedAt,
		&job.FailureReason,
		&job.RetryCount,
	); err != nil {
		return nil, err
	}

	if _, err := q.writeDB.ExecContext(
		ctx,
		"UPDATE queuelite_job SET state = ? WHERE rowid = ?",
		JobStateRunning,
		job.ID,
	); err != nil {
		return nil, err
	}

	return job, nil
}

// IsEmpty returns true if the queue has no jobs with the state [JobStatePending] otherwise returns false.
func (q *SQLiteQueue) IsEmpty(ctx context.Context) (bool, error) {
	var jobsCount int

	row := q.readDB.QueryRowContext(
		ctx,
		"SELECT COUNT() FROM queuelite_job WHERE state IN (?, ?)",
		JobStatePending,
		JobStateRetry,
	)
	if err := row.Scan(&jobsCount); err != nil {
		return false, err
	}

	return (jobsCount < 1), nil
}

// Complete sets a [Job] in the queue with [JobStateCompleted] state.
// If the job is not in the queue returns [JobNotFoundErr].
func (q *SQLiteQueue) Complete(ctx context.Context, jobID int64) error {
	row := q.readDB.QueryRowContext(
		ctx,
		"SELECT EXISTS(SELECT rowid FROM queuelite_job WHERE rowid = ?)",
		jobID,
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
		"UPDATE queuelite_job SET state = ? WHERE rowid = ?",
		JobStateCompleted,
		jobID,
	); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Retry re-adds a [Job] in the queue with [JobStateRetry] state.
// If the job is not in the queue returns [JobNotFoundErr].
func (q *SQLiteQueue) Retry(ctx context.Context, jobID int64) error {
	row := q.readDB.QueryRowContext(
		ctx,
		"SELECT EXISTS(SELECT rowid FROM queuelite_job WHERE rowid = ?)",
		jobID,
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
		"UPDATE queuelite_job SET state = ?, retry_count = (retry_count + 1) WHERE rowid = ?",
		JobStateRetry,
		jobID,
	); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// Fail sets a job in [JobStateFailed] state. Returns [JobNotFoundErr] if the job is not in the queue.
func (q *SQLiteQueue) Fail(ctx context.Context, jobID int64, reason string) error {
	row := q.readDB.QueryRowContext(
		ctx,
		"SELECT EXISTS(SELECT rowid FROM queuelite_job WHERE rowid = ?)",
		jobID,
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
		"UPDATE queuelite_job SET state = ?, failure_reason = ? WHERE rowid = ?",
		JobStateFailed,
		reason,
		jobID,
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
		`SELECT COUNT(rowid) AS total,
    SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as pending,
    SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as running,
    SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as retry,
    SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN state = ? THEN 1 ELSE 0 END) as completed
    from queuelite_job`,
		JobStatePending,
		JobStateRunning,
		JobStateRetry,
		JobStateFailed,
		JobStateCompleted,
	)

	count := new(JobCount)
	if err := row.Scan(
		&count.Total,
		&count.Pending,
		&count.Running,
		&count.Retry,
		&count.Failed,
		&count.Completed,
	); err != nil {
		return nil, err
	}

	return count, nil
}

// ListPending returns a list of [Job] with the [JobStatePending] state.
func (q *SQLiteQueue) ListPending(ctx context.Context, opts ...ListOption) ([]Job, error) {
	options := listOptions{
		limit: 20,
	}
	for _, o := range opts {
		o.apply(&options)
	}

	rows, err := q.readDB.QueryContext(
		ctx,
		`SELECT rowid, state, data, added_at, failure_reason, retry_count from queuelite_job
		WHERE state = ? LIMIT ?`,
		JobStatePending,
		options.limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		job := Job{}
		if err = rows.Scan(
			&job.ID,
			&job.State,
			&job.Data,
			&job.AddedAt,
			&job.FailureReason,
			&job.RetryCount,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// ListRunning returns a list of [Job] with the [JobStateRunning] state.
func (q *SQLiteQueue) ListRunning(ctx context.Context, opts ...ListOption) ([]Job, error) {
	options := listOptions{
		limit: 20,
	}
	for _, o := range opts {
		o.apply(&options)
	}

	rows, err := q.readDB.QueryContext(
		ctx,
		`SELECT rowid, state, data, added_at, failure_reason, retry_count from queuelite_job
		WHERE state = ? LIMIT ?`,
		JobStateRunning,
		options.limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		job := Job{}
		if err = rows.Scan(
			&job.ID,
			&job.State,
			&job.Data,
			&job.AddedAt,
			&job.FailureReason,
			&job.RetryCount,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// ListRetry returns a list of [Job] with the [JobStateRetry] state.
func (q *SQLiteQueue) ListRetry(ctx context.Context, opts ...ListOption) ([]Job, error) {
	options := listOptions{
		limit: 20,
	}
	for _, o := range opts {
		o.apply(&options)
	}

	rows, err := q.readDB.QueryContext(
		ctx,
		`SELECT rowid, state, data, added_at, failure_reason, retry_count from queuelite_job
		WHERE state = ? LIMIT ?`,
		JobStateRetry,
		options.limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		job := Job{}
		if err = rows.Scan(
			&job.ID,
			&job.State,
			&job.Data,
			&job.AddedAt,
			&job.FailureReason,
			&job.RetryCount,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// ListFailed returns a list of [Job] with the [JobStateFailed] state.
func (q *SQLiteQueue) ListFailed(ctx context.Context, opts ...ListOption) ([]Job, error) {
	options := listOptions{
		limit: 20,
	}
	for _, o := range opts {
		o.apply(&options)
	}

	rows, err := q.readDB.QueryContext(
		ctx,
		`SELECT rowid, state, data, added_at, failure_reason, retry_count from queuelite_job
		WHERE state = ? LIMIT ?`,
		JobStateFailed,
		options.limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		job := Job{}
		if err = rows.Scan(
			&job.ID,
			&job.State,
			&job.Data,
			&job.AddedAt,
			&job.FailureReason,
			&job.RetryCount,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// GetJob returns a job by its id. If the job is not in the queue returns [JobNotFoundErr].
func (q *SQLiteQueue) GetJob(ctx context.Context, id int64) (*Job, error) {
	job := new(Job)

	row := q.readDB.QueryRowContext(
		ctx,
		"SELECT rowid, state, data, added_at, failure_reason, retry_count FROM queuelite_job WHERE rowid = ?",
		id,
	)
	if err := row.Scan(
		&job.ID,
		&job.State,
		&job.Data,
		&job.AddedAt,
		&job.FailureReason,
		&job.RetryCount,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, JobNotFoundErr
		}
		return nil, err
	}

	return job, nil
}

// RemoveJob removes a job from the queue. If the job is not in the queue return [JobNotFoundErr].
func (q *SQLiteQueue) RemoveJob(ctx context.Context, id int64) error {
	_, err := q.GetJob(ctx, id)
	if err != nil {
		return err
	}

	if _, err = q.writeDB.ExecContext(
		ctx,
		"DELETE FROM queuelite_job WHERE rowid = ?",
		id,
	); err != nil {
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

type listOptions struct {
	limit int
}

type ListOption interface {
	apply(*listOptions)
}

type limitOption int

func (l limitOption) apply(opts *listOptions) {
	opts.limit = int(l)
}

func WithLimit(limit int) ListOption {
	return limitOption(limit)
}
