package queuelite

import (
	"database/sql"
	"errors"
)

// JobState represents the state of a job in the queue.
type JobState string

const (
	JobStatePending   JobState = "pending"
	JobStateRunning   JobState = "running"
	JobStateRetry     JobState = "retry"
	JobStateFailed    JobState = "failed"
	JobStateCompleted JobState = "completed"
)

var JobNotFoundErr = errors.New("job not found in the queue")

// Job represents a job in the queue.
type Job struct {
	ID            int64          `json:"id"`
	State         JobState       `json:"state"`
	Data          []byte         `json:"data"`
	AddedAt       int64          `json:"added_at"`
	RetryCount    int            `json:"retry_count"`
	FailureReason sql.NullString `json:"failure_reason"`
}

type JobCount struct {
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Retry     int `json:"retry"`
	Failed    int `json:"failed"`
	Completed int `json:"completed"`
	Total     int `json:"total"`
}

// NewJob returns a [Job] instance.
func NewJob(data []byte) Job {
	return Job{
		Data:  data,
		State: JobStatePending,
	}
}
