package queuelite

import (
	"database/sql"
	"errors"
)

// JobStatus represents the status of a job in the queue.
type JobStatus string

const (
	JobStatusPending JobStatus = "pending"
	JobStatusRunning JobStatus = "running"
	JobStatusRetry   JobStatus = "retry"
	JobStatusError   JobStatus = "error"
)

var JobNotFoundErr = errors.New("job not found in the queue")

// Job represents a job in the queue.
type Job struct {
	ID          string         `json:"id"`
	Status      JobStatus      `json:"status"`
	Data        []byte         `json:"data"`
	AddedAt     int64          `json:"added_at"`
	RetryCount  int            `json:"retry_count"`
	ErrorReason sql.NullString `json:"error_reason"`
}

type JobCount struct {
	Pending int `json:"pending"`
	Running int `json:"running"`
	Retry   int `json:"retry"`
	Error   int `json:"error"`
	Total   int `json:"total"`
}

// NewJob returns a [Job] instance.
func NewJob(data []byte) Job {
	return Job{
		ID:     newRandomID(),
		Data:   data,
		Status: JobStatusPending,
	}
}
