package queuelite

import (
	"database/sql"
)

// JobStatus represents the status of a job in the queue.
type JobStatus string

const (
	JobStatusPending JobStatus = "pending"
	JobStatusRunning JobStatus = "running"
	JobStatusError   JobStatus = "error"
)

// Job represents a job in the queue.
type Job struct {
	ID          string         `json:"id"`
	Status      JobStatus      `json:"status"`
	Data        []byte         `json:"data"`
	AddedAt     int64          `json:"added_at"`
	ErrorReason sql.NullString `json:"error_reason"`
}

// NewJob returns a [Job] instance.
func NewJob(data []byte) Job {
	return Job{
		ID:     newRandomID(),
		Data:   data,
		Status: JobStatusPending,
	}
}
