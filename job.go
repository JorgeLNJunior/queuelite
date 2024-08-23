package queuelite

import (
	"database/sql"
)

type JobStatus string

const (
	JobStatusPending JobStatus = "pending"
	JobStatusRunning JobStatus = "running"
	JobStatusError   JobStatus = "error"
)

type Job struct {
	ID          string         `json:"id"`
	Status      JobStatus      `json:"status"`
	Data        []byte         `json:"data"`
	AddedAt     int64          `json:"added_at"`
	ErrorReason sql.NullString `json:"error_reason"`
}

func NewJob(data []byte) Job {
	return Job{
		ID:     newRandomID(),
		Data:   data,
		Status: JobStatusPending,
	}
}
