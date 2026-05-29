package types

import (
	"time"
)

type PingRequest struct {
	Message string `json:"message"`
}

type PingResponse struct {
	Message string `json:"message"`
}

type Task struct {
	ID         int       `json:"id"`
	Payload    string    `json:"payload"`
	Status     string    `json:"status"`
	AssignedAt time.Time `json:"assigned_at,omitempty"`
	WorkerID   string    `json:"worker_id,omitempty"`
	RetryCount int       `json:"retry_count"`
	MaxRetries int       `json:"max_retries"`
}

const (
	Pending    = "pending"
	InProgress = "in_progress"
	Completed  = "completed"
	DeadLetter = "dead_letter"
)
