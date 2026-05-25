package types

type PingRequest struct {
	Message string `json:"message"`
}

type PingResponse struct {
	Message string `json:"message"`
}

type Task struct {
	ID      int    `json:"id"`
	Payload string `json:"payload"`
	Status  string `json:"status"`
}

const (
	Pending    = "pending"
	InProgress = "in_progress"
	Completed  = "completed"
)
