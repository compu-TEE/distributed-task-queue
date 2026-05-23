package types

type PingRequest struct {
	Message string `json:"message"`
}

type PingResponse struct {
	Message string `json:"message"`
}

type Task struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}
