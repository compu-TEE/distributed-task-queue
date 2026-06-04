package main

import (
	grpc "dtq/internal/grpc"
	"dtq/internal/persistence"
	"dtq/internal/queue"
	"dtq/internal/types"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type AckRequest struct {
	TaskID int64 `json:"task_id"`
}

var BrokerStartTime = time.Now()

type MetricsResponse struct {
	UptimeSeconds int64 `json:"uptime_seconds"`

	PendingTasks    int `json:"pending_tasks"`
	InProgressTasks int `json:"in_progress_tasks"`
	CompletedTasks  int `json:"completed_tasks"`
	DeadLetterTasks int `json:"dead_letter_tasks"`

	ConnectedWorkers int `json:"connected_workers"`
	BusyWorkers      int `json:"busy_workers"`
	IdleWorkers      int `json:"idle_workers"`

	TotalRetries   int `json:"total_retries"`
	TasksSubmitted int `json:"tasks_submitted"`
	TasksCompleted int `json:"tasks_completed"`
}

func ping(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	log.Println("Received request from", workerID)
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{"message": "broker alive"}
	json.NewEncoder(w).Encode(response)
}

func task(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var newTask types.Task

	if err := json.NewDecoder(r.Body).Decode(&newTask); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := queue.AddTask(newTask); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newTask)
}

func poll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	task, found, err := queue.PollTask(
		r.URL.Query().Get("worker"),
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !found {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

func ack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ackReq AckRequest

	if err := json.NewDecoder(r.Body).Decode(&ackReq); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := queue.AckTask(ackReq.TaskID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func visibilityTimeoutChecker() {
	for {
		needDispatch := false
		queue.State.Mutex.Lock()
		for id, task := range queue.State.InProgressTasks {
			if time.Since(task.AssignedAt) > 10*time.Second {
				task.RetryCount++
				queue.State.Metrics.TotalRetries++
				log.Println("Task", id, "Retry count:", task.RetryCount, task.MaxRetries)
				err := persistence.AppendLog(fmt.Sprintf("RETRY %d %d", task.ID, task.RetryCount))
				if err != nil {
					log.Println("Failed to persist retry:", err)
				}
				delete(queue.State.InProgressTasks, id)

				if task.RetryCount >= task.MaxRetries {
					task.Status = types.DeadLetter

					queue.State.DeadLetterTasks[id] = task
					err := persistence.AppendLog(fmt.Sprintf("DLQ %d", task.ID))
					if err != nil {
						log.Println("Failed to persist DLQ:", err)
					}
					log.Println("Task", id, "moved to dead letter queue")
				} else {
					timedOutWorker := task.WorkerID

					task.Status = types.Pending
					task.AssignedAt = time.Time{}
					task.WorkerID = ""

					queue.State.PendingTasks[id] = task

					grpc.WorkersMu.Lock()
					if worker, ok := grpc.ConnectedWorkers[timedOutWorker]; ok {
						worker.Info.Status = "idle"
					}
					grpc.WorkersMu.Unlock()

					log.Println("Task", id, "requeued")
					needDispatch = true

				}
			}
		}
		queue.State.Mutex.Unlock()
		if needDispatch {
			go grpc.DispatchToIdleWorkers()
		}
		time.Sleep(1 * time.Second)
	}
}

func dlq(w http.ResponseWriter, r *http.Request) {
	queue.State.Mutex.Lock()
	defer queue.State.Mutex.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(queue.State.DeadLetterTasks)
}

func workers(w http.ResponseWriter, r *http.Request) {
	grpc.WorkersMu.RLock()
	defer grpc.WorkersMu.RUnlock()

	type WorkerResponse struct {
		Status         string    `json:"status"`
		TasksCompleted int       `json:"tasks_completed"`
		LastHeartbeat  time.Time `json:"last_heartbeat"`
	}

	resp := make(map[string]WorkerResponse)

	for id, worker := range grpc.ConnectedWorkers {
		resp[id] = WorkerResponse{
			Status:         worker.Info.Status,
			TasksCompleted: worker.Info.TasksCompleted,
			LastHeartbeat:  worker.Info.LastHeartbeat,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func metrics(w http.ResponseWriter, r *http.Request) {
	queue.State.Mutex.Lock()
	defer queue.State.Mutex.Unlock()
	resp := MetricsResponse{
		UptimeSeconds: int64(time.Since(BrokerStartTime).Seconds()),

		PendingTasks:    len(queue.State.PendingTasks),
		InProgressTasks: len(queue.State.InProgressTasks),
		CompletedTasks:  len(queue.State.CompletedTasks),
		DeadLetterTasks: len(queue.State.DeadLetterTasks),

		TotalRetries:   queue.State.Metrics.TotalRetries,
		TasksSubmitted: queue.State.Metrics.TasksSubmitted,
		TasksCompleted: queue.State.Metrics.TasksCompleted,
	}

	grpc.WorkersMu.RLock()

	resp.ConnectedWorkers = len(grpc.ConnectedWorkers)

	for _, worker := range grpc.ConnectedWorkers {
		switch worker.Info.Status {
		case "busy":
			resp.BusyWorkers++
		case "idle":
			resp.IdleWorkers++
		}
	}

	grpc.WorkersMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

func main() {
	http.HandleFunc("/ping", ping)
	http.HandleFunc("/task", task)
	http.HandleFunc("/poll", poll)
	http.HandleFunc("/ack", ack)
	http.HandleFunc("/dlq", dlq)
	http.HandleFunc("/workers", workers)
	http.HandleFunc("/metrics", metrics)
	http.HandleFunc("/health", health)
	go visibilityTimeoutChecker()
	replayTasks, err := persistence.ReplayLog()
	if err != nil {
		log.Println(err)
	}
	for id, task := range replayTasks {
		if task.Status == types.Pending {
			queue.State.PendingTasks[id] = task
		} else if task.Status == types.Completed {
			queue.State.CompletedTasks[id] = task
		} else if task.Status == types.DeadLetter {
			queue.State.DeadLetterTasks[id] = task
		}
	}
	log.Printf(
		"Recovered %d pending tasks and %d completed tasks %d dlq",
		len(queue.State.PendingTasks),
		len(queue.State.CompletedTasks),
		len(queue.State.DeadLetterTasks),
	)
	log.Println("Broker running on port 8080")
	err = persistence.AppendLog("BROKER_STARTED")
	if err != nil {
		log.Println(err)
	}
	go StartGRPCServer()
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Println("Server failed: ", err)
	}
}
