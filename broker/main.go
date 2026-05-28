package main

import (
	"dtq/internal/persistence"
	"dtq/internal/types"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type AckRequest struct {
	TaskID int `json:"task_id"`
}

type BrokerState struct {
	PendingTasks    map[int]types.Task
	InProgressTasks map[int]types.Task
	CompletedTasks  map[int]types.Task

	Mutex sync.Mutex
}

var brokerState = BrokerState{
	PendingTasks:    make(map[int]types.Task),
	InProgressTasks: make(map[int]types.Task),
	CompletedTasks:  make(map[int]types.Task),
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
	brokerState.Mutex.Lock()
	defer brokerState.Mutex.Unlock()
	var newTask types.Task
	if err := json.NewDecoder(r.Body).Decode(&newTask); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if _, exists := brokerState.PendingTasks[newTask.ID]; exists {
		http.Error(w, "Task ID already exists", http.StatusBadRequest)
		return
	}
	if _, exists := brokerState.InProgressTasks[newTask.ID]; exists {
		http.Error(w, "Task ID already exists", http.StatusBadRequest)
		return
	}
	if _, exists := brokerState.CompletedTasks[newTask.ID]; exists {
		http.Error(w, "Task ID already exists", http.StatusBadRequest)
		return
	}
	newTask.Status = types.Pending
	brokerState.PendingTasks[newTask.ID] = newTask
	err := persistence.AppendLog(fmt.Sprintf("ENQUEUE %d %s", newTask.ID, newTask.Payload))
	if err != nil {
		log.Println("Failed to persist enqueue:", err)
	}
	log.Println(newTask.ID, "added to task queue", "Payload:", newTask.Payload, "Total Pending tasks:", len(brokerState.PendingTasks))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newTask)
}

func poll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	brokerState.Mutex.Lock()
	defer brokerState.Mutex.Unlock()
	if len(brokerState.PendingTasks) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var task types.Task
	for _, t := range brokerState.PendingTasks {
		task = t
		break
	}
	delete(brokerState.PendingTasks, task.ID)
	task.Status = types.InProgress
	task.AssignedAt = time.Now()
	workerID := r.URL.Query().Get("worker")
	task.WorkerID = workerID
	brokerState.InProgressTasks[task.ID] = task
	err := persistence.AppendLog(fmt.Sprintf("ASSIGN %d %s", task.ID, task.WorkerID))
	if err != nil {
		log.Println("Failed to persist dequeue:", err)
	}
	log.Println("Task", task.ID, "moved to in_progress")
	log.Println("Assigned task", task.ID, "to worker", workerID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

func ack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	brokerState.Mutex.Lock()
	defer brokerState.Mutex.Unlock()
	var ackReq AckRequest

	if err := json.NewDecoder(r.Body).Decode(&ackReq); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	task, exists := brokerState.InProgressTasks[ackReq.TaskID]

	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	delete(brokerState.InProgressTasks, ackReq.TaskID)
	task.Status = types.Completed
	brokerState.CompletedTasks[task.ID] = task
	err := persistence.AppendLog(fmt.Sprintf("ACK %d", task.ID))
	if err != nil {
		log.Println("Failed to persist ACK:", err)
	}
	log.Println("Task", task.ID, "marked completed")
	w.WriteHeader(http.StatusOK)
}

func visibilityTimeoutChecker() {
	for {
		brokerState.Mutex.Lock()
		for id, task := range brokerState.InProgressTasks {
			if time.Since(task.AssignedAt) > 10*time.Second {
				log.Println("Task", id, "timed out. Requeueing....")
				task.Status = types.Pending
				task.AssignedAt = time.Time{}
				task.WorkerID = ""

				brokerState.PendingTasks[id] = task
				delete(brokerState.InProgressTasks, id)
			}
		}
		brokerState.Mutex.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func main() {
	http.HandleFunc("/ping", ping)
	http.HandleFunc("/task", task)
	http.HandleFunc("/poll", poll)
	http.HandleFunc("/ack", ack)
	go visibilityTimeoutChecker()
	replayTasks, err := persistence.ReplayLog()
	if err != nil {
		log.Println(err)
	}
	for id, task := range replayTasks {
		if task.Status == types.Pending {
			brokerState.PendingTasks[id] = task
		} else if task.Status == types.Completed {
			brokerState.CompletedTasks[id] = task
		}
	}
	log.Printf(
		"Recovered %d pending tasks and %d completed tasks",
		len(brokerState.PendingTasks),
		len(brokerState.CompletedTasks),
	)
	log.Println("Broker running on port 8080")
	err = persistence.AppendLog("BROKER_STARTED")
	if err != nil {
		log.Println(err)
	}
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Println("Server failed: ", err)
	}
}
