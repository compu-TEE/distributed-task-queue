package main

import (
	"encoding/json"
	//"fmt"
	"dtq/internal/types"
	"log"
	"net/http"
)

type AckRequest struct {
	TaskID int `json:"task_id"`
}

var pendingTasks = make(map[int]types.Task)
var inProgressTasks = make(map[int]types.Task)
var completedTasks = make(map[int]types.Task)

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
	if _, exists := pendingTasks[newTask.ID]; exists {
		http.Error(w, "Task ID already exists", http.StatusBadRequest)
		return
	}
	if _, exists := inProgressTasks[newTask.ID]; exists {
		http.Error(w, "Task ID already exists", http.StatusBadRequest)
		return
	}
	if _, exists := completedTasks[newTask.ID]; exists {
		http.Error(w, "Task ID already exists", http.StatusBadRequest)
		return
	}
	newTask.Status = types.Pending
	pendingTasks[newTask.ID] = newTask
	log.Println(newTask.ID, "added to task queue", "Payload:", newTask.Payload, "Total Pending tasks:", len(pendingTasks))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newTask)
}

func poll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(pendingTasks) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var task types.Task
	for _, t := range pendingTasks {
		task = t
		break
	}
	delete(pendingTasks, task.ID)
	task.Status = types.InProgress
	inProgressTasks[task.ID] = task
	log.Println("Task", task.ID, "moved to in_progress")
	workerID := r.URL.Query().Get("worker")
	log.Println("Assigned task", task.ID, "to worker", workerID)
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

	task, exists := inProgressTasks[ackReq.TaskID]

	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	delete(inProgressTasks, ackReq.TaskID)
	task.Status = types.Completed
	completedTasks[task.ID] = task
	log.Println("Task", task.ID, "marked completed")
	w.WriteHeader(http.StatusOK)
}

func main() {
	log.Println("Broker running on port 8080")
	http.HandleFunc("/ping", ping)
	http.HandleFunc("/task", task)
	http.HandleFunc("/poll", poll)
	http.HandleFunc("/ack", ack)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Println("Server failed: ", err)
	}
}
