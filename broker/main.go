package main

import (
	"encoding/json"
	//"fmt"
	"dtq/internal/types"
	"log"
	"net/http"
)

var tasks []types.Task

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
	tasks = append(tasks, newTask)
	log.Println(newTask.ID, "added to task queue", "Payload:", newTask.Payload, "Total tasks:", len(tasks))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(newTask)
}

func poll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(tasks) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	task := tasks[0]
	tasks = tasks[1:]
	workerID := r.URL.Query().Get("worker")
	log.Println("Assigned task", task.ID, "to worker", workerID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

func main() {
	log.Println("Broker running on port 8080")
	http.HandleFunc("/ping", ping)
	http.HandleFunc("/task", task)
	http.HandleFunc("/poll", poll)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Println("Server failed: ", err)
	}
}
