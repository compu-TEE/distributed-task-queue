package main

import (
	"bytes"
	"dtq/internal/types"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
)

type AckRequest struct {
	TaskID int `json:"task_id"`
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime)

	workerID := os.Args[1]

	for {
		resp, err := http.Get("http://localhost:8080/poll?worker=" + workerID)
		if err != nil {
			log.Println("Failed to poll broker:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// No tasks available
		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		var task types.Task

		err = json.NewDecoder(resp.Body).Decode(&task)
		resp.Body.Close()

		if err != nil {
			log.Println("Failed to decode task:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Println(
			"Executing task",
			task.ID,
			"on worker",
			workerID,
			"Payload:",
			task.Payload,
		)

		time.Sleep(3 * time.Second)
		log.Println("Sending ACK for task", task.ID)
		ack := AckRequest{
			TaskID: task.ID,
		}
		jsonData, err := json.Marshal(ack)
		if err != nil {
			log.Println("Failed to marshal ACK:", err)
			continue
		}

		ackResp, err := http.Post(
			"http://localhost:8080/ack",
			"application/json",
			bytes.NewBuffer(jsonData),
		)
		if err != nil {
			log.Println("Failed to send ACK:", err)
			continue
		}
		ackResp.Body.Close()
	}
}
