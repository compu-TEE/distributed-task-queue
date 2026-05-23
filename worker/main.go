package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"dtq/internal/types"
)

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
			task.ID,
			"processed by worker",
			workerID,
			"Payload:",
			task.Payload,
		)

		time.Sleep(3 * time.Second)
	}
}
