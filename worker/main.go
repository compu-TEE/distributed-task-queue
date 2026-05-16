package main

import (
	"bufio"
	//"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	workerID := "worker-1"
	for {
		resp, err := http.Get("http://localhost:8080/ping?worker=" + workerID)
		if err != nil {
			panic(err)
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			log.Println(scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		resp.Body.Close()

		time.Sleep(1 * time.Second)
	}
}
