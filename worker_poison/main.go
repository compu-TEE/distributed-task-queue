package main

import (
	"context"
	"log"
	"os"
	"time"

	pb "dtq/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime)

	if len(os.Args) < 2 {
		log.Fatal("usage: go run . <worker-id>")
	}

	workerID := os.Args[1]

	conn, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)

	go func() {
		sendHeartbeat := func() {
			_, err := client.Heartbeat(
				context.Background(),
				&pb.HeartbeatRequest{
					WorkerId: workerID,
				},
			)

			if err != nil {
				log.Printf("heartbeat failed: %v", err)
				return
			}

			log.Printf("heartbeat sent")
		}

		sendHeartbeat()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			sendHeartbeat()
		}
	}()

	stream, err := client.StreamTasks(
		context.Background(),
		&pb.StreamRequest{
			WorkerId: workerID,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Connected as %s", workerID)

	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}

		task := msg.Task

		log.Printf(
			"POISON WORKER received task %d",
			task.Id,
		)

		time.Sleep(60 * time.Second)

		if err != nil {
			log.Printf("ACK failed: %v", err)
		}
	}
}
