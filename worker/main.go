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

	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "localhost:50051"
	}

	conn, err := grpc.Dial(
		brokerAddr,
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
			"Executing task %d on worker %s Payload: %s",
			task.Id,
			workerID,
			task.Payload,
		)

		time.Sleep(10 * time.Second)

		log.Printf("Sending ACK for task %d", task.Id)

		_, err = client.AckTask(
			context.Background(),
			&pb.AckTaskRequest{
				TaskId:   task.Id,
				WorkerId: workerID,
			},
		)

		if err != nil {
			log.Printf("ACK failed: %v", err)
		}
	}
}
