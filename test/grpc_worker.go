package main

import (
	"context"
	"log"
	"time"

	pb "dtq/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(
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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	resp, err := client.PollTask(
		ctx,
		&pb.PollTaskRequest{
			WorkerId: "worker-1",
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Found:", resp.Found)

	if resp.Found {
		log.Printf(
			"Task ID=%d Payload=%s\n",
			resp.Task.Id,
			resp.Task.Payload,
		)
	}
}
