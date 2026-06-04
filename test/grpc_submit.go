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
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)

	for i := 111; i < 120; i++ {
		id := time.Now().UnixNano()

		resp, err := client.SubmitTask(
			context.Background(),
			&pb.SubmitTaskRequest{
				Id:      id,
				Payload: "hello",
			},
		)

		if err != nil {
			log.Printf("task %d failed: %v", i+1, err)
			continue
		}

		log.Printf("task %d submitted (success=%v)", i+1, resp.Success)

		time.Sleep(1 * time.Millisecond)
	}
}
