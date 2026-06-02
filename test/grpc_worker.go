package main

import (
	"context"
	"log"

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

	stream, err := client.StreamTasks(
		context.Background(),
		&pb.StreamRequest{
			WorkerId: "worker-1",
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("received %+v", msg.Task)
	}
}
