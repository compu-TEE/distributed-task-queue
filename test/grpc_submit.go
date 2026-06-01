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

	resp, err := client.SubmitTask(
		context.Background(),
		&pb.SubmitTaskRequest{
			Id:      1,
			Payload: "hello",
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("success:", resp.Success)
}
