package main

import (
	"log"
	"net"

	grpcserver "dtq/internal/grpc"
	pb "dtq/proto"

	"google.golang.org/grpc"
)

func StartGRPCServer() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	s := grpc.NewServer()

	pb.RegisterBrokerServiceServer(
		s,
		&grpcserver.BrokerServer{},
	)

	log.Println("gRPC server listening on :50051")

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
