package grpc

import (
	"context"

	pb "dtq/proto"
)

type BrokerServer struct {
	pb.UnimplementedBrokerServiceServer
}

func (s *BrokerServer) Ping(
	ctx context.Context,
	req *pb.PingRequest,
) (*pb.PingResponse, error) {

	return &pb.PingResponse{
		Message: "broker alive",
	}, nil
}
