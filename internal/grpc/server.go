package grpc

import (
	"context"
	"dtq/internal/queue"
	"dtq/internal/types"
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

func (s *BrokerServer) SubmitTask(
	ctx context.Context,
	req *pb.SubmitTaskRequest,
) (*pb.SubmitTaskResponse, error) {

	task := types.Task{
		ID:      int(req.Id),
		Payload: req.Payload,
	}

	if err := queue.AddTask(task); err != nil {
		return &pb.SubmitTaskResponse{
			Success: false,
		}, err
	}

	return &pb.SubmitTaskResponse{
		Success: true,
	}, nil
}

func (s *BrokerServer) PollTask(
	ctx context.Context,
	req *pb.PollTaskRequest,
) (*pb.PollTaskResponse, error) {

	task, found, err := queue.PollTask(req.WorkerId)
	if err != nil {
		return nil, err
	}

	if !found {
		return &pb.PollTaskResponse{
			Found: false,
		}, nil
	}

	return &pb.PollTaskResponse{
		Found: true,
		Task: &pb.Task{
			Id:      int32(task.ID),
			Payload: task.Payload,
		},
	}, nil
}

func (s *BrokerServer) AckTask(
	ctx context.Context,
	req *pb.AckTaskRequest,
) (*pb.AckTaskResponse, error) {

	err := queue.AckTask(int(req.TaskId))

	if err != nil {
		return &pb.AckTaskResponse{
			Success: false,
		}, err
	}

	return &pb.AckTaskResponse{
		Success: true,
	}, nil
}
