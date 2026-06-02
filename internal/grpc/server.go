package grpc

import (
	"context"
	"dtq/internal/queue"
	"dtq/internal/types"
	pb "dtq/proto"
	"fmt"
	"log"
	"sync"
)

type BrokerServer struct {
	pb.UnimplementedBrokerServiceServer
}

type WorkerConnection struct {
	WorkerID string
	Stream   pb.BrokerService_StreamTasksServer
}

var (
	ConnectedWorkers = make(map[string]*WorkerConnection)
	WorkersMu        sync.RWMutex
)

func PushTask(workerID string, task types.Task) error {
	WorkersMu.RLock()
	conn, ok := ConnectedWorkers[workerID]
	WorkersMu.RUnlock()

	if !ok {
		return fmt.Errorf("worker not connected")
	}

	msg := &pb.StreamMessage{
		Task: &pb.Task{
			Id:      int32(task.ID),
			Payload: task.Payload,
		},
	}

	if err := conn.Stream.Send(msg); err != nil {
		return err
	}

	log.Printf("pushed task %d to %s", task.ID, workerID)

	return nil
}

func (s *BrokerServer) Ping(
	ctx context.Context,
	req *pb.PingRequest,
) (*pb.PingResponse, error) {

	return &pb.PingResponse{
		Message: "broker alive",
	}, nil
}

func (s *BrokerServer) StreamTasks(
	req *pb.StreamRequest,
	stream pb.BrokerService_StreamTasksServer,
) error {

	log.Printf("%s connected to stream", req.WorkerId)

	conn := &WorkerConnection{
		WorkerID: req.WorkerId,
		Stream:   stream,
	}

	WorkersMu.Lock()
	ConnectedWorkers[req.WorkerId] = conn
	WorkersMu.Unlock()

	go DispatchPendingTasks(req.WorkerId)

	defer func() {
		WorkersMu.Lock()
		delete(ConnectedWorkers, req.WorkerId)
		WorkersMu.Unlock()

		log.Printf("%s disconnected", req.WorkerId)
	}()

	<-stream.Context().Done()

	return nil
}

func DispatchPendingTasks(workerID string) {
	for {
		task, found, err := queue.PollTask(workerID)
		if err != nil {
			log.Printf("dispatch failed: %v", err)
			return
		}

		if !found {
			return
		}

		if err := PushTask(workerID, *task); err != nil {
			log.Printf("push failed: %v", err)
			return
		}

		log.Printf(
			"dispatched pending task %d to %s",
			task.ID,
			workerID,
		)
	}
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

	// Push to first connected worker
	var workerID string

	WorkersMu.RLock()
	for id := range ConnectedWorkers {
		workerID = id
		break
	}
	WorkersMu.RUnlock()

	if workerID != "" {
		if err := PushTask(workerID, task); err != nil {
			log.Printf("push failed: %v", err)
		}
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
