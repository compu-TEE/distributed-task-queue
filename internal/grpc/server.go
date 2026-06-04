package grpc

import (
	"context"
	"dtq/internal/queue"
	"dtq/internal/types"
	pb "dtq/proto"
	"fmt"
	"log"
	"sync"
	"time"
)

type BrokerServer struct {
	pb.UnimplementedBrokerServiceServer
}

type WorkerInfo struct {
	ID             string
	Status         string
	LastHeartbeat  time.Time
	TasksCompleted int
}

type WorkerConnection struct {
	Info   WorkerInfo
	Stream pb.BrokerService_StreamTasksServer
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
			Id:      int64(task.ID),
			Payload: task.Payload,
		},
	}

	if err := conn.Stream.Send(msg); err != nil {
		return err
	}

	WorkersMu.Lock()
	conn.Info.Status = "busy"
	WorkersMu.Unlock()

	log.Printf("%s -> busy", workerID)

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
		Info: WorkerInfo{
			ID:             req.WorkerId,
			Status:         "idle",
			LastHeartbeat:  time.Now(),
			TasksCompleted: 0,
		},
		Stream: stream,
	}
	WorkersMu.Lock()
	ConnectedWorkers[req.WorkerId] = conn
	log.Printf("registered worker: %+v", conn.Info)
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

func (s *BrokerServer) SubmitTask(
	ctx context.Context,
	req *pb.SubmitTaskRequest,
) (*pb.SubmitTaskResponse, error) {

	task := types.Task{
		ID:      int64(req.Id),
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
	for id, worker := range ConnectedWorkers {
		if worker.Info.Status == "idle" {
			workerID = id
			break
		}
	}
	WorkersMu.RUnlock()

	if workerID != "" {
		assignedTask, found, err := queue.PollTask(workerID)
		if err != nil {
			log.Printf("assignment failed: %v", err)
		} else if found {
			if err := PushTask(workerID, *assignedTask); err != nil {
				log.Printf("push failed: %v", err)
			}
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
			Id:      int64(task.ID),
			Payload: task.Payload,
		},
	}, nil
}

func (s *BrokerServer) AckTask(
	ctx context.Context,
	req *pb.AckTaskRequest,
) (*pb.AckTaskResponse, error) {

	err := queue.AckTask(int64(req.TaskId))

	if err != nil {
		return &pb.AckTaskResponse{
			Success: false,
		}, err
	}

	WorkersMu.Lock()

	workerExists := false

	if worker, ok := ConnectedWorkers[req.WorkerId]; ok {
		worker.Info.Status = "idle"
		worker.Info.TasksCompleted++
		workerExists = true

		log.Printf(
			"%s -> idle (completed=%d)",
			req.WorkerId,
			worker.Info.TasksCompleted,
		)
	}

	WorkersMu.Unlock()

	if workerExists {
		go DispatchPendingTasks(req.WorkerId)
	}

	return &pb.AckTaskResponse{
		Success: true,
	}, nil
}

func (s *BrokerServer) Heartbeat(
	ctx context.Context,
	req *pb.HeartbeatRequest,
) (*pb.HeartbeatResponse, error) {

	WorkersMu.Lock()
	defer WorkersMu.Unlock()

	if worker, ok := ConnectedWorkers[req.WorkerId]; ok {
		worker.Info.LastHeartbeat = time.Now()
	}

	return &pb.HeartbeatResponse{
		Success: true,
	}, nil
}

func WorkerMonitor() {
	for {
		log.Println("worker monitor tick")

		WorkersMu.Lock()

		for id, worker := range ConnectedWorkers {
			log.Printf(
				"%s age=%v",
				id,
				time.Since(worker.Info.LastHeartbeat),
			)

			if time.Since(worker.Info.LastHeartbeat) > 15*time.Second {
				log.Printf("%s timed out", id)
				delete(ConnectedWorkers, id)
			}
		}

		WorkersMu.Unlock()

		time.Sleep(5 * time.Second)
	}
}

func DispatchToIdleWorkers() {
	WorkersMu.RLock()

	var idleWorkers []string

	for id, worker := range ConnectedWorkers {
		if worker.Info.Status == "idle" {
			idleWorkers = append(idleWorkers, id)
		}
	}

	WorkersMu.RUnlock()

	for _, id := range idleWorkers {
		go DispatchPendingTasks(id)
	}
}
