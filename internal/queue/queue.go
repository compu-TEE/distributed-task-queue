package queue

import (
	"dtq/internal/persistence"
	"dtq/internal/types"
	"fmt"
	"log"
	"sync"
	"time"
)

type BrokerState struct {
	PendingTasks    map[int]types.Task
	InProgressTasks map[int]types.Task
	CompletedTasks  map[int]types.Task
	DeadLetterTasks map[int]types.Task

	Mutex sync.Mutex
}

var State = BrokerState{
	PendingTasks:    make(map[int]types.Task),
	InProgressTasks: make(map[int]types.Task),
	CompletedTasks:  make(map[int]types.Task),
	DeadLetterTasks: make(map[int]types.Task),
}

func AddTask(newTask types.Task) error {
	State.Mutex.Lock()
	defer State.Mutex.Unlock()

	if _, exists := State.PendingTasks[newTask.ID]; exists {
		return fmt.Errorf("task ID already exists")
	}

	if _, exists := State.InProgressTasks[newTask.ID]; exists {
		return fmt.Errorf("task ID already exists")
	}

	if _, exists := State.CompletedTasks[newTask.ID]; exists {
		return fmt.Errorf("task ID already exists")
	}

	newTask.Status = types.Pending
	newTask.RetryCount = 0
	newTask.MaxRetries = 3

	State.PendingTasks[newTask.ID] = newTask

	err := persistence.AppendLog(
		fmt.Sprintf("ENQUEUE %d %s", newTask.ID, newTask.Payload),
	)
	if err != nil {
		log.Println("Failed to persist enqueue:", err)
	}

	log.Println(
		newTask.ID,
		"added to task queue",
		"Payload:",
		newTask.Payload,
		"Total Pending tasks:",
		len(State.PendingTasks),
	)

	return nil
}

func PollTask(workerID string) (*types.Task, bool, error) {
	State.Mutex.Lock()
	defer State.Mutex.Unlock()

	if len(State.PendingTasks) == 0 {
		return nil, false, nil
	}

	var task types.Task
	for _, t := range State.PendingTasks {
		task = t
		break
	}

	delete(State.PendingTasks, task.ID)

	task.Status = types.InProgress
	task.AssignedAt = time.Now()
	task.WorkerID = workerID

	State.InProgressTasks[task.ID] = task

	err := persistence.AppendLog(
		fmt.Sprintf("ASSIGN %d %s", task.ID, task.WorkerID),
	)
	if err != nil {
		log.Println("Failed to persist dequeue:", err)
	}

	log.Println("Task", task.ID, "moved to in_progress")
	log.Println("Assigned task", task.ID, "to worker", workerID)

	return &task, true, nil
}

func AckTask(taskID int) error {
	State.Mutex.Lock()
	defer State.Mutex.Unlock()

	task, exists := State.InProgressTasks[taskID]
	if !exists {
		return fmt.Errorf("task not found")
	}

	delete(State.InProgressTasks, taskID)

	task.Status = types.Completed
	State.CompletedTasks[task.ID] = task

	err := persistence.AppendLog(
		fmt.Sprintf("ACK %d", task.ID),
	)
	if err != nil {
		log.Println("Failed to persist ACK:", err)
	}

	log.Println("Task", task.ID, "marked completed")

	return nil
}
