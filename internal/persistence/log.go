package persistence

import (
	"bufio"
	"dtq/internal/types"

	//"fmt"
	"os"
	"strconv"
	"strings"
)

const LogFile = "broker.log"

func AppendLog(entry string) error {
	file, err := os.OpenFile(
		LogFile,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return err
	}

	defer file.Close()

	_, err = file.WriteString(entry + "\n")

	if err != nil {
		return err
	}

	return nil
}

func ReplayLog() (map[int64]types.Task, error) {
	file, err := os.Open(LogFile)
	if os.IsNotExist(err) {
		return make(map[int64]types.Task), nil
	}
	if err != nil {
		return nil, err
	}

	defer file.Close()
	replayTasks := make(map[int64]types.Task)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.Fields(line)

		eventType := parts[0]

		if eventType == "ENQUEUE" {
			taskID, err := strconv.ParseInt(parts[1], 10, 64)

			if err != nil {
				return nil, err
			}

			payload := parts[2]

			replayTasks[int64(taskID)] = types.Task{
				ID:         int64(taskID),
				Payload:    payload,
				Status:     types.Pending,
				RetryCount: 0,
				MaxRetries: 3,
			}
		}

		if eventType == "ACK" {
			taskID, err := strconv.ParseInt(parts[1], 10, 64)

			if err != nil {
				return nil, err
			}

			task := replayTasks[int64(taskID)]

			task.Status = types.Completed

			replayTasks[int64(taskID)] = task
		}

		if eventType == "RETRY" {
			taskID, err := strconv.ParseInt(parts[1], 10, 64)

			if err != nil {
				return nil, err
			}

			retryCount, err := strconv.Atoi(parts[2])

			if err != nil {
				return nil, err
			}

			task := replayTasks[int64(taskID)]

			task.RetryCount = retryCount

			replayTasks[int64(taskID)] = task
		}
		if eventType == "DLQ" {
			taskID, err := strconv.ParseInt(parts[1], 10, 64)

			if err != nil {
				return nil, err
			}

			task := replayTasks[int64(taskID)]

			task.Status = types.DeadLetter

			replayTasks[int64(taskID)] = task
		}
	}
	return replayTasks, scanner.Err()
}
