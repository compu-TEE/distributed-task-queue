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

func ReplayLog() (map[int]types.Task, error) {
	file, err := os.Open(LogFile)
	if os.IsNotExist(err) {
		return make(map[int]types.Task), nil
	}
	if err != nil {
		return nil, err
	}

	defer file.Close()
	replayTasks := make(map[int]types.Task)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.Fields(line)

		eventType := parts[0]

		if eventType == "ENQUEUE" {
			taskID, err := strconv.Atoi(parts[1])

			if err != nil {
				return nil, err
			}

			payload := parts[2]

			replayTasks[taskID] = types.Task{
				ID:      taskID,
				Payload: payload,
				Status:  types.Pending,
			}
		}

		if eventType == "ACK" {
			taskID, err := strconv.Atoi(parts[1])

			if err != nil {
				return nil, err
			}

			task := replayTasks[taskID]

			task.Status = types.Completed

			replayTasks[taskID] = task
		}
	}
	return replayTasks, scanner.Err()
}
