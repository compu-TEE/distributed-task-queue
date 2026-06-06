# Distributed Task Queue

A fault-tolerant distributed task queue built in Go using gRPC and Protocol Buffers.

Designed to execute tasks reliably across multiple worker nodes while handling worker failures, retries, task recovery, and broker restarts.

## Highlights

* Processed **1000+ tasks**
* Scaled to **16 worker nodes**
* Achieved **100% task completion**
* One-command deployment using Docker Compose

---

## Features

### Core Execution

* Distributed task execution using gRPC
* Streaming RPC communication between broker and workers
* Dynamic task dispatching
* Multi-worker load balancing

### Fault Tolerance

* Worker heartbeats
* Worker failure detection
* Visibility timeouts
* Automatic task retries
* Dead Letter Queue (DLQ)
* Broker restart recovery
* Persistent task storage

### Observability

* Worker status endpoint
* Metrics endpoint
* Task completion tracking
* Worker utilization metrics

### Deployment

* Dockerized broker and worker services
* Docker Compose cluster deployment
* Service discovery through Docker networking

---

## Architecture

```text
                    +---------+
                    | Client  |
                    +---------+
                         |
                    SubmitTask
                         |
                         v

                +----------------+
                |     Broker     |
                +----------------+
                 |      |      |
                 |      |      |
                 |      |      |
           Dispatch   Metrics  Persistence
                 |
                 |
      +----------+----------+
      |          |          |
      v          v          v

 +---------+ +---------+ +---------+
 | Worker  | | Worker  | | Worker  |
 |    1    | |    2    | |    N    |
 +---------+ +---------+ +---------+

      ^          ^          ^
      |          |          |
      +----------+----------+
             Heartbeats
```

---

## Task Lifecycle

```text
Pending
   |
   v
In Progress
   |
   +----------------+
   |                |
   v                |
Completed      Worker Failure
                    |
                    v
             Visibility Timeout
                    |
                    v
                 Retry
                    |
          +---------+---------+
          |                   |
          v                   v
      Success            Retry Limit
                              |
                              v
                            DLQ
```

---

## Fault Tolerance

### Worker Failure Recovery

```text
Worker Crash
      |
      v
Heartbeat Timeout
      |
      v
Task Reclaimed
      |
      v
Task Reassigned
      |
      v
Execution Continues
```

### Broker Recovery

```text
Broker Restart
      |
      v
Load Persisted State
      |
      v
Recover Pending Tasks
      |
      v
Resume Processing
```

---

## Project Structure

```text
distributed-task-queue/
│
├── broker/
├── client/
├── worker/
├── worker_poison/
│
├── internal/
│   ├── grpc/
│   ├── persistence/
│   ├── queue/
│   ├── task/
│   └── types/
│
├── proto/
├── test/
├── docker/
│
├── docker-compose.yml
└── README.md
```

---

## Benchmark Results

| Tasks | Workers | Completion Rate |
| ----- | ------- | --------------- |
| 50    | 4       | 100%            |
| 500   | 8       | 100%            |
| 1000  | 16      | 100%            |

---

## Metrics Endpoint

```bash
curl localhost:8080/metrics
```

Example:

```json
{
  "uptime_seconds": 43,
  "pending_tasks": 0,
  "in_progress_tasks": 0,
  "completed_tasks": 1000,
  "connected_workers": 16,
  "idle_workers": 16,
  "total_retries": 0
}
```

---

## Worker Status

```bash
curl localhost:8080/workers
```

Example:

```json
{
  "worker-1": {
    "status": "idle",
    "tasks_completed": 125
  }
}
```

---

## Running Locally

### Docker Compose

```bash
git clone https://github.com/compu-TEE/distributed-task-queue.git

cd distributed-task-queue

docker compose up
```

Broker and worker services will start automatically.

---

## Technologies

* Go
* gRPC
* Protocol Buffers
* Docker
* Docker Compose

---

## Design Goals

This project was built to explore the core concepts behind distributed execution systems:

* Reliable task execution
* Fault tolerance
* Worker coordination
* Failure recovery
* Service communication using gRPC
* Distributed systems observability

---
