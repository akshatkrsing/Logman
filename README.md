# Logman

**A distributed log aggregation system built with Go, gRPC, and the Raft consensus algorithm.**

Logman is a Go-based distributed logging system designed to provide reliable log ingestion, replicated storage, and efficient log retrieval through a gRPC API. It uses the Raft consensus algorithm to coordinate log replication across multiple nodes and maintain a consistent log state.

The project combines distributed consensus, persistent storage, batch ingestion, indexed querying, retention management, and Prometheus-based observability into a single system.

## Key Features

- **Raft-based consensus:** Coordinates multiple Raft nodes and replicates log entries through a consensus-based architecture.
- **gRPC API:** Provides high-performance APIs for log ingestion and retrieval.
- **Single log ingestion:** Accepts individual log entries through the `SendLog` RPC.
- **Batch ingestion:** Supports submitting multiple log entries through a single `BatchSendLogs` RPC.
- **Indexed log retrieval:** Supports filtering logs by request ID, log level, service name, metadata, timestamp, and keyword.
- **Request deduplication:** Uses request IDs to prevent duplicate log entries from being applied.
- **Persistent storage:** Uses Raft's persistent storage and snapshots to retain replicated state.
- **Log retention:** Supports automatic removal of logs older than the configured retention period.
- **Health and readiness checks:** Exposes endpoints to check application health and cluster readiness.
- **Prometheus monitoring:** Exposes ingestion counters, error metrics, and Raft node state metrics.
- **Docker deployment:** Includes Docker and Docker Compose configuration for running the application and Prometheus.

## Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Go |
| Distributed Consensus | HashiCorp Raft |
| RPC Framework | gRPC |
| Serialization | Protocol Buffers |
| Persistent Raft Storage | BoltDB |
| Monitoring | Prometheus |
| Containerization | Docker |
| Container Orchestration | Docker Compose |
| Configuration | YAML and environment variables |

## Architecture

Logman uses a Raft-based architecture in which multiple nodes participate in maintaining a consistent replicated log state.

```text
                  gRPC Client
                       |
                       v
                Logman gRPC API
                   :50051
                       |
                       v
                Raft Cluster
              /      |      \
             v       v       v
          Node 1   Node 2   Node 3
             \       |       /
              \      |      /
               Replicated
                Log State
                       |
                       v
              Persistent Storage

               Monitoring API
                   :8081
                 /    |    \
             Health Ready Metrics
                       |
                       v
                  Prometheus
                    :9090
```

The application starts three Raft nodes and exposes a gRPC endpoint that routes requests through the current leader.

**Deployment note:** The current Docker Compose configuration runs all three Raft nodes within one Logman container. This demonstrates Raft-based replication and consensus, but does not provide independent container or host failure isolation for each node.

## Project Structure

```text
logman/
├── cmd/
│   ├── cluster/
│   │   └── main.go
│   └── server/
│       └── main.go
├── configs/
│   ├── node1.yaml
│   ├── node2.yaml
│   └── node3.yaml
├── gen/
│   └── logmanpb/
│       ├── logman.pb.go
│       └── logman_grpc.pb.go
├── internal/
│   ├── api/
│   │   └── grpc_server.go
│   ├── config/
│   │   └── config.go
│   ├── observability/
│   │   └── metrics.go
│   ├── raft/
│   │   ├── fsm.go
│   │   ├── node.go
│   │   ├── fsm_bench_test.go
│   │   └── cluster_bench_test.go
│   └── storage/
├── proto/
│   └── logman.proto
├── Dockerfile
├── docker-compose.yml
├── prometheus.yml
├── .dockerignore
├── .gitignore
├── go.mod
└── go.sum
```

## Getting Started

### Prerequisites

- Go 1.26 or compatible version
- Docker Desktop with Docker Compose
- Git
- `grpcurl` for testing gRPC endpoints

### Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/logman.git
cd logman
```

### Run Using Docker Compose

Build the Docker image:

```bash
docker compose build
```

Start Logman and Prometheus:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

View application logs:

```bash
docker compose logs -f logman
```

Stop the application:

```bash
docker compose down
```

The Docker Compose deployment starts the Logman cluster and Prometheus monitoring service.

## API Usage

Logman exposes its gRPC service on port `50051`.

The following examples use `grpcurl` with gRPC reflection enabled.

### 1. Send a Log

RPC: `LogService/SendLog`

```bash
grpcurl -plaintext \
  -d '{
    "requestId": "demo-001",
    "message": "Payment processed successfully",
    "level": "INFO",
    "serviceName": "payment-service"
  }' \
  localhost:50051 logman.LogService/SendLog
```

### 2. Send Logs in a Batch

RPC: `LogService/BatchSendLogs`

```bash
grpcurl -plaintext \
  -d '{
    "logs": [
      {
        "requestId": "demo-002",
        "message": "Order created",
        "level": "INFO",
        "serviceName": "order-service"
      },
      {
        "requestId": "demo-003",
        "message": "Payment failed",
        "level": "ERROR",
        "serviceName": "payment-service"
      }
    ]
  }' \
  localhost:50051 logman.LogService/BatchSendLogs
```

### 3. Retrieve Logs

RPC: `LogService/GetLogs`

```bash
grpcurl -plaintext \
  -d '{
    "pageNumber": 1,
    "pageSize": 10
  }' \
  localhost:50051 logman.LogService/GetLogs
```

The retrieval API supports pagination and filtering based on supported query fields, including:

- Request ID
- Log level
- Service name
- Keyword
- Timestamp
- Metadata

Refer to `proto/logman.proto` for the complete RPC definitions and request/response schemas.

## Health Checks and Monitoring

Logman exposes an HTTP monitoring server on port `8081`.

| Endpoint | Purpose |
|---|---|
| `/health` | Checks whether the application is alive |
| `/ready` | Checks whether the cluster has an elected leader |
| `/metrics` | Exposes Prometheus metrics |

### Prometheus Metrics

The application exposes the following metrics:

| Metric | Description |
|---|---|
| `logman_logs_ingested_total` | Total successfully ingested individual logs |
| `logman_batches_ingested_total` | Total successfully ingested batches |
| `logman_ingestion_errors_total` | Number of ingestion errors |
| `logman_raft_node_state` | Raft node state, labelled by node ID and state |

Access the monitoring endpoints:

- Health: http://localhost:8081/health
- Readiness: http://localhost:8081/ready
- Metrics: http://localhost:8081/metrics
- Prometheus: http://localhost:9090

## Configuration

Each Raft node has its own YAML configuration file under `configs/`.

```text
configs/
├── node1.yaml
├── node2.yaml
└── node3.yaml
```

Configuration covers node identity, Raft addresses, gRPC settings, data directories, cluster bootstrap/join behavior, snapshots, batch limits, and retention settings.

Environment variables can override supported configuration values. Refer to `internal/config/config.go` and the node configuration files for the exact supported settings.

## Persistence Verification

To verify that log data survives an application restart:

1. Submit a log using `SendLog`.
2. Retrieve it using `GetLogs`.
3. Restart the Logman container.

```bash
docker compose restart logman
```

4. Retrieve the same log again.

The Docker Compose configuration mounts persistent host directories for the three node data directories.

## Development

Download dependencies:

```bash
go mod download
```

Run the cluster directly:

```bash
go run ./cmd/cluster
```

Run tests:

```bash
go test ./...
```

Run tests with the race detector:

```bash
go test -race ./...
```

Run benchmarks:

```bash
go test ./internal/raft -bench=. -benchmem
```

## Current Scope and Limitations

Logman is an MVP implementation focused on distributed log ingestion, Raft-based replication, persistence, querying, and observability.

- The current Docker Compose deployment runs three Raft nodes inside a single application container.
- Independent deployment and failure isolation of each Raft node require a separate multi-container deployment configuration.
- Authentication and authorization are not currently included.
- Prometheus node-state metrics should be refreshed periodically to reflect leadership changes accurately.

The project is intended as an implementation of distributed systems concepts and a foundation for further development, rather than a production-ready managed logging platform.

## License

This project is currently provided without an explicit open-source license. Add a `LICENSE` file before granting others permission to reuse, modify, or distribute the code.
