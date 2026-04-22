# GPU Telemetry Pipeline

Streams DCGM GPU metrics through a custom message queue into PostgreSQL, exposed via a REST API.

Data flow: `streamer → mq-server → collector → postgres ← api-gateway`

## Links

- [Swagger UI](http://localhost:8080/swagger/index.html) — available when stack is running
- [OpenAPI spec](docs/swagger.yaml)
- [Architecture diagram](docs/architecture.excalidraw) — open at excalidraw.com

## Design

**Custom message queue.** The MQ server is a purpose-built in-memory broker — no Kafka, no RabbitMQ. Messages are partitioned by GPU UUID using FNV-1a consistent hashing across 8 partitions. Each collector registers in a consumer group; when a new collector joins the group, the broker rebalances partition assignments so all partitions remain covered. Collectors ack offsets only after a successful PostgreSQL write, giving at-least-once delivery. Compaction runs after every ack round, reclaiming memory for messages all groups have consumed. Backpressure is applied at the partition level: a full partition returns HTTP 429 so streamers slow down rather than OOM the broker. An optional WAL (append-only, one JSON line per message) lets the broker replay unacknowledged messages across restarts.

**Collector persistence.** The collector uses the PostgreSQL COPY protocol via `pgx.CopyFrom` for bulk inserts rather than individual `INSERT` statements. This gives roughly 10× higher throughput for the bursty 50-row batches the streamer emits.

**Timestamps.** `collected_at` is set to `time.Now()` at the moment the streamer processes each CSV row — not the timestamp field in the CSV — per the spec requirement that "the time at which a specific telemetry log is processed should be considered as the timestamp of that telemetry."

**Scaling.** Streamers are stateless; scale them freely. Collectors are stateless with respect to the DB; adding a collector triggers automatic partition rebalance. The MQ server itself is a single node (no horizontal scaling) — the known trade-off for a single-exercise scope.

**Single point of failure.** The MQ server is a single process. In production this would be addressed with a replicated Raft log or by replacing the custom broker with a managed service. For this scope, the WAL provides crash recovery for the single instance.

## Architecture

```mermaid
flowchart LR
    CSV[dcgm_metrics.csv]

    subgraph Producers
        S1[Streamer 1]
        SN[Streamer N]
    end

    subgraph MQ[MQ Server]
        P0[Partition 0]
        P1[Partition 1]
        PN[Partition N]
    end

    subgraph Consumers
        C1[Collector 1]
        CN[Collector N]
    end

    DB[(PostgreSQL)]
    API[API Gateway :8080]
    Client[curl / UI]

    CSV --> Producers
    Producers -->|POST /publish| MQ
    MQ -->|GET /consume| Consumers
    Consumers -->|COPY protocol| DB
    DB --> API
    API --> Client
```

## Prerequisites

- Go 1.22+
- Docker 24+
- Helm 3+ (for Kubernetes deploy only)

```
make check-deps
```

## Setup and Run

```
git clone https://github.com/shivangi-975/elastic-gpu-telemetry-pipeline-ai.git
cd elastic-gpu-telemetry-pipeline-ai
make up
```

Starts 5 containers:

| Container | Role | Port |
|---|---|---|
| postgres | Persistent store | 5433 (host) |
| mq-server | Custom message broker | 9001 (host) |
| streamer | Reads CSV, publishes to MQ | — |
| collector | Consumes MQ, writes to DB | — |
| api-gateway | REST API | 8080 (host) |

On first run Docker builds all images from source — takes 3-5 minutes. Wait for collector to start inserting before querying:

```
make logs
# wait for: {"msg":"bulk insert telemetry","rows":50}
# then Ctrl+C
```

## Database

PostgreSQL runs as a Docker container. No local installation needed.

```
Host:     localhost:5433
Database: telemetry
User:     telemetry
Password: changeme
DSN:      postgres://telemetry:changeme@localhost:5433/telemetry?sslmode=disable
```

Connect directly:

```
psql postgres://telemetry:changeme@localhost:5433/telemetry
```

## API

First get a GPU UUID:

```bash
curl http://localhost:8080/api/v1/gpus
```

Then query telemetry using the UUID from above. Note: `collected_at` is stamped as `time.Now()` when the streamer publishes, so timestamps reflect when you ran `make up`.

```bash
# health
curl http://localhost:8080/health
curl http://localhost:8080/healthz

# all telemetry for a GPU (paginated, default limit 1000)
curl "http://localhost:8080/api/v1/gpus/<UUID>/telemetry"

# filter by metric name
curl "http://localhost:8080/api/v1/gpus/<UUID>/telemetry?metric_name=DCGM_FI_DEV_GPU_UTIL"

# filter by time range — use collected_at values returned above as start_time/end_time
curl "http://localhost:8080/api/v1/gpus/<UUID>/telemetry?start_time=<RFC3339>&end_time=<RFC3339>"

# metric + time range combined
curl "http://localhost:8080/api/v1/gpus/<UUID>/telemetry?metric_name=DCGM_FI_DEV_GPU_UTIL&start_time=<RFC3339>&end_time=<RFC3339>"

# pagination
curl "http://localhost:8080/api/v1/gpus/<UUID>/telemetry?limit=10&offset=0"

# MQ metrics (partition depths, consumer offsets)
curl http://localhost:9001/metrics
```

Swagger UI: http://localhost:8080/swagger/index.html

## Stop

```
make down          # stop containers, keep data volume
make down -v       # stop containers, wipe data volume
```

## Test

```
make test                # unit tests with race detector
make test-integration    # store tests, requires Docker
make test-coverage       # coverage report → coverage.html
```

## Scale

```
make scale SERVICE=streamer N=3
make scale SERVICE=collector N=2
```

## Docker Images

Pre-built images are published at `ghcr.io/shivangi-975`:

```
ghcr.io/shivangi-975/api-gateway:latest
ghcr.io/shivangi-975/mq-server:latest
ghcr.io/shivangi-975/streamer:latest
ghcr.io/shivangi-975/collector:latest
```

To build and push your own:

```
make docker-build REGISTRY=ghcr.io/<your-org>
make docker-push  REGISTRY=ghcr.io/<your-org>
```

## Kubernetes (minikube)

Install minikube:

```
brew install minikube
minikube start --cpus 4 --memory 7000 --driver docker
```

Deploy using pre-built images:

```
helm upgrade --install gpu-telemetry helm/gpu-telemetry-pipeline \
  --namespace telemetry \
  --create-namespace \
  --set global.imageRegistry=ghcr.io/shivangi-975 \
  --set image.tag=latest
```

Check pods:

```
kubectl get pods -n telemetry
```

Access the API:

```
kubectl port-forward svc/gpu-telemetry-gpu-telemetry-pipeline-api-gateway -n telemetry 8080:8080
curl http://localhost:8080/health
```

Tear down:

```
make helm-uninstall
minikube stop
```

## AI Usage

See [AI_USAGE.md](AI_USAGE.md) for a detailed breakdown of which parts of the codebase were generated by AI, the exact prompts used, and where manual intervention was required.

## Maintainers

parasharshivangi5@gmail.com
