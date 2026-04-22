# GPU Telemetry Pipeline

Streams DCGM GPU metrics through a custom message queue into PostgreSQL, exposed via a REST API.

Data flow: `streamer → mq-server → collector → postgres ← api-gateway`

## Links

- [Swagger UI](http://localhost:8080/swagger/index.html) — available when stack is running
- [OpenAPI spec](docs/swagger.yaml)

## Design

### Custom Message Queue

The MQ server is a purpose-built in-memory broker written from scratch in Go — no Kafka, RabbitMQ, ZeroMQ, NATS, or Redis Streams under the hood. It exposes four HTTP endpoints (`/publish`, `/consume`, `/ack`, `/metrics`) and is designed around five ideas:

- **Partitioning by GPU UUID.** Every message carries a key (the GPU UUID); the broker routes it to one of 8 partitions using FNV-1a consistent hashing. Same UUID always lands on the same partition, so per-GPU ordering is preserved end-to-end.
- **Consumer groups with auto-rebalance.** Collectors join a named consumer group. The broker tracks group membership; when a collector joins or leaves, partitions are reassigned across the surviving members so all 8 partitions stay covered. No external coordination service (ZooKeeper, etcd) needed — the broker is the single source of truth.
- **At-least-once delivery.** Collectors ack offsets only after the PostgreSQL write succeeds. If the collector crashes between consume and ack, the broker redelivers on the next poll. Duplicates are tolerated by the schema (id is a `BIGSERIAL`, no unique constraint on telemetry rows).
- **Backpressure via HTTP 429.** Each partition has a configurable max queue depth (default 4096). When full, `/publish` returns 429 and streamers exponentially back off — preventing the broker from OOMing under sustained burst load.
- **Optional WAL for crash recovery.** When `WAL_PATH` is set, every published message is appended as a JSON line. On restart the broker replays the WAL and re-stages messages that haven't been compacted away. Trade-off: WAL writes are synchronous, so enabling it caps throughput at disk fsync speed. Disabled by default.

Compaction runs after every ack: messages with offsets below the minimum-acked-offset across all consumer groups are dropped from memory. This keeps the broker's RSS bounded regardless of total throughput.

### Streamer

Reads `data/dcgm_metrics.csv` row-by-row (not all into memory) using `encoding/csv` with `FieldsPerRecord = -1` to tolerate the variable-width DCGM rows. Wraps to file start on EOF for continuous simulation. Batches 50 rows by default and flushes either when the batch is full or every 100ms (whichever comes first). Malformed rows are logged at WARN and skipped — a single bad row never aborts the stream.

`collected_at` is **not** taken from the CSV `timestamp` column. Per the PDF spec — *"the time at which a specific telemetry log is processed should be considered as the timestamp of that telemetry"* — `time.Now().UTC()` is stamped at processing time. This matches how a real DCGM exporter would behave when the stream is replayed.

### Collector

Polls the MQ for one batch at a time, parses the JSON payload, upserts the GPU into the `gpus` table, then bulk-inserts the telemetry rows using PostgreSQL's COPY protocol via `pgx.CopyFrom`. COPY is 5–10× faster than parameterised `INSERT` for batch sizes of 50+ — the bottleneck shifts from the network to disk IOPS.

The ack happens **only after** all DB writes in the batch commit successfully. If any DB write fails, the batch is not acked and the MQ redelivers it on the next poll. This is at-least-once delivery; the price is occasional duplicates after collector or DB failures.

### API Gateway

Stateless HTTP server using `gorilla/mux`. Three categories of endpoints:
- **Telemetry queries** (`GET /api/v1/gpus`, `GET /api/v1/gpus/{id}/telemetry`) with optional `metric_name`, `start_time`, `end_time`, `limit`, `offset` filters. The handler builds a single SQL query with `($N::TYPE IS NULL OR column = $N)` patterns so optional filters don't fan out into separate query branches.
- **Health** (`/health`, `/healthz`) — pings the DB; returns 503 if unreachable. Both routes alias the same handler so K8s liveness probes work either way.
- **Documentation** (`/swagger/*`) — serves the Swagger UI generated from `swag init` annotations on every handler. Spec is auto-built by `make openapi`.

### Persistence Layer

Two tables managed by version-tracked SQL migrations under `internal/store/migrations/`:

```
gpus(uuid PRIMARY KEY, hostname, last_seen)
telemetry(id BIGSERIAL, gpu_uuid → gpus, metric_name, metric_value, collected_at)
```

`telemetry` has a composite index on `(gpu_uuid, collected_at DESC)` so the time-range query plan is an index range scan. Migrations run on service startup wrapped in a Postgres advisory lock (`pg_advisory_lock(4242)`) so the api-gateway and collector starting in parallel can't race each other on schema creation.

### Scaling Model

| Component | Scales? | How |
|---|---|---|
| Streamer | Yes — horizontally | Stateless; each instance independently reads the CSV and publishes. `make scale SERVICE=streamer N=3` |
| Collector | Yes — horizontally | Stateless; joining the consumer group triggers automatic partition rebalance. `make scale SERVICE=collector N=2` |
| API Gateway | Yes — horizontally | Stateless; put any number behind a load balancer |
| PostgreSQL | Vertically; horizontally with read replicas | Standard pg scaling |
| MQ Server | **Single node only** | See trade-off below |

The PDF caps the assignment at 10 instances *for streamer/collector* — explicitly: *"For the exercise we will not scale the nodes beyond 10 instances for the streamer/collector."* The MQ is not subject to that cap; the design choice to keep it single-node is mine.

### MQ Single-Point-of-Failure — Trade-offs

A horizontally scaled MQ would require:
- A consensus protocol (Raft or Paxos) to replicate the partition log across N nodes
- Leader election per partition
- Replica reconciliation after network partitions
- A client-side discovery mechanism so streamers/collectors find the current leader

This is roughly 4-6 weeks of additional work to do correctly, and largely re-implements what Kafka already provides. For this assignment's scope I made the explicit choice to ship a single-node MQ with the following mitigations:

- **WAL-based crash recovery.** With `WAL_PATH` set, the broker replays unacknowledged messages on restart. No data loss if the process crashes — only downtime equal to restart time (typically <5s in K8s with a readiness probe).
- **Backpressure prevents cascading failure.** A misbehaving consumer can't take down the broker because partition queues are bounded — streamers get 429s and back off.
- **Stateless services around it.** Streamers and collectors recover automatically when the broker comes back up; they treat MQ unreachability as a transient error.

In production the recommended path is either:
1. Replace the custom broker with Kafka/Pulsar/Redpanda (managed availability, partition replication, established tooling), or
2. Run the broker as a 3-node Raft cluster (significant engineering investment).

### Observability

- **Structured JSON logs** via `log/slog` on every component — fields like `component`, `instance`, `gpu_uuid`, `partition`, `offset` make logs grep-friendly and ready to feed into ELK or Loki.
- **`/metrics` endpoint** on the MQ exposes per-partition queue depth, total published, total consumed, and per-consumer-group committed offsets — enough to plot lag in Grafana.
- **`/health` and `/healthz`** on the API for K8s liveness/readiness probes.

### Why these choices match the data

The reference CSV (`dcgm_metrics_*.csv`) contains 2,470 rows of DCGM exporter output across ~40 H100 GPUs on a single host (`mtv5-dgx1-hgpu-031`). Each row is one metric sample (`DCGM_FI_DEV_GPU_UTIL`, `DCGM_FI_DEV_FB_USED`, `DCGM_FI_DEV_GPU_TEMP`, etc.) for one GPU at one timestamp. Real DCGM exporters emit ~30-50 metrics per GPU per scrape, so a 1000-GPU cluster scraping every 15s produces 2-3M datapoints/minute. The design above (partitioning by UUID, COPY-protocol inserts, batched flushes) is sized to that target — not just the small bundled sample.

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
