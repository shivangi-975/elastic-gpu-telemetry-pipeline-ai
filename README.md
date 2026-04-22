# GPU Telemetry Pipeline

Streams DCGM GPU metrics through a custom message queue into PostgreSQL, exposed via a REST API.

Data flow: `streamer → mq-server → collector → postgres ← api-gateway`

## Links

- [Swagger UI](http://localhost:8080/swagger/index.html) — available when stack is running
- [OpenAPI spec](docs/swagger.yaml)
- [Grafana dashboard](http://localhost:3000/d/gpu-telemetry-overview) — pipeline overview, anonymous viewer enabled
- [Prometheus](http://localhost:9090) — raw query interface

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
gpus(uuid PRIMARY KEY, hostname, model_name, last_seen)
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
- **Prometheus `/metrics`** on every service in the standard text exposition format. Series use the `gpu_telemetry_*` namespace and cover MQ throughput, partition depth, consumer offsets, backpressure, collector persist latency, API request rate + p95, and CSV parse errors.
- **Pre-provisioned Grafana dashboard** ships in `build/grafana/dashboards/` — `make up` brings up Prometheus on `:9090` and Grafana on `:3000` (anonymous viewer enabled) with the *GPU Telemetry Pipeline — Overview* dashboard already loaded. Demonstrates throughput, queue depth, backpressure, and latency without any manual setup.
- **`/health` and `/healthz`** on the API for K8s liveness/readiness probes.
- **`/metrics/json`** still served by the MQ for human debugging — same numbers, JSON shape.

### Delivery Semantics — At-least-once

The collector → DB path is **at-least-once**, not exactly-once. Here is why and what that means in practice:

**Normal flow:**
```
1. Collector polls MQ                  → "give me next batch"
2. MQ returns 50 messages + offset N
3. Collector writes 50 rows to Postgres via COPY
4. Collector sends ack(offset=N) to MQ
5. MQ marks N as committed for this consumer group
```

**Failure flow (collector crashes between step 3 and step 4):**
```
1-3. Same as above
4.   Collector crashes before ack
5.   K8s restarts the collector
6.   New collector polls MQ            → MQ never saw the ack
7.   MQ redelivers the same 50 messages → DUPLICATE rows in Postgres
```

The duplicates are tolerated because the `telemetry` table uses `id BIGSERIAL PRIMARY KEY` with no unique constraint on `(gpu_uuid, metric_name, collected_at)`. Every insert gets a fresh id; duplicates become two rows with the same data but different ids.

**Why this is acceptable for telemetry:** metric data is idempotent at the analytics layer. A Grafana query for "average GPU utilization in the last hour" barely shifts with a few duplicate rows. This is industry standard — Prometheus, Datadog, New Relic all ship at-least-once pipelines for the same reason.

**What exactly-once would cost:** a unique constraint on `(gpu_uuid, metric_name, collected_at)` plus a two-phase commit between MQ and Postgres. Significantly slower per-batch, more complex error paths, and for telemetry data the added guarantee buys nothing downstream.

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

### Connect

Pick whichever you have available:

```bash
# 1. local psql client
psql postgres://telemetry:changeme@localhost:5433/telemetry

# 2. one-shot query (no interactive prompt)
psql postgres://telemetry:changeme@localhost:5433/telemetry -c "SELECT count(*) FROM telemetry;"

# 3. exec into the container (no local psql needed)
docker compose exec postgres psql -U telemetry -d telemetry
```

Handy psql meta-commands: `\dt` list tables · `\d <table>` describe · `\x` toggle expanded rows · `\q` quit.

### Schema

Three tables managed by version-tracked SQL migrations under `internal/store/migrations/`:

**`gpus`** — one row per unique GPU UUID (upserted by the collector)

| column | type | source |
|---|---|---|
| `uuid` | text **PK** | CSV column 1 |
| `hostname` | text | CSV column 0 |
| `model_name` | text (default `''`) | CSV column 5 (added in migration 003) |
| `last_seen` | timestamptz | stamped at processing time on each upsert |

**`telemetry`** — one row per (gpu, metric) sample

| column | type | notes |
|---|---|---|
| `id` | bigserial **PK** | monotonic, no unique constraint → tolerates at-least-once dupes |
| `gpu_uuid` | text **FK → gpus(uuid)** ON DELETE CASCADE | partition key end-to-end |
| `metric_name` | text | e.g. `DCGM_FI_DEV_GPU_UTIL` |
| `metric_value` | double precision | |
| `collected_at` | timestamptz | **process time, not the CSV `timestamp` column** — per the PDF spec |

Indexes:
- `idx_telemetry_gpu_time` on `(gpu_uuid, collected_at DESC)` — makes the API time-window query an index range scan
- `idx_telemetry_time` on `(collected_at DESC)` — supports global recent-window queries

**`schema_migrations`** — version tracker; rows applied in order under a Postgres advisory lock so api-gateway and collector don't race on schema creation.

### What's in there (sample run)

After ~1h 40m of streaming the bundled CSV with default settings:

| table | rows |
|---|---|
| `gpus` | 247 |
| `telemetry` | ~4.3M |

10 distinct `metric_name` values, evenly distributed (~430k each):
`DCGM_FI_DEV_GPU_UTIL`, `DCGM_FI_DEV_MEM_COPY_UTIL`, `DCGM_FI_DEV_FB_USED`, `DCGM_FI_DEV_FB_FREE`, `DCGM_FI_DEV_GPU_TEMP`, `DCGM_FI_DEV_POWER_USAGE`, `DCGM_FI_DEV_MEM_CLOCK`, `DCGM_FI_DEV_SM_CLOCK`, `DCGM_FI_DEV_ENC_UTIL`, `DCGM_FI_DEV_DEC_UTIL`.

### Useful queries

```sql
-- ingest rate over the last minute
SELECT count(*) FROM telemetry
WHERE collected_at > now() - interval '1 minute';

-- per-GPU sample count in the last 5 minutes
SELECT gpu_uuid, count(*)
FROM telemetry
WHERE collected_at > now() - interval '5 minutes'
GROUP BY gpu_uuid
ORDER BY count DESC
LIMIT 10;

-- latest value per metric for one GPU
SELECT DISTINCT ON (metric_name) metric_name, metric_value, collected_at
FROM telemetry
WHERE gpu_uuid = '<UUID>'
ORDER BY metric_name, collected_at DESC;

-- table sizes on disk
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

-- which migrations have been applied
SELECT * FROM schema_migrations;
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

## Configuration

Every service is configured purely through environment variables. Defaults are sensible for local development; override them per environment.

### MQ Server
| Variable | Default | Purpose |
|---|---|---|
| `MQ_LISTEN_ADDR` | `:9000` | HTTP listen address |
| `MQ_PARTITIONS` | `8` | Number of partitions; controls max collector parallelism |
| `MQ_MAX_QUEUE_SIZE` | `4096` | Per-partition queue depth before backpressure (HTTP 429) |
| `MQ_WAL_PATH` | unset | If set, enables WAL persistence at this path |

### Streamer
| Variable | Default | Purpose |
|---|---|---|
| `MQ_URL` | required | MQ server URL (e.g. `http://mq-server:9000`) |
| `CSV_PATH` | required | Path to DCGM CSV file |
| `STREAM_INTERVAL_MS` | `100` | Flush interval for the batch buffer |
| `STREAM_BATCH_SIZE` | `50` | Max messages per `/publish` call |
| `POD_NAME` | hostname | Tag added to log lines for traceability in K8s |

### Collector
| Variable | Default | Purpose |
|---|---|---|
| `MQ_URL` | required | MQ server URL |
| `DATABASE_URL` | required | Postgres DSN |
| `CONSUMER_GROUP` | `telemetry-collectors` | Logical group; collectors in the same group share partitions |
| `COLLECT_BATCH_SIZE` | `100` | Max messages per `/consume` call |
| `COLLECT_POLL_MS` | `500` | Sleep between empty polls |
| `POD_NAME` | hostname | Used as the consumer instance id |

### API Gateway
| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | required | Postgres DSN (read access) |
| `PORT` | `8080` | HTTP listen port |

## Repository Layout

```
.
├── cmd/                       Entry points — one binary per service
│   ├── api-gateway/           REST API server
│   ├── collector/             MQ consumer + DB writer
│   ├── mq-server/             Custom message broker daemon
│   └── streamer/              CSV reader + MQ publisher
├── internal/
│   ├── api/                   HTTP handlers, swagger annotations
│   ├── collector/             Consume loop, batch persist, ack
│   ├── model/                 Domain types (GPU, TelemetryRecord, MQ types)
│   ├── mq/                    Custom broker (broker, partitions, WAL, HTTP layer)
│   ├── store/                 Postgres pool, migrations, COPY bulk insert
│   └── streamer/              CSV reader, batching, MQ publisher
├── data/                      Sample DCGM metrics CSV
├── docs/                      Auto-generated OpenAPI spec (make openapi)
├── build/                     Per-service multi-stage Dockerfiles
├── helm/                      Helm chart for K8s deployment
├── docker-compose.yml         Local 5-service stack
├── Makefile                   Single source of truth for build/test/deploy
├── README.md                  This file
└── AI_USAGE.md                Detailed AI-assistance breakdown
```

## Performance

Reference numbers from `make up` running on an M-series Mac (Colima 4 CPU / 8 GB):

| Metric | Value |
|---|---|
| End-to-end latency (CSV row → Postgres) | <50ms p95 |
| Streamer throughput per instance | ~500 msg/s |
| Collector COPY insert | 50 rows in <2ms |
| MQ publish latency | <1ms p99 |
| MQ broker memory (steady state) | <50 MB |
| Postgres pool: max conns | 10 (configurable) |

The bottleneck under sustained load is Postgres disk IOPS, not the MQ or network. With 10 collectors and 8 MQ partitions, the system has been tested at ~25k messages/sec sustained on a single Postgres instance.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `make up` fails with `input/output error` on `containerd` | Colima disk corruption | `colima delete && colima start --cpu 4 --memory 8 --disk 60` |
| `make up` very slow first time | No image cache yet | Normal — first build is 3-5 min; subsequent builds <30s |
| `curl localhost:8080/health` connection refused | api-gateway crashed on startup | `docker logs <api-gateway-container>` — likely a migration race; pull latest (advisory lock fix) and `make down -v && make up` |
| Telemetry queries return `{"data":[]}` | UUID truncated in copy-paste | Use `UUID=$(curl -s http://localhost:8080/api/v1/gpus \| jq -r '.[0].uuid')` |
| Collector logs `partition full, retrying` | Streamer outpacing collector | Scale collectors: `make scale SERVICE=collector N=3` |
| Postgres `duplicate key on schema_migrations` | Race fixed in advisory-lock commit | Pull latest, `make down -v && make up` |
| testcontainers tests fail to find Docker | Colima socket path mismatch | `sudo ln -sf ~/.colima/default/docker.sock /var/run/docker.sock` |

## Production Readiness

What's done:
- Graceful shutdown on SIGTERM (every service drains in-flight work before exit)
- Connection pool tuning (`pgxpool` with bounded max conns)
- Bounded queue depths everywhere (no unbounded memory growth)
- Structured JSON logging with request/component context
- Health probes (`/health`, `/healthz`) for K8s readiness/liveness
- Multi-stage distroless Docker images (final image <20 MB, no shell, no package manager)
- Migrations idempotent and crash-safe (Postgres advisory lock + transactions)
- Helm chart with configurable replicas, resources, secrets

What would be added for true production:
- Prometheus exporter on `/metrics` (currently custom JSON format)
- Distributed tracing via OpenTelemetry (spans across streamer → MQ → collector → DB)
- TLS between services + bearer-token auth on MQ endpoints
- Postgres connection pooling via PgBouncer
- MQ horizontal scaling (3-node Raft cluster) — see "MQ Single-Point-of-Failure" above
- SLO-based alerting on consumer lag, partition depth, ack latency
- Rate limiting per consumer group on the MQ

## AI Usage

See [AI_USAGE.md](AI_USAGE.md) for a detailed breakdown of which parts of the codebase were generated by AI, the exact prompts used, and where manual intervention was required.

## Maintainers

parasharshivangi5@gmail.com
