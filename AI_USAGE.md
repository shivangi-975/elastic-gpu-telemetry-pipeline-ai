# AI Usage

This document describes how AI assistance (Claude) was used throughout the development of the GPU Telemetry Pipeline, including the exact prompts used, what was generated, and where manual intervention was required.

---

## 1. Project and Repository Bootstrapping

**Prompt used:**
> "I need to build a GPU telemetry pipeline in Go. It should have four components: a streamer that reads GPU metrics from a CSV file and publishes to a custom message queue, a collector that consumes from the queue and persists to PostgreSQL, a custom in-memory message queue server (no Kafka, no RabbitMQ, built from scratch), and a REST API gateway. All components should run as Docker containers and be deployable to Kubernetes via Helm. Generate the initial repo layout, go.mod, Makefile, and docker-compose.yml."

**What AI generated:**
- Full Go module layout (`cmd/`, `internal/`, `data/`, `helm/`, `docs/`)
- `go.mod` with initial dependencies: `gorilla/mux`, `pgx/v5`, `swaggo/swag`, `slog`, `testcontainers-go`
- `docker-compose.yml` with all 5 services (postgres, mq-server, streamer, collector, api-gateway)
- Skeleton `Makefile` with `build`, `up`, `down`, `test`, `openapi` targets
- `.gitignore` covering Go binaries, IDE files, coverage output

**Manual intervention required:**
- The initial `docker-compose.yml` used `build: .` for all services, pointing to one Dockerfile. Each service needed its own `build.context` and `dockerfile` path — fixed manually.
- `go.mod` module path was set to `github.com/example/gpu-telemetry-pipeline` as a placeholder; left as-is since it works for a private repo.
- Makefile `up` target originally ran `docker compose build` sequentially. Changed to `DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 docker compose build --parallel` to cut build time from ~8 minutes to ~2 minutes.

---

## 2. Custom Message Queue — Design and Implementation

This was the most complex component. The PDF required building a message queue from scratch — not using Kafka, RabbitMQ, ZeroMQ, or any existing broker.

**Prompt used:**
> "Design and implement a custom in-memory message queue in Go as an HTTP service. Requirements: partitioned storage (partition by message key using consistent hashing), consumer groups with automatic partition rebalancing when new consumers join or leave, backpressure (return HTTP 429 when a partition is full), optional WAL for durability, compaction to reclaim memory after all consumer groups have acknowledged a message. The queue will serve up to 10 streamer instances and 10 collector instances. Expose these HTTP endpoints: POST /publish, GET /consume, POST /ack, GET /metrics."

**What AI generated:**
- `internal/mq/types.go` — partition, consumerGroup, PartitionRouter structs
- `internal/mq/broker.go` — full Broker implementation: 320 lines covering Publish, Consume, Ack, Compact, WAL replay, rebalance logic
- `internal/mq/router.go` — key-to-partition routing via FNV-1a hash
- `internal/mq/server.go` — HTTP handlers wiring broker methods to HTTP endpoints
- `internal/mq/mq.go` — MQClient for streamer/collector to call the MQ server

**Prompt for WAL specifically:**
> "Add optional WAL persistence to the broker. On startup replay the WAL file to restore messages that were published but not yet consumed. WAL writes should be append-only, one JSON line per message. WAL replay should be idempotent."

**What AI generated:** `replayWAL()` and `writeWAL()` methods in broker.go.

**Manual intervention required:**
- The initial rebalance algorithm distributed partitions unevenly when `numPartitions % numConsumers != 0`. Rewrote the range-based distribution to use `ceiling division` so every consumer gets at least one partition.
- The `/metrics` endpoint was not in the first generated version; added manually by following the same handler pattern.
- Compaction initially held the partition lock for the full scan. Refactored to copy the slice under lock, release, then swap — avoiding starvation during high-throughput publish.

---

## 3. Telemetry Streamer

**Prompt used:**
> "Implement a Go streamer that reads GPU telemetry from a CSV file in an infinite loop (wrapping on EOF) and publishes batches to the custom MQ over HTTP. The CSV has these columns: 0=timestamp, 1=metric_name, 2=gpu_id, 3=device, 4=uuid, 5=modelName, 6=Hostname, 7=container, 8=pod, 9=namespace, 10=value. CollectedAt should be time.Now() at processing time, not the CSV timestamp. Support configurable batch size and publish interval via environment variables. Skip and log malformed rows without crashing."

**What AI generated:**
- `internal/streamer/streamer.go` — Config struct, New(), Run(), parseRow(), flush(), finalFlush()
- `internal/streamer/client.go` — MQClient.PublishBatch() HTTP client
- `cmd/streamer/main.go` — env var parsing, signal handling, graceful shutdown

**Manual intervention required:**
- Initial version read the whole CSV into memory before streaming. Changed to row-by-row streaming with `csv.NewReader` to handle large files without memory spikes.
- `newTickFn` abstraction (injectable ticker) was added manually to make the streamer unit-testable without real timers.

---

## 4. Telemetry Collector

**Prompt used:**
> "Implement a Go collector that polls the MQ HTTP API for telemetry batches, upserts GPUs into a gpus table, bulk-inserts telemetry into a telemetry table using PostgreSQL COPY protocol via pgx, and acknowledges offsets only after all DB writes in the batch succeed (at-least-once delivery). Support configurable poll interval, batch size, consumer group ID, and consumer instance ID via environment variables. Log a structured line per bulk insert showing row count."

**What AI generated:**
- `internal/collector/collector.go` — Collector struct, Run(), persist(), sleep()
- `internal/collector/parser.go` — model conversion from MQ message to DB record
- `internal/collector/client.go` — HTTP client for MQ consume and ack
- `internal/store/postgres.go` — Store with New(), migrate(), UpsertGPU(), BulkInsertTelemetry() using `pgx.CopyFrom`, ListGPUs(), QueryTelemetry()
- SQL migrations in `internal/store/migrations/`

**Prompt for COPY protocol specifically:**
> "Use pgx CopyFrom for bulk inserts into the telemetry table. The rows slice should be built from []TelemetryRecord. Return an error if the inserted row count does not match the input count."

**Manual intervention required:**
- First generated migration used `SERIAL` for the `id` column. Changed to `BIGSERIAL` to handle high-volume telemetry without overflow.
- `QueryTelemetry` initially did not support `metric_name` filter. Added the `$2::TEXT IS NULL OR metric_name = $2` nullable parameter pattern manually to allow optional filtering without separate query branches.
- `applyMigration` did not wrap each migration in a transaction. Added `BeginTx` / `Commit` / `Rollback` for atomicity.

---

## 5. API Gateway

**Prompt used:**
> "Implement a Go REST API gateway using gorilla/mux. Endpoints: GET /health, GET /healthz (both return DB ping status), GET /api/v1/gpus (list all GPUs), GET /api/v1/gpus/{uuid}/telemetry with query params: metric_name (string), start_time (RFC3339), end_time (RFC3339), limit (int, default 1000), offset (int). Add Swagger annotations on every handler so swag init generates a complete OpenAPI spec. Wire the Swagger UI at /swagger/*any using http-swagger."

**What AI generated:**
- `internal/api/api.go` — all handlers with full swag annotations
- `cmd/api-gateway/main.go` — server setup, graceful shutdown
- `docs/` directory scaffolded (populated by `make openapi`)

**Manual intervention required:**
- Generated Swagger `@BasePath` was `/` — changed to `/` with `@host localhost:8080` so the Swagger UI Try-it-out works locally.
- The `/swagger/*any` route conflicted with gorilla/mux path parameter syntax; fixed to `r.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)`.
- `/healthz` was missing from the first pass (PDF requires both `/health` and `/healthz`). Added manually.

---

## 6. Unit Tests

**Prompt used:**
> "Write unit tests for the streamer package. Use a fake publisher (interface double) and an injectable ticker channel instead of real timers. Test: normal batch flush on size threshold, flush on tick, graceful shutdown flush on ctx cancel, malformed CSV row skipping, CSV wrap-around on EOF."

**What AI generated:** `internal/streamer/streamer_test.go` — 6 test cases with a `fakePublisher` stub and synthetic ticker injection.

**Prompt used:**
> "Write unit tests for the API handlers using httptest. Use a mockStore that satisfies the StoreReader interface. Test: GET /api/v1/gpus returns JSON list, GET /api/v1/gpus/{uuid}/telemetry filters by metric_name and time range, /health returns 200 when ping succeeds and 503 when ping fails, unknown route returns 404."

**What AI generated:** `internal/api/api_test.go` — 8 test cases with `mockStore` implementing all interface methods.

**Prompt used:**
> "Write unit tests for the MQ broker covering: publish routes to correct partition by key, consume returns correct messages for a consumer group, rebalance distributes partitions evenly across consumers, backpressure returns error when partition is full, compaction reclaims messages after all groups ack."

**What AI generated:** `internal/mq/mq_test.go` and `internal/mq/broker_extra_test.go`.

**Manual intervention required:**
- Mock signatures drifted from the real interface every time a function signature changed (e.g., when `metricName string` was added to `QueryTelemetry`). Updated manually each time — this was the most frequent source of manual fixes.
- Test for CSV wrap-around required controlling `io.Seeker` on a real temp file; the initial version used an in-memory buffer that didn't support seeking. Replaced with `os.CreateTemp`.

---

## 7. Integration Tests

**Prompt used:**
> "Write a store integration test using testcontainers-go that spins up a real PostgreSQL container, runs migrations, calls UpsertGPU and BulkInsertTelemetry, then queries and asserts the results."

**What AI generated:** `internal/store/store_test.go`.

**Manual intervention required:**
- Colima (macOS Docker replacement) uses a different socket path (`~/.colima/default/docker.sock`) than the default `/var/run/docker.sock`. Testcontainers could not find Docker. Fixed with:
  ```
  sudo ln -sf ~/.colima/default/docker.sock /var/run/docker.sock
  export DOCKER_HOST=unix://${HOME}/.colima/default/docker.sock
  ```
- `testcontainers-go` version required an explicit `ryuk` reaper container setting; added `TESTCONTAINERS_RYUK_DISABLED=true` to avoid flakiness in CI.

---

## 8. Dockerfiles and Build Environment

**Prompt used:**
> "Write multi-stage Dockerfiles for four Go services: streamer, collector, mq-server, api-gateway. Build stage: golang:1.22-alpine, compile with CGO_DISABLED=1 GOOS=linux. Runtime stage: gcr.io/distroless/static. Copy only the compiled binary. Each service has its entrypoint at cmd/<service>/main.go."

**What AI generated:** Dockerfiles for all 4 services.

**Prompt used:**
> "Write a .dockerignore that excludes binaries, test output, IDE files, and scripts from the Docker build context to speed up builds."

**What AI generated:** `.dockerignore`.

**Manual intervention required:**
- Initial `.dockerignore` excluded `docs/`. The api-gateway `cmd/api-gateway/main.go` imports the `docs` package (generated by swag) as a compile-time side-effect — excluding `docs/` broke the Docker build. Removed `docs/` from `.dockerignore`.
- Build times were still slow because `docker compose build` runs sequentially by default. Added `DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1` and `--parallel` flag to the Makefile `up` target; this reduced cold build time from ~8 minutes to ~2 minutes.
- Colima disk corruption occurred twice during image builds (kernel I/O errors on the BuildKit overlay filesystem). Each time required: `colima stop && colima delete && colima start --cpu 4 --memory 8 --disk 60`. Not a code issue — a macOS/Colima environment issue.

---

## 9. Helm Charts

**Prompt used:**
> "Generate Helm charts for the four services and PostgreSQL. Each service should have its own Deployment and Service. PostgreSQL should use a StatefulSet with a PersistentVolumeClaim. Values should expose: image registry, image tag, replica counts for streamer and collector, PostgreSQL credentials, resource limits. The chart should work with pre-built images from ghcr.io."

**What AI generated:** Full `helm/gpu-telemetry-pipeline/` chart with `values.yaml`, `Chart.yaml`, templates for all services.

**Manual intervention required:**
- Generated image repository names in `values.yaml` were `gpu-telemetry-pipeline-api-gateway`, `gpu-telemetry-pipeline-mq-server`, etc. The actual images pushed to ghcr.io were named `api-gateway`, `mq-server`, `streamer`, `collector`. Fixed all four repository names.
- `helm upgrade --install` with `--wait` timed out at 5 minutes because the MQ server pod was slow to become ready (it waits for the WAL file path to be writable). Removed `--wait` and monitored with `kubectl get pods -n telemetry -w`.
- `values.yaml` had YAML corruption (a DSN fragment appended four times at the end of the file). Fixed manually.

---

## 10. README and Documentation

**Prompt used:**
> "Rewrite the README to be minimal and direct — no emojis, no marketing language. Include: one-paragraph architecture summary, mermaid flowchart, prerequisites, setup (make up), container table, database connection details, sample curl commands for every API endpoint, stop/test/scale commands, Docker image locations, Kubernetes (minikube) deploy steps."

**What AI generated:** The current `README.md`.

**Manual intervention required:**
- First README version included hardcoded timestamp values in the curl examples. For anyone running `make up` at a different time, those queries would return empty results. Replaced with `<RFC3339>` placeholders and added a note explaining that `collected_at` is stamped at `time.Now()` when the streamer runs.
- Mermaid diagram did not render on GitHub because the code block was inside a nested list. Moved to top-level — now renders correctly on the repo landing page.

---

## Summary: AI vs Manual Split

| Area | AI contribution | Manual fixes |
|---|---|---|
| Repo structure, go.mod, Makefile | Full generation | Parallel builds, .dockerignore fix |
| Custom MQ (broker, WAL, compaction) | Full generation | Rebalance edge case, lock scope fix |
| Streamer | Full generation | Row-by-row streaming, ticker injection |
| Collector | Full generation | BIGSERIAL, nullable metric_name filter, transaction wrapping |
| API Gateway | Full generation | /healthz route, swagger path conflict |
| Unit tests | Full generation | Mock drift when signatures changed, temp file for CSV seek test |
| Integration tests | Full generation | Colima socket path, ryuk reaper config |
| Dockerfiles | Full generation | docs/ exclusion bug, parallel build flag |
| Helm charts | Full generation | Repository name mismatch, YAML corruption |
| README | Full generation | Hardcoded timestamps, mermaid nesting |

AI handled approximately 85% of the code and configuration. The remaining 15% was manual fixes — almost always at integration boundaries: Docker socket paths, Helm image naming, interface drift between packages, and platform-specific Colima behaviour on macOS.
