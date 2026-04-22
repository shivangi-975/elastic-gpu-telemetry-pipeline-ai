# AI Usage

This document is an honest account of where I used AI assistance (Claude) during this assessment, what I asked it for, and what I did myself. The intent is transparency: AI was a *tool* I used to move faster on well-understood tasks and to pressure-test my design choices, not an autopilot.

The core architectural decisions, the message-queue design, the delivery-semantics model, the SQL schema, and the trade-off analysis in the README are mine. AI helped most with **scaffolding, test generation, and acting as a sparring partner** when I wanted a second opinion on a design.

---

## Where AI helped

### 1. Design sparring — message queue trade-offs

Before writing any MQ code I used AI as a whiteboard partner. I had already decided on a partitioned in-memory broker with consumer groups; I wanted a critique.

**What I asked (paraphrased):**
> "I'm going to build a single-process partitioned in-memory MQ in Go with consumer groups, FNV-1a key hashing, optional WAL, and HTTP transport. The pipeline must scale to 10 streamer + 10 collector instances. Walk me through where this design breaks under load and what I'd need to change to make it survive a process restart."

**What it gave me:**
- Pushed me on the rebalance edge case when `numPartitions % numConsumers != 0` (I had assumed even division).
- Suggested I document the SPOF explicitly rather than try to hide it — which led to the *Single-Node MQ Trade-Off* section in the README.
- Reminded me that compaction under a held partition lock would starve publishers — I refactored to copy-under-lock then swap.

**What I did myself:** the actual partitioning scheme, the offset-per-consumer-group model, the backpressure contract (HTTP 429), and the WAL replay ordering. Those are decisions I had to defend in code, so I made them.

---

### 2. Drilling into concepts I wanted to be sure about

A few topics I deliberately drilled into with AI before writing the README sections, because I wanted my explanations to be accurate, not hand-wavy:

- **At-least-once delivery semantics** — I asked AI to challenge my schema for duplicate tolerance. Result: I kept the `(gpu_uuid, metric_name, collected_at)` natural key understanding but did *not* add a unique constraint, since the assessment treats duplicates as acceptable. The README now spells out exactly when a duplicate can occur (collector crash after DB commit, before ack) and why it's tolerable.
- **Postgres advisory locks for migrations** — when api-gateway and collector raced on schema creation and I hit `duplicate key value violates unique constraint pg_type_typname_nsp_index`, I asked AI to confirm `pg_advisory_lock` was the right tool versus a `CREATE TABLE IF NOT EXISTS schema_migrations` + `SELECT FOR UPDATE` pattern. Advisory lock won on simplicity.
- **Scaling boundary** — the PDF says "do not scale beyond 10 instances." I wanted to be sure I understood whether that cap applies to the MQ too. AI helped me articulate that the cap is a *consumer-side* limit (partition count = parallelism ceiling) and that scaling the MQ itself is a different axis — that distinction is now in the README.

---

### 3. Test generation

This is where I leaned on AI most heavily, and deliberately. Once the production code and interfaces were settled, generating exhaustive table-driven tests is mechanical work.

**What I asked AI to write:**
- `internal/streamer/streamer_test.go` — fake publisher, injected ticker, batch-on-size, batch-on-tick, graceful shutdown flush, malformed-row skipping, CSV wrap-on-EOF.
- `internal/api/api_test.go` — `mockStore` against the `StoreReader` interface; happy path + 400/404/500/503 paths for every handler.
- `internal/mq/mq_test.go` and `broker_extra_test.go` — partition routing by key, consumer-group offsets, rebalance distribution, backpressure on full partition, compaction after all groups ack.
- `internal/store/store_test.go` — testcontainers-go integration test against a real Postgres, exercising migrations, upsert idempotency, and `QueryTelemetry` filters.

**What I did myself:**
- Designed the `StoreReader` interface so the API was mockable in the first place.
- Added the `newTickFn` injection point in the streamer specifically so timer-based code stayed testable.
- Fixed every round of mock drift when I changed an interface (e.g. when I added the optional `metricName` filter to `QueryTelemetry`, the mock and three call sites needed updating — AI didn't catch those, I did).
- Wired up Colima's Docker socket so testcontainers could find a daemon on macOS.

The judgment about *what to test* and *which interfaces to expose for testing* was mine. AI just typed faster than I do.

---

### 4. Boilerplate I didn't want to hand-write

- **Swagger/OpenAPI annotations** — the `// @Summary`, `// @Param`, `// @Success` blocks above each handler. I knew exactly what I wanted in the spec; AI converted my route table into the annotation syntax.
- **Multi-stage Dockerfiles** — distroless base, `CGO_DISABLED=1`, multi-arch `GOOS=linux`. Same shape four times.
- **Helm chart skeleton** — Deployment + Service templates per component, `values.yaml` keys for image/tag/replicas/resources, Postgres StatefulSet with PVC.
- **README structure** — first-pass section headings and the mermaid diagram syntax. Every word of the *Design*, *Scaling Model*, *Delivery Semantics*, and *Trade-Offs* sections I wrote myself, because those are the parts I'd be asked to defend in an interview.

---

### 5. Debugging help

When something broke, I'd often paste the error into AI alongside the relevant file and ask "what's the most likely cause." Examples:

- The `cmd/` directory wasn't being committed — root cause was `.gitignore` patterns missing leading slashes (`/api-gateway` vs `api-gateway`), so the binary-ignore rules were also matching `cmd/api-gateway/`. AI spotted it once I gave it both the gitignore and the `git status` output.
- OpenAPI spec was rendering `github_com_example_gpu-telemetry-pipeline_internal_model.GPU` instead of `model.GPU` — AI pointed me at swag's `--parseInternal` vs `--parseDependency` flags.
- A Helm `helm upgrade --install --wait` was timing out at 5 minutes; AI suggested dropping `--wait` and using `kubectl get pods -w` so I could see *which* pod was stuck (it was the MQ server waiting on a writable WAL path).

These are the cases where AI was a faster `man` page or Stack Overflow.

---

## Where I deliberately did *not* use AI

- **The MQ broker's core data structures** (`partition`, `consumerGroup`, `PartitionRouter`) and the publish/consume/ack/compact state machine. I wanted to be able to walk through this on a whiteboard.
- **The SQL schema and migration ordering.** I wrote these by hand because I needed to reason about index coverage, FK direction, and the `BIGSERIAL` choice for telemetry id (you'd overflow a SERIAL in a few weeks at modest throughput).
- **The trade-off sections in the README** — Single-Node MQ SPOF, Scaling Model, Delivery Semantics, Production Readiness. These are the parts an interviewer is most likely to drill into, so they're written in my own words.
- **Commit hygiene** — splitting work into 21 logically-grouped commits across the 3-day timeline was a manual exercise so the history reads as a real build progression rather than a single dump.

---

## Honest summary

| Area | Who drove it |
|---|---|
| MQ design (partitioning, consumer groups, backpressure, WAL semantics) | Me, with AI as critic |
| MQ implementation | Mostly me; AI helped with HTTP handler boilerplate |
| Streamer / collector business logic | Me |
| Postgres schema, migrations, COPY-protocol path | Me |
| API handlers + middleware | Me |
| **Unit & integration tests** | **AI generated, I designed the seams and reviewed** |
| Swagger annotations, Dockerfiles, Helm chart skeleton, docker-compose | AI generated, I tuned |
| README — architecture diagram & section structure | AI scaffolded |
| README — design / trade-offs / scaling / delivery semantics | Me |
| Debugging (gitignore, swag flags, Colima, helm timeouts) | Me, with AI as a faster reference |

If you want to verify the split, the git history is intentionally fine-grained: each commit corresponds to one focused chunk of work, and the commit messages describe what was added or fixed in that step.
