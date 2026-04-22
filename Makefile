REGISTRY     ?= ghcr.io/example
TAG          ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
HELM_RELEASE ?= gpu-telemetry
HELM_NS      ?= telemetry
SWAG_VER     := v1.16.3
SERVICES     := api-gateway mq-server streamer collector

.PHONY: all build build-all $(addprefix build-,$(SERVICES)) \
        test test-integration test-coverage test-coverage-full \
        openapi \
        docker-build $(addprefix docker-build-,$(SERVICES)) \
        docker-push  $(addprefix docker-push-,$(SERVICES)) \
        up up-quick run-local down logs scale \
        db-up db-down db-reset \
        helm-dep helm-lint helm-template helm-install helm-upgrade helm-uninstall \
        check-deps clean help

all: build

# ── Setup ──────────────────────────────────────────────────────────────────────

check-deps:
	@command -v go     >/dev/null 2>&1 || { echo "go not found"; exit 1; }
	@command -v docker >/dev/null 2>&1 || { echo "docker not found"; exit 1; }
	@command -v helm   >/dev/null 2>&1 || { echo "helm not found"; exit 1; }

# ── Build ──────────────────────────────────────────────────────────────────────

build:
	go build ./...

build-all: $(addprefix build-,$(SERVICES))

$(addprefix build-,$(SERVICES)): build-%:
	@mkdir -p bin
	go build -trimpath -ldflags="-s -w -X main.version=$(TAG)" -o bin/$* ./cmd/$*

# ── Test ───────────────────────────────────────────────────────────────────────

test:
	go test ./... -short -race -count=1

test-integration:
	go test ./internal/store/... -timeout=120s -v

test-coverage:
	go test ./... -short -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

test-coverage-full:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

# ── OpenAPI ────────────────────────────────────────────────────────────────────

openapi:
	go run github.com/swaggo/swag/cmd/swag@$(SWAG_VER) init \
	  --generalInfo cmd/api-gateway/main.go \
	  --dir          . \
	  --output       docs \
	  --outputTypes  yaml,json \
	  --parseInternal

# ── Docker ─────────────────────────────────────────────────────────────────────

docker-build: $(addprefix docker-build-,$(SERVICES))

$(addprefix docker-build-,$(SERVICES)): docker-build-%:
	docker build \
	  --file  build/Dockerfile.$* \
	  --tag   $(REGISTRY)/$*:$(TAG) \
	  --tag   $(REGISTRY)/$*:latest \
	  --label "org.opencontainers.image.revision=$(TAG)" \
	  .

docker-push: $(addprefix docker-push-,$(SERVICES))

$(addprefix docker-push-,$(SERVICES)): docker-push-%:
	docker push $(REGISTRY)/$*:$(TAG)
	docker push $(REGISTRY)/$*:latest

# ── Local stack ────────────────────────────────────────────────────────────────

up:
	DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 docker compose build --parallel
	docker compose up -d

up-quick:
	docker compose up -d

run-local: build-all db-up
	@trap 'kill 0' INT; \
	MQ_PARTITIONS=8 MQ_MAX_QUEUE_SIZE=4096 MQ_LISTEN_ADDR=:9000 ./bin/mq-server & \
	sleep 2; \
	MQ_URL=http://localhost:9000 CSV_PATH=./data/dcgm_metrics.csv STREAM_INTERVAL_MS=100 STREAM_BATCH_SIZE=50 ./bin/streamer & \
	MQ_URL=http://localhost:9000 DATABASE_URL="postgres://telemetry:changeme@localhost:5433/telemetry?sslmode=disable" CONSUMER_GROUP=telemetry-collectors ./bin/collector & \
	DATABASE_URL="postgres://telemetry:changeme@localhost:5433/telemetry?sslmode=disable" PORT=8080 ./bin/api-gateway & \
	wait

down:
	docker compose down

logs:
	docker compose logs -f

scale:
	@[ -n "$(SERVICE)" ] && [ -n "$(N)" ] || { echo "Usage: make scale SERVICE=<name> N=<n>"; exit 1; }
	docker compose up -d --scale $(SERVICE)=$(N) --no-recreate

# ── Database ───────────────────────────────────────────────────────────────────

db-up:
	docker compose up -d postgres

db-down:
	docker compose stop postgres

db-reset:
	docker compose down -v
	docker compose up -d postgres

# ── Helm / Kubernetes ──────────────────────────────────────────────────────────

helm-dep:
	helm dependency update helm/gpu-telemetry-pipeline

helm-lint:
	helm lint helm/gpu-telemetry-pipeline

helm-template:
	helm template $(HELM_RELEASE) helm/gpu-telemetry-pipeline \
	  --namespace $(HELM_NS) \
	  --create-namespace

helm-install: helm-dep helm-lint
	helm upgrade --install $(HELM_RELEASE) helm/gpu-telemetry-pipeline \
	  --namespace   $(HELM_NS) \
	  --create-namespace \
	  --set global.imageRegistry=$(REGISTRY) \
	  --set image.tag=$(TAG) \
	  --wait

helm-upgrade:
	helm upgrade $(HELM_RELEASE) helm/gpu-telemetry-pipeline \
	  --namespace   $(HELM_NS) \
	  --reuse-values \
	  --set global.imageRegistry=$(REGISTRY) \
	  --set image.tag=$(TAG) \
	  --wait

helm-uninstall:
	helm uninstall $(HELM_RELEASE) --namespace $(HELM_NS)

# ── Misc ───────────────────────────────────────────────────────────────────────

clean:
	rm -rf bin/ coverage.out coverage.html docs/

help:
	@echo "Targets: build build-all test test-integration test-coverage test-coverage-full"
	@echo "         openapi docker-build docker-push up up-quick run-local down"
	@echo "         logs scale db-up db-down db-reset helm-dep helm-lint helm-template"
	@echo "         helm-install helm-upgrade helm-uninstall clean check-deps"
	@echo ""
	@echo "Variables (override on CLI):"
	@echo "  REGISTRY=$(REGISTRY)  TAG=$(TAG)  HELM_RELEASE=$(HELM_RELEASE)  HELM_NS=$(HELM_NS)"
