// Package collector implements the Telemetry Collector service. It pulls
// batches of GPU telemetry from the MQ HTTP API, persists them to PostgreSQL,
// and acknowledges offsets only after every DB write in the batch succeeds,
// providing an at-least-once delivery guarantee.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/metrics"
	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// StoreInterface is the persistence contract the Collector depends on.
// Using an interface decouples the collector from *store.Store, making it
// straightforward to substitute a mock in unit tests.
type StoreInterface interface {
	UpsertGPU(ctx context.Context, gpu model.GPU) error
	BulkInsertTelemetry(ctx context.Context, records []model.TelemetryRecord) error
	Close()
}

// Config holds all runtime parameters for the Collector.
type Config struct {
	// MQURL is the base URL of the MQ HTTP API (required).
	// Example: http://mq-server:9000
	MQURL string

	// DatabaseURL is the PostgreSQL DSN (required); used by cmd/collector
	// to construct the real store before calling New.
	DatabaseURL string

	// ConsumerGroup identifies the logical consumer group. Defaults to
	// "telemetry-collectors".
	ConsumerGroup string

	// BatchSize is the maximum number of messages to fetch per Consume call.
	// Defaults to 100.
	BatchSize int

	// PollInterval is how long the collector waits before re-polling when the
	// queue is empty or a transient error occurs. Defaults to 100 ms.
	PollInterval time.Duration

	// InstanceID is a unique identifier for this collector process
	// (typically POD_NAME or os.Hostname). Used as the MQ consumer ID.
	InstanceID string
}

// Collector consumes telemetry batches from the MQ HTTP API, persists them to
// PostgreSQL, and acknowledges offsets only after successful DB writes.
type Collector struct {
	cfg    Config
	store  StoreInterface
	client *Client
	log    *slog.Logger
}

// New validates cfg, applies defaults, wires an MQ HTTP client, and returns a
// Collector ready to be started with Run.
func New(cfg Config, store StoreInterface) (*Collector, error) {
	if cfg.MQURL == "" {
		return nil, fmt.Errorf("collector: MQURL is required")
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "telemetry-collectors"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = "unknown"
	}

	return &Collector{
		cfg:    cfg,
		store:  store,
		client: NewClient(cfg.MQURL),
		log:    slog.With("component", "collector", "instance", cfg.InstanceID),
	}, nil
}

// Run starts the consume → persist → ack loop. It blocks until ctx is
// cancelled, at which point it closes the store and returns nil.
//
// Delivery guarantee: Ack is called only after every DB write in a batch
// succeeds. On any failure the batch is not acknowledged, so the MQ will
// redeliver it. Duplicate records on retry are acceptable.
func (c *Collector) Run(ctx context.Context) error {
	c.log.Info("collector started",
		"mq_url", c.cfg.MQURL,
		"consumer_group", c.cfg.ConsumerGroup,
		"batch_size", c.cfg.BatchSize,
		"poll_interval", c.cfg.PollInterval,
	)
	defer func() {
		// Deregister from the MQ so our partitions are immediately rebalanced
		// to surviving collectors rather than staying frozen until the next join.
		leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.client.Leave(leaveCtx, c.cfg.ConsumerGroup, c.cfg.InstanceID); err != nil {
			c.log.Warn("leave failed during shutdown", "error", err)
		}
		leaveCancel()
		c.store.Close()
		c.log.Info("collector stopped - graceful shutdown")
	}()

	for {
		// Honour cancellation at the top of every iteration.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		resp, err := c.client.Consume(ctx, c.cfg.ConsumerGroup, c.cfg.InstanceID, c.cfg.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Don't log context.Canceled as error - it's normal when queue is empty
			if !isContextError(err) {
				c.log.Error("consume failed", "error", err)
			}
			c.sleep(ctx)
			continue
		}

		if len(resp.Messages) == 0 {
			c.sleep(ctx)
			continue
		}

		c.log.Info("batch consumed", "count", len(resp.Messages))
		metrics.CollectorBatchesConsumed.Inc()

		persistStart := time.Now()
		if err := c.persist(ctx, resp.Messages); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("DB write failed - batch will be redelivered", "error", err)
			metrics.CollectorPersistErrors.Inc()
			c.sleep(ctx)
			continue
		}
		metrics.CollectorPersistDuration.Observe(time.Since(persistStart).Seconds())
		metrics.CollectorRowsInserted.Add(float64(len(resp.Messages)))

		// Ack only after all DB writes have succeeded.
		ackReq := model.AckRequest{
			ConsumerGroup: c.cfg.ConsumerGroup,
			Partition:     resp.Partition, // use the partition we actually consumed from
			Offset:        resp.Offset,
		}
		if err := c.client.Ack(ctx, ackReq); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("ack failed", "error", err, "offset", resp.Offset)
			metrics.CollectorPersistErrors.Inc()
		} else {
			c.log.Info("ack success", "offset", resp.Offset)
		}
	}
}

// persist writes all GPU registrations and telemetry records for a batch to
// the database. It returns an error if any single write fails so the caller
// can skip the Ack.
func (c *Collector) persist(ctx context.Context, msgs []model.PublishMessage) error {
	gpus := ExtractGPUs(msgs)
	records := ExtractTelemetry(msgs)

	// Upsert GPU rows first to satisfy the FK constraint on the telemetry table.
	for _, gpu := range gpus {
		if err := c.store.UpsertGPU(ctx, gpu); err != nil {
			return fmt.Errorf("collector: upsert gpu %s: %w", gpu.UUID, err)
		}
	}
	c.log.Info("GPUs upserted", "count", len(gpus))

	if err := c.store.BulkInsertTelemetry(ctx, records); err != nil {
		return fmt.Errorf("collector: bulk insert telemetry: %w", err)
	}
	c.log.Info("telemetry inserted", "count", len(records))

	return nil
}

// sleep pauses for PollInterval, returning early on context cancellation.
func (c *Collector) sleep(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(c.cfg.PollInterval):
	}
}

// isContextError checks if the error is due to context cancellation or deadline.
// These errors are expected when the MQ has no new messages and the request times out.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

