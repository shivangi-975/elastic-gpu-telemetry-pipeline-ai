package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/collector"
	"github.com/example/gpu-telemetry-pipeline/internal/metrics"
	"github.com/example/gpu-telemetry-pipeline/internal/store"
)

func main() {
	// Structured JSON logging for the entire process.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := loadConfig()

	slog.Info("collector starting",
		"mq_url", cfg.MQURL,
		"consumer_group", cfg.ConsumerGroup,
		"batch_size", cfg.BatchSize,
		"poll_interval", cfg.PollInterval,
		"instance_id", cfg.InstanceID,
	)

	// Propagate SIGINT / SIGTERM so the collector can finish its current batch.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Open the PostgreSQL store (runs migrations automatically).
	initCtx, initCancel := context.WithTimeout(ctx, 30*time.Second)
	st, err := store.New(initCtx, cfg.DatabaseURL)
	initCancel()
	if err != nil {
		slog.Error("failed to open store", "error", err)
		os.Exit(1)
	}
	// store.Close is called by collector.Run via StoreInterface.Close on shutdown.

	// Expose Prometheus /metrics on a sidecar port (collector is otherwise
	// headless). Address overridable via METRICS_ADDR.
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9100"
	}
	stopMetrics := metrics.ServeAsync(metricsAddr, metrics.MustRegister(
		metrics.CollectorBatchesConsumed,
		metrics.CollectorRowsInserted,
		metrics.CollectorPersistErrors,
		metrics.CollectorPersistDuration,
	))
	defer stopMetrics()

	c, err := collector.New(cfg, st)
	if err != nil {
		slog.Error("failed to create collector", "error", err)
		os.Exit(1)
	}

	if err := c.Run(ctx); err != nil {
		slog.Error("collector exited with error", "error", err)
		os.Exit(1)
	}

	slog.Info("collector shut down cleanly")
}

// loadConfig reads configuration from environment variables and applies
// sensible defaults for optional fields.
func loadConfig() collector.Config {
	cfg := collector.Config{
		MQURL:       requireEnv("MQ_URL"),
		DatabaseURL: requireEnv("DATABASE_URL"),
	}

	if v := os.Getenv("CONSUMER_GROUP"); v != "" {
		cfg.ConsumerGroup = v
	}

	if v := os.Getenv("COLLECT_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			slog.Warn("invalid COLLECT_BATCH_SIZE, using default 100", "value", v)
		} else {
			cfg.BatchSize = n
		}
	}

	if v := os.Getenv("COLLECT_POLL_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 {
			slog.Warn("invalid COLLECT_POLL_MS, using default 100ms", "value", v)
		} else {
			cfg.PollInterval = time.Duration(ms) * time.Millisecond
		}
	}

	// Prefer POD_NAME; fall back to os.Hostname so every instance is unique.
	if v := os.Getenv("POD_NAME"); v != "" {
		cfg.InstanceID = v
	} else {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}
		cfg.InstanceID = host
	}

	return cfg
}

// requireEnv returns the value of the named environment variable or exits if
// it is unset or empty.
func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}
