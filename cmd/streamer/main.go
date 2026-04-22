package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/example/gpu-telemetry-pipeline/internal/streamer"
)

func main() {
	cfg := streamer.Config{
		MQURL:           mustEnv("MQ_URL"),
		CSVPath:         mustEnv("CSV_PATH"),
		StreamIntervalMS: envInt("STREAM_INTERVAL_MS", 100),
		StreamBatchSize:  envInt("STREAM_BATCH_SIZE", 50),
		PodName:         os.Getenv("POD_NAME"),
	}

	s, err := streamer.New(cfg)
	if err != nil {
		slog.Error("failed to create streamer", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := s.Run(ctx); err != nil {
		slog.Error("streamer stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("streamer shut down cleanly")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return fallback
}
