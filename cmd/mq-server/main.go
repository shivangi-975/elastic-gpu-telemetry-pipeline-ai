package main

import (
	"log/slog"
	"os"
	"strconv"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
	"github.com/example/gpu-telemetry-pipeline/internal/mq"
)

func main() {
	cfg := model.Config{
		Partitions:   envInt("MQ_PARTITIONS", 8),
		MaxQueueSize: envInt("MQ_MAX_QUEUE_SIZE", 4096),
		WALPath:      os.Getenv("MQ_WAL_PATH"),
		ListenAddr:   envStr("MQ_LISTEN_ADDR", ":9000"),
	}

	srv, err := mq.NewServer(cfg)
	if err != nil {
		slog.Error("failed to create mq server", "component", "mq", "error", err)
		os.Exit(1)
	}

	if err := srv.ListenAndServe(); err != nil {
		slog.Error("mq server stopped", "component", "mq", "error", err)
		os.Exit(1)
	}
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
