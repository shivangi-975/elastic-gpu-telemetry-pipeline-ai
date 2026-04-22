package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
	"github.com/example/gpu-telemetry-pipeline/internal/mq"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := model.Config{
		Partitions:    envInt("MQ_PARTITIONS", 16),
		MaxQueueSize:  envInt("MQ_MAX_QUEUE_SIZE", 4096),
		WALPath:       os.Getenv("MQ_WAL_PATH"),
		ListenAddr:    envStr("MQ_LISTEN_ADDR", ":9000"),
		ConsumerTTL:   envDuration("MQ_CONSUMER_TTL", 30*time.Second),
		EvictInterval: envDuration("MQ_EVICT_INTERVAL", 10*time.Second),
	}

	mqSrv, err := mq.NewServer(cfg)
	if err != nil {
		slog.Error("failed to create mq server", "component", "mq", "error", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mqSrv,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("mq server starting", "component", "mq", "addr", cfg.ListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("mq server error", "component", "mq", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down mq server", "component", "mq")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.Error("graceful shutdown failed", "component", "mq", "error", err)
		os.Exit(1)
	}
	mqSrv.Shutdown() // stop eviction loop and flush WAL file
	slog.Info("mq server stopped cleanly", "component", "mq")
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
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
