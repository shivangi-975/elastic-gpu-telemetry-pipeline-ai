// Package main is the entry point for the API Gateway service.
//
// @title           GPU Telemetry Pipeline API
// @version         1.0
// @description     REST API for querying GPU telemetry data collected from an AI cluster.
//
// @contact.name    Platform Team
//
// @host            localhost:8080
// @BasePath        /
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/api"
	"github.com/example/gpu-telemetry-pipeline/internal/store"

	// Import generated docs for Swagger
	_ "github.com/example/gpu-telemetry-pipeline/docs"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	dbURL := requireEnv("DATABASE_URL")
	port := envStr("PORT", "8080")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Open the PostgreSQL store (runs migrations automatically).
	initCtx, initCancel := context.WithTimeout(ctx, 30*time.Second)
	st, err := store.New(initCtx, dbURL)
	initCancel()
	if err != nil {
		slog.Error("failed to open store", "component", "api-gateway", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      api.Handler(st),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("api-gateway listening", "component", "api-gateway", "addr", ":"+port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "component", "api-gateway", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down api-gateway", "component", "api-gateway")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("graceful shutdown failed", "component", "api-gateway", "error", err)
		os.Exit(1)
	}
	slog.Info("api-gateway stopped cleanly", "component", "api-gateway")
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
