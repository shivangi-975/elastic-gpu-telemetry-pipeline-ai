package mq

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/example/gpu-telemetry-pipeline/internal/metrics"
	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// Server wraps Broker behind an HTTP interface using the standard library ServeMux.
type Server struct {
	broker *Broker
	mux    *http.ServeMux
	cfg    model.Config
}

// NewServer creates a Broker from cfg and registers all HTTP routes.
func NewServer(cfg model.Config) (*Server, error) {
	broker, err := NewBroker(cfg)
	if err != nil {
		return nil, fmt.Errorf("mq: %w", err)
	}

	s := &Server{
		broker: broker,
		mux:    http.NewServeMux(),
		cfg:    cfg,
	}
	s.registerRoutes()
	return s, nil
}

// ServeHTTP implements http.Handler so Server can be used directly with httptest.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// ListenAndServe starts the HTTP server on cfg.ListenAddr.
func (s *Server) ListenAndServe() error {
	slog.Info("mq server starting", "component", "mq", "addr", s.cfg.ListenAddr)
	return http.ListenAndServe(s.cfg.ListenAddr, s.mux)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /publish", s.handlePublish)
	s.mux.HandleFunc("GET /consume", s.handleConsume)
	s.mux.HandleFunc("POST /ack", s.handleAck)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /metrics/json", s.handleMetricsJSON)

	// Prometheus exposition. Refresh broker gauges on each scrape so the
	// numbers reflect live partition state, not just deltas from publish/ack.
	reg := metrics.MustRegister(
		metrics.MQPublishedTotal,
		metrics.MQConsumedTotal,
		metrics.MQAckedTotal,
		metrics.MQBackpressureTotal,
		metrics.MQPartitionLength,
		metrics.MQConsumerOffset,
	)
	promHandler := metrics.Handler(reg)
	s.mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		_ = s.broker.Metrics() // side-effect: refreshes gauges
		promHandler.ServeHTTP(w, r)
	})
}

// POST /publish
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	var batch model.PublishBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, fmt.Errorf("mq: %w", err).Error(), http.StatusBadRequest)
		return
	}

	if err := s.broker.Publish(batch); err != nil {
		slog.Error("publish failed", "component", "mq", "error", err)
		w.Header().Set("Retry-After", "1")
		http.Error(w, fmt.Errorf("mq: %w", err).Error(), http.StatusTooManyRequests)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /consume?group=&consumer_id=&max=
func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	group := r.URL.Query().Get("group")
	consumerID := r.URL.Query().Get("consumer_id")
	max := 100
	if v, err := strconv.Atoi(r.URL.Query().Get("max")); err == nil && v > 0 {
		max = v
	}

	resp, err := s.broker.Consume(group, consumerID, max)
	if err != nil {
		slog.Error("consume failed", "component", "mq", "error", err)
		http.Error(w, fmt.Errorf("mq: %w", err).Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// POST /ack
func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	var req model.AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Errorf("mq: %w", err).Error(), http.StatusBadRequest)
		return
	}

	if err := s.broker.Ack(req); err != nil {
		slog.Error("ack failed", "component", "mq", "error", err)
		http.Error(w, fmt.Errorf("mq: %w", err).Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.broker.Health(); err != nil {
		http.Error(w, fmt.Errorf("mq: %w", err).Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /metrics/json — debug-friendly snapshot of broker state.
// /metrics serves the standard Prometheus exposition format (registered above).
func (s *Server) handleMetricsJSON(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.broker.Metrics())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
