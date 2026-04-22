// Package api implements the HTTP handlers for the API Gateway service.
// It exposes GPU registry and telemetry query endpoints backed by PostgreSQL.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

const (
	defaultPageLimit = 1000
	maxPageLimit     = 10000
)

// TelemetryPage is the paginated response envelope for the telemetry endpoint.
//
// @Description Paginated telemetry response
type TelemetryPage struct {
	Data    []model.TelemetryRecord `json:"data"`
	Limit   int                     `json:"limit"`
	Offset  int                     `json:"offset"`
	HasMore bool                    `json:"has_more"`
}

// StoreReader is the read-only database contract the API depends on.
// Using an interface keeps the package decoupled from *store.Store and
// makes handlers trivial to test with a mock.
type StoreReader interface {
	ListGPUs(ctx context.Context) ([]model.GPU, error)
	QueryTelemetry(ctx context.Context, gpuUUID string, metricName string, start *time.Time, end *time.Time, limit int, offset int) ([]model.TelemetryRecord, error)
	Ping(ctx context.Context) error
}

// Handler builds and returns an http.Handler with all API routes registered.
//
// This API Gateway provides:
// - RESTful endpoints for GPU telemetry data
// - Auto-generated OpenAPI specification
// - Swagger UI for API documentation
// - CORS support for web clients
// - Health checks and monitoring
//
// Routes:
//
//	GET /health                              → liveness + db ping
//	GET /api/v1/gpus                         → list all known GPUs
//	GET /api/v1/gpus/{uuid}/telemetry        → query telemetry (optional ?start_time=&end_time= in RFC3339)
//	GET /swagger/*                           → Swagger UI and OpenAPI spec
//	GET /api/v1/openapi.json                 → OpenAPI specification (JSON)
func Handler(store StoreReader) http.Handler {
	r := mux.NewRouter()

	// Apply middleware
	r.Use(corsMiddleware)
	r.Use(loggingMiddleware)
	r.Use(jsonContentType)

	// Health check endpoints (/health and /healthz are both supported)
	r.HandleFunc("/health", handleHealth(store)).Methods(http.MethodGet)
	r.HandleFunc("/healthz", handleHealth(store)).Methods(http.MethodGet)

	// API v1 routes
	apiV1 := r.PathPrefix("/api/v1").Subrouter()
	apiV1.HandleFunc("/gpus", handleListGPUs(store)).Methods(http.MethodGet, http.MethodOptions)
	apiV1.HandleFunc("/gpus/{uuid}/telemetry", handleQueryTelemetry(store)).Methods(http.MethodGet, http.MethodOptions)
	apiV1.HandleFunc("/openapi.json", handleOpenAPISpec()).Methods(http.MethodGet, http.MethodOptions)

	// Documentation routes
	r.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)

	return r
}

// jsonContentType is a middleware that sets Content-Type for all responses.
func jsonContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// corsMiddleware adds CORS headers to support web clients
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers for all requests
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight OPTIONS request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware logs HTTP requests for observability
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)

		slog.Info("http request",
			"component", "api-gateway",
			"method", r.Method,
			"path", r.URL.Path,
			"duration_ms", duration.Milliseconds(),
			"user_agent", r.UserAgent(),
		)
	})
}

// handleHealth godoc
//
// @Summary      Health check
// @Description  Returns 200 and {"status":"ok"} when the database is reachable; 503 otherwise.
// @Tags         health
// @Produce      json
// @Success      200  {object}  map[string]string
// @Failure      503  {object}  map[string]string
// @Router       /health [get]
func handleHealth(store StoreReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := store.Ping(ctx); err != nil {
			slog.Error("health check: db ping failed",
				"component", "api-gateway", "error", err)
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": fmt.Sprintf("db ping: %s", err.Error())})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// handleListGPUs godoc
//
// @Summary      List all GPUs
// @Description  Returns all GPU devices for which telemetry data is available, ordered by UUID.
// @Tags         gpus
// @Produce      json
// @Success      200  {array}   model.GPU
// @Failure      500  {object}  map[string]string
// @Router       /api/v1/gpus [get]
func handleListGPUs(store StoreReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gpus, err := store.ListGPUs(r.Context())
		if err != nil {
			slog.Error("list GPUs failed",
				"component", "api-gateway", "error", err)
			writeJSON(w, http.StatusInternalServerError,
				map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, gpus)
	}
}

// handleQueryTelemetry godoc
//
// @Summary      Query telemetry for a GPU
// @Description  Returns paginated telemetry records for the specified GPU ordered by collected_at ascending.
// @Tags         gpus
// @Produce      json
// @Param        id           path   string  true   "GPU UUID"
// @Param        metric_name  query  string  false  "Filter by metric name (e.g. DCGM_FI_DEV_GPU_UTIL)"
// @Param        start_time   query  string  false  "Start time filter, inclusive (RFC3339, e.g. 2006-01-02T15:04:05Z)"
// @Param        end_time     query  string  false  "End time filter, inclusive (RFC3339)"
// @Param        limit        query  int     false  "Max records per page (default 1000, max 10000)"
// @Param        offset       query  int     false  "Number of records to skip (default 0)"
// @Success      200  {object}  TelemetryPage
// @Failure      400  {object}  map[string]string
// @Failure      500  {object}  map[string]string
// @Router       /api/v1/gpus/{id}/telemetry [get]
func handleQueryTelemetry(store StoreReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uuid := mux.Vars(r)["uuid"]
		q := r.URL.Query()

		metricName := q.Get("metric_name")

		var start, end *time.Time
		if s := q.Get("start_time"); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				writeJSON(w, http.StatusBadRequest,
					map[string]string{"error": "invalid start_time; use RFC3339 (e.g. 2006-01-02T15:04:05Z)"})
				return
			}
			start = &t
		}
		if e := q.Get("end_time"); e != "" {
			t, err := time.Parse(time.RFC3339, e)
			if err != nil {
				writeJSON(w, http.StatusBadRequest,
					map[string]string{"error": "invalid end_time; use RFC3339 (e.g. 2006-01-02T15:04:05Z)"})
				return
			}
			end = &t
		}

		limit := defaultPageLimit
		if l := q.Get("limit"); l != "" {
			v, err := strconv.Atoi(l)
			if err != nil || v <= 0 {
				writeJSON(w, http.StatusBadRequest,
					map[string]string{"error": "limit must be a positive integer"})
				return
			}
			if v > maxPageLimit {
				v = maxPageLimit
			}
			limit = v
		}

		offset := 0
		if o := q.Get("offset"); o != "" {
			v, err := strconv.Atoi(o)
			if err != nil || v < 0 {
				writeJSON(w, http.StatusBadRequest,
					map[string]string{"error": "offset must be a non-negative integer"})
				return
			}
			offset = v
		}

		qCtx, qCancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer qCancel()
		// Fetch limit+1 rows so we can detect whether a next page exists.
		records, err := store.QueryTelemetry(qCtx, uuid, metricName, start, end, limit+1, offset)
		if err != nil {
			slog.Error("query telemetry failed",
				"component", "api-gateway", "gpu_uuid", uuid, "error", err)
			writeJSON(w, http.StatusInternalServerError,
				map[string]string{"error": err.Error()})
			return
		}

		hasMore := len(records) > limit
		if hasMore {
			records = records[:limit]
		}
		writeJSON(w, http.StatusOK, TelemetryPage{
			Data:    records,
			Limit:   limit,
			Offset:  offset,
			HasMore: hasMore,
		})
	}
}

// handleOpenAPISpec godoc
//
// @Summary      OpenAPI Specification
// @Description  Returns the OpenAPI 3.0 specification for this API in JSON format
// @Tags         documentation
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Router       /api/v1/openapi.json [get]
func handleOpenAPISpec() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// This could dynamically generate the spec or serve the static one
		w.Header().Set("Content-Type", "application/json")

		// For now, redirect to swagger.json or return a basic spec
		http.Redirect(w, r, "/swagger/doc.json", http.StatusTemporaryRedirect)
	}
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
