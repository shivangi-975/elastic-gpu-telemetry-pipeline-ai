package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/api"
	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// --------------------------------------------------------------------------
// Mock store
// --------------------------------------------------------------------------

type mockStore struct {
	listGPUsFn func(context.Context) ([]model.GPU, error)
	queryTelFn func(context.Context, string, string, *time.Time, *time.Time, int, int) ([]model.TelemetryRecord, error)
	pingFn     func(context.Context) error
}

func (m *mockStore) ListGPUs(ctx context.Context) ([]model.GPU, error) {
	return m.listGPUsFn(ctx)
}

func (m *mockStore) QueryTelemetry(ctx context.Context, uuid string, metricName string, start, end *time.Time, limit, offset int) ([]model.TelemetryRecord, error) {
	return m.queryTelFn(ctx, uuid, metricName, start, end, limit, offset)
}

func (m *mockStore) Ping(ctx context.Context) error {
	return m.pingFn(ctx)
}

// telemetryPage mirrors api.TelemetryPage for JSON decoding in tests.
type telemetryPage struct {
	Data    []model.TelemetryRecord `json:"data"`
	Limit   int                     `json:"limit"`
	Offset  int                     `json:"offset"`
	HasMore bool                    `json:"has_more"`
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

func newHandler(store api.StoreReader) http.Handler {
	return api.Handler(store)
}

func doRequest(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// noopQueryTelFn is a queryTelFn that returns an empty slice and never errors.
func noopQueryTelFn(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
	return []model.TelemetryRecord{}, nil
}

// --------------------------------------------------------------------------
// /health
// --------------------------------------------------------------------------

func TestHandleHealth_OK(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn: func(ctx context.Context) error { return nil },
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/health")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
}

func TestHandleHealth_DBDown(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn: func(ctx context.Context) error { return errors.New("connection refused") },
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/health")

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["error"] == "" {
		t.Error("expected non-empty error field in response")
	}
}

// --------------------------------------------------------------------------
// GET /api/v1/gpus
// --------------------------------------------------------------------------

func TestHandleListGPUs_Empty(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		listGPUsFn: func(ctx context.Context) ([]model.GPU, error) { return []model.GPU{}, nil },
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var gpus []model.GPU
	if err := json.Unmarshal(rr.Body.Bytes(), &gpus); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(gpus) != 0 {
		t.Errorf("expected empty array, got %d items", len(gpus))
	}
}

func TestHandleListGPUs_Multiple(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	want := []model.GPU{
		{UUID: "GPU-aabb0001", Hostname: "node-1", LastSeen: now},
		{UUID: "GPU-aabb0002", Hostname: "node-2", LastSeen: now},
	}
	store := &mockStore{
		listGPUsFn: func(ctx context.Context) ([]model.GPU, error) { return want, nil },
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var gpus []model.GPU
	if err := json.Unmarshal(rr.Body.Bytes(), &gpus); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(gpus) != 2 {
		t.Fatalf("expected 2 GPUs, got %d", len(gpus))
	}
	if gpus[0].UUID != "GPU-aabb0001" || gpus[1].UUID != "GPU-aabb0002" {
		t.Errorf("unexpected UUIDs: %v", gpus)
	}
}

func TestHandleListGPUs_DBError(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		listGPUsFn: func(ctx context.Context) ([]model.GPU, error) {
			return nil, errors.New("db error")
		},
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus")

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

// --------------------------------------------------------------------------
// GET /api/v1/gpus/{uuid}/telemetry — pagination
// --------------------------------------------------------------------------

func TestHandleQueryTelemetry_NoFilter(t *testing.T) {
	t.Parallel()

	records := []model.TelemetryRecord{
		{GPUUUID: "GPU-aabb0001", MetricName: "GPU_UTIL", MetricValue: 47.0},
	}
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, start, end *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
			if start != nil || end != nil {
				t.Error("expected nil start/end for no-filter request")
			}
			return records, nil
		},
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-aabb0001/telemetry")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var page telemetryPage
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(page.Data) != 1 {
		t.Fatalf("expected 1 record, got %d", len(page.Data))
	}
}

func TestHandleQueryTelemetry_DefaultLimit(t *testing.T) {
	t.Parallel()

	var gotLimit int
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, limit, _ int) ([]model.TelemetryRecord, error) {
			gotLimit = limit
			return []model.TelemetryRecord{}, nil
		},
	}
	doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-aabb0001/telemetry")

	// Handler fetches limit+1 to detect has_more; default limit=1000 → store sees 1001.
	if gotLimit != 1001 {
		t.Errorf("expected store called with limit=1001 (default 1000+1), got %d", gotLimit)
	}
}

func TestHandleQueryTelemetry_CustomLimit(t *testing.T) {
	t.Parallel()

	var gotLimit, gotOffset int
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, limit, offset int) ([]model.TelemetryRecord, error) {
			gotLimit = limit
			gotOffset = offset
			return []model.TelemetryRecord{}, nil
		},
	}
	doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-aabb0001/telemetry?limit=50&offset=100")

	if gotLimit != 51 { // 50+1
		t.Errorf("expected store called with limit=51, got %d", gotLimit)
	}
	if gotOffset != 100 {
		t.Errorf("expected offset=100, got %d", gotOffset)
	}
}

func TestHandleQueryTelemetry_HasMore(t *testing.T) {
	t.Parallel()

	// Return limit+1 rows to trigger has_more=true.
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, limit, _ int) ([]model.TelemetryRecord, error) {
			rows := make([]model.TelemetryRecord, limit) // exactly limit+1 rows
			for i := range rows {
				rows[i] = model.TelemetryRecord{MetricValue: float64(i)}
			}
			return rows, nil
		},
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-aabb0001/telemetry?limit=5")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var page telemetryPage
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !page.HasMore {
		t.Error("expected has_more=true when store returns limit+1 rows")
	}
	if len(page.Data) != 5 {
		t.Errorf("expected data trimmed to limit=5, got %d", len(page.Data))
	}
}

func TestHandleQueryTelemetry_HasMoreFalse(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
			return []model.TelemetryRecord{
				{MetricValue: 1},
				{MetricValue: 2},
			}, nil // fewer than limit rows → last page
		},
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-aabb0001/telemetry?limit=10")

	var page telemetryPage
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if page.HasMore {
		t.Error("expected has_more=false when fewer rows than limit are returned")
	}
	if len(page.Data) != 2 {
		t.Errorf("expected 2 records, got %d", len(page.Data))
	}
}

func TestHandleQueryTelemetry_InvalidLimit(t *testing.T) {
	t.Parallel()

	store := &mockStore{queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
		t.Error("store should not be called for invalid limit")
		return nil, nil
	}}

	for _, bad := range []string{"limit=0", "limit=-1", "limit=abc"} {
		rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/G/telemetry?"+bad)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("?%s: expected 400, got %d", bad, rr.Code)
		}
	}
}

func TestHandleQueryTelemetry_InvalidOffset(t *testing.T) {
	t.Parallel()

	store := &mockStore{queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
		t.Error("store should not be called for invalid offset")
		return nil, nil
	}}

	for _, bad := range []string{"offset=-1", "offset=abc"} {
		rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/G/telemetry?"+bad)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("?%s: expected 400, got %d", bad, rr.Code)
		}
	}
}

func TestHandleQueryTelemetry_LimitCappedAtMax(t *testing.T) {
	t.Parallel()

	var gotLimit int
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, limit, _ int) ([]model.TelemetryRecord, error) {
			gotLimit = limit
			return []model.TelemetryRecord{}, nil
		},
	}
	doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/G/telemetry?limit=99999")

	// max is 10000; store receives 10001 (max+1 for has_more detection).
	if gotLimit != 10001 {
		t.Errorf("expected limit capped to 10001 (max 10000+1), got %d", gotLimit)
	}
}

func TestHandleQueryTelemetry_WithTimeFilter(t *testing.T) {
	t.Parallel()

	startStr := "2026-04-19T17:25:00Z"
	endStr := "2026-04-19T17:26:00Z"
	wantStart, _ := time.Parse(time.RFC3339, startStr)
	wantEnd, _ := time.Parse(time.RFC3339, endStr)

	var gotStart, gotEnd *time.Time
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, start, end *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
			gotStart = start
			gotEnd = end
			return []model.TelemetryRecord{}, nil
		},
	}
	rr := doRequest(t, newHandler(store), http.MethodGet,
		"/api/v1/gpus/GPU-aabb0001/telemetry?start_time="+startStr+"&end_time="+endStr)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotStart == nil || !gotStart.Equal(wantStart) {
		t.Errorf("start: expected %v, got %v", wantStart, gotStart)
	}
	if gotEnd == nil || !gotEnd.Equal(wantEnd) {
		t.Errorf("end: expected %v, got %v", wantEnd, gotEnd)
	}
}

func TestHandleQueryTelemetry_InvalidStartTime(t *testing.T) {
	t.Parallel()

	store := &mockStore{queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
		t.Error("store should not be called for invalid start time")
		return nil, nil
	}}
	rr := doRequest(t, newHandler(store), http.MethodGet,
		"/api/v1/gpus/GPU-aabb0001/telemetry?start_time=not-a-time")

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestHandleQueryTelemetry_InvalidEndTime(t *testing.T) {
	t.Parallel()

	store := &mockStore{queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
		t.Error("store should not be called for invalid end time")
		return nil, nil
	}}
	rr := doRequest(t, newHandler(store), http.MethodGet,
		"/api/v1/gpus/GPU-aabb0001/telemetry?end_time=not-a-time")

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestHandleQueryTelemetry_UnknownGPU(t *testing.T) {
	t.Parallel()

	store := &mockStore{queryTelFn: noopQueryTelFn}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-unknown/telemetry")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 (empty page), got %d", rr.Code)
	}
	var page telemetryPage
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(page.Data) != 0 {
		t.Errorf("expected empty data for unknown GPU, got %d records", len(page.Data))
	}
	if page.HasMore {
		t.Error("has_more should be false for empty result")
	}
}

func TestHandleQueryTelemetry_DBError(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
			return nil, errors.New("query failed")
		},
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-aabb0001/telemetry")

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestHandleQueryTelemetry_MetricValuesPreserved(t *testing.T) {
	t.Parallel()

	csvValues := []struct {
		metric string
		value  float64
	}{
		{"GPU_UTIL", 47.0},
		{"MEM_COPY_UTIL", 13.0},
		{"POWER_USAGE", 224.1},
	}
	records := make([]model.TelemetryRecord, len(csvValues))
	for i, v := range csvValues {
		records[i] = model.TelemetryRecord{GPUUUID: "GPU-aabb0001", MetricName: v.metric, MetricValue: v.value}
	}
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
			return records, nil
		},
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-aabb0001/telemetry")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var page telemetryPage
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(page.Data) != len(csvValues) {
		t.Fatalf("expected %d records, got %d", len(csvValues), len(page.Data))
	}
	for i, rec := range page.Data {
		if rec.MetricValue != csvValues[i].value {
			t.Errorf("record %d: expected value %v, got %v", i, csvValues[i].value, rec.MetricValue)
		}
	}
}

func TestHandleQueryTelemetry_MetricNameFilter(t *testing.T) {
	t.Parallel()

	var gotMetric string
	store := &mockStore{
		queryTelFn: func(_ context.Context, _ string, metricName string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
			gotMetric = metricName
			return []model.TelemetryRecord{}, nil
		},
	}
	doRequest(t, newHandler(store), http.MethodGet,
		"/api/v1/gpus/GPU-aabb0001/telemetry?metric_name=DCGM_FI_DEV_GPU_UTIL")

	if gotMetric != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("expected metric_name=DCGM_FI_DEV_GPU_UTIL, got %q", gotMetric)
	}
}

func TestHandleHealthz(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn: func(ctx context.Context) error { return nil },
	}
	rr := doRequest(t, newHandler(store), http.MethodGet, "/healthz")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
}

// --------------------------------------------------------------------------
// Content-Type and method enforcement
// --------------------------------------------------------------------------

func TestAllResponsesAreJSON(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn:     func(ctx context.Context) error { return nil },
		listGPUsFn: func(ctx context.Context) ([]model.GPU, error) { return []model.GPU{}, nil },
		queryTelFn: noopQueryTelFn,
	}
	h := newHandler(store)

	for _, ep := range []string{"/health", "/api/v1/gpus", "/api/v1/gpus/GPU-aabb0001/telemetry"} {
		rr := doRequest(t, h, http.MethodGet, ep)
		if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("%s: Content-Type = %q, want application/json", ep, ct)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn:     func(ctx context.Context) error { return nil },
		listGPUsFn: func(ctx context.Context) ([]model.GPU, error) { return nil, nil },
		queryTelFn: noopQueryTelFn,
	}
	h := newHandler(store)

	cases := []struct{ method, path string }{
		{http.MethodPost, "/health"},
		{http.MethodPost, "/api/v1/gpus"},
		{http.MethodPost, "/api/v1/gpus/GPU-aabb0001/telemetry"},
		{http.MethodDelete, "/api/v1/gpus"},
	}
	for _, tc := range cases {
		rr := doRequest(t, h, tc.method, tc.path)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: expected 405, got %d", tc.method, tc.path, rr.Code)
		}
	}
}

func TestHandleQueryTelemetry_UUIDPassedToStore(t *testing.T) {
	t.Parallel()

	var capturedUUID string
	store := &mockStore{
		queryTelFn: func(_ context.Context, uuid string, _ string, _, _ *time.Time, _, _ int) ([]model.TelemetryRecord, error) {
			capturedUUID = uuid
			return []model.TelemetryRecord{}, nil
		},
	}
	doRequest(t, newHandler(store), http.MethodGet, "/api/v1/gpus/GPU-deadbeef/telemetry")

	if capturedUUID != "GPU-deadbeef" {
		t.Errorf("expected UUID GPU-deadbeef, got %q", capturedUUID)
	}
}

// --------------------------------------------------------------------------
// Swagger and OpenAPI tests
// --------------------------------------------------------------------------

func TestSwaggerUI(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn: func(_ context.Context) error {
			return nil
		},
	}

	// Test Swagger UI redirect
	rr := doRequest(t, newHandler(store), http.MethodGet, "/swagger/")
	if rr.Code != http.StatusMovedPermanently && rr.Code != http.StatusFound {
		t.Errorf("expected redirect status for /swagger/, got %d", rr.Code)
	}
}

func TestOpenAPIEndpoint(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn: func(_ context.Context) error {
			return nil
		},
	}

	// Test OpenAPI JSON endpoint
	rr := doRequest(t, newHandler(store), http.MethodGet, "/api/v1/openapi.json")
	if rr.Code != http.StatusTemporaryRedirect {
		t.Errorf("expected redirect status for /api/v1/openapi.json, got %d", rr.Code)
	}
}

func TestCORSHeaders(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		pingFn: func(_ context.Context) error {
			return nil
		},
	}

	// Test CORS preflight request
	rr := doRequest(t, newHandler(store), http.MethodOptions, "/api/v1/gpus")
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for OPTIONS request, got %d", rr.Code)
	}

	// Check CORS headers
	headers := rr.Header()
	if headers.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing or incorrect Access-Control-Allow-Origin header")
	}
	if headers.Get("Access-Control-Allow-Methods") == "" {
		t.Error("missing Access-Control-Allow-Methods header")
	}
}

func TestAPIGatewayRouting(t *testing.T) {
	t.Parallel()

	store := &mockStore{
		listGPUsFn: func(_ context.Context) ([]model.GPU, error) {
			return []model.GPU{{UUID: "test-gpu"}}, nil
		},
		pingFn: func(_ context.Context) error {
			return nil
		},
	}

	tests := []struct {
		name       string
		path       string
		method     string
		wantStatus int
	}{
		{"health check", "/health", http.MethodGet, http.StatusOK},
		{"list GPUs", "/api/v1/gpus", http.MethodGet, http.StatusOK},
		{"swagger redirect", "/swagger/", http.MethodGet, http.StatusMovedPermanently},
		{"openapi redirect", "/api/v1/openapi.json", http.MethodGet, http.StatusTemporaryRedirect},
		{"cors options", "/api/v1/gpus", http.MethodOptions, http.StatusOK},
	}

	handler := newHandler(store)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := doRequest(t, handler, tt.method, tt.path)
			if rr.Code != tt.wantStatus && rr.Code != http.StatusFound { // Allow Found as well as MovedPermanently
				t.Errorf("path %s %s: expected status %d, got %d", tt.method, tt.path, tt.wantStatus, rr.Code)
			}
		})
	}
}
