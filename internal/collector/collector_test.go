package collector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// ── Mock store ───────────────────────────────────────────────────────────────

type mockStore struct {
	upsertGPUFn           func(ctx context.Context, gpu model.GPU) error
	bulkInsertTelemetryFn func(ctx context.Context, records []model.TelemetryRecord) error
	closeFn               func()
}

func (m *mockStore) UpsertGPU(ctx context.Context, gpu model.GPU) error {
	if m.upsertGPUFn != nil {
		return m.upsertGPUFn(ctx, gpu)
	}
	return nil
}

func (m *mockStore) BulkInsertTelemetry(ctx context.Context, records []model.TelemetryRecord) error {
	if m.bulkInsertTelemetryFn != nil {
		return m.bulkInsertTelemetryFn(ctx, records)
	}
	return nil
}

func (m *mockStore) Close() {
	if m.closeFn != nil {
		m.closeFn()
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func testConfig(mqURL string) Config {
	return Config{
		MQURL:         mqURL,
		ConsumerGroup: "test-group",
		BatchSize:     10,
		PollInterval:  time.Millisecond, // fast for tests
		InstanceID:    "test-instance",
	}
}

// writeJSON encodes v as JSON into w.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func emptyConsumeResponse() model.ConsumeResponse { return model.ConsumeResponse{} }

// sampleMessages returns 3 messages across 2 unique GPUs.
func sampleMessages(t0 time.Time) []model.PublishMessage {
	return []model.PublishMessage{
		{Key: "gpu-1", Payload: model.TelemetryRecord{GPUUUID: "gpu-1", Hostname: "host-a", MetricName: "util", MetricValue: 10, CollectedAt: t0}},
		{Key: "gpu-1", Payload: model.TelemetryRecord{GPUUUID: "gpu-1", Hostname: "host-a", MetricName: "mem", MetricValue: 20, CollectedAt: t0.Add(time.Second)}},
		{Key: "gpu-2", Payload: model.TelemetryRecord{GPUUUID: "gpu-2", Hostname: "host-b", MetricName: "util", MetricValue: 30, CollectedAt: t0}},
	}
}

// runCollector starts c.Run in a goroutine and returns a done channel.
func runCollector(ctx context.Context, c *Collector) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- c.Run(ctx) }()
	return ch
}

// ── TestConsumeAndPersist ────────────────────────────────────────────────────

func TestConsumeAndPersist(t *testing.T) {
	t.Parallel()

	t0 := time.Now().UTC().Truncate(time.Millisecond)
	msgs := sampleMessages(t0)

	var reqIdx atomic.Int32
	ackReceived := make(chan model.AckRequest, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/consume":
			if reqIdx.Add(1) == 1 {
				writeJSON(w, model.ConsumeResponse{Offset: 42, Messages: msgs})
			} else {
				writeJSON(w, emptyConsumeResponse())
			}
		case "/ack":
			var req model.AckRequest
			json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
			ackReceived <- req
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	var mu sync.Mutex
	var upserted []model.GPU
	var inserted []model.TelemetryRecord

	ms := &mockStore{
		upsertGPUFn: func(_ context.Context, gpu model.GPU) error {
			mu.Lock()
			upserted = append(upserted, gpu)
			mu.Unlock()
			return nil
		},
		bulkInsertTelemetryFn: func(_ context.Context, records []model.TelemetryRecord) error {
			mu.Lock()
			inserted = records
			mu.Unlock()
			return nil
		},
	}

	c, err := New(testConfig(srv.URL), ms)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.client.RetryWaits = []time.Duration{0, 0, 0}

	ctx, cancel := context.WithCancel(context.Background())
	done := runCollector(ctx, c)

	// Wait for ack then shut down.
	select {
	case ack := <-ackReceived:
		cancel()
		if ack.Offset != 42 {
			t.Errorf("ack offset: want 42, got %d", ack.Offset)
		}
		if ack.ConsumerGroup != "test-group" {
			t.Errorf("ack consumer_group: want test-group, got %q", ack.ConsumerGroup)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timeout waiting for ack")
	}

	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(inserted) != 3 {
		t.Errorf("BulkInsertTelemetry: want 3 records, got %d", len(inserted))
	}
	// 2 unique GPUs → 2 UpsertGPU calls.
	if len(upserted) != 2 {
		t.Errorf("UpsertGPU: want 2 calls, got %d", len(upserted))
	}
}

// ── TestNoAckOnInsertFailure ─────────────────────────────────────────────────

func TestNoAckOnInsertFailure(t *testing.T) {
	t.Parallel()

	t0 := time.Now().UTC()
	msgs := sampleMessages(t0)
	ackCalled := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/consume":
			writeJSON(w, model.ConsumeResponse{Offset: 1, Messages: msgs})
		case "/ack":
			ackCalled <- struct{}{}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	ms := &mockStore{
		bulkInsertTelemetryFn: func(_ context.Context, _ []model.TelemetryRecord) error {
			return errors.New("simulated DB failure")
		},
	}

	c, err := New(testConfig(srv.URL), ms)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.client.RetryWaits = []time.Duration{0, 0, 0}

	// Let the collector loop for a short window then stop.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	if runErr := c.Run(ctx); runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	select {
	case <-ackCalled:
		t.Fatal("Ack must not be called when BulkInsertTelemetry fails")
	default:
		// Correct: ack was never called.
	}
}

// ── TestEmptyConsumePollsAgain ───────────────────────────────────────────────

func TestEmptyConsumePollsAgain(t *testing.T) {
	t.Parallel()

	t0 := time.Now().UTC()
	msgs := sampleMessages(t0)

	var reqIdx atomic.Int32
	ackReceived := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/consume":
			// First call: empty. All subsequent calls return messages.
			if reqIdx.Add(1) == 1 {
				writeJSON(w, emptyConsumeResponse())
			} else {
				writeJSON(w, model.ConsumeResponse{Offset: 7, Messages: msgs})
			}
		case "/ack":
			ackReceived <- struct{}{}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	c, err := New(testConfig(srv.URL), &mockStore{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.client.RetryWaits = []time.Duration{0, 0, 0}

	ctx, cancel := context.WithCancel(context.Background())
	done := runCollector(ctx, c)

	select {
	case <-ackReceived:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timeout: collector did not poll again after empty response")
	}

	<-done

	if n := reqIdx.Load(); n < 2 {
		t.Errorf("expected at least 2 consume requests, got %d", n)
	}
}

// ── TestExtractGPUsDeduplicates ──────────────────────────────────────────────

func TestExtractGPUsDeduplicates(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		msgs        []model.PublishMessage
		wantUUIDs   int
		wantLastSeen map[string]time.Time
	}{
		{
			name: "two unique GPUs with duplicate uuid keeps latest",
			msgs: []model.PublishMessage{
				{Key: "a", Payload: model.TelemetryRecord{GPUUUID: "a", Hostname: "h1", CollectedAt: base}},
				{Key: "a", Payload: model.TelemetryRecord{GPUUUID: "a", Hostname: "h1", CollectedAt: base.Add(time.Hour)}},
				{Key: "b", Payload: model.TelemetryRecord{GPUUUID: "b", Hostname: "h2", CollectedAt: base.Add(30 * time.Minute)}},
			},
			wantUUIDs: 2,
			wantLastSeen: map[string]time.Time{
				"a": base.Add(time.Hour),
				"b": base.Add(30 * time.Minute),
			},
		},
		{
			name: "all same uuid keeps latest",
			msgs: []model.PublishMessage{
				{Key: "x", Payload: model.TelemetryRecord{GPUUUID: "x", Hostname: "h", CollectedAt: base.Add(2 * time.Hour)}},
				{Key: "x", Payload: model.TelemetryRecord{GPUUUID: "x", Hostname: "h", CollectedAt: base}},
			},
			wantUUIDs:    1,
			wantLastSeen: map[string]time.Time{"x": base.Add(2 * time.Hour)},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gpus := ExtractGPUs(tc.msgs)
			if len(gpus) != tc.wantUUIDs {
				t.Fatalf("want %d unique GPUs, got %d", tc.wantUUIDs, len(gpus))
			}
			for _, g := range gpus {
				want, ok := tc.wantLastSeen[g.UUID]
				if !ok {
					t.Errorf("unexpected UUID %q", g.UUID)
					continue
				}
				if !g.LastSeen.Equal(want) {
					t.Errorf("GPU %q LastSeen: want %v, got %v", g.UUID, want, g.LastSeen)
				}
			}
		})
	}
}

// ── TestExtractTelemetry ─────────────────────────────────────────────────────

func TestExtractTelemetry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	msgs := []model.PublishMessage{
		{Key: "g1", Payload: model.TelemetryRecord{GPUUUID: "g1", Hostname: "h1", MetricName: "util", MetricValue: 55.5, CollectedAt: now}},
		{Key: "g2", Payload: model.TelemetryRecord{GPUUUID: "g2", Hostname: "h2", MetricName: "mem", MetricValue: 99.0, CollectedAt: now.Add(time.Second)}},
	}

	records := ExtractTelemetry(msgs)

	if len(records) != len(msgs) {
		t.Fatalf("want %d records, got %d", len(msgs), len(records))
	}
	for i, r := range records {
		p := msgs[i].Payload
		if r.GPUUUID != p.GPUUUID {
			t.Errorf("[%d] GPUUUID: want %q, got %q", i, p.GPUUUID, r.GPUUUID)
		}
		if r.Hostname != p.Hostname {
			t.Errorf("[%d] Hostname: want %q, got %q", i, p.Hostname, r.Hostname)
		}
		if r.MetricName != p.MetricName {
			t.Errorf("[%d] MetricName: want %q, got %q", i, p.MetricName, r.MetricName)
		}
		if r.MetricValue != p.MetricValue {
			t.Errorf("[%d] MetricValue: want %v, got %v", i, p.MetricValue, r.MetricValue)
		}
		if !r.CollectedAt.Equal(p.CollectedAt) {
			t.Errorf("[%d] CollectedAt: want %v, got %v", i, p.CollectedAt, r.CollectedAt)
		}
	}
}

// ── TestRetriesOnMQ500 ───────────────────────────────────────────────────────

func TestRetriesOnMQ500(t *testing.T) {
	t.Parallel()

	var reqCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqCount.Add(1)
		if n < 3 {
			// First two attempts fail with 500.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// Third attempt succeeds.
		writeJSON(w, emptyConsumeResponse())
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	client.RetryWaits = []time.Duration{0, 0, 0} // no delay in tests

	_, err := client.Consume(context.Background(), "grp", "id", 1)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if got := reqCount.Load(); got != 3 {
		t.Errorf("want 3 total requests (1 initial + 2 retries), got %d", got)
	}
}

// ── TestGracefulShutdown ─────────────────────────────────────────────────────

func TestGracefulShutdown(t *testing.T) {
	t.Parallel()

	// Server should never be reached because context is already cancelled.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("unexpected HTTP request during shutdown test")
	}))
	defer srv.Close()

	c, err := New(testConfig(srv.URL), &mockStore{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Run is called

	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run: expected nil on graceful shutdown, got %v", err)
	}
}
