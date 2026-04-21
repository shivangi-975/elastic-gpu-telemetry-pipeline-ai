package streamer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// mockPublisher records every PublishBatch call and optionally runs a callback.
type mockPublisher struct {
	mu        sync.Mutex
	batches   []model.PublishBatch
	onPublish func() // called after recording batch; may be nil
	err       error  // if non-nil, PublishBatch returns this error immediately
}

func (m *mockPublisher) PublishBatch(_ context.Context, batch model.PublishBatch) error {
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	m.batches = append(m.batches, batch)
	cb := m.onPublish
	m.mu.Unlock()
	if cb != nil {
		cb()
	}
	return nil
}

func (m *mockPublisher) totalMessages() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, b := range m.batches {
		n += len(b.Messages)
	}
	return n
}

func (m *mockPublisher) allMessages() []model.PublishMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.PublishMessage
	for _, b := range m.batches {
		out = append(out, b.Messages...)
	}
	return out
}

// writeCSV writes content to a temp file and returns its path.
func writeCSV(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "metrics.csv")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("writeCSV: %v", err)
	}
	return p
}

// csvWith builds a minimal 11-column CSV where:
//
//	fields[0]  = filler
//	fields[1]  = metric (rows[i][0])
//	fields[4]  = gpu_uuid (rows[i][1])
//	fields[10] = metric_value (rows[i][2])
func csvWith(rows [][3]string) string {
	out := "f0,metric_name,f2,f3,gpu_uuid,f5,f6,f7,f8,f9,metric_value\n"
	for _, r := range rows {
		// metric=r[0], uuid=r[1], value=r[2]
		out += fmt.Sprintf("x,%s,x,x,%s,x,x,x,x,x,%s\n", r[0], r[1], r[2])
	}
	return out
}

// noopTick returns a channel that never fires and a no-op stop function.
func noopTick(_ time.Duration) (<-chan time.Time, func()) {
	return make(chan time.Time), func() {}
}

// manualTick returns a send-only channel the caller can push ticks into,
// plus a newTickFn that wires that channel into the Streamer.
func manualTick() (chan<- time.Time, newTickFn) {
	ch := make(chan time.Time, 1)
	fn := func(_ time.Duration) (<-chan time.Time, func()) {
		return ch, func() {}
	}
	return ch, fn
}

// newTestStreamer builds a Streamer for unit tests without opening a real HTTP
// connection.  batchSize=0 and intervalMS=0 use package defaults.
func newTestStreamer(csvPath string, batchSize, intervalMS int, pub publisher, tick newTickFn) *Streamer {
	cfg := Config{
		CSVPath:          csvPath,
		StreamBatchSize:  batchSize,
		StreamIntervalMS: intervalMS,
	}
	if cfg.StreamBatchSize <= 0 {
		cfg.StreamBatchSize = 50
	}
	if cfg.StreamIntervalMS <= 0 {
		cfg.StreamIntervalMS = 100
	}
	if tick == nil {
		tick = noopTick
	}
	return &Streamer{
		cfg:     cfg,
		pub:     pub,
		logger:  slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(100)})),
		newTick: tick,
	}
}

// ---------------------------------------------------------------------------
// TestCSVLoopsAfterEOF
// ---------------------------------------------------------------------------

// TestCSVLoopsAfterEOF verifies that after reaching EOF the streamer wraps back
// to the first data row and continues publishing indefinitely.
func TestCSVLoopsAfterEOF(t *testing.T) {
	t.Parallel()

	csvPath := writeCSV(t, csvWith([][3]string{
		{"UTIL", "GPU-LOOP-A", "10.0"},
		{"UTIL", "GPU-LOOP-B", "20.0"},
	}))

	pub := &mockPublisher{}
	ctx, cancel := context.WithCancel(context.Background())

	flushes := 0
	pub.onPublish = func() {
		flushes++
		if flushes >= 3 { // 3 flushes × 2-row batches = ≥6 messages → proves looping
			cancel()
		}
	}

	s := newTestStreamer(csvPath, 2 /*batchSize*/, 100_000 /*ticker never fires*/, pub, noopTick)
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if total := pub.totalMessages(); total < 6 {
		t.Errorf("expected ≥6 messages (3 loops × 2 rows), got %d", total)
	}

	for _, m := range pub.allMessages() {
		if m.Key != "GPU-LOOP-A" && m.Key != "GPU-LOOP-B" {
			t.Errorf("unexpected GPU UUID %q in published messages", m.Key)
		}
	}
}

// ---------------------------------------------------------------------------
// TestCollectedAtUsesNow
// ---------------------------------------------------------------------------

// TestCollectedAtUsesNow verifies that CollectedAt is set to approximately the
// current wall-clock time, not sourced from the CSV data.
func TestCollectedAtUsesNow(t *testing.T) {
	t.Parallel()

	csvPath := writeCSV(t, csvWith([][3]string{{"UTIL", "GPU-TIME", "55.0"}}))

	pub := &mockPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	pub.onPublish = func() { cancel() }

	before := time.Now()
	s := newTestStreamer(csvPath, 1, 100_000, pub, noopTick)
	s.Run(ctx) //nolint:errcheck
	after := time.Now().Add(time.Second) // generous upper bound

	msgs := pub.allMessages()
	if len(msgs) == 0 {
		t.Fatal("no messages published")
	}
	for _, m := range msgs {
		ca := m.Payload.CollectedAt
		if ca.IsZero() {
			t.Error("CollectedAt is zero")
		}
		if ca.Before(before) || ca.After(after) {
			t.Errorf("CollectedAt %v not within [%v, %v]", ca, before, after)
		}
	}
}

// ---------------------------------------------------------------------------
// TestFlushByBatchSize
// ---------------------------------------------------------------------------

// TestFlushByBatchSize verifies that a flush is triggered exactly when the
// accumulated batch reaches StreamBatchSize (ticker disabled).
func TestFlushByBatchSize(t *testing.T) {
	t.Parallel()

	const batchSize = 3
	csvPath := writeCSV(t, csvWith([][3]string{
		{"UTIL", "GPU-A", "1.0"},
		{"UTIL", "GPU-B", "2.0"},
		{"UTIL", "GPU-C", "3.0"},
		{"UTIL", "GPU-D", "4.0"},
	}))

	pub := &mockPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	pub.onPublish = func() { cancel() } // cancel after first flush

	s := newTestStreamer(csvPath, batchSize, 100_000, pub, noopTick)
	s.Run(ctx) //nolint:errcheck

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.batches) == 0 {
		t.Fatal("expected at least one batch to be flushed")
	}
	if got := len(pub.batches[0].Messages); got != batchSize {
		t.Errorf("first batch: want %d messages, got %d", batchSize, got)
	}
}

// ---------------------------------------------------------------------------
// TestFlushByTicker
// ---------------------------------------------------------------------------

// TestFlushByTicker verifies that sending on the ticker channel flushes a
// sub-batchSize batch immediately.
func TestFlushByTicker(t *testing.T) {
	t.Parallel()

	csvPath := writeCSV(t, csvWith([][3]string{{"UTIL", "GPU-TICK", "9.0"}}))

	pub := &mockPublisher{}
	tickIn, tickFn := manualTick()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	// Very large batch size: only the ticker can trigger a flush.
	s := newTestStreamer(csvPath, 10_000, 100_000, pub, tickFn)
	go func() { done <- s.Run(ctx) }()

	// Let the streamer accumulate rows.
	time.Sleep(20 * time.Millisecond)

	// Fire the manual ticker.
	tickIn <- time.Now()
	time.Sleep(20 * time.Millisecond)

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if pub.totalMessages() == 0 {
		t.Error("expected messages flushed by ticker, got 0")
	}

	// Every ticker-triggered batch must be smaller than the batch size cap.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	for _, b := range pub.batches {
		if len(b.Messages) >= 10_000 {
			t.Errorf("batch size %d reached batch cap; expected ticker-driven flush to be smaller", len(b.Messages))
		}
	}
}

// ---------------------------------------------------------------------------
// TestRetryOn429
// ---------------------------------------------------------------------------

// TestRetryOn429 verifies that PublishBatch retries on HTTP 429 up to 3 times
// using the Retry-After header and succeeds when the server recovers.
func TestRetryOn429(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n <= 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewMQClient(srv.URL)
	c.sleepFn = func(_ context.Context, _ time.Duration) error { return nil }

	if err := c.PublishBatch(context.Background(), model.PublishBatch{}); err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if got := int(calls.Load()); got != 3 {
		t.Errorf("expected 3 total HTTP calls (1 + 2 retries), got %d", got)
	}
}

// TestRetryOn429Exhausted verifies that an error is returned once all 429
// retries are consumed.
func TestRetryOn429Exhausted(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewMQClient(srv.URL)
	c.sleepFn = func(_ context.Context, _ time.Duration) error { return nil }

	if err := c.PublishBatch(context.Background(), model.PublishBatch{}); err == nil {
		t.Error("expected error after exhausting 429 retries, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestRetryOn500
// ---------------------------------------------------------------------------

// TestRetryOn500 verifies that PublishBatch uses exponential backoff on 5xx
// responses and succeeds when the server recovers within max attempts.
func TestRetryOn500(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n <= 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewMQClient(srv.URL)
	c.sleepFn = func(_ context.Context, _ time.Duration) error { return nil }

	if err := c.PublishBatch(context.Background(), model.PublishBatch{}); err != nil {
		t.Fatalf("expected success after 5xx retries, got: %v", err)
	}
	if got := int(calls.Load()); got != 4 {
		t.Errorf("expected 4 total HTTP calls (1 + 3 retries), got %d", got)
	}
}

// TestRetryOn500Exhausted verifies that an error is returned when all 5xx
// attempts are consumed.
func TestRetryOn500Exhausted(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewMQClient(srv.URL)
	c.sleepFn = func(_ context.Context, _ time.Duration) error { return nil }

	if err := c.PublishBatch(context.Background(), model.PublishBatch{}); err == nil {
		t.Error("expected error after exhausting 5xx retries, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestMalformedRowsSkipped
// ---------------------------------------------------------------------------

// TestMalformedRowsSkipped verifies that rows with too few fields or a
// non-parseable metric value are skipped and the streamer continues publishing
// the good rows.
func TestMalformedRowsSkipped(t *testing.T) {
	t.Parallel()

	csvContent :=
		"f0,metric_name,f2,f3,gpu_uuid,f5,f6,f7,f8,f9,metric_value\n" +
			"x,UTIL,x,x,GPU-GOOD-1,x,x,x,x,x,75.0\n" + // valid
			"short,row\n" + // too few fields (< 11)
			"x,UTIL,x,x,GPU-GOOD-2,x,x,x,x,x,NOTANUM\n" + // non-numeric metric value
			"x,UTIL,x,x,GPU-GOOD-3,x,x,x,x,x,30.0\n" // valid

	csvPath := writeCSV(t, csvContent)

	pub := &mockPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	pub.onPublish = func() { cancel() }

	// batch size = 2: exactly the two good rows trigger a flush
	s := newTestStreamer(csvPath, 2, 100_000, pub, noopTick)
	s.Run(ctx) //nolint:errcheck

	msgs := pub.allMessages()
	if len(msgs) < 2 {
		t.Fatalf("expected ≥2 messages (good rows), got %d", len(msgs))
	}
	for _, m := range msgs {
		switch m.Key {
		case "GPU-GOOD-1", "GPU-GOOD-2", "GPU-GOOD-3":
			// expected
		default:
			t.Errorf("unexpected GPU UUID %q; malformed row must have been skipped", m.Key)
		}
	}
}

// ---------------------------------------------------------------------------
// TestGracefulShutdown
// ---------------------------------------------------------------------------

// TestGracefulShutdown verifies that cancelling the context causes Run to flush
// any buffered messages and return nil (not an error).
func TestGracefulShutdown(t *testing.T) {
	t.Parallel()

	csvPath := writeCSV(t, csvWith([][3]string{
		{"UTIL", "GPU-SHUT-A", "1.0"},
		{"UTIL", "GPU-SHUT-B", "2.0"},
	}))

	pub := &mockPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	// Large batch size and no ticker: messages only flush on shutdown.
	s := newTestStreamer(csvPath, 10_000, 100_000, pub, noopTick)
	go func() { done <- s.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run returned non-nil on graceful shutdown: %v", err)
	}
	if pub.totalMessages() == 0 {
		t.Error("expected buffered messages flushed on shutdown, got 0")
	}
}
