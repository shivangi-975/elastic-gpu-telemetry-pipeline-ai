package mq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// newTestServer creates a Server with a minimal config and wraps it in an httptest.Server.
func newTestServer(t *testing.T, cfg model.Config) *httptest.Server {
	t.Helper()
	if cfg.Partitions == 0 {
		cfg.Partitions = 4
	}
	if cfg.MaxQueueSize == 0 {
		cfg.MaxQueueSize = 16
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return httptest.NewServer(srv)
}

func postJSON(t *testing.T, ts *httptest.Server, path string, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := ts.Client().Post(ts.URL+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func getURL(t *testing.T, ts *httptest.Server, path string) *http.Response {
	t.Helper()
	resp, err := ts.Client().Get(ts.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func readBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

func mustPublish(t *testing.T, ts *httptest.Server, msgs []model.PublishMessage) {
	t.Helper()
	resp := postJSON(t, ts, "/publish", model.PublishBatch{Messages: msgs})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("publish: expected 200, got %d: %s", resp.StatusCode, body)
	}
}

// --------------------------------------------------------------------------
// POST /publish
// --------------------------------------------------------------------------

func TestHTTPPublish_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{})
	defer ts.Close()

	msgs := []model.PublishMessage{
		{Key: "gpu-0", Payload: model.TelemetryRecord{GPUUUID: "GPU-aabb0001", MetricName: "GPU_UTIL", MetricValue: 47.0}},
	}
	resp := postJSON(t, ts, "/publish", model.PublishBatch{Messages: msgs})
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", result["status"])
	}
}

func TestHTTPPublish_BadJSON(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{})
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/publish", "application/json",
		strings.NewReader("{not valid json}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHTTPPublish_PartitionFull(t *testing.T) {
	t.Parallel()
	// MaxQueueSize=2 so we can fill it quickly.
	ts := newTestServer(t, model.Config{Partitions: 1, MaxQueueSize: 2})
	defer ts.Close()

	// Fill to capacity with a fixed key (deterministic partition 0).
	mustPublish(t, ts, []model.PublishMessage{
		{Key: "k", Payload: model.TelemetryRecord{MetricName: "GPU_UTIL", MetricValue: 1}},
		{Key: "k", Payload: model.TelemetryRecord{MetricName: "GPU_UTIL", MetricValue: 2}},
	})

	// Next publish must be rejected with 429.
	resp := postJSON(t, ts, "/publish", model.PublishBatch{Messages: []model.PublishMessage{
		{Key: "k", Payload: model.TelemetryRecord{MetricName: "GPU_UTIL", MetricValue: 3}},
	}})
	readBody(t, resp) // consume body

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "1" {
		t.Errorf("expected Retry-After: 1, got %q", ra)
	}
}

// --------------------------------------------------------------------------
// GET /consume
// --------------------------------------------------------------------------

func TestHTTPConsume_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{Partitions: 1, MaxQueueSize: 100})
	defer ts.Close()

	mustPublish(t, ts, []model.PublishMessage{
		{Key: "k", Payload: model.TelemetryRecord{GPUUUID: "GPU-aabb0001", MetricName: "GPU_UTIL", MetricValue: 47.0}},
		{Key: "k", Payload: model.TelemetryRecord{GPUUUID: "GPU-aabb0001", MetricName: "GPU_UTIL", MetricValue: 82.0}},
	})

	resp := getURL(t, ts, "/consume?group=grp&consumer_id=c1&max=10")
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	var cr model.ConsumeResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(cr.Messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(cr.Messages))
	}
}

func TestHTTPConsume_DefaultMax(t *testing.T) {
	t.Parallel()
	// Publish 150 messages; without max param default=100 should cap response.
	ts := newTestServer(t, model.Config{Partitions: 1, MaxQueueSize: 200})
	defer ts.Close()

	msgs := make([]model.PublishMessage, 150)
	for i := range msgs {
		msgs[i] = model.PublishMessage{Key: "k", Payload: model.TelemetryRecord{MetricValue: float64(i)}}
	}
	mustPublish(t, ts, msgs)

	// No max param → default 100.
	resp := getURL(t, ts, "/consume?group=grp&consumer_id=c1")
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var cr model.ConsumeResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(cr.Messages) != 100 {
		t.Errorf("expected 100 messages (default max), got %d", len(cr.Messages))
	}
}

func TestHTTPConsume_EmptyQueue(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{Partitions: 2, MaxQueueSize: 100})
	defer ts.Close()

	resp := getURL(t, ts, "/consume?group=grp&consumer_id=c1")
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var cr model.ConsumeResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(cr.Messages) != 0 {
		t.Errorf("expected 0 messages on empty queue, got %d", len(cr.Messages))
	}
}

// --------------------------------------------------------------------------
// POST /ack
// --------------------------------------------------------------------------

func TestHTTPAck_OK(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{Partitions: 1, MaxQueueSize: 100})
	defer ts.Close()

	mustPublish(t, ts, []model.PublishMessage{
		{Key: "k", Payload: model.TelemetryRecord{MetricValue: 1}},
	})

	// Consume to get the offset.
	resp := getURL(t, ts, "/consume?group=grp&consumer_id=c1")
	var cr model.ConsumeResponse
	if err := json.Unmarshal(readBody(t, resp), &cr); err != nil {
		t.Fatalf("unmarshal consume: %v", err)
	}

	// Ack.
	ackResp := postJSON(t, ts, "/ack", model.AckRequest{
		ConsumerGroup: "grp",
		Partition:     cr.Partition,
		Offset:        cr.Offset,
	})
	body := readBody(t, ackResp)

	if ackResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", ackResp.StatusCode, body)
	}

	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal ack response: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", result["status"])
	}
}

func TestHTTPAck_BadJSON(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{})
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/ack", "application/json",
		strings.NewReader("not json"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHTTPAck_UnknownGroup(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{})
	defer ts.Close()

	// Acking a group that was never registered must return 500.
	resp := postJSON(t, ts, "/ack", model.AckRequest{
		ConsumerGroup: "nonexistent-group",
		Partition:     0,
		Offset:        1,
	})
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", resp.StatusCode, body)
	}
}

// --------------------------------------------------------------------------
// GET /health
// --------------------------------------------------------------------------

func TestHTTPHealth(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{})
	defer ts.Close()

	resp := getURL(t, ts, "/health")
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", result["status"])
	}
}

// --------------------------------------------------------------------------
// GET /metrics
// --------------------------------------------------------------------------

func TestHTTPMetrics(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{Partitions: 2, MaxQueueSize: 100})
	defer ts.Close()

	// Find keys that hash to partition 0 and 1 respectively.
	router := PartitionRouter{NumPartitions: 2}
	keys := [2]string{}
	found := [2]bool{}
	for i := 0; !(found[0] && found[1]); i++ {
		k := fmt.Sprintf("%d", i)
		p := router.Route(k)
		if !found[p] {
			keys[p] = k
			found[p] = true
		}
	}

	// Publish 3 messages to partition 0, 2 to partition 1.
	p0Msgs := make([]model.PublishMessage, 3)
	for i := range p0Msgs {
		p0Msgs[i] = model.PublishMessage{Key: keys[0], Payload: model.TelemetryRecord{MetricValue: float64(i)}}
	}
	p1Msgs := make([]model.PublishMessage, 2)
	for i := range p1Msgs {
		p1Msgs[i] = model.PublishMessage{Key: keys[1], Payload: model.TelemetryRecord{MetricValue: float64(i)}}
	}
	mustPublish(t, ts, append(p0Msgs, p1Msgs...))

	resp := getURL(t, ts, "/metrics")
	body := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var m model.BrokerMetrics
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(m.PartitionLengths) != 2 {
		t.Fatalf("expected 2 partition lengths, got %d", len(m.PartitionLengths))
	}
	if m.PartitionLengths[0] != 3 {
		t.Errorf("partition 0: expected 3, got %d", m.PartitionLengths[0])
	}
	if m.PartitionLengths[1] != 2 {
		t.Errorf("partition 1: expected 2, got %d", m.PartitionLengths[1])
	}
}

// --------------------------------------------------------------------------
// Response Content-Type
// --------------------------------------------------------------------------

func TestHTTPContentType(t *testing.T) {
	t.Parallel()
	ts := newTestServer(t, model.Config{})
	defer ts.Close()

	endpoints := []struct {
		method string
		path   string
		body   any
	}{
		{"GET", "/health", nil},
		{"GET", "/metrics", nil},
		{"GET", "/consume?group=g&consumer_id=c", nil},
	}
	for _, ep := range endpoints {
		var resp *http.Response
		if ep.body != nil {
			resp = postJSON(t, ts, ep.path, ep.body)
		} else {
			resp = getURL(t, ts, ep.path)
		}
		resp.Body.Close()
		ct := resp.Header.Get("Content-Type")
		if !strings.HasPrefix(ct, "application/json") {
			t.Errorf("%s %s: Content-Type = %q, want application/json", ep.method, ep.path, ct)
		}
	}
}
