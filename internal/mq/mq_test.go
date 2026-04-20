package mq

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// newBroker is a test helper that creates a Broker and fatals on error.
func newBroker(t *testing.T, cfg model.Config) *Broker {
	t.Helper()
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	return b
}

// makeMsgs builds a slice of PublishMessages all sharing the same key.
func makeMsgs(key string, n int) []model.PublishMessage {
	msgs := make([]model.PublishMessage, n)
	for i := range msgs {
		msgs[i] = model.PublishMessage{
			Key:     key,
			Payload: model.TelemetryRecord{GPUUUID: fmt.Sprintf("%s-gpu-%d", key, i)},
		}
	}
	return msgs
}

// TestPublishAndConsume publishes a batch of 5 messages and verifies all are returned.
func TestPublishAndConsume(t *testing.T) {
	t.Parallel()

	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: 100})

	msgs := makeMsgs("key", 5)
	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	resp, err := b.Consume("grp", "c1", 100)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(resp.Messages) != 5 {
		t.Errorf("expected 5 messages, got %d", len(resp.Messages))
	}
}

// TestPartitionRouting verifies deterministic routing: same key → same partition,
// and that distinct keys can land on different partitions.
func TestPartitionRouting(t *testing.T) {
	t.Parallel()

	r := PartitionRouter{NumPartitions: 8}

	tests := []struct {
		key string
	}{
		{"gpu-0"}, {"gpu-1"}, {"gpu-2"}, {"host-A"}, {"host-B"},
	}

	for _, tc := range tests {
		got1 := r.Route(tc.key)
		got2 := r.Route(tc.key)
		if got1 != got2 {
			t.Errorf("key %q: non-deterministic: %d != %d", tc.key, got1, got2)
		}
		if got1 < 0 || got1 >= r.NumPartitions {
			t.Errorf("key %q: partition %d out of range [0,%d)", tc.key, got1, r.NumPartitions)
		}
	}

	// Verify that at least two distinct keys map to different partitions.
	seen := make(map[int]bool)
	for i := 0; i < 64; i++ {
		k := fmt.Sprintf("key-%d", i)
		seen[r.Route(k)] = true
	}
	if len(seen) < 2 {
		t.Error("expected keys to distribute across at least 2 partitions")
	}
}

// TestPartitionOrdering publishes 10 messages with the same key (same partition)
// and verifies consume returns them in publish order.
func TestPartitionOrdering(t *testing.T) {
	t.Parallel()

	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: 100})
	msgs := makeMsgs("ordered-key", 10)

	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	resp, err := b.Consume("grp", "c1", 100)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(resp.Messages) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(resp.Messages))
	}
	for i, m := range resp.Messages {
		if m.Payload.GPUUUID != msgs[i].Payload.GPUUUID {
			t.Errorf("msg[%d]: expected GPUUUID %q, got %q", i, msgs[i].Payload.GPUUUID, m.Payload.GPUUUID)
		}
	}
}

// TestConsumerGroupDistribution verifies that two consumers in the same group
// are assigned different partitions and receive different messages.
func TestConsumerGroupDistribution(t *testing.T) {
	t.Parallel()

	// Use 2 partitions so the math is clear: c1→[0], c2→[1].
	cfg := model.Config{Partitions: 2, MaxQueueSize: 100}
	b := newBroker(t, cfg)

	router := PartitionRouter{NumPartitions: 2}

	// Find one key that hashes to partition 0 and one to partition 1.
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

	if err := b.Publish(model.PublishBatch{Messages: []model.PublishMessage{
		{Key: keys[0], Payload: model.TelemetryRecord{GPUUUID: "gpu-p0"}},
		{Key: keys[1], Payload: model.TelemetryRecord{GPUUUID: "gpu-p1"}},
	}}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// c1 joins first → assigned [0,1], reads from partition 0.
	resp1, err := b.Consume("grp", "c1", 10)
	if err != nil {
		t.Fatalf("Consume c1: %v", err)
	}

	// c2 joins → rebalance: c1=[0], c2=[1]; c2 reads from partition 1.
	resp2, err := b.Consume("grp", "c2", 10)
	if err != nil {
		t.Fatalf("Consume c2: %v", err)
	}

	if len(resp1.Messages) == 0 {
		t.Error("c1: expected at least 1 message")
	}
	if len(resp2.Messages) == 0 {
		t.Error("c2: expected at least 1 message")
	}
	if resp1.Partition == resp2.Partition {
		t.Errorf("expected different partitions, both consumers got partition %d", resp1.Partition)
	}
}

// TestAckAdvancesOffset verifies that acking an offset prevents re-delivery of
// already-consumed messages and allows consumption of newer messages.
func TestAckAdvancesOffset(t *testing.T) {
	t.Parallel()

	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: 100})
	msgs := makeMsgs("k", 5)

	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	resp, err := b.Consume("grp", "c1", 100)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(resp.Messages) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(resp.Messages))
	}

	// Ack the offset returned by Consume (= next read position).
	if err := b.Ack(model.AckRequest{
		ConsumerGroup: "grp",
		Partition:     resp.Partition,
		Offset:        resp.Offset,
	}); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Re-consume: no new messages → empty response.
	resp2, err := b.Consume("grp", "c1", 100)
	if err != nil {
		t.Fatalf("Consume after ack: %v", err)
	}
	if len(resp2.Messages) != 0 {
		t.Errorf("expected 0 messages after ack, got %d", len(resp2.Messages))
	}

	// Publish 3 more messages.
	more := makeMsgs("k", 3)
	if err := b.Publish(model.PublishBatch{Messages: more}); err != nil {
		t.Fatalf("Publish more: %v", err)
	}

	// Now only the 3 new messages should be returned.
	resp3, err := b.Consume("grp", "c1", 100)
	if err != nil {
		t.Fatalf("Consume new messages: %v", err)
	}
	if len(resp3.Messages) != 3 {
		t.Errorf("expected 3 new messages, got %d", len(resp3.Messages))
	}
}

// TestBackpressure fills a partition to MaxQueueSize and verifies the next publish errors.
func TestBackpressure(t *testing.T) {
	t.Parallel()

	const max = 4
	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: max})
	msgs := makeMsgs("bp-key", max)

	// Fill to capacity.
	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("fill: %v", err)
	}

	// One more message must be rejected.
	overflow := model.PublishBatch{Messages: makeMsgs("bp-key", 1)}
	if err := b.Publish(overflow); err == nil {
		t.Error("expected error when partition is full, got nil")
	}
}

// TestWALReplay publishes 5 messages with WAL enabled, then creates a new Broker
// from the same WAL path and verifies all messages are present.
func TestWALReplay(t *testing.T) {
	t.Parallel()

	walPath := filepath.Join(t.TempDir(), "wal.jsonl")
	cfg := model.Config{Partitions: 1, MaxQueueSize: 100, WALPath: walPath}

	b1 := newBroker(t, cfg)
	msgs := makeMsgs("wal-key", 5)
	if err := b1.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// New broker replays the WAL.
	b2 := newBroker(t, cfg)
	resp, err := b2.Consume("grp", "c1", 100)
	if err != nil {
		t.Fatalf("Consume after replay: %v", err)
	}
	if len(resp.Messages) != 5 {
		t.Errorf("expected 5 messages after WAL replay, got %d", len(resp.Messages))
	}
}

// TestEmptyConsume verifies that consuming from an empty broker returns an empty
// response with no error.
func TestEmptyConsume(t *testing.T) {
	t.Parallel()

	b := newBroker(t, model.Config{Partitions: 4, MaxQueueSize: 100})

	resp, err := b.Consume("grp", "c1", 100)
	if err != nil {
		t.Fatalf("Consume on empty broker: %v", err)
	}
	if len(resp.Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(resp.Messages))
	}
}

// TestMetrics verifies that BrokerMetrics reflects the correct per-partition message counts.
func TestMetrics(t *testing.T) {
	t.Parallel()

	cfg := model.Config{Partitions: 2, MaxQueueSize: 100}
	b := newBroker(t, cfg)

	router := PartitionRouter{NumPartitions: 2}

	// Find one key per partition.
	keys := [2]string{}
	found := [2]bool{}
	for i := 0; !(found[0] && found[1]); i++ {
		k := fmt.Sprintf("m-%d", i)
		p := router.Route(k)
		if !found[p] {
			keys[p] = k
			found[p] = true
		}
	}

	// Publish 2 messages to partition 0 and 3 to partition 1.
	p0Msgs := makeMsgs(keys[0], 2)
	p1Msgs := makeMsgs(keys[1], 3)
	batch := append(p0Msgs, p1Msgs...)
	if err := b.Publish(model.PublishBatch{Messages: batch}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	m := b.Metrics()

	if len(m.PartitionLengths) != 2 {
		t.Fatalf("expected 2 partition lengths, got %d", len(m.PartitionLengths))
	}
	if m.PartitionLengths[0] != 2 {
		t.Errorf("partition 0: expected 2 messages, got %d", m.PartitionLengths[0])
	}
	if m.PartitionLengths[1] != 3 {
		t.Errorf("partition 1: expected 3 messages, got %d", m.PartitionLengths[1])
	}
}
