package mq

import (
	"fmt"
	"testing"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// TestCompactionAllowsRepublish fills a partition to capacity, acks all messages,
// and verifies that new messages can be published — proving compaction frees space.
func TestCompactionAllowsRepublish(t *testing.T) {
	t.Parallel()

	const max = 4
	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: max})
	msgs := makeMsgs("fill-key", max)

	// Fill to capacity.
	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("fill: %v", err)
	}

	// Consume to get the offset.
	resp, err := b.Consume("grp", "c1", max)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(resp.Messages) != max {
		t.Fatalf("expected %d messages, got %d", max, len(resp.Messages))
	}

	// Ack all messages.
	if err := b.Ack(model.AckRequest{
		ConsumerGroup: "grp",
		Partition:     resp.Partition,
		Offset:        resp.Offset,
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Partition should be compacted now — publishing new messages must succeed.
	newMsgs := makeMsgs("fill-key", max)
	if err := b.Publish(model.PublishBatch{Messages: newMsgs}); err != nil {
		t.Errorf("expected publish to succeed after compaction, got: %v", err)
	}
}

// TestAckUnknownGroup verifies that acking a consumer group that was never
// registered returns an error.
func TestAckUnknownGroup(t *testing.T) {
	t.Parallel()

	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: 10})

	err := b.Ack(model.AckRequest{
		ConsumerGroup: "no-such-group",
		Partition:     0,
		Offset:        1,
	})
	if err == nil {
		t.Error("expected error when acking unknown consumer group, got nil")
	}
}

// TestConsumeMaxMessages verifies that the max parameter caps the number of
// returned messages per Consume call.
func TestConsumeMaxMessages(t *testing.T) {
	t.Parallel()

	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: 100})
	msgs := makeMsgs("max-key", 10)

	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Request only 3 of 10 available.
	resp, err := b.Consume("grp", "c1", 3)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Errorf("expected 3 messages (max=3), got %d", len(resp.Messages))
	}

	// The offset must point to the 4th message (absolute offset 3).
	if resp.Offset != 3 {
		t.Errorf("expected next offset 3, got %d", resp.Offset)
	}
}

// TestMultipleGroupsNoCompactionUntilBothAck verifies that messages are not
// trimmed from a partition until every registered consumer group has acked past them.
func TestMultipleGroupsNoCompactionUntilBothAck(t *testing.T) {
	t.Parallel()

	const msgCount = 4
	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: msgCount + 4})
	msgs := makeMsgs("shared-key", msgCount)

	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Register group-a and group-b by consuming.
	respA, err := b.Consume("group-a", "a1", 100)
	if err != nil {
		t.Fatalf("consume group-a: %v", err)
	}
	respB, err := b.Consume("group-b", "b1", 100)
	if err != nil {
		t.Fatalf("consume group-b: %v", err)
	}

	// group-a acks — but group-b hasn't acked yet; messages must NOT be trimmed.
	if err := b.Ack(model.AckRequest{
		ConsumerGroup: "group-a",
		Partition:     respA.Partition,
		Offset:        respA.Offset,
	}); err != nil {
		t.Fatalf("ack group-a: %v", err)
	}

	// Verify messages are still present in partition (not compacted).
	p := b.partitions[respA.Partition]
	p.mu.Lock()
	pendingAfterGroupAAck := len(p.messages)
	p.mu.Unlock()

	if pendingAfterGroupAAck == 0 {
		t.Error("partition was compacted after only one group acked; expected messages to remain for group-b")
	}

	// group-b acks — now both groups have consumed; compaction should trigger.
	if err := b.Ack(model.AckRequest{
		ConsumerGroup: "group-b",
		Partition:     respB.Partition,
		Offset:        respB.Offset,
	}); err != nil {
		t.Fatalf("ack group-b: %v", err)
	}

	p.mu.Lock()
	pendingAfterBothAck := len(p.messages)
	p.mu.Unlock()

	if pendingAfterBothAck != 0 {
		t.Errorf("expected partition to be empty after both groups acked, got %d messages", pendingAfterBothAck)
	}
}

// TestConsumeResumesFromAckedOffset verifies that consecutive consume+ack cycles
// deliver all messages exactly once with no gaps or re-deliveries.
func TestConsumeResumesFromAckedOffset(t *testing.T) {
	t.Parallel()

	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: 100})

	// Publish 6 messages; consume 2 at a time.
	msgs := makeMsgs("resume-key", 6)
	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	seen := make([]string, 0, 6)
	for i := 0; i < 3; i++ { // 3 rounds of consume-ack
		resp, err := b.Consume("grp", "c1", 2)
		if err != nil {
			t.Fatalf("round %d consume: %v", i, err)
		}
		if len(resp.Messages) != 2 {
			t.Fatalf("round %d: expected 2 messages, got %d", i, len(resp.Messages))
		}
		for _, m := range resp.Messages {
			seen = append(seen, m.Payload.GPUUUID)
		}
		if err := b.Ack(model.AckRequest{
			ConsumerGroup: "grp",
			Partition:     resp.Partition,
			Offset:        resp.Offset,
		}); err != nil {
			t.Fatalf("round %d ack: %v", i, err)
		}
	}

	// All 6 messages must have been seen exactly once.
	if len(seen) != 6 {
		t.Fatalf("expected 6 unique deliveries, got %d", len(seen))
	}
	for i, uuid := range seen {
		want := fmt.Sprintf("resume-key-gpu-%d", i)
		if uuid != want {
			t.Errorf("delivery[%d]: expected %q, got %q", i, want, uuid)
		}
	}
}

// TestBaseOffsetAdvancesAfterCompaction verifies that baseOffset on the partition
// is advanced correctly after compaction.
func TestBaseOffsetAdvancesAfterCompaction(t *testing.T) {
	t.Parallel()

	const msgCount = 5
	b := newBroker(t, model.Config{Partitions: 1, MaxQueueSize: 100})

	msgs := makeMsgs("base-key", msgCount)
	if err := b.Publish(model.PublishBatch{Messages: msgs}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	resp, err := b.Consume("grp", "c1", msgCount)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Before ack, baseOffset should still be 0.
	p := b.partitions[resp.Partition]
	p.mu.Lock()
	baseBeforeAck := p.baseOffset
	p.mu.Unlock()
	if baseBeforeAck != 0 {
		t.Errorf("expected baseOffset=0 before ack, got %d", baseBeforeAck)
	}

	if err := b.Ack(model.AckRequest{
		ConsumerGroup: "grp",
		Partition:     resp.Partition,
		Offset:        resp.Offset,
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// After ack, baseOffset must be msgCount (all messages trimmed).
	p.mu.Lock()
	baseAfterAck := p.baseOffset
	p.mu.Unlock()
	if baseAfterAck != int64(msgCount) {
		t.Errorf("expected baseOffset=%d after ack, got %d", msgCount, baseAfterAck)
	}
}
