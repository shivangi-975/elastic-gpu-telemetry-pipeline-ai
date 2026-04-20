package mq

import (
	"sync"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// partition holds the in-memory state for a single broker partition.
//
// Offsets are absolute and ever-increasing:
//   - baseOffset is the absolute offset of messages[0].
//   - nextOffset is the absolute offset that will be assigned to the next published message.
//
// When messages are trimmed after an Ack (compaction), baseOffset advances by
// the number of trimmed messages so that consumer committedOffsets remain valid.
type partition struct {
	mu         sync.Mutex
	messages   []model.PublishMessage
	baseOffset int64 // absolute offset of messages[0]; increases on compaction
	nextOffset int64 // absolute offset of the next message to publish
}

// consumerGroup tracks partition assignments and committed offsets for a named group.
type consumerGroup struct {
	mu                 sync.Mutex
	consumers          []string         // insertion-ordered consumer IDs for deterministic round-robin
	assignedPartitions map[string][]int // consumerID -> assigned partition indices
	committedOffsets   map[int]int64    // partitionIdx -> absolute offset of next message to read
}

// rebalance redistributes partitions across all registered consumers using round-robin.
// Must be called with g.mu held.
func (g *consumerGroup) rebalance(numPartitions int) {
	for k := range g.assignedPartitions {
		g.assignedPartitions[k] = nil
	}
	for p := 0; p < numPartitions; p++ {
		idx := p % len(g.consumers)
		c := g.consumers[idx]
		g.assignedPartitions[c] = append(g.assignedPartitions[c], p)
	}
}
