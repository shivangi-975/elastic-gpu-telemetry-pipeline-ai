package mq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// Broker is the in-memory message queue broker.
// It manages partitioned message storage, consumer group tracking, and optional WAL persistence.
type Broker struct {
	cfg        model.Config
	router     *PartitionRouter
	partitions []*partition

	groupsMu sync.RWMutex
	groups   map[string]*consumerGroup

	walMu   sync.Mutex
	walFile *os.File // nil when WAL is disabled
}

// NewBroker creates a new Broker with the given config.
// Applies defaults for zero values, replays WAL if configured.
func NewBroker(cfg model.Config) (*Broker, error) {
	if cfg.Partitions == 0 {
		cfg.Partitions = 8
	}
	if cfg.MaxQueueSize == 0 {
		cfg.MaxQueueSize = 4096
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":9000"
	}

	partitions := make([]*partition, cfg.Partitions)
	for i := range partitions {
		partitions[i] = &partition{}
	}

	b := &Broker{
		cfg:        cfg,
		router:     &PartitionRouter{NumPartitions: cfg.Partitions},
		partitions: partitions,
		groups:     make(map[string]*consumerGroup),
	}

	if cfg.WALPath != "" {
		if err := b.replayWAL(); err != nil {
			return nil, fmt.Errorf("mq: replay wal: %w", err)
		}
		f, err := os.OpenFile(cfg.WALPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("mq: open wal: %w", err)
		}
		b.walFile = f
	}

	return b, nil
}

// replayWAL reads the WAL file line by line and re-publishes each message.
// If the file does not exist, returns nil silently.
func (b *Broker) replayWAL() error {
	f, err := os.Open(b.cfg.WALPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg model.PublishMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		if err := b.publishOne(msg, false); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
	}
	return scanner.Err()
}

// Publish routes each message in the batch to its partition and stores it.
// Returns an error (HTTP layer should return 429) if any target partition is full.
func (b *Broker) Publish(batch model.PublishBatch) error {
	for _, msg := range batch.Messages {
		if err := b.publishOne(msg, true); err != nil {
			return err
		}
	}
	return nil
}

// publishOne appends a single message to its routed partition.
// writeWAL controls whether to persist to the WAL file (false during replay).
func (b *Broker) publishOne(msg model.PublishMessage, writeWAL bool) error {
	partIdx := b.router.Route(msg.Key)
	p := b.partitions[partIdx]

	p.mu.Lock()
	if len(p.messages) >= b.cfg.MaxQueueSize {
		p.mu.Unlock()
		return fmt.Errorf("mq: partition %d is full", partIdx)
	}
	p.messages = append(p.messages, msg)
	p.nextOffset++
	p.mu.Unlock()

	if writeWAL && b.walFile != nil {
		if err := b.appendWAL(msg); err != nil {
			return fmt.Errorf("mq: %w", err)
		}
	}
	return nil
}

// appendWAL serialises msg as a JSON line and appends it to the WAL file.
func (b *Broker) appendWAL(msg model.PublishMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	data = append(data, '\n')

	b.walMu.Lock()
	defer b.walMu.Unlock()
	_, err = b.walFile.Write(data)
	return err
}

// Consume returns up to maxMessages messages for the given consumer within its group.
// Auto-creates the group and registers the consumer on first call; triggers partition
// rebalance when a new consumer joins.
//
// Offsets are absolute: ConsumeResponse.Offset is the absolute offset of the next
// unread message, and should be passed verbatim to Ack.
func (b *Broker) Consume(group, consumerID string, maxMessages int) (model.ConsumeResponse, error) {
	b.groupsMu.Lock()
	g, ok := b.groups[group]
	if !ok {
		g = &consumerGroup{
			consumers:          []string{},
			assignedPartitions: make(map[string][]int),
			committedOffsets:   make(map[int]int64),
		}
		b.groups[group] = g
	}
	b.groupsMu.Unlock()

	g.mu.Lock()
	found := false
	for _, c := range g.consumers {
		if c == consumerID {
			found = true
			break
		}
	}
	if !found {
		g.consumers = append(g.consumers, consumerID)
		g.rebalance(b.cfg.Partitions)
	}

	assigned := make([]int, len(g.assignedPartitions[consumerID]))
	copy(assigned, g.assignedPartitions[consumerID])

	offsets := make(map[int]int64, len(assigned))
	for _, partIdx := range assigned {
		offsets[partIdx] = g.committedOffsets[partIdx]
	}
	g.mu.Unlock()

	for _, partIdx := range assigned {
		absOffset := offsets[partIdx] // absolute committed offset for this partition
		p := b.partitions[partIdx]

		p.mu.Lock()

		// If compaction trimmed past the consumer's position, fast-forward.
		if absOffset < p.baseOffset {
			absOffset = p.baseOffset
		}
		// No new messages in this partition.
		if absOffset >= p.nextOffset {
			p.mu.Unlock()
			continue
		}

		// Convert absolute offset to index within the (possibly trimmed) messages slice.
		start := int(absOffset - p.baseOffset)
		end := start + maxMessages
		if end > len(p.messages) {
			end = len(p.messages)
		}

		msgs := make([]model.PublishMessage, end-start)
		copy(msgs, p.messages[start:end])
		p.mu.Unlock()

		return model.ConsumeResponse{
			Partition: partIdx,
			Offset:    absOffset + int64(end-start), // absolute offset of next unread message
			Messages:  msgs,
		}, nil
	}

	return model.ConsumeResponse{}, nil
}

// Ack stores the committed offset for the given consumer group and partition,
// then compacts the partition by trimming messages that all groups have consumed.
//
// req.Offset must be the absolute offset returned by a prior Consume call.
func (b *Broker) Ack(req model.AckRequest) error {
	b.groupsMu.RLock()
	g, ok := b.groups[req.ConsumerGroup]
	b.groupsMu.RUnlock()

	if !ok {
		return fmt.Errorf("mq: unknown consumer group %q", req.ConsumerGroup)
	}

	g.mu.Lock()
	g.committedOffsets[req.Partition] = req.Offset
	g.mu.Unlock()

	// Compact: remove messages all known groups have already consumed.
	b.compactPartition(req.Partition)
	return nil
}

// compactPartition trims messages from the front of the partition that every
// registered consumer group has already acknowledged (committed past).
//
// The absolute baseOffset of the partition is advanced by the number of trimmed
// messages so that all existing absolute offsets remain valid.
func (b *Broker) compactPartition(partIdx int) {
	// Snapshot the committed offset for this partition from every group.
	b.groupsMu.RLock()
	groups := make([]*consumerGroup, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
	}
	b.groupsMu.RUnlock()

	if len(groups) == 0 {
		return
	}

	// Find the minimum committed offset — we can only trim up to that point.
	var minOffset int64 = -1
	for _, g := range groups {
		g.mu.Lock()
		off := g.committedOffsets[partIdx]
		g.mu.Unlock()
		if minOffset < 0 || off < minOffset {
			minOffset = off
		}
	}
	if minOffset <= 0 {
		return
	}

	p := b.partitions[partIdx]
	p.mu.Lock()
	defer p.mu.Unlock()

	trimCount := int(minOffset - p.baseOffset)
	if trimCount <= 0 || trimCount > len(p.messages) {
		return
	}

	// Trim and advance the base pointer.
	p.messages = p.messages[trimCount:]
	p.baseOffset += int64(trimCount)
}

// Metrics returns a snapshot of pending message counts and committed offsets.
// PartitionLengths reflects only unconsumed (pending) messages after compaction.
func (b *Broker) Metrics() model.BrokerMetrics {
	lengths := make([]int, len(b.partitions))
	for i, p := range b.partitions {
		p.mu.Lock()
		lengths[i] = len(p.messages)
		p.mu.Unlock()
	}

	b.groupsMu.RLock()
	offsets := make(map[string]int64)
	for groupName, g := range b.groups {
		g.mu.Lock()
		for partIdx, off := range g.committedOffsets {
			key := fmt.Sprintf("%s:%d", groupName, partIdx)
			offsets[key] = off
		}
		g.mu.Unlock()
	}
	b.groupsMu.RUnlock()

	return model.BrokerMetrics{
		PartitionLengths: lengths,
		ConsumerOffsets:  offsets,
	}
}

// Health returns nil; the broker is always healthy after successful construction.
func (b *Broker) Health() error {
	return nil
}
