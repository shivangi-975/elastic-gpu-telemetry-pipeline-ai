package mq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/metrics"
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

	done chan struct{} // closed by Stop to terminate background goroutines
}

// NewBroker creates a new Broker with the given config.
// Applies defaults for zero values, replays WAL if configured.
func NewBroker(cfg model.Config) (*Broker, error) {
	if cfg.Partitions == 0 {
		cfg.Partitions = 16
	}
	if cfg.MaxQueueSize == 0 {
		cfg.MaxQueueSize = 4096
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":9000"
	}
	if cfg.EvictInterval == 0 {
		cfg.EvictInterval = 10 * time.Second
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
		done:       make(chan struct{}),
	}

	if cfg.WALPath != "" {
		// Restore committed offsets before replaying messages so consumers
		// resume from where they left off rather than re-consuming from 0.
		if err := b.loadOffsetsCheckpoint(); err != nil {
			return nil, fmt.Errorf("mq: load offsets checkpoint: %w", err)
		}
		if err := b.replayWAL(); err != nil {
			return nil, fmt.Errorf("mq: replay wal: %w", err)
		}
		f, err := os.OpenFile(cfg.WALPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("mq: open wal: %w", err)
		}
		b.walFile = f
	}

	// Start background eviction loop when a TTL is configured.
	if cfg.ConsumerTTL > 0 {
		go b.evictionLoop()
	}

	return b, nil
}

// Stop shuts down background goroutines and closes the WAL file.
// Must be called once when the broker is no longer needed.
func (b *Broker) Stop() {
	close(b.done)
	b.walMu.Lock()
	defer b.walMu.Unlock()
	if b.walFile != nil {
		b.walFile.Close()
		b.walFile = nil
	}
}

// evictionLoop runs on cfg.EvictInterval and removes consumers whose last
// Consume call is older than cfg.ConsumerTTL, then triggers a rebalance.
func (b *Broker) evictionLoop() {
	ticker := time.NewTicker(b.cfg.EvictInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.evictStaleConsumers()
		case <-b.done:
			return
		}
	}
}

// evictStaleConsumers removes consumers that have not called Consume within
// cfg.ConsumerTTL. Their partitions are immediately rebalanced to the
// remaining live consumers, preventing those partitions from staying frozen
// after an ungraceful pod crash.
func (b *Broker) evictStaleConsumers() {
	cutoff := time.Now().Add(-b.cfg.ConsumerTTL)

	b.groupsMu.RLock()
	groups := make(map[string]*consumerGroup, len(b.groups))
	for name, g := range b.groups {
		groups[name] = g
	}
	b.groupsMu.RUnlock()

	for groupName, g := range groups {
		g.mu.Lock()
		var evicted []string
		for id, seen := range g.lastSeen {
			if seen.Before(cutoff) {
				evicted = append(evicted, id)
			}
		}
		if len(evicted) > 0 {
			for _, id := range evicted {
				delete(g.lastSeen, id)
				delete(g.assignedPartitions, id)
				updated := g.consumers[:0]
				for _, c := range g.consumers {
					if c != id {
						updated = append(updated, c)
					}
				}
				g.consumers = updated
				slog.Warn("evicted stale consumer", "component", "mq",
					"group", groupName, "consumer", id, "ttl", b.cfg.ConsumerTTL)
			}
			if len(g.consumers) > 0 {
				g.rebalance(b.cfg.Partitions)
			}
		}
		g.mu.Unlock()
	}
}

// offsetsCheckpointPath derives the offsets checkpoint file path from the WAL path.
func offsetsCheckpointPath(walPath string) string {
	return walPath + ".offsets"
}

// loadOffsetsCheckpoint restores committed consumer group offsets from the
// checkpoint file written by saveOffsetsCheckpoint.
// The checkpoint is a flat JSON object: {"group:partitionIdx": absoluteOffset, ...}
// If the file does not exist, returns nil silently (first-time startup).
func (b *Broker) loadOffsetsCheckpoint() error {
	path := offsetsCheckpointPath(b.cfg.WALPath)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	var snapshot map[string]int64
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	for key, offset := range snapshot {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		groupName := parts[0]
		partIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		b.groupsMu.Lock()
		g, ok := b.groups[groupName]
		if !ok {
			g = &consumerGroup{
				consumers:          []string{},
				assignedPartitions: make(map[string][]int),
				committedOffsets:   make(map[int]int64),
				lastSeen:           make(map[string]time.Time),
			}
			b.groups[groupName] = g
		}
		b.groupsMu.Unlock()

		g.mu.Lock()
		g.committedOffsets[partIdx] = offset
		g.mu.Unlock()
	}

	slog.Info("restored consumer offsets from checkpoint", "component", "mq",
		"entries", len(snapshot))
	return nil
}

// saveOffsetsCheckpoint atomically writes all committed consumer group offsets
// to disk so they survive an MQ server restart.
// Uses write-then-rename for atomicity — readers never see a partial file.
func (b *Broker) saveOffsetsCheckpoint() error {
	if b.cfg.WALPath == "" {
		return nil
	}

	snapshot := make(map[string]int64)
	b.groupsMu.RLock()
	for groupName, g := range b.groups {
		g.mu.Lock()
		for partIdx, offset := range g.committedOffsets {
			snapshot[fmt.Sprintf("%s:%d", groupName, partIdx)] = offset
		}
		g.mu.Unlock()
	}
	b.groupsMu.RUnlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	path := offsetsCheckpointPath(b.cfg.WALPath)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	return os.Rename(tmp, path)
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
		metrics.MQBackpressureTotal.Inc()
		return fmt.Errorf("mq: partition %d is full", partIdx)
	}
	p.messages = append(p.messages, msg)
	p.nextOffset++
	p.mu.Unlock()
	if writeWAL {
		metrics.MQPublishedTotal.Inc()
	}

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
			lastSeen:           make(map[string]time.Time),
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
	// Refresh heartbeat so the eviction loop knows this consumer is alive.
	if g.lastSeen == nil {
		g.lastSeen = make(map[string]time.Time)
	}
	g.lastSeen[consumerID] = time.Now()

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

		metrics.MQConsumedTotal.WithLabelValues(group).Add(float64(len(msgs)))
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

	metrics.MQAckedTotal.WithLabelValues(req.ConsumerGroup).Inc()

	// Compact: remove messages all known groups have already consumed.
	b.compactPartition(req.Partition)

	// Persist committed offsets so the MQ can survive a restart without
	// forcing collectors to re-consume from offset 0.
	if err := b.saveOffsetsCheckpoint(); err != nil {
		slog.Warn("failed to save offsets checkpoint", "component", "mq", "error", err)
	}
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

// Leave removes a consumer from its group and triggers a partition rebalance.
// This is the counterpart to the implicit registration that happens on first Consume.
// Collectors should call Leave during graceful shutdown so that their assigned
// partitions are redistributed to remaining consumers immediately, rather than
// sitting frozen until the next consumer join event.
//
// Returns nil if the group or consumer is not found (idempotent).
func (b *Broker) Leave(group, consumerID string) error {
	b.groupsMu.RLock()
	g, ok := b.groups[group]
	b.groupsMu.RUnlock()
	if !ok {
		return nil // nothing to do
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	newConsumers := g.consumers[:0]
	found := false
	for _, c := range g.consumers {
		if c == consumerID {
			found = true
			continue
		}
		newConsumers = append(newConsumers, c)
	}
	if !found {
		return nil // idempotent
	}
	g.consumers = newConsumers
	delete(g.assignedPartitions, consumerID)
	delete(g.lastSeen, consumerID)

	if len(g.consumers) > 0 {
		g.rebalance(b.cfg.Partitions)
	}
	return nil
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

	// Mirror the snapshot into Prometheus gauges so /metrics reflects current state.
	for i, n := range lengths {
		metrics.MQPartitionLength.WithLabelValues(strconv.Itoa(i)).Set(float64(n))
	}
	for k, off := range offsets {
		// k is "group:partition"
		parts := strings.SplitN(k, ":", 2)
		if len(parts) == 2 {
			metrics.MQConsumerOffset.WithLabelValues(parts[0], parts[1]).Set(float64(off))
		}
	}

	return model.BrokerMetrics{
		PartitionLengths: lengths,
		ConsumerOffsets:  offsets,
	}
}

// Health returns nil; the broker is always healthy after successful construction.
func (b *Broker) Health() error {
	return nil
}
