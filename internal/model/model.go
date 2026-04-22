package model

import "time"

// BrokerMetrics holds runtime statistics for the message queue broker.
type BrokerMetrics struct {
	PartitionLengths []int            `json:"partition_lengths"` // pending message count per partition
	ConsumerOffsets  map[string]int64 `json:"consumer_offsets"`  // "group:partition" -> committed offset
}

// Config holds configuration for the message queue broker and HTTP server.
type Config struct {
	Partitions   int           `json:"partitions"`      // default 16
	MaxQueueSize int           `json:"max_queue_size"`  // default 4096 messages per partition
	WALPath      string        `json:"wal_path"`        // empty = WAL disabled
	ListenAddr   string        `json:"listen_addr"`     // default ":9000"
	ConsumerTTL  time.Duration `json:"consumer_ttl"`   // how long before an idle consumer is evicted; 0 = disabled
	EvictInterval time.Duration `json:"evict_interval"` // how often the eviction loop runs; default 10s
}
