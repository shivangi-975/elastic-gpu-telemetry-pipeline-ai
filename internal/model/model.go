package model

type TelemetryData struct {
	ID        int64  `json:"id"`
	GPUId     string `json:"gpu_id"`
	Metric    string `json:"metric"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

// BrokerMetrics holds runtime statistics for the message queue broker.
type BrokerMetrics struct {
	PartitionLengths []int            `json:"partition_lengths"` // pending message count per partition
	ConsumerOffsets  map[string]int64 `json:"consumer_offsets"`  // "group:partition" -> committed offset
}

// Config holds configuration for the message queue broker and HTTP server.
type Config struct {
	Partitions   int    `json:"partitions"`     // default 8
	MaxQueueSize int    `json:"max_queue_size"` // default 4096 messages per partition
	WALPath      string `json:"wal_path"`       // empty = WAL disabled
	ListenAddr   string `json:"listen_addr"`    // default ":9000"
}
