package model

import "time"

// TelemetryRecord represents a single telemetry data point collected from a GPU.
//
// ID is the unique identifier for the record (assigned by the database).
// GPUUUID is the UUID of the GPU that generated the record.
// Hostname is the hostname of the machine where the GPU resides; used by the
// collector to upsert the GPU registry and not stored in the telemetry table.
// ModelName is the human-readable GPU model from DCGM (e.g. "NVIDIA H100 80GB
// HBM3"); used by the collector to upsert the GPU registry and not stored in
// the telemetry table.
// MetricName is the name of the metric collected (e.g. DCGM_FI_DEV_GPU_UTIL).
// MetricValue is the numeric value of the metric.
// CollectedAt is the time.Now() at point of publish, never sourced from CSV.
//
// @Description Single telemetry data point
type TelemetryRecord struct {
	ID          int64     `json:"id"`
	GPUUUID     string    `json:"gpu_uuid"`
	Hostname    string    `json:"hostname"`
	ModelName   string    `json:"model_name,omitempty"`
	MetricName  string    `json:"metric_name"`
	MetricValue float64   `json:"metric_value"`
	CollectedAt time.Time `json:"collected_at"`
}

// PublishMessage is the message sent by the streamer to the message queue.
//
// Key is the GPU UUID, used as the partition key.
// Payload is the telemetry record being published.
type PublishMessage struct {
	Key     string          `json:"key"`
	Payload TelemetryRecord `json:"payload"`
}

// PublishBatch represents a batch of messages to be published to the message queue.
//
// Messages is a slice of PublishMessage objects.
type PublishBatch struct {
	Messages []PublishMessage `json:"messages"`
}

// ConsumeResponse is the response from the message queue to the collector.
//
// Partition is the partition the messages were read from.
// Offset is the next offset to consume from (committed offset after acking).
// Messages is a slice of PublishMessage objects.
type ConsumeResponse struct {
	Partition int              `json:"partition"`
	Offset    int64            `json:"offset"`
	Messages  []PublishMessage `json:"messages"`
}

// AckRequest represents an acknowledgement request from a consumer.
//
// ConsumerGroup is the name of the consumer group.
// Partition is the partition number.
// Offset is the offset being acknowledged.
type AckRequest struct {
	ConsumerGroup string `json:"consumer_group"`
	Partition     int    `json:"partition"`
	Offset        int64  `json:"offset"`
}
