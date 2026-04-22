package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestGPUJSONSerialization(t *testing.T) {
	t.Parallel()

	gpu := GPU{
		UUID:     "GPU-aabb0001-0001-0001-0001-000000000001",
		Hostname: "node-001",
		LastSeen: time.Date(2025, 7, 18, 13, 42, 33, 0, time.UTC),
	}

	data, err := json.Marshal(gpu)
	if err != nil {
		t.Fatalf("failed to marshal GPU: %v", err)
	}

	var decoded GPU
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal GPU: %v", err)
	}

	if decoded.UUID != gpu.UUID {
		t.Errorf("UUID mismatch: got %q, want %q", decoded.UUID, gpu.UUID)
	}
	if decoded.Hostname != gpu.Hostname {
		t.Errorf("Hostname mismatch: got %q, want %q", decoded.Hostname, gpu.Hostname)
	}
	if !decoded.LastSeen.Equal(gpu.LastSeen) {
		t.Errorf("LastSeen mismatch: got %v, want %v", decoded.LastSeen, gpu.LastSeen)
	}
}

func TestTelemetryRecordJSONSerialization(t *testing.T) {
	t.Parallel()

	record := TelemetryRecord{
		ID:          123,
		GPUUUID:     "GPU-aabb0001",
		Hostname:    "node-001",
		MetricName:  "DCGM_FI_DEV_GPU_UTIL",
		MetricValue: 78.5,
		CollectedAt: time.Date(2025, 7, 18, 13, 42, 33, 0, time.UTC),
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("failed to marshal TelemetryRecord: %v", err)
	}

	var decoded TelemetryRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal TelemetryRecord: %v", err)
	}

	if decoded.ID != record.ID {
		t.Errorf("ID mismatch: got %d, want %d", decoded.ID, record.ID)
	}
	if decoded.GPUUUID != record.GPUUUID {
		t.Errorf("GPUUUID mismatch: got %q, want %q", decoded.GPUUUID, record.GPUUUID)
	}
	if decoded.MetricName != record.MetricName {
		t.Errorf("MetricName mismatch: got %q, want %q", decoded.MetricName, record.MetricName)
	}
	if decoded.MetricValue != record.MetricValue {
		t.Errorf("MetricValue mismatch: got %f, want %f", decoded.MetricValue, record.MetricValue)
	}
}

func TestPublishMessageJSONSerialization(t *testing.T) {
	t.Parallel()

	msg := PublishMessage{
		Key: "GPU-aabb0001",
		Payload: TelemetryRecord{
			GPUUUID:     "GPU-aabb0001",
			MetricName:  "GPU_UTIL",
			MetricValue: 45.0,
			CollectedAt: time.Now().UTC(),
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal PublishMessage: %v", err)
	}

	var decoded PublishMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal PublishMessage: %v", err)
	}

	if decoded.Key != msg.Key {
		t.Errorf("Key mismatch: got %q, want %q", decoded.Key, msg.Key)
	}
	if decoded.Payload.GPUUUID != msg.Payload.GPUUUID {
		t.Errorf("Payload.GPUUUID mismatch: got %q, want %q", decoded.Payload.GPUUUID, msg.Payload.GPUUUID)
	}
}

func TestPublishBatchJSONSerialization(t *testing.T) {
	t.Parallel()

	batch := PublishBatch{
		Messages: []PublishMessage{
			{Key: "GPU-001", Payload: TelemetryRecord{GPUUUID: "GPU-001", MetricValue: 10.0}},
			{Key: "GPU-002", Payload: TelemetryRecord{GPUUUID: "GPU-002", MetricValue: 20.0}},
		},
	}

	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("failed to marshal PublishBatch: %v", err)
	}

	var decoded PublishBatch
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal PublishBatch: %v", err)
	}

	if len(decoded.Messages) != 2 {
		t.Fatalf("Messages length mismatch: got %d, want 2", len(decoded.Messages))
	}
	if decoded.Messages[0].Key != "GPU-001" {
		t.Errorf("Messages[0].Key mismatch: got %q, want %q", decoded.Messages[0].Key, "GPU-001")
	}
}

func TestConsumeResponseJSONSerialization(t *testing.T) {
	t.Parallel()

	resp := ConsumeResponse{
		Partition: 3,
		Offset:    42,
		Messages: []PublishMessage{
			{Key: "GPU-001", Payload: TelemetryRecord{GPUUUID: "GPU-001", MetricValue: 50.0}},
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal ConsumeResponse: %v", err)
	}

	var decoded ConsumeResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal ConsumeResponse: %v", err)
	}

	if decoded.Partition != resp.Partition {
		t.Errorf("Partition mismatch: got %d, want %d", decoded.Partition, resp.Partition)
	}
	if decoded.Offset != resp.Offset {
		t.Errorf("Offset mismatch: got %d, want %d", decoded.Offset, resp.Offset)
	}
	if len(decoded.Messages) != 1 {
		t.Fatalf("Messages length mismatch: got %d, want 1", len(decoded.Messages))
	}
}

func TestAckRequestJSONSerialization(t *testing.T) {
	t.Parallel()

	req := AckRequest{
		ConsumerGroup: "telemetry-collectors",
		Partition:     5,
		Offset:        100,
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal AckRequest: %v", err)
	}

	var decoded AckRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal AckRequest: %v", err)
	}

	if decoded.ConsumerGroup != req.ConsumerGroup {
		t.Errorf("ConsumerGroup mismatch: got %q, want %q", decoded.ConsumerGroup, req.ConsumerGroup)
	}
	if decoded.Partition != req.Partition {
		t.Errorf("Partition mismatch: got %d, want %d", decoded.Partition, req.Partition)
	}
	if decoded.Offset != req.Offset {
		t.Errorf("Offset mismatch: got %d, want %d", decoded.Offset, req.Offset)
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{}

	if cfg.Partitions != 0 {
		t.Errorf("Default Partitions should be 0, got %d", cfg.Partitions)
	}
	if cfg.MaxQueueSize != 0 {
		t.Errorf("Default MaxQueueSize should be 0, got %d", cfg.MaxQueueSize)
	}
	if cfg.WALPath != "" {
		t.Errorf("Default WALPath should be empty, got %q", cfg.WALPath)
	}
	if cfg.ListenAddr != "" {
		t.Errorf("Default ListenAddr should be empty, got %q", cfg.ListenAddr)
	}
}

func TestBrokerMetricsJSONSerialization(t *testing.T) {
	t.Parallel()

	metrics := BrokerMetrics{
		PartitionLengths: []int{10, 20, 5, 0},
		ConsumerOffsets:  map[string]int64{"group1:0": 100, "group1:1": 200},
	}

	data, err := json.Marshal(metrics)
	if err != nil {
		t.Fatalf("failed to marshal BrokerMetrics: %v", err)
	}

	var decoded BrokerMetrics
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal BrokerMetrics: %v", err)
	}

	if len(decoded.PartitionLengths) != 4 {
		t.Fatalf("PartitionLengths length mismatch: got %d, want 4", len(decoded.PartitionLengths))
	}
	if decoded.PartitionLengths[0] != 10 {
		t.Errorf("PartitionLengths[0] mismatch: got %d, want 10", decoded.PartitionLengths[0])
	}
	if decoded.ConsumerOffsets["group1:0"] != 100 {
		t.Errorf("ConsumerOffsets[group1:0] mismatch: got %d, want 100", decoded.ConsumerOffsets["group1:0"])
	}
}


