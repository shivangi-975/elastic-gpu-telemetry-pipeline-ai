package collector

import (
	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// ExtractGPUs derives unique GPU registrations from a batch of messages.
// When the same UUID appears more than once the entry with the latest
// LastSeen (CollectedAt) is retained, ensuring the GPU registry always
// reflects the most recent observation.
func ExtractGPUs(msgs []model.PublishMessage) []model.GPU {
	seen := make(map[string]model.GPU, len(msgs))

	for _, msg := range msgs {
		p := msg.Payload
		existing, ok := seen[p.GPUUUID]
		if !ok || p.CollectedAt.After(existing.LastSeen) {
			seen[p.GPUUUID] = model.GPU{
				UUID:      p.GPUUUID,
				Hostname:  p.Hostname,
				ModelName: p.ModelName,
				LastSeen:  p.CollectedAt,
			}
		}
	}

	gpus := make([]model.GPU, 0, len(seen))
	for _, g := range seen {
		gpus = append(gpus, g)
	}
	return gpus
}

// ExtractTelemetry converts a batch of MQ messages into TelemetryRecord values
// ready for bulk insertion. Fields are copied directly from the message
// payload; no transformation is applied.
func ExtractTelemetry(msgs []model.PublishMessage) []model.TelemetryRecord {
	records := make([]model.TelemetryRecord, len(msgs))
	for i, msg := range msgs {
		records[i] = msg.Payload
	}
	return records
}
