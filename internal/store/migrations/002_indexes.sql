CREATE INDEX idx_telemetry_gpu_time
    ON telemetry (gpu_uuid, collected_at DESC);

CREATE INDEX idx_telemetry_time
    ON telemetry (collected_at DESC);
