CREATE TABLE gpus (
    uuid      TEXT        PRIMARY KEY,
    hostname  TEXT        NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL
);

CREATE TABLE telemetry (
    id           BIGSERIAL   PRIMARY KEY,
    gpu_uuid     TEXT        NOT NULL REFERENCES gpus(uuid) ON DELETE CASCADE,
    metric_name  TEXT        NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL
);
