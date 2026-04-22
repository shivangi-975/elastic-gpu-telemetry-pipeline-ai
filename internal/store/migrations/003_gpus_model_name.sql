-- Add model_name to the gpus registry so the API can surface human-readable
-- GPU model strings (e.g. "NVIDIA H100 80GB HBM3") sourced from DCGM.
-- Defaults to '' so existing rows remain valid; the collector will fill it
-- on the next telemetry message for each GPU.
ALTER TABLE gpus ADD COLUMN IF NOT EXISTS model_name TEXT NOT NULL DEFAULT '';
