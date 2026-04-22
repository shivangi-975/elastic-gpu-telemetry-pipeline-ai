package main

import (
	"os"
	"testing"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/collector"
)

func TestLoadConfigDefaults(t *testing.T) {
	origMQURL := os.Getenv("MQ_URL")
	origDBURL := os.Getenv("DATABASE_URL")
	defer func() {
		if origMQURL != "" {
			os.Setenv("MQ_URL", origMQURL)
		}
		if origDBURL != "" {
			os.Setenv("DATABASE_URL", origDBURL)
		}
	}()

	os.Setenv("MQ_URL", "http://localhost:9000")
	os.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/test")
	os.Unsetenv("CONSUMER_GROUP")
	os.Unsetenv("COLLECT_BATCH_SIZE")
	os.Unsetenv("COLLECT_POLL_MS")
	os.Unsetenv("POD_NAME")

	cfg := loadConfig()

	if cfg.MQURL != "http://localhost:9000" {
		t.Errorf("MQURL = %q; want %q", cfg.MQURL, "http://localhost:9000")
	}
	if cfg.DatabaseURL != "postgres://test:test@localhost:5432/test" {
		t.Errorf("DatabaseURL = %q; want postgres URL", cfg.DatabaseURL)
	}
	if cfg.InstanceID == "" {
		t.Error("InstanceID should default to hostname, got empty")
	}
}

func TestLoadConfigWithEnvOverrides(t *testing.T) {
	origMQURL := os.Getenv("MQ_URL")
	origDBURL := os.Getenv("DATABASE_URL")
	origGroup := os.Getenv("CONSUMER_GROUP")
	origBatch := os.Getenv("COLLECT_BATCH_SIZE")
	origPoll := os.Getenv("COLLECT_POLL_MS")
	origPod := os.Getenv("POD_NAME")

	defer func() {
		restoreEnv("MQ_URL", origMQURL)
		restoreEnv("DATABASE_URL", origDBURL)
		restoreEnv("CONSUMER_GROUP", origGroup)
		restoreEnv("COLLECT_BATCH_SIZE", origBatch)
		restoreEnv("COLLECT_POLL_MS", origPoll)
		restoreEnv("POD_NAME", origPod)
	}()

	os.Setenv("MQ_URL", "http://mq:9000")
	os.Setenv("DATABASE_URL", "postgres://u:p@db:5432/telemetry")
	os.Setenv("CONSUMER_GROUP", "my-group")
	os.Setenv("COLLECT_BATCH_SIZE", "200")
	os.Setenv("COLLECT_POLL_MS", "500")
	os.Setenv("POD_NAME", "collector-pod-0")

	cfg := loadConfig()

	if cfg.MQURL != "http://mq:9000" {
		t.Errorf("MQURL = %q; want %q", cfg.MQURL, "http://mq:9000")
	}
	if cfg.ConsumerGroup != "my-group" {
		t.Errorf("ConsumerGroup = %q; want %q", cfg.ConsumerGroup, "my-group")
	}
	if cfg.BatchSize != 200 {
		t.Errorf("BatchSize = %d; want 200", cfg.BatchSize)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Errorf("PollInterval = %v; want 500ms", cfg.PollInterval)
	}
	if cfg.InstanceID != "collector-pod-0" {
		t.Errorf("InstanceID = %q; want %q", cfg.InstanceID, "collector-pod-0")
	}
}

func TestLoadConfigInvalidBatchSize(t *testing.T) {
	origMQURL := os.Getenv("MQ_URL")
	origDBURL := os.Getenv("DATABASE_URL")
	origBatch := os.Getenv("COLLECT_BATCH_SIZE")

	defer func() {
		restoreEnv("MQ_URL", origMQURL)
		restoreEnv("DATABASE_URL", origDBURL)
		restoreEnv("COLLECT_BATCH_SIZE", origBatch)
	}()

	os.Setenv("MQ_URL", "http://localhost:9000")
	os.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/test")
	os.Setenv("COLLECT_BATCH_SIZE", "invalid")

	cfg := loadConfig()

	if cfg.BatchSize != 0 {
		t.Errorf("BatchSize should be 0 (default from collector.Config) for invalid value, got %d", cfg.BatchSize)
	}
}

func TestLoadConfigInvalidPollInterval(t *testing.T) {
	origMQURL := os.Getenv("MQ_URL")
	origDBURL := os.Getenv("DATABASE_URL")
	origPoll := os.Getenv("COLLECT_POLL_MS")

	defer func() {
		restoreEnv("MQ_URL", origMQURL)
		restoreEnv("DATABASE_URL", origDBURL)
		restoreEnv("COLLECT_POLL_MS", origPoll)
	}()

	os.Setenv("MQ_URL", "http://localhost:9000")
	os.Setenv("DATABASE_URL", "postgres://test:test@localhost:5432/test")
	os.Setenv("COLLECT_POLL_MS", "not-a-number")

	cfg := loadConfig()

	if cfg.PollInterval != 0 {
		t.Errorf("PollInterval should be 0 (default) for invalid value, got %v", cfg.PollInterval)
	}
}

func TestCollectorConfigStruct(t *testing.T) {
	cfg := collector.Config{
		MQURL:         "http://test:9000",
		DatabaseURL:   "postgres://test@localhost/db",
		ConsumerGroup: "test-group",
		BatchSize:     100,
		PollInterval:  time.Second,
		InstanceID:    "test-instance",
	}

	if cfg.MQURL != "http://test:9000" {
		t.Errorf("unexpected MQURL: %s", cfg.MQURL)
	}
	if cfg.ConsumerGroup != "test-group" {
		t.Errorf("unexpected ConsumerGroup: %s", cfg.ConsumerGroup)
	}
}

func restoreEnv(key, value string) {
	if value != "" {
		os.Setenv(key, value)
	} else {
		os.Unsetenv(key)
	}
}

