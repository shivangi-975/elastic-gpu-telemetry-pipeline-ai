// Package streamer reads GPU telemetry from a DCGM CSV file and publishes
// batches to the custom in-memory message queue over HTTP.
package streamer

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

// publisher is satisfied by *MQClient and by test doubles.
type publisher interface {
	PublishBatch(ctx context.Context, batch model.PublishBatch) error
}

// newTickFn creates a ticker channel and returns it together with a stop function.
// Abstracted so tests can inject a controllable channel.
type newTickFn func(d time.Duration) (<-chan time.Time, func())

func defaultNewTick(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

// Config holds all runtime configuration for the Streamer.
type Config struct {
	MQURL           string // MQ_URL – required
	CSVPath         string // CSV_PATH – required
	StreamIntervalMS int    // STREAM_INTERVAL_MS – default 100 ms
	StreamBatchSize  int    // STREAM_BATCH_SIZE – default 50 messages
	PodName         string // POD_NAME – optional, used as "instance" log field
}

// Streamer reads DCGM telemetry rows from a CSV file in an infinite loop
// and publishes them to the message queue in batches.
type Streamer struct {
	cfg     Config
	pub     publisher
	logger  *slog.Logger
	newTick newTickFn // injectable for tests
}

// New validates cfg, applies defaults, and returns a ready Streamer.
func New(cfg Config) (*Streamer, error) {
	if cfg.MQURL == "" {
		return nil, fmt.Errorf("streamer: MQ_URL is required")
	}
	if cfg.CSVPath == "" {
		return nil, fmt.Errorf("streamer: CSV_PATH is required")
	}
	if _, err := os.Stat(cfg.CSVPath); err != nil {
		return nil, fmt.Errorf("streamer: %w", err)
	}
	if cfg.StreamIntervalMS <= 0 {
		cfg.StreamIntervalMS = 100
	}
	if cfg.StreamBatchSize <= 0 {
		cfg.StreamBatchSize = 50
	}

	instance := cfg.PodName
	if instance == "" {
		if h, err := os.Hostname(); err == nil {
			instance = h
		}
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With(
		"component", "streamer",
		"instance", instance,
	)

	return &Streamer{
		cfg:     cfg,
		pub:     NewMQClient(cfg.MQURL),
		logger:  logger,
		newTick: defaultNewTick,
	}, nil
}

// Run streams telemetry until ctx is cancelled.
//
// Loop behaviour:
//  1. Open the CSV, read and discard the header row.
//  2. Read rows one at a time; on EOF seek back to the start and re-read the header.
//  3. Parse each row: GPUUUID=fields[4], MetricName=fields[1], MetricValue=fields[10].
//  4. Malformed rows (too few fields, unparseable float) are logged as warnings and skipped.
//  5. Flush batch when it reaches StreamBatchSize or when the ticker fires.
//  6. On ctx.Done flush any remaining messages and return nil.
func (s *Streamer) Run(ctx context.Context) error {
	f, err := os.Open(s.cfg.CSVPath)
	if err != nil {
		return fmt.Errorf("streamer: open csv: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // allow variable field count; we validate manually

	// Consume header row.
	if _, err := r.Read(); err != nil {
		return fmt.Errorf("streamer: read csv header: %w", err)
	}

	interval := time.Duration(s.cfg.StreamIntervalMS) * time.Millisecond
	tickCh, stopTick := s.newTick(interval)
	defer stopTick()

	var batch []model.PublishMessage

	for {
		record, err := r.Read()
		if err == io.EOF {
			// Wrap around: seek to file start and skip header.
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("streamer: seek csv: %w", err)
			}
			r = csv.NewReader(f)
			r.FieldsPerRecord = -1
			if _, err := r.Read(); err != nil { // skip header
				return fmt.Errorf("streamer: skip csv header after wrap: %w", err)
			}
			continue
		}
		if err != nil {
			s.logger.Warn("csv read error, skipping row", "error", err)
			continue
		}

		msg, ok := s.parseRow(record)
		if !ok {
			continue // warning already logged inside parseRow
		}
		batch = append(batch, msg)

		// Flush when batch is full.
		if len(batch) >= s.cfg.StreamBatchSize {
			s.flush(ctx, batch)
			batch = batch[:0]
		}

		// Non-blocking check for ticker or shutdown.
		select {
		case <-tickCh:
			if len(batch) > 0 {
				s.flush(ctx, batch)
				batch = batch[:0]
			}
		case <-ctx.Done():
			s.finalFlush(batch)
			return nil
		default:
		}
	}
}

// parseRow extracts the telemetry fields from a raw CSV record.
//
// Expected columns (0-indexed) — matches dcgm_metrics.csv:
//
//	0  timestamp   1  metric_name  2  gpu_id   3  device
//	4  uuid        5  modelName    6  Hostname  7  container
//	8  pod         9  namespace   10  value    11  labels_raw
//
// CollectedAt is set to time.Now() at processing time (not the CSV timestamp),
// per spec: "the time at which a specific telemetry log is processed should be
// considered as the timestamp of that telemetry."
//
// Returns (msg, true) on success, (zero, false) with a warning log on any error.
func (s *Streamer) parseRow(record []string) (model.PublishMessage, bool) {
	const minFields = 11
	if len(record) < minFields {
		s.logger.Warn("malformed csv row: too few fields",
			"got", len(record), "want", minFields)
		return model.PublishMessage{}, false
	}

	metricValue, err := strconv.ParseFloat(record[10], 64)
	if err != nil {
		s.logger.Warn("malformed csv row: invalid metric value",
			"raw", record[10], "error", err)
		return model.PublishMessage{}, false
	}

	rec := model.TelemetryRecord{
		GPUUUID:     record[4],
		Hostname:    record[6],
		MetricName:  record[1],
		MetricValue: metricValue,
		CollectedAt: time.Now().UTC(),
	}
	return model.PublishMessage{Key: record[4], Payload: rec}, true
}

// flush publishes batch using the live context; errors are logged but not returned.
func (s *Streamer) flush(ctx context.Context, batch []model.PublishMessage) {
	if err := s.pub.PublishBatch(ctx, model.PublishBatch{Messages: batch}); err != nil {
		s.logger.Error("flush failed", "error", err, "size", len(batch))
	}
}

// finalFlush publishes any remaining messages on a fresh context so that a
// cancelled parent context does not prevent the shutdown flush.
func (s *Streamer) finalFlush(batch []model.PublishMessage) {
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.pub.PublishBatch(ctx, model.PublishBatch{Messages: batch}); err != nil {
		s.logger.Error("final flush failed", "error", err, "size", len(batch))
	}
}
