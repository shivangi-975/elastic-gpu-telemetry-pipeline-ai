// Package store provides a PostgreSQL-backed persistence layer for the GPU
// telemetry pipeline. All exported methods are safe for concurrent use.
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store wraps a pgxpool.Pool and exposes domain-level database operations.
type Store struct {
	pool *pgxpool.Pool
}

// New parses dsn, configures a connection pool with sensible defaults, pings
// the database to verify connectivity, runs all pending SQL migrations in
// ascending version order, and returns a ready-to-use Store.
//
// Errors from parsing, connecting, or migrating are wrapped with "store: ".
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}

	cfg.MinConns = 2
	cfg.MaxConns = 20
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}

	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: %w", err)
	}

	return s, nil
}

// Close releases all connections held by the pool. It is safe to call more
// than once.
func (s *Store) Close() {
	s.pool.Close()
}

// Ping reports whether the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}

// migrate creates the schema_migrations tracking table if absent, then applies
// every *.sql file under migrations/ whose version has not yet been recorded.
// Each file runs inside its own SERIALIZABLE transaction; the version is
// recorded only after a successful commit.
func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INT         PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		var version int
		if _, err := fmt.Sscanf(entry.Name(), "%d", &version); err != nil {
			return fmt.Errorf("parse version from %q: %w", entry.Name(), err)
		}

		var applied bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`,
			version,
		).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %d: %w", version, err)
		}
		if applied {
			slog.Info("migration already applied", "version", version)
			continue
		}

		sql, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %d: %w", version, err)
		}
		if err := s.applyMigration(ctx, version, string(sql)); err != nil {
			return err
		}
		slog.Info("migration applied", "version", version)
	}
	return nil
}

// applyMigration executes sql and records version inside a single SERIALIZABLE
// transaction, rolling back automatically on any failure.
func (s *Store) applyMigration(ctx context.Context, version int, sql string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", version, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, sql); err != nil {
		return fmt.Errorf("exec migration %d: %w", version, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`, version,
	); err != nil {
		return fmt.Errorf("record migration %d: %w", version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d: %w", version, err)
	}
	return nil
}

// UpsertGPU inserts a new GPU row. On uuid conflict it updates hostname and
// last_seen only when the incoming last_seen is strictly newer than the stored
// value, preventing stale writes from overwriting fresher state.
func (s *Store) UpsertGPU(ctx context.Context, gpu model.GPU) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO gpus (uuid, hostname, last_seen)
		VALUES ($1, $2, $3)
		ON CONFLICT (uuid) DO UPDATE
			SET hostname  = EXCLUDED.hostname,
			    last_seen = EXCLUDED.last_seen
			WHERE gpus.last_seen < EXCLUDED.last_seen`,
		gpu.UUID, gpu.Hostname, gpu.LastSeen,
	)
	if err != nil {
		return fmt.Errorf("store: upsert gpu: %w", err)
	}
	return nil
}

// ListGPUs returns all GPU rows ordered by uuid ascending. It always returns
// a non-nil slice; an empty table yields an empty slice.
func (s *Store) ListGPUs(ctx context.Context) ([]model.GPU, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT uuid, hostname, last_seen FROM gpus ORDER BY uuid ASC`)
	if err != nil {
		return nil, fmt.Errorf("store: list gpus: %w", err)
	}
	defer rows.Close()

	gpus := []model.GPU{}
	for rows.Next() {
		var g model.GPU
		if err := rows.Scan(&g.UUID, &g.Hostname, &g.LastSeen); err != nil {
			return nil, fmt.Errorf("store: scan gpu: %w", err)
		}
		gpus = append(gpus, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list gpus: %w", err)
	}
	return gpus, nil
}

// BulkInsertTelemetry writes records to the telemetry table via the PostgreSQL
// COPY protocol for high throughput. It is a no-op for nil or empty input.
// It returns an error if the number of rows inserted differs from len(records).
func (s *Store) BulkInsertTelemetry(ctx context.Context, records []model.TelemetryRecord) error {
	if len(records) == 0 {
		return nil
	}

	rows := make([][]any, len(records))
	for i, r := range records {
		rows[i] = []any{r.GPUUUID, r.MetricName, r.MetricValue, r.CollectedAt}
	}

	n, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"telemetry"},
		[]string{"gpu_uuid", "metric_name", "metric_value", "collected_at"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("store: bulk insert telemetry: %w", err)
	}
	if int(n) != len(records) {
		return fmt.Errorf("store: bulk insert telemetry: inserted %d rows, expected %d", n, len(records))
	}
	return nil
}

// QueryTelemetry returns up to limit telemetry records for gpuUUID starting at
// offset, optionally filtered by metricName and bounded by start and end (both inclusive).
// Pass limit+1 from the caller to detect whether a next page exists.
// Results are ordered by collected_at ascending. It always returns a non-nil
// slice; no matching rows yields an empty slice.
func (s *Store) QueryTelemetry(
	ctx context.Context,
	gpuUUID string,
	metricName string,
	start *time.Time,
	end *time.Time,
	limit int,
	offset int,
) ([]model.TelemetryRecord, error) {
	var mn *string
	if metricName != "" {
		mn = &metricName
	}
	rows, err := s.pool.Query(ctx, `
		SELECT t.id, t.gpu_uuid, g.hostname, t.metric_name, t.metric_value, t.collected_at
		FROM telemetry t
		JOIN gpus g ON g.uuid = t.gpu_uuid
		WHERE t.gpu_uuid = $1
		  AND ($2::TEXT        IS NULL OR t.metric_name  = $2)
		  AND ($3::TIMESTAMPTZ IS NULL OR t.collected_at >= $3)
		  AND ($4::TIMESTAMPTZ IS NULL OR t.collected_at <= $4)
		ORDER BY t.collected_at ASC
		LIMIT NULLIF($5, 0) OFFSET $6`,
		gpuUUID, mn, start, end, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("store: query telemetry: %w", err)
	}
	defer rows.Close()

	records := []model.TelemetryRecord{}
	for rows.Next() {
		var r model.TelemetryRecord
		if err := rows.Scan(&r.ID, &r.GPUUUID, &r.Hostname, &r.MetricName, &r.MetricValue, &r.CollectedAt); err != nil {
			return nil, fmt.Errorf("store: scan telemetry: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: query telemetry: %w", err)
	}
	return records, nil
}
