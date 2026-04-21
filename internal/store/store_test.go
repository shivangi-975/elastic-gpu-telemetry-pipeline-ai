package store_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/example/gpu-telemetry-pipeline/internal/model"
	"github.com/example/gpu-telemetry-pipeline/internal/store"
)

var (
	testDSN   string
	cleanPool *pgxpool.Pool
)

func TestMain(m *testing.M) {
	flag.Parse() // must be called before testing.Short() in Go 1.21+
	if testing.Short() {
		fmt.Fprintln(os.Stdout, "skipping integration tests in short mode (-short)")
		os.Exit(0)
	}

	ctx := context.Background()

	pgc, err := tcpostgres.RunContainer(ctx,
		testcontainers.WithImage("docker.io/postgres:16-alpine"),
		tcpostgres.WithDatabase("telemetry_test"),
		tcpostgres.WithUsername("telemetry"),
		tcpostgres.WithPassword("changeme"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}
	defer pgc.Terminate(ctx) //nolint:errcheck

	testDSN, err = pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("get connection string: %v", err)
	}

	// Run migrations once; subsequent store.New calls will skip them.
	s, err := store.New(ctx, testDSN)
	if err != nil {
		log.Fatalf("bootstrap store: %v", err)
	}
	s.Close()

	cleanPool, err = pgxpool.New(ctx, testDSN)
	if err != nil {
		log.Fatalf("create cleanup pool: %v", err)
	}
	defer cleanPool.Close()

	os.Exit(m.Run())
}

// truncateTables removes all rows from data tables before a test that writes.
func truncateTables(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cleanPool.Exec(ctx, "TRUNCATE TABLE telemetry, gpus CASCADE"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

// newTestStore creates a Store backed by the shared test container.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	s, err := store.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// ── New / migration ──────────────────────────────────────────────────────────

func TestNew_ConnectsAndMigrates(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := store.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.Close()
}

func TestNew_IdempotentMigrations(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Migrations already applied by TestMain; calling New again must succeed.
	s, err := store.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("New (second call): %v", err)
	}
	s.Close()

	s2, err := store.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("New (third call): %v", err)
	}
	s2.Close()
}

func TestNew_BadDSN(t *testing.T) {
	t.Parallel()
	// Port 19999 is not postgres; connect_timeout=1 keeps the test fast.
	const badDSN = "postgres://nobody:nothing@127.0.0.1:19999/nodb?sslmode=disable&connect_timeout=1"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := store.New(ctx, badDSN)
	if err == nil {
		t.Fatal("expected error for unreachable host, got nil")
	}
}

// ── UpsertGPU ────────────────────────────────────────────────────────────────

func TestUpsertGPU_Insert(t *testing.T) {
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	gpu := model.GPU{
		UUID:     "gpu-insert-001",
		Hostname: "host-a",
		LastSeen: time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := s.UpsertGPU(ctx, gpu); err != nil {
		t.Fatalf("UpsertGPU: %v", err)
	}

	gpus, err := s.ListGPUs(ctx)
	if err != nil {
		t.Fatalf("ListGPUs: %v", err)
	}
	if len(gpus) != 1 {
		t.Fatalf("expected 1 GPU, got %d", len(gpus))
	}
	if gpus[0].UUID != gpu.UUID || gpus[0].Hostname != gpu.Hostname {
		t.Fatalf("unexpected GPU: %+v", gpus[0])
	}
}

func TestUpsertGPU_UpdatesWhenNewer(t *testing.T) {
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Millisecond)
	first := model.GPU{UUID: "gpu-upd-001", Hostname: "host-old", LastSeen: base}
	newer := model.GPU{UUID: "gpu-upd-001", Hostname: "host-new", LastSeen: base.Add(time.Hour)}

	if err := s.UpsertGPU(ctx, first); err != nil {
		t.Fatalf("UpsertGPU first: %v", err)
	}
	if err := s.UpsertGPU(ctx, newer); err != nil {
		t.Fatalf("UpsertGPU newer: %v", err)
	}

	gpus, err := s.ListGPUs(ctx)
	if err != nil {
		t.Fatalf("ListGPUs: %v", err)
	}
	if len(gpus) != 1 {
		t.Fatalf("expected 1 GPU, got %d", len(gpus))
	}
	if gpus[0].Hostname != "host-new" {
		t.Fatalf("hostname: want %q, got %q", "host-new", gpus[0].Hostname)
	}
}

func TestUpsertGPU_SkipsWhenOlder(t *testing.T) {
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Millisecond)
	current := model.GPU{UUID: "gpu-skip-001", Hostname: "host-current", LastSeen: base}
	stale := model.GPU{UUID: "gpu-skip-001", Hostname: "host-stale", LastSeen: base.Add(-time.Hour)}

	if err := s.UpsertGPU(ctx, current); err != nil {
		t.Fatalf("UpsertGPU current: %v", err)
	}
	if err := s.UpsertGPU(ctx, stale); err != nil {
		t.Fatalf("UpsertGPU stale: %v", err)
	}

	gpus, err := s.ListGPUs(ctx)
	if err != nil {
		t.Fatalf("ListGPUs: %v", err)
	}
	if len(gpus) != 1 {
		t.Fatalf("expected 1 GPU, got %d", len(gpus))
	}
	if gpus[0].Hostname != "host-current" {
		t.Fatalf("hostname: want %q, got %q", "host-current", gpus[0].Hostname)
	}
}

// ── ListGPUs ─────────────────────────────────────────────────────────────────

func TestListGPUs_Empty(t *testing.T) {
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	gpus, err := s.ListGPUs(ctx)
	if err != nil {
		t.Fatalf("ListGPUs: %v", err)
	}
	if gpus == nil {
		t.Fatal("expected non-nil slice, got nil")
	}
	if len(gpus) != 0 {
		t.Fatalf("expected 0 GPUs, got %d", len(gpus))
	}
}

func TestListGPUs_Multiple(t *testing.T) {
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	seeds := []model.GPU{
		{UUID: "gpu-zzz", Hostname: "host-z", LastSeen: now},
		{UUID: "gpu-aaa", Hostname: "host-a", LastSeen: now},
		{UUID: "gpu-mmm", Hostname: "host-m", LastSeen: now},
	}
	for _, g := range seeds {
		if err := s.UpsertGPU(ctx, g); err != nil {
			t.Fatalf("UpsertGPU %s: %v", g.UUID, err)
		}
	}

	gpus, err := s.ListGPUs(ctx)
	if err != nil {
		t.Fatalf("ListGPUs: %v", err)
	}
	if len(gpus) != 3 {
		t.Fatalf("expected 3 GPUs, got %d", len(gpus))
	}
	for i := 1; i < len(gpus); i++ {
		if gpus[i].UUID < gpus[i-1].UUID {
			t.Fatalf("GPUs not ordered by uuid ASC: %v > %v", gpus[i-1].UUID, gpus[i].UUID)
		}
	}
}

// ── BulkInsertTelemetry ──────────────────────────────────────────────────────

func TestBulkInsertTelemetry_NoOp(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.BulkInsertTelemetry(ctx, nil); err != nil {
		t.Fatalf("BulkInsertTelemetry(nil): %v", err)
	}
	if err := s.BulkInsertTelemetry(ctx, []model.TelemetryRecord{}); err != nil {
		t.Fatalf("BulkInsertTelemetry(empty): %v", err)
	}
}

func TestBulkInsertTelemetry_Single(t *testing.T) {
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	gpu := model.GPU{UUID: "gpu-single", Hostname: "host-s", LastSeen: time.Now().UTC().Truncate(time.Millisecond)}
	if err := s.UpsertGPU(ctx, gpu); err != nil {
		t.Fatalf("UpsertGPU: %v", err)
	}

	rec := model.TelemetryRecord{
		GPUUUID:     "gpu-single",
		MetricName:  "DCGM_FI_DEV_GPU_UTIL",
		MetricValue: 42.5,
		CollectedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	if err := s.BulkInsertTelemetry(ctx, []model.TelemetryRecord{rec}); err != nil {
		t.Fatalf("BulkInsertTelemetry: %v", err)
	}

	records, err := s.QueryTelemetry(ctx, "gpu-single", nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].MetricValue != 42.5 {
		t.Fatalf("metric_value: want 42.5, got %v", records[0].MetricValue)
	}
	if records[0].ID == 0 {
		t.Fatal("expected non-zero ID from BIGSERIAL")
	}
	if records[0].Hostname != "host-s" {
		t.Fatalf("hostname: want %q, got %q", "host-s", records[0].Hostname)
	}
}

func TestBulkInsertTelemetry_LargeBatch_1000(t *testing.T) {
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	gpu := model.GPU{UUID: "gpu-bulk", Hostname: "host-bulk", LastSeen: time.Now().UTC().Truncate(time.Millisecond)}
	if err := s.UpsertGPU(ctx, gpu); err != nil {
		t.Fatalf("UpsertGPU: %v", err)
	}

	const n = 1000
	records := make([]model.TelemetryRecord, n)
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := range records {
		records[i] = model.TelemetryRecord{
			GPUUUID:     "gpu-bulk",
			MetricName:  "DCGM_FI_DEV_GPU_UTIL",
			MetricValue: float64(i),
			CollectedAt: base.Add(time.Duration(i) * time.Millisecond),
		}
	}

	if err := s.BulkInsertTelemetry(ctx, records); err != nil {
		t.Fatalf("BulkInsertTelemetry: %v", err)
	}

	got, err := s.QueryTelemetry(ctx, "gpu-bulk", nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if len(got) != n {
		t.Fatalf("expected %d records, got %d", n, len(got))
	}
}

// ── QueryTelemetry ───────────────────────────────────────────────────────────

// setupQueryTest inserts a GPU with 5 telemetry points one hour apart.
// Values are 1..5 in ascending time order.
func setupQueryTest(t *testing.T) (*store.Store, time.Time) {
	t.Helper()
	truncateTables(t)
	s := newTestStore(t)
	ctx := context.Background()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := s.UpsertGPU(ctx, model.GPU{UUID: "gpu-q", Hostname: "host-q", LastSeen: base}); err != nil {
		t.Fatalf("UpsertGPU: %v", err)
	}

	records := make([]model.TelemetryRecord, 5)
	for i := range records {
		records[i] = model.TelemetryRecord{
			GPUUUID:     "gpu-q",
			MetricName:  "metric",
			MetricValue: float64(i + 1),
			CollectedAt: base.Add(time.Duration(i) * time.Hour),
		}
	}
	if err := s.BulkInsertTelemetry(ctx, records); err != nil {
		t.Fatalf("BulkInsertTelemetry: %v", err)
	}
	return s, base
}

func TestQueryTelemetry_TimeRange(t *testing.T) {
	s, base := setupQueryTest(t)
	ctx := context.Background()

	// base+1h to base+3h inclusive → values 2, 3, 4.
	start := base.Add(time.Hour)
	end := base.Add(3 * time.Hour)
	records, err := s.QueryTelemetry(ctx, "gpu-q", &start, &end, 0, 0)
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0].MetricValue != 2 || records[2].MetricValue != 4 {
		t.Fatalf("unexpected values: %v %v", records[0].MetricValue, records[2].MetricValue)
	}
}

func TestQueryTelemetry_NoStartBound(t *testing.T) {
	s, base := setupQueryTest(t)
	ctx := context.Background()

	// No lower bound, end at base+2h → values 1, 2, 3.
	end := base.Add(2 * time.Hour)
	records, err := s.QueryTelemetry(ctx, "gpu-q", nil, &end, 0, 0)
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
}

func TestQueryTelemetry_NoEndBound(t *testing.T) {
	s, base := setupQueryTest(t)
	ctx := context.Background()

	// Start at base+3h, no upper bound → values 4, 5.
	start := base.Add(3 * time.Hour)
	records, err := s.QueryTelemetry(ctx, "gpu-q", &start, nil, 0, 0)
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestQueryTelemetry_NoBounds(t *testing.T) {
	s, _ := setupQueryTest(t)
	ctx := context.Background()

	records, err := s.QueryTelemetry(ctx, "gpu-q", nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}
	// Verify ascending order.
	for i := 1; i < len(records); i++ {
		if !records[i].CollectedAt.After(records[i-1].CollectedAt) {
			t.Fatalf("records not in ascending order at index %d", i)
		}
	}
	// Verify hostname is joined from gpus table.
	for i, r := range records {
		if r.Hostname != "host-q" {
			t.Fatalf("record[%d].hostname: want %q, got %q", i, "host-q", r.Hostname)
		}
	}
}

func TestQueryTelemetry_UnknownGPU(t *testing.T) {
	s, _ := setupQueryTest(t)
	ctx := context.Background()

	records, err := s.QueryTelemetry(ctx, "gpu-does-not-exist", nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if records == nil {
		t.Fatal("expected non-nil slice for unknown GPU, got nil")
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}
