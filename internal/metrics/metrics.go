// Package metrics centralises Prometheus instrumentation for every service in
// the pipeline. Each service registers the subset of collectors it needs at
// startup and exposes them on /metrics in the standard text exposition format,
// so a single Prometheus scrape config covers the whole stack.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace is the common Prometheus namespace ("subsystem prefix") applied
// to every metric so dashboards can group by the pipeline.
const Namespace = "gpu_telemetry"

// ----------------------------------------------------------------------------
// MQ broker metrics
// ----------------------------------------------------------------------------

var (
	MQPublishedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "mq", Name: "published_total",
		Help: "Total messages accepted by the broker.",
	})
	MQConsumedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "mq", Name: "consumed_total",
		Help: "Total messages dispatched to consumers.",
	}, []string{"group"})
	MQAckedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "mq", Name: "acked_total",
		Help: "Total ack requests recorded by the broker.",
	}, []string{"group"})
	MQBackpressureTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "mq", Name: "backpressure_total",
		Help: "Publish attempts rejected with HTTP 429 because a partition was full.",
	})
	MQPartitionLength = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "mq", Name: "partition_length",
		Help: "Number of unconsumed messages currently held in each partition.",
	}, []string{"partition"})
	MQConsumerOffset = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "mq", Name: "consumer_offset",
		Help: "Last committed offset for each (consumer-group, partition) pair.",
	}, []string{"group", "partition"})
)

// ----------------------------------------------------------------------------
// Streamer metrics
// ----------------------------------------------------------------------------

var (
	StreamerRowsPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "streamer", Name: "rows_published_total",
		Help: "CSV rows successfully serialised into MQ messages.",
	})
	StreamerBatchesPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "streamer", Name: "batches_published_total",
		Help: "Batches accepted by the broker.",
	})
	StreamerPublishErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "streamer", Name: "publish_errors_total",
		Help: "Batch publish attempts that failed (network, 4xx, 5xx).",
	})
	StreamerCSVParseErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "streamer", Name: "csv_parse_errors_total",
		Help: "Malformed CSV rows skipped during streaming.",
	})
)

// ----------------------------------------------------------------------------
// Collector metrics
// ----------------------------------------------------------------------------

var (
	CollectorBatchesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "collector", Name: "batches_consumed_total",
		Help: "Batches the collector successfully read from the MQ.",
	})
	CollectorRowsInserted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "collector", Name: "rows_inserted_total",
		Help: "Telemetry rows persisted to PostgreSQL.",
	})
	CollectorPersistErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "collector", Name: "persist_errors_total",
		Help: "Persist or ack failures; non-acked batches will be redelivered.",
	})
	CollectorPersistDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "collector", Name: "persist_duration_seconds",
		Help:    "Wall-clock time to upsert GPUs and bulk-insert one batch.",
		Buckets: prometheus.DefBuckets,
	})
)

// ----------------------------------------------------------------------------
// API gateway metrics
// ----------------------------------------------------------------------------

var (
	APIRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "api", Name: "requests_total",
		Help: "Total HTTP requests served by the API gateway.",
	}, []string{"route", "method", "status"})
	APIRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "api", Name: "request_duration_seconds",
		Help:    "Latency distribution per route.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})
)

// ----------------------------------------------------------------------------
// Registry helpers
// ----------------------------------------------------------------------------

// MustRegister registers the supplied collectors plus the standard Go runtime
// and process collectors on a fresh Registry, returning that Registry so callers
// can attach a /metrics handler scoped to it.
func MustRegister(cs ...prometheus.Collector) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	for _, c := range cs {
		reg.MustRegister(c)
	}
	return reg
}

// Handler returns an http.Handler that exposes reg in the Prometheus text
// exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// ServeAsync starts a goroutine that exposes /metrics on addr. Used by the
// streamer and collector, which otherwise have no HTTP surface. It returns a
// shutdown func the caller can invoke during graceful shutdown.
func ServeAsync(addr string, reg *prometheus.Registry) func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(reg))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = srv.ListenAndServe() }()
	return func() { _ = srv.Close() }
}
