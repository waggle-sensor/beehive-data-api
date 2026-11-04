// metrics.go records query and stream handler outcomes. Every label value is
// created at startup, so an unexpected error string cannot become a new series.
// The unlabeled latency histogram stays for scrapes that do not use the format label.

package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const metricNamespace = "dataapi"

var queryOutcomes = []string{"success", "invalid_request", "backend_error", "stream_error", "close_error", "canceled", "panic"}
var streamOutcomes = []string{"canceled", "invalid_request", "broker_error", "write_error", "unsupported", "panic"}
var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600}

type apiMetrics struct {
	activeMu                                                              sync.Mutex
	active                                                                map[string]map[*queryObservation]struct{}
	oldest                                                                *prometheus.GaugeVec
	configuration                                                         *prometheus.GaugeVec
	queries                                                               *prometheus.CounterVec
	inProgress                                                            *prometheus.GaugeVec
	duration, responseLatency, firstWrite, backendDuration, closeDuration *prometheus.HistogramVec
	responseLatencySeconds                                                prometheus.Histogram
	responseBytes, rows, queryErrors                                      *prometheus.CounterVec
	responseSize, resultSize                                              *prometheus.HistogramVec
	streamSessions                                                        *prometheus.CounterVec
	streamDuration                                                        *prometheus.HistogramVec
	streamMessages, streamErrors                                          *prometheus.CounterVec
	streamBytes, streamHeartbeats, streamStarted                          prometheus.Counter
	readRenewals                                                          *prometheus.CounterVec
	readRenewalDuration                                                   *prometheus.HistogramVec
}

var defaultAPIMetrics = newAPIMetrics(prometheus.DefaultRegisterer)

// newAPIMetrics registers every series on registerer and creates each allowed
// label set. An idle format or outcome is then a real zero rather than a missing series.
func newAPIMetrics(registerer prometheus.Registerer) *apiMetrics {
	factory := promauto.With(registerer)
	metrics := &apiMetrics{active: map[string]map[*queryObservation]struct{}{"arrow": {}, "ndjson": {}}}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return factory.NewCounterVec(prometheus.CounterOpts{Namespace: metricNamespace, Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		return factory.NewHistogramVec(prometheus.HistogramOpts{Namespace: metricNamespace, Name: name, Help: help, Buckets: buckets}, labels)
	}
	metrics.queries = counter("queries_total", "Finished query HTTP handlers, including rejected requests and aborted streams.", "format", "outcome", "status")
	metrics.inProgress = factory.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricNamespace, Name: "queries_in_progress", Help: "Query handlers currently running, including backend setup and result cleanup."}, []string{"format"})
	metrics.oldest = factory.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricNamespace, Name: "query_oldest_in_progress_timestamp_seconds", Help: "Oldest active handler start by format, including setup and cleanup; zero while idle."}, []string{"format"})

	metrics.duration = histogram("query_duration_seconds", "Full handler wall time through result cleanup; not client dataframe completion time.", latencyBuckets, "format", "outcome")
	metrics.responseLatency = histogram("response_latency_by_format_seconds", "Legacy response-latency observation separated by format: before Arrow encoding or first NDJSON row write.", latencyBuckets, "format")
	// Unlabeled series kept for existing scrapes. Arrow and NDJSON share it.
	metrics.responseLatencySeconds = factory.NewHistogram(prometheus.HistogramOpts{Namespace: metricNamespace, Name: "response_latency_seconds", Help: "A histogram of backend latency duration."})
	metrics.firstWrite = histogram("time_to_first_body_write_seconds", "Time until first nonempty HTTP 200 body write accepted by the response writer; Arrow schema bytes can precede data.", latencyBuckets, "format")
	metrics.backendDuration = histogram("backend_query_duration_seconds", "Backend Query call wall time; deferred scan/fetch work can happen after it returns.", latencyBuckets, "format", "outcome")
	metrics.closeDuration = histogram("result_close_duration_seconds", "Result cleanup wall time, including any read-grant release.", latencyBuckets, "format", "outcome")
	metrics.responseBytes = counter("query_response_bytes_total", "Body bytes accepted by the response writer, including error bodies; published in 64 KiB increments and at handler exit.", "format")
	metrics.rows = counter("query_rows_total", "Encoder-reported processed rows at handler completion; aborted Arrow counts can include buffered rows, not delivered rows.", "format", "outcome")
	metrics.queryErrors = counter("query_errors_total", "Observed non-cancellation query errors by bounded stage; one query may have multiple errors.", "format", "stage")
	metrics.responseSize = histogram("query_response_size_bytes", "Accepted HTTP body bytes per completed handler, not network or uncompressed storage bytes.", []float64{0, 1024, 64 << 10, 1 << 20, 16 << 20, 128 << 20, 1 << 30, 8 << 30}, "format", "outcome")
	metrics.resultSize = histogram("query_result_rows", "Encoder-reported rows per completed handler; zero for requests rejected before results.", []float64{0, 1, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000}, "format", "outcome")
	metrics.streamSessions = counter("stream_sessions_total", "Ended SSE handler attempts by outcome; client cancellation is normal session termination.", "outcome")
	metrics.streamDuration = histogram("stream_session_duration_seconds", "SSE handler lifetime, including setup and broker resource cleanup.", []float64{.1, 1, 10, 60, 300, 900, 3600, 21600, 86400, 604800}, "outcome")
	metrics.streamMessages = counter("stream_messages_total", "Broker deliveries decoded by SSE: written, filtered or invalid; not delivery acknowledgements from clients.", "outcome")
	metrics.streamErrors = counter("stream_errors_total", "SSE errors by bounded stage, including invalid broker messages.", "stage")
	metrics.streamBytes = factory.NewCounter(prometheus.CounterOpts{Namespace: metricNamespace, Name: "stream_response_bytes_total", Help: "SSE event and heartbeat bytes accepted by the response writer."})
	metrics.streamHeartbeats = factory.NewCounter(prometheus.CounterOpts{Namespace: metricNamespace, Name: "stream_heartbeats_total", Help: "SSE heartbeat writes completed and flushed without error."})
	metrics.streamStarted = factory.NewCounter(prometheus.CounterOpts{Namespace: metricNamespace, Name: "stream_connections_started_total", Help: "SSE GET handler starts with Flusher support, including broker setup attempts."})
	// Retain the reader-renewal family names documented for API deployments.
	metrics.readRenewals = factory.NewCounterVec(prometheus.CounterOpts{Name: "hot_to_cold_read_renewals_total", Help: "Read renewal attempts observed by this API process, including transport and validation failures."}, []string{"outcome"})
	metrics.readRenewalDuration = factory.NewHistogramVec(prometheus.HistogramOpts{Name: "hot_to_cold_read_renewal_duration_seconds", Help: "Read renewal attempt wall time observed by this API process.", Buckets: []float64{.005, .01, .05, .1, .5, 1, 5, 10, 30}}, []string{"outcome"})
	metrics.configuration = factory.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricNamespace, Name: "configuration_info", Help: "Effective API frontier mode and Arrow compression, independent of request format."}, []string{"cold_frontier", "arrow_compression"})
	metrics.configuration.WithLabelValues("unknown", "unknown").Set(1)
	version, revision, dirty := "unknown", "unknown", "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" {
			version = info.Main.Version
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				dirty = setting.Value
			}
		}
	}
	build := factory.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricNamespace, Name: "build_info", Help: "API binary build metadata."}, []string{"version", "revision", "dirty"})
	build.WithLabelValues(version, revision, dirty).Set(1)

	for _, format := range []string{"arrow", "ndjson"} {
		metrics.inProgress.WithLabelValues(format)
		metrics.oldest.WithLabelValues(format)
		metrics.responseBytes.WithLabelValues(format)
		metrics.responseLatency.WithLabelValues(format)
		metrics.firstWrite.WithLabelValues(format)
		for _, outcome := range queryOutcomes {
			metrics.duration.WithLabelValues(format, outcome)
			metrics.rows.WithLabelValues(format, outcome)
			metrics.responseSize.WithLabelValues(format, outcome)
			metrics.resultSize.WithLabelValues(format, outcome)
			for _, status := range []string{"200", "400", "500", "none", "other"} {
				metrics.queries.WithLabelValues(format, outcome, status)
			}
		}
		for _, outcome := range []string{"success", "error", "canceled"} {
			metrics.backendDuration.WithLabelValues(format, outcome)
			metrics.closeDuration.WithLabelValues(format, outcome)
		}
		for _, stage := range []string{"request", "parse", "backend", "encode", "iterate", "write", "close", "panic"} {
			metrics.queryErrors.WithLabelValues(format, stage)
		}
	}
	for _, outcome := range streamOutcomes {
		metrics.streamSessions.WithLabelValues(outcome)
		metrics.streamDuration.WithLabelValues(outcome)
	}
	for _, outcome := range []string{"written", "filtered", "invalid"} {
		metrics.streamMessages.WithLabelValues(outcome)
	}
	for _, stage := range []string{"request", "filter", "dial", "channel", "queue", "bind", "consume", "decode", "encode", "write", "broker_close", "panic"} {
		metrics.streamErrors.WithLabelValues(stage)
	}
	for _, outcome := range []string{"success", "error", "canceled"} {
		metrics.readRenewals.WithLabelValues(outcome)
		metrics.readRenewalDuration.WithLabelValues(outcome)
	}
	return metrics
}

// canceled reports whether the context has ended or err is a cancellation.
func canceled(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// observationOutcome is success, canceled, or error. A nil error is success
// even when the context has already ended.
func observationOutcome(ctx context.Context, err error) string {
	if err == nil {
		return "success"
	}
	if canceled(ctx, err) {
		return "canceled"
	}
	if err != nil {
		return "error"
	}
	return "success"
}

// queryObservation follows one query handler. The outcome stays success until
// the first failure. bytes counts accepted response bytes, and publishedBytes
// is how many of those have already been added to the counter.
type queryObservation struct {
	metrics               *apiMetrics // the metrics
	ctx                   context.Context // the context
	format, outcome       string // the format and outcome
	started               time.Time // the started time
	status                int // the status
	bytes, publishedBytes int64 // the bytes and published bytes
	rows                  int // the rows
	first                 bool // the first
	writeError            error // the write error
}

// beginQuery marks this handler in progress so the oldest-start gauge includes it.
func (metrics *apiMetrics) beginQuery(ctx context.Context, format string, started time.Time) *queryObservation {
	observation := &queryObservation{metrics: metrics, ctx: ctx, format: format, outcome: "success", started: started}
	metrics.activeMu.Lock()
	defer metrics.activeMu.Unlock()
	metrics.active[format][observation] = struct{}{}
	metrics.inProgress.WithLabelValues(format).Inc()
	metrics.refreshOldest(format)
	return observation
}

// refreshOldest sets the oldest active start for format, or zero when none remain.
// The caller holds activeMu. The handler pointer is not a metric label.
func (metrics *apiMetrics) refreshOldest(format string) {
	oldest := time.Time{}
	for observation := range metrics.active[format] {
		if oldest.IsZero() || observation.started.Before(oldest) {
			oldest = observation.started
		}
	}
	unixSeconds := float64(0)
	if !oldest.IsZero() {
		unixSeconds = float64(oldest.UnixNano()) / 1e9
	}
	metrics.oldest.WithLabelValues(format).Set(unixSeconds)
}

// fail keeps the first outcome. A later failure does not replace it, and a
// cancellation does not increment the query error counter.
func (observation *queryObservation) fail(stage, outcome string, err error) {
	if canceled(observation.ctx, err) {
		outcome = "canceled"
	} else {
		observation.metrics.queryErrors.WithLabelValues(observation.format, stage).Inc()
	}
	if observation.outcome == "success" {
		observation.outcome = outcome
	}
}

// publishBytes adds the bytes accepted since the previous publish.
func (observation *queryObservation) publishBytes() {
	observation.metrics.responseBytes.WithLabelValues(observation.format).Add(float64(observation.bytes - observation.publishedBytes))
	observation.publishedBytes = observation.bytes
}

// finish records the handler and removes it from the in-progress set. A missing
// status is "none". Only 200, 400, and 500 keep their code; every other code is "other".
func (observation *queryObservation) finish() {
	status := "other"
	switch observation.status {
	case 0:
		status = "none"
	case 200, 400, 500:
		status = strconv.Itoa(observation.status)
	}
	observation.publishBytes()
	observation.metrics.queries.WithLabelValues(observation.format, observation.outcome, status).Inc()
	observation.metrics.duration.WithLabelValues(observation.format, observation.outcome).Observe(time.Since(observation.started).Seconds())
	observation.metrics.rows.WithLabelValues(observation.format, observation.outcome).Add(float64(observation.rows))
	observation.metrics.responseSize.WithLabelValues(observation.format, observation.outcome).Observe(float64(observation.bytes))
	observation.metrics.resultSize.WithLabelValues(observation.format, observation.outcome).Observe(float64(observation.rows))
	observation.metrics.activeMu.Lock()
	delete(observation.metrics.active[observation.format], observation)
	observation.metrics.inProgress.WithLabelValues(observation.format).Dec()
	observation.metrics.refreshOldest(observation.format)
	observation.metrics.activeMu.Unlock()
}

// queryResponseWriter counts accepted body bytes and the time of the first
// nonempty HTTP 200 write. A 1xx header is not the response status, and a
// second status is ignored.
type queryResponseWriter struct {
	http.ResponseWriter
	observation *queryObservation
}

func (writer *queryResponseWriter) Unwrap() http.ResponseWriter { return writer.ResponseWriter }
func (writer *queryResponseWriter) WriteHeader(status int) {
	if status >= 100 && status < 200 {
		writer.ResponseWriter.WriteHeader(status)
		return
	}
	if writer.observation.status != 0 {
		return
	}
	writer.observation.status = status
	writer.ResponseWriter.WriteHeader(status)
}
func (writer *queryResponseWriter) Write(body []byte) (int, error) {
	if writer.observation.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	written, err := writer.ResponseWriter.Write(body)
	if err == nil && written < len(body) {
		err = io.ErrShortWrite
	}
	observation := writer.observation
	observation.bytes += int64(written)
	if written > 0 && observation.status == http.StatusOK && !observation.first {
		observation.first = true
		observation.metrics.firstWrite.WithLabelValues(observation.format).Observe(time.Since(observation.started).Seconds())
	}
	if observation.bytes-observation.publishedBytes >= 64<<10 {
		observation.publishBytes()
	}
	if err != nil {
		observation.writeError = err
	}
	return written, err
}

// ObserveReadRenewal records one renewal on the process metrics. An outcome
// other than success, error, or canceled is ignored.
func ObserveReadRenewal(outcome string, seconds float64) {
	if outcome != "success" && outcome != "error" && outcome != "canceled" {
		return
	}
	defaultAPIMetrics.readRenewals.WithLabelValues(outcome).Inc()
	defaultAPIMetrics.readRenewalDuration.WithLabelValues(outcome).Observe(seconds)
}

// streamObservation follows one SSE handler. fail replaces the outcome, because
// the handler has one terminal result, and counts that stage's error.
type streamObservation struct {
	metrics *apiMetrics
	started time.Time
	outcome string
}

func (observation *streamObservation) fail(stage, outcome string) {
	observation.metrics.streamErrors.WithLabelValues(stage).Inc()
	observation.outcome = outcome
}
func (observation *streamObservation) finish() {
	observation.metrics.streamSessions.WithLabelValues(observation.outcome).Inc()
	observation.metrics.streamDuration.WithLabelValues(observation.outcome).Observe(time.Since(observation.started).Seconds())
}

// ConfigureMetrics publishes one configuration series. An unrecognized frontier
// or compression is recorded as unknown, and the previous series is cleared.
func ConfigureMetrics(frontier, compression string) {
	if frontier != "hot" && frontier != "static" && frontier != "dynamic" {
		frontier = "unknown"
	}
	if compression != "none" && compression != "zstd" && compression != "lz4" {
		compression = "unknown"
	}
	defaultAPIMetrics.configuration.Reset()
	defaultAPIMetrics.configuration.WithLabelValues(frontier, compression).Set(1)
}
