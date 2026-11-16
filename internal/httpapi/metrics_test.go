// metrics_test.go checks query, stream, and renewal observations, including
// which failures stay cancellations and which series keep their existing names.

package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	amqp "github.com/rabbitmq/amqp091-go"
)

// metricBackend is a test utility for metrics.
type metricBackend func(context.Context, *model.Query) (model.Results, error)

// Query implements the backend interface.
func (backend metricBackend) Query(ctx context.Context, q *model.Query) (model.Results, error) {
	return backend(ctx, q)
}

// metricSample finds a sample by name and labels.
func metricSample(t *testing.T, registry *prometheus.Registry, name string, labels ...string) *dto.Metric {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, sample := range family.Metric {
			match := true
			for i := 0; i < len(labels); i += 2 {
				found := false
				for _, label := range sample.Label {
					if label.GetName() == labels[i] && label.GetValue() == labels[i+1] {
						found = true
					}
				}
				if !found {
					match = false
				}
			}
			if match {
				return sample
			}
		}
	}
	t.Fatal("missing metric", name, labels)
	return nil
}

// queryMetricsService creates a new Service with a new registry.
func queryMetricsService(backend model.Backend) (*Service, *prometheus.Registry) {
	registry := prometheus.NewRegistry()
	svc := NewService(&ServiceConfig{Backend: backend})
	svc.metrics = newAPIMetrics(registry)
	return svc, registry
}

// metricRequest creates a new HTTP request for the given format.
func metricRequest(format string) *http.Request {
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"start":"2025-03-01T00:00:00Z","end":"2025-03-01T01:00:00Z"}`))
	if format == "arrow" {
		req.Header.Set("Accept", arrowio.MIME)
	}
	return req
}

// metricRows creates a new metric row.
func metricRows() []*model.Record {
	return []*model.Record{{Timestamp: time.Unix(0, 1740787200000000194), Name: "temperature", Value: 11.7, Meta: map[string]string{"vsn": "W0A0"}}}
}

// TestQueryMetricsSplitFormatsAndKeepLegacy tests the query metrics split formats and keep legacy.
func TestQueryMetricsSplitFormatsAndKeepLegacy(t *testing.T) {
	for _, format := range []string{"arrow", "ndjson"} {
		t.Run(format, func(t *testing.T) {
			results := &testutil.Results{Records: metricRows()}
			svc, registry := queryMetricsService(fixedResultsBackend{results})
			w := httptest.NewRecorder()
			svc.ServeHTTP(w, metricRequest(format))
			if metricSample(t, registry, "dataapi_response_latency_seconds").GetHistogram().GetSampleCount() != 1 {
				t.Fatal("legacy latency observation changed")
			}
			if !results.Closed || w.Code != 200 {
				t.Fatal("response changed", w.Code, results.Closed)
			}
			labels := []string{"format", format, "outcome", "success"}
			if metricSample(t, registry, "dataapi_queries_total", append(labels, "status", "200")...).GetCounter().GetValue() != 1 {
				t.Fatal("missing completed request")
			}
			if metricSample(t, registry, "dataapi_query_rows_total", labels...).GetCounter().GetValue() != 1 {
				t.Fatal("wrong rows")
			}
			if metricSample(t, registry, "dataapi_query_response_bytes_total", "format", format).GetCounter().GetValue() != float64(w.Body.Len()) {
				t.Fatal("wrong body byte accounting")
			}
			for _, name := range []string{"dataapi_query_duration_seconds", "dataapi_query_response_size_bytes", "dataapi_query_result_rows"} {
				if metricSample(t, registry, name, labels...).GetHistogram().GetSampleCount() != 1 {
					t.Fatal(name)
				}
			}
			for _, name := range []string{"dataapi_response_latency_by_format_seconds", "dataapi_time_to_first_body_write_seconds"} {
				if metricSample(t, registry, name, "format", format).GetHistogram().GetSampleCount() != 1 {
					t.Fatal(name)
				}
			}
			if metricSample(t, registry, "dataapi_query_oldest_in_progress_timestamp_seconds", "format", format).GetGauge().GetValue() != 0 {
				t.Fatal("active age leaked")
			}
			if metricSample(t, registry, "dataapi_queries_in_progress", "format", format).GetGauge().GetValue() != 0 {
				t.Fatal("in-progress leaked")
			}
		})
	}
}

// TestQueryMetricsEmptyAndInvalidRequests tests the query metrics empty and invalid requests.
func TestQueryMetricsEmptyAndInvalidRequests(t *testing.T) {
	for _, format := range []string{"arrow", "ndjson"} {
		t.Run(format, func(t *testing.T) {
			svc, registry := queryMetricsService(&testutil.Backend{})
			w := httptest.NewRecorder()
			svc.ServeHTTP(w, metricRequest(format))
			want := uint64(0)
			if format == "arrow" {
				want = 1
			}
			if metricSample(t, registry, "dataapi_response_latency_by_format_seconds", "format", format).GetHistogram().GetSampleCount() != want {
				t.Fatal("empty response latency semantics changed")
			}
			if metricSample(t, registry, "dataapi_time_to_first_body_write_seconds", "format", format).GetHistogram().GetSampleCount() != want {
				t.Fatal("empty body fabricated a first write")
			}
			req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"secret_filter":"bad"}`))
			if format == "arrow" {
				req.Header.Set("Accept", arrowio.MIME)
			}
			w = httptest.NewRecorder()
			svc.ServeHTTP(w, req)
			if w.Code != 400 || metricSample(t, registry, "dataapi_queries_total", "format", format, "outcome", "invalid_request", "status", "400").GetCounter().GetValue() != 1 {
				t.Fatal("invalid request not classified")
			}
		})
	}
}

// TestQueryMetricsFailuresAndCleanup tests the query metrics failures and cleanup.
func TestQueryMetricsFailuresAndCleanup(t *testing.T) {
	for _, format := range []string{"arrow", "ndjson"} {
		for _, tc := range []struct {
			name    string
			err     error
			outcome string
		}{{"backend", errors.New("backend failure"), "backend_error"}, {"canceled", context.Canceled, "canceled"}} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				svc, registry := queryMetricsService(metricBackend(func(context.Context, *model.Query) (model.Results, error) { return nil, tc.err }))
				svc.ServeHTTP(httptest.NewRecorder(), metricRequest(format))
				if metricSample(t, registry, "dataapi_queries_total", "format", format, "outcome", tc.outcome, "status", "500").GetCounter().GetValue() != 1 {
					t.Fatal("wrong outcome")
				}
			})
		}
	}
	for _, format := range []string{"arrow", "ndjson"} {
		t.Run("late/"+format, func(t *testing.T) {
			results := &testutil.Results{Records: metricRows(), ErrValue: errors.New("late read failure")}
			svc, registry := queryMetricsService(fixedResultsBackend{results})
			func() {
				defer func() {
					if got := recover(); got != http.ErrAbortHandler {
						t.Fatal("abort changed", got)
					}
				}()
				svc.ServeHTTP(httptest.NewRecorder(), metricRequest(format))
			}()
			if !results.Closed || metricSample(t, registry, "dataapi_queries_total", "format", format, "outcome", "stream_error", "status", "200").GetCounter().GetValue() != 1 {
				t.Fatal("late error counted as success")
			}
			if metricSample(t, registry, "dataapi_queries_in_progress", "format", format).GetGauge().GetValue() != 0 {
				t.Fatal("in-progress leaked on abort")
			}
		})
	}
}

// metricCloseResults is a test utility for metrics.
type metricCloseResults struct {
	*testutil.Results
	close func() error
}

// Close implements the Results interface.
func (results metricCloseResults) Close() error { results.Results.Close(); return results.close() }

// TestQueryMetricsIncludeResourceCleanup tests the query metrics include resource cleanup.
func TestQueryMetricsIncludeResourceCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	results := metricCloseResults{Results: &testutil.Results{Records: metricRows()}, close: func() error { close(entered); <-release; return errors.New("uncertain close") }}
	svc, registry := queryMetricsService(fixedResultsBackend{results})
	done := make(chan struct{})
	go func() { defer close(done); svc.ServeHTTP(httptest.NewRecorder(), metricRequest("ndjson")) }()
	<-entered
	if metricSample(t, registry, "dataapi_queries_in_progress", "format", "ndjson").GetGauge().GetValue() != 1 {
		t.Fatal("cleanup excluded from active handler")
	}
	close(release)
	<-done
	if metricSample(t, registry, "dataapi_queries_total", "format", "ndjson", "outcome", "close_error", "status", "200").GetCounter().GetValue() != 1 {
		t.Fatal("close error was hidden")
	}
	if metricSample(t, registry, "dataapi_result_close_duration_seconds", "format", "ndjson", "outcome", "error").GetHistogram().GetSampleCount() != 1 {
		t.Fatal("missing close timing")
	}
}

// gatedMetricWriter is a test utility for metrics.
type gatedMetricWriter struct {
	*httptest.ResponseRecorder
	entered, release chan struct{}
	once             sync.Once
}

// Write implements the http.ResponseWriter interface.
func (writer *gatedMetricWriter) Write(body []byte) (int, error) {
	writer.once.Do(func() { close(writer.entered); <-writer.release })
	return writer.ResponseRecorder.Write(body)
}

// TestFirstBodyWriteIsNotLegacyBackendReady tests the first body write is not legacy backend ready.
func TestFirstBodyWriteIsNotLegacyBackendReady(t *testing.T) {
	svc, registry := queryMetricsService(&testutil.Backend{Records: metricRows()})
	w := &gatedMetricWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); svc.ServeHTTP(w, metricRequest("ndjson")) }()
	<-w.entered
	if metricSample(t, registry, "dataapi_response_latency_by_format_seconds", "format", "ndjson").GetHistogram().GetSampleCount() != 1 {
		t.Fatal("legacy point moved")
	}
	if metricSample(t, registry, "dataapi_time_to_first_body_write_seconds", "format", "ndjson").GetHistogram().GetSampleCount() != 0 {
		t.Fatal("first write observed before writer accepted data")
	}
	if metricSample(t, registry, "dataapi_query_oldest_in_progress_timestamp_seconds", "format", "ndjson").GetGauge().GetValue() <= 0 {
		t.Fatal("missing active request start")
	}
	close(w.release)
	<-done
	if metricSample(t, registry, "dataapi_time_to_first_body_write_seconds", "format", "ndjson").GetHistogram().GetSampleCount() != 1 {
		t.Fatal("first write missing")
	}
}

// brokenMetricWriter is a test utility for metrics.
type brokenMetricWriter struct{ *httptest.ResponseRecorder }

// Write implements the http.ResponseWriter interface.
func (writer brokenMetricWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// TestQueryWriteFailureAndPanicMetrics tests the query write failure and panic metrics.
func TestQueryWriteFailureAndPanicMetrics(t *testing.T) {
	for _, format := range []string{"arrow", "ndjson"} {
		t.Run(format, func(t *testing.T) {
			svc, registry := queryMetricsService(&testutil.Backend{Records: metricRows()})
			func() {
				defer func() {
					if got := recover(); got != http.ErrAbortHandler {
						t.Fatal(got)
					}
				}()
				svc.ServeHTTP(brokenMetricWriter{httptest.NewRecorder()}, metricRequest(format))
			}()
			if metricSample(t, registry, "dataapi_query_errors_total", "format", format, "stage", "write").GetCounter().GetValue() != 1 {
				t.Fatal("write failure hidden")
			}
			if metricSample(t, registry, "dataapi_queries_total", "format", format, "outcome", "stream_error", "status", "200").GetCounter().GetValue() != 1 {
				t.Fatal("write failure counted as success")
			}
		})
	}
	svc, registry := queryMetricsService(metricBackend(func(context.Context, *model.Query) (model.Results, error) { panic("backend panic") }))
	func() {
		defer func() {
			if got := recover(); got != "backend panic" {
				t.Fatal("panic was swallowed", got)
			}
		}()
		svc.ServeHTTP(httptest.NewRecorder(), metricRequest("ndjson"))
	}()
	if metricSample(t, registry, "dataapi_queries_total", "format", "ndjson", "outcome", "panic", "status", "none").GetCounter().GetValue() != 1 {
		t.Fatal("panic not observed")
	}
}

// TestSSEMetricsForSetupAndDeliveries tests the SSE metrics for setup and deliveries.
func TestSSEMetricsForSetupAndDeliveries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newAPIMetrics(registry)
	svc := &StreamService{RabbitMQURL: "unsupported://no-network", HeartbeatDuration: time.Hour, metrics: metrics}
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, httptest.NewRequest("GET", "/api/v0/stream", nil))
	if w.Code != 502 || metricSample(t, registry, "dataapi_stream_sessions_total", "outcome", "broker_error").GetCounter().GetValue() != 1 {
		t.Fatal("broker setup failure hidden", w.Code)
	}
	if metricSample(t, registry, "dataapi_stream_connections_started_total").GetCounter().GetValue() != 1 {
		t.Fatal("missing setup attempt")
	}
	legacy := &dto.Metric{}
	streamConnectionsTotal.Write(legacy)
	if legacy.Gauge == nil || legacy.GetGauge().GetValue() != 0 {
		t.Fatal("legacy stream gauge changed/leaked")
	}
	messages := make(chan amqp.Delivery, 3)
	messages <- amqp.Delivery{Body: []byte(`{"name":"temp","ts":1740787200000000194,"val":11.7,"meta":{"plugin":"yes"}}`)}
	messages <- amqp.Delivery{Body: []byte(`{"name":"temp","ts":1740787200000000194,"val":11.7,"meta":{"plugin":"no"}}`)}
	messages <- amqp.Delivery{Body: []byte(`bad-json`)}
	close(messages)
	observation := &streamObservation{metrics: metrics, started: time.Now(), outcome: "canceled"}
	w = httptest.NewRecorder()
	svc.streamEvents(w, httptest.NewRequest("GET", "/api/v0/stream", nil), messages, map[string]*regexp.Regexp{"plugin": regexp.MustCompile("^yes$")}, observation)
	observation.finish()
	for _, outcome := range []string{"written", "filtered", "invalid"} {
		if metricSample(t, registry, "dataapi_stream_messages_total", "outcome", outcome).GetCounter().GetValue() != 1 {
			t.Fatal(outcome)
		}
	}
	if !strings.Contains(w.Body.String(), "event: message") || observation.outcome != "broker_error" {
		t.Fatal("closed broker channel not handled")
	}
}

// brokenFlushMetricWriter is a test utility for metrics.
type brokenFlushMetricWriter struct{ *httptest.ResponseRecorder }

// FlushError implements the http.ResponseWriter interface.
func (writer brokenFlushMetricWriter) FlushError() error { return io.ErrClosedPipe }

// TestSSEFlushErrorAndMetricLabelBounds tests the SSE flush error and metric label bounds.
func TestSSEFlushErrorAndMetricLabelBounds(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newAPIMetrics(registry)
	svc := &StreamService{HeartbeatDuration: time.Hour, metrics: metrics}
	messages := make(chan amqp.Delivery, 1)
	messages <- amqp.Delivery{Body: []byte(`{"name":"temp","ts":1,"val":11.7,"meta":{}}`)}
	close(messages)
	observation := &streamObservation{metrics: metrics, started: time.Now(), outcome: "canceled"}
	svc.streamEvents(brokenFlushMetricWriter{httptest.NewRecorder()}, httptest.NewRequest("GET", "/api/v0/stream", nil), messages, nil, observation)
	observation.finish()
	if observation.outcome != "write_error" || metricSample(t, registry, "dataapi_stream_errors_total", "stage", "write").GetCounter().GetValue() != 1 {
		t.Fatal("flush failure hidden")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"format": true, "outcome": true, "status": true, "stage": true, "cold_frontier": true, "arrow_compression": true, "version": true, "revision": true, "dirty": true}
	for _, family := range families {
		for _, sample := range family.Metric {
			for _, label := range sample.Label {
				if !allowed[label.GetName()] {
					t.Fatal("unbounded label", label.GetName())
				}
			}
		}
	}
}

// TestCompletedResponseNotCanceledByLaterCleanupDisconnect tests the completed response not canceled by later cleanup disconnect.
func TestCompletedResponseNotCanceledByLaterCleanupDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := metricCloseResults{Results: &testutil.Results{Records: metricRows()}, close: func() error { cancel(); return nil }}
	svc, registry := queryMetricsService(fixedResultsBackend{results})
	svc.ServeHTTP(httptest.NewRecorder(), metricRequest("arrow").WithContext(ctx))
	if metricSample(t, registry, "dataapi_queries_total", "format", "arrow", "outcome", "success", "status", "200").GetCounter().GetValue() != 1 {
		t.Fatal("late caller disconnect mislabeled a completed response")
	}
}

// heartbeatMetricWriter is a test utility for metrics.
type heartbeatMetricWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

// FlushError implements the http.ResponseWriter interface.
func (writer heartbeatMetricWriter) FlushError() error {
	writer.ResponseRecorder.Flush()
	writer.cancel()
	return nil
}

// TestSSEHeartbeatAndCancellationMetrics tests the SSE heartbeat and cancellation metrics.
func TestSSEHeartbeatAndCancellationMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newAPIMetrics(registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := heartbeatMetricWriter{httptest.NewRecorder(), cancel}
	svc := &StreamService{HeartbeatDuration: time.Millisecond, metrics: metrics}
	observation := &streamObservation{metrics: metrics, started: time.Now(), outcome: "canceled"}
	svc.streamEvents(w, httptest.NewRequest("GET", "/api/v0/stream", nil).WithContext(ctx), make(chan amqp.Delivery), nil, observation)
	observation.finish()
	if metricSample(t, registry, "dataapi_stream_heartbeats_total").GetCounter().GetValue() < 1 || !strings.Contains(w.Body.String(), ":keepalive") {
		t.Fatal("heartbeat was not observed")
	}
	if metricSample(t, registry, "dataapi_stream_sessions_total", "outcome", "canceled").GetCounter().GetValue() != 1 {
		t.Fatal("normal cancellation counted as an error")
	}
}

// TestRenewalMetricsKeepNamesAndBoundedOutcomes tests the renewal metrics keep names and bounded outcomes.
func TestRenewalMetricsKeepNamesAndBoundedOutcomes(t *testing.T) {
	before := &dto.Metric{}
	defaultAPIMetrics.readRenewals.WithLabelValues("success").Write(before)
	ObserveReadRenewal("success", .01)
	ObserveReadRenewal("private-error-text", .01)
	after := &dto.Metric{}
	defaultAPIMetrics.readRenewals.WithLabelValues("success").Write(after)
	if after.GetCounter().GetValue() != before.GetCounter().GetValue()+1 {
		t.Fatal("renewal observation missing")
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "hot_to_cold_read_renewals_total" || family.GetName() == "hot_to_cold_read_renewal_duration_seconds" {
			if len(family.Metric) != 3 {
				t.Fatal("unbounded renewal outcomes", family.GetName())
			}
		}
	}
}
