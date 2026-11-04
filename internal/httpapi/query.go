// query.go is POST /api/v1/query. Arrow is selected by Accept before the
// backend runs, and a failure after the status line aborts the response.

package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"time"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
)

// ServiceConfig selects the query backend and the Arrow buffer codec.
type ServiceConfig struct {
	Backend          model.Backend
	ArrowCompression arrowio.Compression
}

// Service answers query requests with the configured backend and Arrow compression.
type Service struct {
	backend          model.Backend
	arrowCompression arrowio.Compression
	metrics          *apiMetrics
}

// NewService uses zstd when the config leaves compression unset.
func NewService(config *ServiceConfig) *Service {
	compression := config.ArrowCompression
	if compression == "" {
		compression = arrowio.CompressionZstd
	}
	return &Service{backend: config.Backend, arrowCompression: compression, metrics: defaultAPIMetrics}
}

// ServeHTTP runs one query and records its outcome. A failure after the status
// line aborts the response instead of ending as a successful short body.
func (svc *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestStartTime := time.Now()
	arrowResult := arrowio.AcceptsStream(r.Header.Get("Accept"))
	format := "ndjson"
	if arrowResult {
		format = "arrow"
	}
	metrics := svc.metrics
	if metrics == nil {
		metrics = defaultAPIMetrics
	}
	observation := metrics.beginQuery(r.Context(), format, requestStartTime)
	underlyingWriter := w
	w = &queryResponseWriter{ResponseWriter: w, observation: observation}
	queryCount := 0
	defer func() {
		recovered := recover()
		observation.rows = queryCount
		if recovered != nil {
			if recovered != http.ErrAbortHandler {
				metrics.queryErrors.WithLabelValues(format, "panic").Inc()
				if observation.outcome == "success" {
					observation.outcome = "panic"
				}
			} else if observation.outcome == "success" {
				observation.fail("iterate", "stream_error", http.ErrAbortHandler)
			}
		}
		observation.finish()
		if recovered != nil {
			panic(recovered)
		}
	}()

	remoteAddr := getRemoteAddr(r)
	log.Printf("received request from %s", remoteAddr)

	r.Body = http.MaxBytesReader(underlyingWriter, r.Body, 4096)
	defer r.Body.Close()

	queryBody, err := io.ReadAll(r.Body)
	if err == io.EOF || len(queryBody) == 0 {
		observation.fail("request", "invalid_request", err)
		log.Printf("%s error: no query provided", remoteAddr)
		http.Error(w, "error: no query provided", http.StatusBadRequest)
		return
	}
	if _, ok := err.(*http.MaxBytesError); ok {
		observation.fail("request", "invalid_request", err)
		log.Printf("%s error: rejected large request", remoteAddr)
		http.Error(w, "error: query is too large - must be <1KB", http.StatusBadRequest)
		return
	}
	if err != nil {
		observation.fail("request", "invalid_request", err)
		log.Printf("%s error: failed to read query body: %s", remoteAddr, err.Error())
		http.Error(w, "error: failed to read query body", http.StatusBadRequest)
		return
	}

	query, err := parseQuery(queryBody)
	if err != nil {
		observation.fail("parse", "invalid_request", err)
		log.Printf("%s error: failed to parse query: %s", remoteAddr, err.Error())
		http.Error(w, fmt.Sprintf("error: failed to parse query: %s", err.Error()), http.StatusBadRequest)
		return
	}

	log.Printf("%s query: %q", remoteAddr, queryBody)

	queryStart := time.Now()

	ctx := r.Context()
	compression := svc.arrowCompression
	if arrowResult {
		ctx = arrowio.WithResult(ctx)
	}
	backendStart := time.Now()
	results, err := svc.backend.Query(ctx, query)
	metrics.backendDuration.WithLabelValues(format, observationOutcome(ctx, err)).Observe(time.Since(backendStart).Seconds())
	if err != nil {
		observation.fail("backend", "backend_error", err)
		log.Printf("%s error: failed to query backend: %s", remoteAddr, err.Error())
		http.Error(w, fmt.Sprintf("error: failed to query backend: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer func() {
		began := time.Now()
		err := results.Close()
		metrics.closeDuration.WithLabelValues(format, observationOutcome(ctx, err)).Observe(time.Since(began).Seconds())
		if err != nil {
			observation.fail("close", "close_error", err)
			log.Printf("%s error: failed to close results: %v", remoteAddr, err)
		}
	}()

	w.Header().Add("Access-Control-Allow-Origin", "*")
	if arrowResult {
		w.Header().Set("Content-Type", arrowio.MIME)
		w.Header().Set(arrowio.CompressionHeader, string(compression))
		writeContentDispositionHeader(w, ".arrows")
	} else {
		writeContentDispositionHeader(w, ".ndjson")
	}
	w.WriteHeader(http.StatusOK)

	if arrowResult {
		// observe latency to start of response body. this is what the user actually sees so its what we care about.
		latency := time.Since(requestStartTime).Seconds()
		metrics.responseLatencySeconds.Observe(latency)
		metrics.responseLatency.WithLabelValues(format).Observe(latency)
		n, err := arrowio.WriteIPC(w, results, compression)
		queryCount = n
		if err != nil {
			stage := "encode"
			if observation.writeError != nil {
				stage = "write"
			}
			observation.fail(stage, "stream_error", err)
			log.Printf("%s error: %s", remoteAddr, err)
			// A streaming query can fail after headers or batches were sent.
			// Abort HTTP framing so the client cannot accept partial rows as a
			// successful complete extract. net/http handles this sentinel.
			panic(http.ErrAbortHandler)
		}
	} else {
		startedWritingResults := false
		for results.Next() {
			record := results.Record()
			if !startedWritingResults {
				latency := time.Since(requestStartTime).Seconds()
				metrics.responseLatencySeconds.Observe(latency)
				metrics.responseLatency.WithLabelValues(format).Observe(latency)
				startedWritingResults = true
			}
			if err := writeRecord(w, record); err != nil {
				stage := "encode"
				if observation.writeError != nil {
					stage = "write"
				}
				observation.fail(stage, "stream_error", err)
				log.Printf("%s error: failed to write query response: %v", remoteAddr, err)
				panic(http.ErrAbortHandler)
			}
			queryCount++
		}
	}

	if err := results.Err(); err != nil {
		observation.fail("iterate", "stream_error", err)
		log.Printf("%s error: %s", remoteAddr, err)
		// A timed-out managed read must not look like a complete extract.
		panic(http.ErrAbortHandler)
	}

	queryDuration := time.Since(queryStart)
	responseRate := float64(queryCount) / queryDuration.Seconds()
	log.Printf("%s served %d records in %s - %f records/s", remoteAddr, queryCount, queryDuration, responseRate)
}

// metaRE is a regular expression to match metadata keys.
var metaRE = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")

func parseQuery(data []byte) (*model.Query, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	query := &model.Query{}
	if err := decoder.Decode(query); err != nil {
		return nil, err
	}
	if query.Start == "" {
		return nil, fmt.Errorf("missing start field")
	}
	for k := range query.Filter {
		if !metaRE.MatchString(k) {
			return nil, fmt.Errorf("invalid filter key: %q", k)
		}
	}
	return query, nil
}

// writeRecord appends one JSON record and a newline.
func writeRecord(w io.Writer, rec *model.Record) error {
	return json.NewEncoder(w).Encode(rec)
}

// writeContentDispositionHeader writes a content disposition header to the writer.
func writeContentDispositionHeader(w http.ResponseWriter, ext string) {
	w.Header().Add("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, contentDispositionName(ext)))
}

// getRemoteAddr prefers X-Forwarded-For when a proxy set it.
func getRemoteAddr(r *http.Request) string {
	if addr := r.Header.Get("X-Forwarded-For"); addr != "" {
		return addr
	}
	return r.RemoteAddr
}

// contentDispositionName is the download name.
// ext includes the dot.
func contentDispositionName(ext string) string {
	return time.Now().UTC().Format("sage-download-20060102150405") + ext
}
