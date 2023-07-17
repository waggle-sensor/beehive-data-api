package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const metricNamespace = "dataapi"

var (
	responseLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Name:      "response_latency_seconds",
		Help:      "A histogram of backend latency duration.",
	})
)

type ServiceConfig struct {
	Backend Backend
}

// Service keeps the service configuration for the SDR API service.
type Service struct {
	backend Backend
}

func NewService(config *ServiceConfig) *Service {
	return &Service{backend: config.Backend}
}

// ServeHTTP parses a query request, translates and forwards it to InfluxDB
// and writes the results back to the client.
func (svc *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestStartTime := time.Now()

	remoteAddr := getRemoteAddr(r)
	log.Printf("received request from %s", remoteAddr)

	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	defer r.Body.Close()

	queryBody, err := io.ReadAll(r.Body)
	if err == io.EOF || len(queryBody) == 0 {
		log.Printf("%s error: no query provided", remoteAddr)
		http.Error(w, "error: no query provided", http.StatusBadRequest)
		return
	}
	if _, ok := err.(*http.MaxBytesError); ok {
		log.Printf("%s error: rejected large request", remoteAddr)
		http.Error(w, "error: query is too large - must be <1KB", http.StatusBadRequest)
		return
	}
	if err != nil {
		log.Printf("%s error: failed to read query body: %s", remoteAddr, err.Error())
		http.Error(w, "error: failed to read query body", http.StatusBadRequest)
		return
	}

	query, err := parseQuery(queryBody)
	if err != nil {
		log.Printf("%s error: failed to parse query: %s", remoteAddr, err.Error())
		http.Error(w, fmt.Sprintf("error: failed to parse query: %s", err.Error()), http.StatusBadRequest)
		return
	}

	log.Printf("%s query: %q", remoteAddr, queryBody)

	queryCount := 0
	queryStart := time.Now()

	results, err := svc.backend.Query(r.Context(), query)
	if err != nil {
		log.Printf("%s error: failed to query backend: %s", remoteAddr, err.Error())
		http.Error(w, fmt.Sprintf("error: failed to query backend: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer results.Close()

	w.Header().Add("Access-Control-Allow-Origin", "*")
	writeContentDispositionHeader(w)
	w.WriteHeader(http.StatusOK)

	startedWritingResults := false
	for results.Next() {
		record := results.Record()
		// observe latency to start of response body. this is what the user actually sees so its what we care about.
		if !startedWritingResults {
			responseLatencySeconds.Observe(time.Since(requestStartTime).Seconds())
			startedWritingResults = true
		}
		if err := writeRecord(w, record); err != nil {
			break
		}
		queryCount++
	}

	if err := results.Err(); err != nil {
		log.Printf("%s error: %s", remoteAddr, err)
	}

	queryDuration := time.Since(queryStart)
	responseRate := float64(queryCount) / queryDuration.Seconds()
	log.Printf("%s served %d records in %s - %f records/s", remoteAddr, queryCount, queryDuration, responseRate)
}

var metaRE = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")

func parseQuery(data []byte) (*Query, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	query := &Query{}
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

func writeRecord(w io.Writer, rec *Record) error {
	return json.NewEncoder(w).Encode(rec)
}

func writeContentDispositionHeader(w http.ResponseWriter) {
	filename := time.Now().Format("sage-download-20060102150405.ndjson")
	w.Header().Add("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
}

func getRemoteAddr(r *http.Request) string {
	if addr := r.Header.Get("X-Forwarded-For"); addr != "" {
		return addr
	}
	return r.RemoteAddr
}
