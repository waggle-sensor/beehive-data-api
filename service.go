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
)

type ServiceConfig struct {
	Backend             Backend
	RequestQueueSize    *int
	RequestQueueTimeout *time.Duration
	// TODO(sean) make queue size part of config
}

// Service keeps the service configuration for the SDR API service.
type Service struct {
	backend      Backend
	requestQueue *RequestQueue
}

func NewService(config *ServiceConfig) *Service {
	requestQueueSize := 10
	if config.RequestQueueSize != nil {
		requestQueueSize = *config.RequestQueueSize
	}

	requestQueueTimeout := 10 * time.Second
	if config.RequestQueueTimeout != nil {
		requestQueueTimeout = *config.RequestQueueTimeout
	}

	return &Service{
		backend:      config.Backend,
		requestQueue: NewRequestQueue(requestQueueSize, requestQueueTimeout),
	}
}

// ServeHTTP parses a query request, translates and forwards it to InfluxDB
// and writes the results back to the client.
func (svc *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	remoteAddr := getRemoteAddr(r)

	log.Printf("%s request queued", remoteAddr)
	if !svc.requestQueue.Enter() {
		log.Printf("%s error: request queue timeout", remoteAddr)
		http.Error(w, "error: request queue timeout", http.StatusServiceUnavailable)
		return
	}
	defer svc.requestQueue.Leave()
	log.Printf("%s serving request", remoteAddr)

	queryBody, err := io.ReadAll(r.Body)
	if err == io.EOF || len(queryBody) == 0 {
		log.Printf("%s error: no query provided", remoteAddr)
		http.Error(w, "error: no query provided", http.StatusBadRequest)
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
	r.Body.Close()

	log.Printf("%s query: %q", remoteAddr, queryBody)

	queryStart := time.Now()
	queryCount := 0

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

	for results.Next() {
		if err := writeRecord(w, results.Record()); err != nil {
			break
		}
		queryCount++
	}

	if err := results.Err(); err != nil {
		log.Printf("%s error: %s", remoteAddr, err)
	}

	queryDuration := time.Since(queryStart)
	log.Printf("%s served %d records in %s - %f records/s", remoteAddr, queryCount, queryDuration, float64(queryCount)/queryDuration.Seconds())
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
	// TODO(sean) figure out the right headers to use
	// // if reverse proxy provides a remote address, use that
	// forwardedFor := r.Header.Get("X-Forwarded-For")
	// if forwardedFor != "" {
	// 	return forwardedFor
	// }
	// // otherwise, fallback to built-in remote address
	return r.RemoteAddr
}
