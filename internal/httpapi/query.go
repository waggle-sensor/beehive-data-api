// query.go contains the Service type and its associated functions for the public HTTP API.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
)

const metricNamespace = "dataapi"

var (
	responseLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Name:      "response_latency_seconds",
		Help:      "A histogram of backend latency duration.",
	})
)

// ServiceConfig is the configuration for the Service.
type ServiceConfig struct {
	Backend          model.Backend
	ArrowCompression arrowio.Compression
}

// Service is the service for the SDR API.
type Service struct {
	backend          model.Backend
	arrowCompression arrowio.Compression
}

// NewService creates a new Service.
func NewService(config *ServiceConfig) *Service {
	compression := config.ArrowCompression
	if compression == "" {
		compression = arrowio.CompressionZstd
	}
	return &Service{backend: config.Backend, arrowCompression: compression}
}

// ServeHTTP parses a query request, translates and forwards it to the backend
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

	ctx := r.Context()
	arrowResult := arrowio.AcceptsStream(r.Header.Get("Accept"))
	compression := svc.arrowCompression
	if arrowResult {
		ctx = arrowio.WithResult(ctx)
	}
	results, err := svc.backend.Query(ctx, query)
	if err != nil {
		log.Printf("%s error: failed to query backend: %s", remoteAddr, err.Error())
		http.Error(w, fmt.Sprintf("error: failed to query backend: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	defer results.Close()

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
		responseLatencySeconds.Observe(time.Since(requestStartTime).Seconds())
		n, err := arrowio.WriteIPC(w, results, compression)
		queryCount = n
		if err != nil {
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
				responseLatencySeconds.Observe(time.Since(requestStartTime).Seconds())
				startedWritingResults = true
			}
			if err := writeRecord(w, record); err != nil {
				break
			}
			queryCount++
		}
	}

	if err := results.Err(); err != nil {
		log.Printf("%s error: %s", remoteAddr, err)
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

// writeRecord writes a record to the writer.
func writeRecord(w io.Writer, rec *model.Record) error {
	return json.NewEncoder(w).Encode(rec)
}

// writeContentDispositionHeader writes a content disposition header to the writer.
func writeContentDispositionHeader(w http.ResponseWriter, ext string) {
	w.Header().Add("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, contentDispositionName(ext)))
}

// getRemoteAddr gets the remote address from the request.
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
