// arrow_compression_query_test.go contains the tests for the arrow compression service.
package httpapi

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
)

// TestArrowCompressionSelection tests the arrow compression selection.
func TestArrowCompressionSelection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured arrowio.Compression
		requested  string
		want       string
	}{
		{"default", "", "lz4", "zstd"},
		{"configured", arrowio.CompressionZstd, "none", "zstd"},
		{"operator none", arrowio.CompressionNone, "zstd", "none"},
		{"operator lz4", arrowio.CompressionLZ4, "zstd", "lz4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &compressionCountingBackend{}
			// Create a new compression service with the given configuration.
			svc := NewService(&ServiceConfig{Backend: backend, ArrowCompression: tc.configured})
			// Create a new HTTP request with the given configuration.
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"start":"-4h"}`))
			// Set the accept header to the arrow stream MIME type.
			req.Header.Set("Accept", arrowio.MIME)
			req.Header.Set(arrowio.CompressionHeader, tc.requested)
			// Create a new HTTP response recorder.
			response := httptest.NewRecorder()
			// Serve the request.
			svc.ServeHTTP(response, req)
			// Check the response code and header.
			if response.Code != http.StatusOK || response.Header().Get(arrowio.CompressionHeader) != tc.want {
				t.Fatalf("response %d %v", response.Code, response.Header())
			}
			// Check the content encoding header.
			if response.Header().Get("Content-Encoding") != "" {
				t.Fatal("IPC codec incorrectly advertised as HTTP encoding")
			}
			// Create a new IPC reader from the response body.
			rdr, err := ipc.NewReader(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			defer rdr.Release()
			// Check the next record batch.
			if !rdr.Next() || rdr.RecordBatch().NumRows() != 1 {
				t.Fatal("missing row")
			}
			// Check the next record batch.
			if rdr.Next() || rdr.Err() != nil {
				t.Fatal("invalid stream")
			}
			// Check the backend calls.
			if backend.calls != 1 {
				t.Fatalf("backend calls %d", backend.calls)
			}
		})
	}
}

// TestArrowCompressionInvalidRequestAndNDJSON tests the arrow compression invalid request and NDJSON.
func TestArrowCompressionInvalidRequestAndNDJSON(t *testing.T) {
	for _, arrow := range []bool{false, true} {
		backend := &compressionCountingBackend{}
		svc := NewService(&ServiceConfig{Backend: backend, ArrowCompression: arrowio.CompressionZstd})
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"start":"-4h"}`))
		if arrow {
			req.Header.Set("Accept", arrowio.MIME)
		}
		req.Header.Set(arrowio.CompressionHeader, "invalid")
		response := httptest.NewRecorder()
		svc.ServeHTTP(response, req)
		if arrow {
			if response.Code != http.StatusOK || backend.calls != 1 {
				t.Fatal("request codec header was not ignored")
			}
			if response.Header().Get(arrowio.CompressionHeader) != string(arrowio.CompressionZstd) {
				t.Fatal("request codec header changed the response")
			}
		} else {
			if response.Code != http.StatusOK || backend.calls != 1 {
				t.Fatal("NDJSON request affected by Arrow setting")
			}
			if response.Header().Get(arrowio.CompressionHeader) != "" {
				t.Fatal("NDJSON advertised IPC compression")
			}
			raw, err := io.ReadAll(response.Body)
			if err != nil || !bytes.Contains(raw, []byte(`"value":1`)) {
				t.Fatalf("NDJSON body %q, %v", raw, err)
			}
		}
	}
}

// compressionCountingBackend is the compression counting backend type.
type compressionCountingBackend struct{ calls int }

// Query queries the compression counting backend.
func (b *compressionCountingBackend) Query(ctx context.Context, q *model.Query) (model.Results, error) {
	b.calls++
	return (&testutil.Backend{Records: []*model.Record{{Timestamp: time.Unix(0, 0), Name: "test", Value: 1}}}).Query(ctx, q)
}
