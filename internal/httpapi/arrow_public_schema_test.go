// arrow_public_schema_test.go contains the tests for the public schema across storage tiers.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"beehive-data-api/internal/arrowio"
	"beehive-data-api/internal/backend/tiered"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
)

// Check the HTTP contract without a Python client: record-built hot results,
// direct cold batches, and a stream spanning both must expose one value field.
func TestPublicSchemaAcrossStorageTiers(t *testing.T) {
	watermark := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	cold := &publicSchemaBackend{native: true, records: []*model.Record{{Timestamp: watermark.Add(-time.Hour), Name: "cold", Value: 1.25}}}
	hot := &publicSchemaBackend{records: []*model.Record{{Timestamp: watermark.Add(time.Hour), Name: "hot", Value: "image.jpg"}}}
	for _, tc := range []struct {
		name    string
		backend model.Backend
		rows    int
	}{
		{"hot", hot, 1}, {"cold", cold, 1}, {"both", &tiered.Backend{Hot: hot, Cold: cold, Watermark: watermark}, 2},
	} {
		for _, accept := range []string{"", arrowio.MIME, arrowio.MIME + "; sage-schema=2"} {
			t.Run(tc.name+"/"+accept, func(t *testing.T) {
				svc := NewService(&ServiceConfig{Backend: tc.backend})
				request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"start":"2025-04-30T00:00:00Z","end":"2025-05-02T00:00:00Z"}`))
				request.Header.Set("Accept", accept)
				response := httptest.NewRecorder()
				svc.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					t.Fatal(response.Code)
				}
				count := 0
				if accept == "" {
					decoder := json.NewDecoder(response.Body)
					for decoder.More() {
						var row map[string]json.RawMessage
						if err := decoder.Decode(&row); err != nil {
							t.Fatal(err)
						}
						if len(row) != 4 || row["value"] == nil {
							t.Fatalf("unexpected fields: %v", row)
						}
						count++
					}
				} else {
					if response.Header().Get("Content-Type") != arrowio.MIME {
						t.Fatal(response.Header())
					}
					reader, err := ipc.NewReader(response.Body)
					if err != nil {
						t.Fatal(err)
					}
					defer reader.Release()
					if !reader.Schema().Equal(arrowio.Schema) {
						t.Fatalf("public schema changed: %s", reader.Schema())
					}
					for reader.Next() {
						count += int(reader.RecordBatch().NumRows())
					}
					if reader.Err() != nil {
						t.Fatal(reader.Err())
					}
				}
				if count != tc.rows {
					t.Fatalf("rows=%d want=%d", count, tc.rows)
				}
			})
		}
	}
}

// publicSchemaBackend is the public schema backend type.
type publicSchemaBackend struct {
	native  bool
	records []*model.Record
}

// Query queries the public schema backend.
func (b *publicSchemaBackend) Query(ctx context.Context, q *model.Query) (model.Results, error) {
	if !b.native || !arrowio.WantsResult(ctx) {
		return (&testutil.Backend{Records: b.records}).Query(ctx, q)
	}
	results, err := (&testutil.Backend{Records: b.records}).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer results.Close()
	var body bytes.Buffer
	if _, err := arrowio.WriteIPC(&body, results, arrowio.CompressionNone); err != nil {
		return nil, err
	}
	reader, err := ipc.NewReader(&body)
	if err != nil {
		return nil, err
	}
	return &publicSchemaResults{reader: reader}, nil
}

// publicSchemaResults is the public schema results type.
type publicSchemaResults struct{ reader array.RecordReader }

// ArrowReader returns the arrow reader for the public schema results.
func (r *publicSchemaResults) ArrowReader() array.RecordReader { return r.reader }

// Next is unused on this result.
// The arrow handler reads ArrowReader.
func (r *publicSchemaResults) Next() bool { return false }

// Record returns the record for the public schema results.
// The arrow handler reads ArrowReader.
func (r *publicSchemaResults) Record() *model.Record { return nil }

// Err returns the error for the public schema results.
func (r *publicSchemaResults) Err() error { return r.reader.Err() }

// Close closes the public schema results.
// Releases the arrow reader.
func (r *publicSchemaResults) Close() error { r.reader.Release(); return nil }
