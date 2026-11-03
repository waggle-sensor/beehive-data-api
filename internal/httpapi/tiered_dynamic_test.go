// tiered_dynamic_test.go exercises the HTTP encoders around a managed grant,
// including the native Arrow reader that must survive the ownership wrapper.

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"beehive-data-api/internal/arrowio"
	"beehive-data-api/internal/backend/tiered"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type dynamicTransport func(*http.Request) (*http.Response, error)

func (f dynamicTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type dynamicInner func(context.Context, *model.Query) (model.Results, error)

func (f dynamicInner) Query(ctx context.Context, q *model.Query) (model.Results, error) {
	return f(ctx, q)
}

type dynamicNative struct {
	*testutil.Results
	reader array.RecordReader
	native bool
	t      *testing.T
}

func (r *dynamicNative) ArrowReader() array.RecordReader { return r.reader }
func (r *dynamicNative) Next() bool {
	if r.native {
		r.t.Fatal("managed Arrow fell back to Go row iteration")
	}
	return r.Results.Next()
}
func (r *dynamicNative) Close() error { r.reader.Release(); return r.Results.Close() }

// Exercise the real HTTP encoders around a managed grant, including the native
// Arrow capability that must survive the lifetime/ownership wrapper.
func TestDynamicTieredHTTPFormatsPinViewAndReleaseAfterClose(t *testing.T) {
	warehouse := "https://origin/sage/cold-storage-archive-test/warehouse"
	stamp := time.Unix(0, 1740787200000000194).UTC()
	record := &model.Record{Timestamp: stamp, Name: "count", Value: int64(9007199254740993), Meta: map[string]string{"node": "n1"}}
	var selected string
	var frontier time.Time
	var active *dynamicNative
	acquired, released := 0, 0
	client := &http.Client{Transport: dynamicTransport(func(req *http.Request) (*http.Response, error) {
		var body map[string]string
		if e := json.NewDecoder(req.Body).Decode(&body); e != nil {
			t.Fatal(e)
		}
		payload := []byte(`{"ok":true}`)
		if strings.HasSuffix(req.URL.Path, "acquire") {
			acquired++
			frontier = time.Date(2025, 3, 1+acquired, 0, 0, 0, 0, time.UTC)
			selected = warehouse + "/sage/obs/metadata/head-" + frontier.Format("20060102") + ".metadata.json"
			payload, _ = json.Marshal(tiered.View{ExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339Nano), ID: body["id"], Warehouse: warehouse, Metadata: selected, Frontier: frontier.Format(time.RFC3339Nano), CoverageStart: "2025-03-01T00:00:00Z"})
		} else {
			if active == nil || !active.Closed {
				t.Fatal("released read grant before result resources closed")
			}
			released++
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header)}, nil
	})}
	reader := dynamicInner(func(ctx context.Context, _ *model.Query) (model.Results, error) {
		if model.ColdMetadata(ctx) != selected {
			t.Fatal("cold execution used a different snapshot")
		}
		var plain bytes.Buffer
		if _, e := arrowio.WriteIPC(&plain, &testutil.Results{Records: []*model.Record{record}}, arrowio.CompressionNone); e != nil {
			t.Fatal(e)
		}
		reader, e := ipc.NewReader(bytes.NewReader(plain.Bytes()))
		if e != nil {
			t.Fatal(e)
		}
		active = &dynamicNative{Results: &testutil.Results{Records: []*model.Record{record}}, reader: reader, native: arrowio.WantsResult(ctx), t: t}
		return active, nil
	})
	backend := &tiered.Backend{Cold: reader, Hot: dynamicInner(func(context.Context, *model.Query) (model.Results, error) {
		t.Fatal("cold request routed to hot tier")
		return nil, nil
	}), Coordinator: &tiered.Coordinator{URL: "https://coordinator", Token: "reader", Owner: "api-pod", Bucket: "waggle", Warehouse: warehouse, Client: client}}
	service := NewService(&ServiceConfig{Backend: backend})
	for _, format := range []string{arrowio.MIME, "application/x-ndjson"} {
		req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"start":"2025-03-01T00:00:00Z","end":"2025-03-01T01:00:00Z"}`))
		req.Header.Set("Accept", format)
		response := httptest.NewRecorder()
		service.ServeHTTP(response, req)
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
		if released != acquired {
			t.Fatal("read grant not released")
		}
		if format == arrowio.MIME {
			reader, e := ipc.NewReader(bytes.NewReader(response.Body.Bytes()))
			if e != nil {
				t.Fatal(e)
			}
			if !reader.Next() {
				t.Fatal(reader.Err())
			}
			batch := reader.RecordBatch()
			if batch.NumCols() != 4 || batch.Column(0).(*array.Timestamp).Value(0) != 1740787200000000194 || batch.Column(2).(*array.String).Value(0) != "9007199254740993" {
				t.Fatal(batch)
			}
			if reader.Next() || reader.Err() != nil {
				t.Fatal("unexpected additional batch", reader.Err())
			}
			reader.Release()
		} else {
			var row struct {
				Timestamp string          `json:"timestamp"`
				Value     json.RawMessage `json:"value"`
			}
			if e := json.Unmarshal(response.Body.Bytes(), &row); e != nil {
				t.Fatal(e)
			}
			if row.Timestamp != stamp.Format(time.RFC3339Nano) || string(row.Value) != "9007199254740993" {
				t.Fatal(row)
			}
		}
	}
}
