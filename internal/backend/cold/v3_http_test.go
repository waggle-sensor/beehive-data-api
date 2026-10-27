package cold

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"beehive-data-api/internal/arrowio"
	"beehive-data-api/internal/backend/tiered"
	"beehive-data-api/internal/httpapi"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
)

// Replaces remote authentication/discovery only. SQL, NDJSON scanning, native
// Arrow streaming, tiered merging and HTTP response writers are production code.
type v3FixtureBackend struct {
	db       *sql.DB
	native   *streamingArrowDB
	metadata string
}

func (b *v3FixtureBackend) Query(ctx context.Context, q *model.Query) (model.Results, error) {
	if arrowio.WantsResult(ctx) {
		statement, err := buildColdArrowSQL(q, b.metadata, time.Now())
		if err != nil {
			return nil, err
		}
		rdr, err := b.native.query(ctx, statement, []string{"SET TimeZone='UTC'"})
		if err != nil {
			return nil, err
		}
		return &coldArrowResults{rdr: rdr, release: func() {}}, nil
	}
	statement, err := buildColdSQL(q, b.metadata, time.Now())
	if err != nil {
		return nil, err
	}
	rows, err := b.db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	return &coldResults{rows: rows, release: func() {}}, nil
}

func testV3HTTPResponses(t *testing.T, db *sql.DB, metadata string) {
	t.Helper()
	native, err := newStreamingArrowDB(context.Background(), coldDuckDBSetup(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	cold := &v3FixtureBackend{db: db, native: native, metadata: metadata}
	base := model.Query{Start: "2025-03-01T00:00:07.420753193Z", End: "2025-03-01T00:00:08.000000003Z"}
	one := 1
	head, tail, bounds := base, base, base
	head.Head = &one
	tail.Tail = &one
	bounds.End = "2025-03-01T00:00:07.420753571Z"
	hot := &testutil.Backend{Records: []*model.Record{
		{Timestamp: time.Unix(0, 1740787208000000001).UTC(), Name: "plugin.status", Value: "123", Meta: map[string]string{"plugin": "notes"}},
		{Timestamp: time.Unix(0, 1740787208000000002).UTC(), Name: "plugin.flag", Value: true, Meta: map[string]string{}},
	}}
	tiered := &tiered.Backend{Cold: cold, Hot: hot, Watermark: time.Unix(0, 1740787208000000001).UTC()}
	for _, tc := range []struct {
		name    string
		backend model.Backend
		query   model.Query
		values  []string
		stamps  []int64
	}{
		{"cold", cold, base, []string{"21.5", "9007199254740993", `"123"`, "true"}, []int64{1740787207420753193, 1740787207420753571, 1740787208000000001, 1740787208000000002}},
		{"head", cold, head, []string{"21.5", `"123"`, "true"}, []int64{1740787207420753193, 1740787208000000001, 1740787208000000002}},
		{"tail", cold, tail, []string{"9007199254740993", `"123"`, "true"}, []int64{1740787207420753571, 1740787208000000001, 1740787208000000002}},
		{"exclusive_ns_bound", cold, bounds, []string{"21.5"}, []int64{1740787207420753193}},
		{"tiered", tiered, base, []string{"21.5", "9007199254740993", `"123"`, "true"}, []int64{1740787207420753193, 1740787207420753571, 1740787208000000001, 1740787208000000002}},
	} {
		for _, format := range []string{"ndjson", "arrow"} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				body, _ := json.Marshal(tc.query)
				req := httptest.NewRequest("POST", "/api/v1/query", bytes.NewReader(body))
				if format == "arrow" {
					req.Header.Set("Accept", arrowio.MIME)
				}
				response := httptest.NewRecorder()
				httpapi.NewService(&httpapi.ServiceConfig{Backend: tc.backend}).ServeHTTP(response, req)
				if response.Code != 200 {
					t.Fatalf("HTTP %d: %s", response.Code, response.Body)
				}
				// Keep an optional copy of the actual production HTTP writer's body
				// for cross-repository Sage-client compatibility checks.
				rawBody := append([]byte(nil), response.Body.Bytes()...)
				var values []string
				var stamps []int64
				var names []string
				var metas []map[string]string
				if format == "arrow" {
					reader, err := ipc.NewReader(response.Body)
					if err != nil {
						t.Fatal(err)
					}
					defer reader.Release()
					if !reader.Schema().Equal(arrowio.Schema) {
						t.Fatalf("public schema changed: %s", reader.Schema())
					}
					for reader.Next() {
						record := reader.RecordBatch()
						for i := 0; i < int(record.NumRows()); i++ {
							values = append(values, record.Column(arrowio.FieldValue).(*array.String).Value(i))
							stamps = append(stamps, int64(record.Column(arrowio.FieldTimestamp).(*array.Timestamp).Value(i)))
							names = append(names, record.Column(arrowio.FieldName).(*array.String).Value(i))
							var meta map[string]string
							if err := json.Unmarshal([]byte(record.Column(arrowio.FieldMeta).(*array.String).Value(i)), &meta); err != nil {
								t.Fatal(err)
							}
							metas = append(metas, meta)
						}
					}
					if err := reader.Err(); err != nil {
						t.Fatal(err)
					}
				} else {
					decoder := json.NewDecoder(response.Body)
					for {
						var record struct {
							Timestamp time.Time
							Name      string
							Value     json.RawMessage
							Meta      map[string]string
						}
						err := decoder.Decode(&record)
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						values = append(values, string(record.Value))
						stamps = append(stamps, record.Timestamp.UnixNano())
						names = append(names, record.Name)
						metas = append(metas, record.Meta)
					}
				}
				if !reflect.DeepEqual(values, tc.values) || !reflect.DeepEqual(stamps, tc.stamps) {
					t.Fatalf("got %v %v; want %v %v", values, stamps, tc.values, tc.stamps)
				}
				for i, name := range names {
					want := map[string]string{}
					if name == "wxt.env.temp" {
						want = map[string]string{"sensor": "sht", "units": "C"}
					}
					if name == "plugin.status" {
						want = map[string]string{"plugin": "notes"}
					}
					if !reflect.DeepEqual(metas[i], want) {
						t.Fatalf("meta for %s: %#v", name, metas[i])
					}
				}
				if root := os.Getenv("COLD_V3_HTTP_FIXTURE_DIR"); root != "" {
					root, err := filepath.Abs(root)
					if err != nil {
						t.Fatal(err)
					}
					for _, part := range strings.Split(filepath.ToSlash(root), "/") {
						if part == "node-data" {
							t.Fatal("fixture exports must be isolated from /node-data")
						}
					}
					if err := os.MkdirAll(root, 0700); err != nil {
						t.Fatal(err)
					}
					stem := tc.name + "-" + format
					// Exclusive creation preserves previous fixtures on accidental replay.
					write := func(name string, data []byte) {
						t.Helper()
						f, err := os.OpenFile(filepath.Join(root, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
						if err != nil {
							t.Fatal(err)
						}
						_, writeErr := f.Write(data)
						closeErr := f.Close()
						if writeErr != nil || closeErr != nil {
							t.Fatalf("fixture write=%v close=%v", writeErr, closeErr)
						}
					}
					write(stem+".body", rawBody)
					manifest, err := json.MarshalIndent(map[string]any{
						"case": tc.name, "format": format, "headers": map[string]string{
							"Content-Type":        response.Header().Get("Content-Type"),
							"X-Arrow-Compression": response.Header().Get("X-Arrow-Compression")},
						"values": tc.values, "timestamps_ns": tc.stamps, "names": names, "metadata": metas,
					}, "", "  ")
					if err != nil {
						t.Fatal(err)
					}
					write(stem+".json", manifest)
				}
			})
		}
	}
}
