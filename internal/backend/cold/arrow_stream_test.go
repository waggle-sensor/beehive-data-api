// arrow_stream_test.go contains the tests for the streaming arrow reader type.
package cold

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	bindings "github.com/duckdb/duckdb-go-bindings"
	duckdb "github.com/duckdb/duckdb-go/v2"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
)

// wideArrowQuery returns the wide arrow query.
func wideArrowQuery(rows int) string {
	return coldArrowProjection(fmt.Sprintf(`SELECT
        TIMESTAMPTZ '2025-03-01' + (i %% 100003) * INTERVAL '1 microsecond' AS timestamp,
        'wxt.env.temp' AS name, i::BIGINT::VARIANT AS value,
        MAP {'vsn':'W0A0', 'plugin':repeat(md5(i::VARCHAR), 8)} AS meta
        FROM range(%d) t(i)`, rows))
}

// TestStreamingColdArrow tests the streaming cold arrow.
func TestStreamingColdArrow(t *testing.T) {
	rdr, err := newStreamingArrowReader(context.Background(), wideArrowQuery(200003),
		[]string{"SET threads=2", "SET memory_limit='256MB'", "SET TimeZone='UTC'"})
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	if !bindings.ResultIsStreaming(rdr.res) {
		t.Fatal("SELECT materialized its result")
	}
	if err := arrowio.SchemaCompatible(rdr.Schema()); err != nil {
		t.Fatal(err)
	}
	var n int
	var previous int64
	seen := make(map[string]bool)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		ts, values := rec.Column(0).(*array.Timestamp), rec.Column(2).(*array.String)
		for i := 0; i < ts.Len(); i++ {
			if int64(ts.Value(i)) < previous {
				t.Fatal("timestamps out of order")
			}
			previous = int64(ts.Value(i))
			if seen[values.Value(i)] {
				t.Fatal("duplicate value")
			}
			// Arrow strings alias native batch buffers, released by Next.
			seen[strings.Clone(values.Value(i))] = true
			n++
		}
	}
	if rdr.Err() != nil {
		t.Fatal(rdr.Err())
	}
	if n != 200003 {
		t.Fatalf("rows %d", n)
	}
	temp := rdr.tempDir
	rdr.Release() // A second Release must not double-free native buffers.
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("spill directory remains: %v", err)
	}
}

// TestStreamingColdArrowCancellationAndErrors tests the streaming cold arrow cancellation and errors.
func TestStreamingColdArrowCancellationAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rdr, err := newStreamingArrowReader(ctx, "SELECT i FROM range(1000000000) t(i)", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rdr.Next() {
		t.Fatal(rdr.Err())
	}
	cancel()
	if rdr.Next() || !errors.Is(rdr.Err(), context.Canceled) {
		t.Fatalf("cancellation: %v", rdr.Err())
	}
	rdr.Release()
	if _, err := newStreamingArrowReader(ctx, "SELECT 1", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context: %v", err)
	}
	if _, err := newStreamingArrowReader(context.Background(), "SELECT missing_column", nil); err == nil {
		t.Fatal("missing prepare error")
	}
	if _, err := newStreamingArrowReader(context.Background(), "SELECT 1", []string{"SET nonexistent_setting=1"}); err == nil {
		t.Fatal("missing setup error")
	}
}

// TestStreamingColdArrowInterruptsExecution tests the streaming cold arrow interrupts execution.
func TestStreamingColdArrowInterruptsExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	// DuckDB 2.0 returns the streaming result before the aggregate runs, so the
	// first fetch is the execution that must stop when the deadline expires.
	rdr, err := newStreamingArrowReader(ctx,
		"SELECT sum(sin(i::DOUBLE)) FROM range(1000000000) t(i)", []string{"SET threads=2"})
	if err == nil {
		defer rdr.Release()
		if rdr.Next() {
			t.Fatal("aggregate produced a row before cancellation")
		}
		err = rdr.Err()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("execution cancellation: %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("canceled execution did not stop promptly")
	}
}

// TestStreamingColdArrowEmptyAndIPC tests the streaming cold arrow empty and IPC.
func TestStreamingColdArrowEmptyAndIPC(t *testing.T) {
	for _, rows := range []int{0, 10003} {
		t.Run(strconv.Itoa(rows), func(t *testing.T) {
			rdr, err := newStreamingArrowReader(context.Background(), wideArrowQuery(rows), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rdr.Release()
			var body bytes.Buffer
			result := &coldArrowResults{rdr: rdr, release: func() {}}
			n, err := arrowio.WriteIPC(&body, result, arrowio.CompressionNone)
			if err != nil || n != rows {
				t.Fatalf("IPC write rows=%d err=%v", n, err)
			}
			reader, err := ipc.NewReader(&body)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			count := int64(0)
			for reader.Next() {
				count += reader.RecordBatch().NumRows()
			}
			if reader.Err() != nil || count != int64(rows) {
				t.Fatalf("IPC read rows=%d err=%v", count, reader.Err())
			}
		})
	}
}

// Exercise the production entry point when official extensions are already
// cached. Default tests stay independent of extension downloads and Pelican.
// TestColdArrowBackendCachedExtensions tests the cold arrow backend cached extensions.
func TestColdArrowBackendCachedExtensions(t *testing.T) {
	if os.Getenv("BEEHIVE_ARROW_CACHED_EXTENSIONS") == "" {
		t.Skip("requires cached httpfs and iceberg extensions")
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	backend := &Backend{connector: connector}
	defer backend.Close()
	var first *streamingArrowDB
	for _, rows := range []int{10003, 0} {
		result, err := backend.queryArrow(context.Background(), "local-test-no-remote-requests", wideArrowQuery(rows))
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = backend.arrowDB
		} else if first != backend.arrowDB {
			t.Fatal("second request recreated the Arrow database")
		}
		n, err := arrowio.WriteIPC(io.Discard, result, arrowio.CompressionNone)
		closeErr := result.Close()
		if err != nil || closeErr != nil || n != rows {
			t.Fatalf("rows=%d err=%v close=%v", n, err, closeErr)
		}
	}
	temp := first.tempDir
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("backend spill directory remains: %v", err)
	}
	if _, err := backend.queryArrow(context.Background(), "unused", wideArrowQuery(0)); err == nil {
		t.Fatal("closed backend accepted a query")
	}
}

// TestStreamingArrowDatabaseReuse tests the streaming arrow database reuse.
func TestStreamingArrowDatabaseReuse(t *testing.T) {
	ctx := context.Background()
	db, err := newStreamingArrowDB(ctx, []string{
		"SET threads=2", "SET memory_limit='256MB'",
		"CREATE TABLE initialization_marker AS SELECT 42::BIGINT AS value",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queryValue := func() {
		t.Helper()
		r, err := db.query(ctx, "SELECT value FROM initialization_marker", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Release()
		if !r.Next() || r.RecordBatch().Column(0).(*array.Int64).Value(0) != 42 {
			t.Fatalf("database state not preserved: %v", r.Err())
		}
	}
	queryValue()
	// Closing an unfinished result releases its connection, not the database.
	r, err := db.query(ctx, "SELECT i FROM range(1000000000) t(i)", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Next() {
		t.Fatal(r.Err())
	}
	r.Release()
	queryValue()
	// An old context canceled after release cannot interrupt the next query.
	old, cancelOld := context.WithCancel(ctx)
	r, err = db.query(old, "SELECT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Release()
	cancelOld()
	queryValue()
	// Cancellation while executing must also leave the shared database usable.
	// DuckDB 2.0 does the aggregate work on the first fetch, after query returns.
	canceled, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	r, err = db.query(canceled, "SELECT sum(sin(i::DOUBLE)) FROM range(1000000000) t(i)", nil)
	if err == nil {
		if r.Next() {
			r.Release()
			t.Fatal("aggregate produced a row before cancellation")
		}
		err = r.Err()
		r.Release()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation: %v", err)
	}
	queryValue()
	if _, err := db.query(ctx, "SELECT nonexistent_column", nil); err == nil {
		t.Fatal("missing prepare error")
	}
	queryValue()
	temp := db.tempDir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("spill directory remains: %v", err)
	}
	if _, err := db.query(ctx, "SELECT 1", nil); err == nil {
		t.Fatal("closed database accepted query")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestStreamingArrowInitializationFailureCleansUp tests the streaming arrow initialization failure cleans up.
func TestStreamingArrowInitializationFailureCleansUp(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	if _, err := newStreamingArrowDB(context.Background(), []string{"SET nonexistent_setting=1"}); err == nil {
		t.Fatal("missing initialization error")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed initialization left spill files: entries=%v err=%v", entries, err)
	}
}

// Measures cached extension setup and connection overhead, excluding Pelican.
func BenchmarkColdArrowStartup(b *testing.B) {
	if os.Getenv("BEEHIVE_ARROW_CACHED_EXTENSIONS") == "" {
		b.Skip("requires cached extensions")
	}
	for _, reuse := range []bool{false, true} {
		label := "per-query-database"
		if reuse {
			label = "reused-database"
		}
		b.Run(label, func(b *testing.B) {
			var db *streamingArrowDB
			if reuse {
				var err error
				db, err = newStreamingArrowDB(context.Background(), coldDuckDBSetup(nil))
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
			}
			query := wideArrowQuery(698)
			perRequest := []string{"SET TimeZone='UTC'", "CREATE OR REPLACE SECRET pelican (TYPE http, EXTRA_HTTP_HEADERS MAP {'Authorization': 'Bearer local-benchmark'})"}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var r *streamingArrowReader
				var err error
				if reuse {
					r, err = db.query(context.Background(), query, perRequest)
				} else {
					r, err = newStreamingArrowReader(context.Background(), query, append(coldDuckDBSetup(nil), perRequest...))
				}
				if err != nil {
					b.Fatal(err)
				}
				n, err := arrowio.WriteIPC(io.Discard, &coldArrowResults{rdr: r, release: func() {}}, arrowio.CompressionNone)
				r.Release()
				if err != nil || n != 698 {
					b.Fatalf("rows=%d error=%v", n, err)
				}
			}
			b.StopTimer()
		})
	}
}

// Run in separate processes to compare native peak RSS, not Go heap alone.
// exp51_cold_arrow_materialization.py drives this opt-in probe.
// TestColdArrowMemoryProbe tests the cold arrow memory probe.
func TestColdArrowMemoryProbe(t *testing.T) {
	mode := os.Getenv("BEEHIVE_ARROW_PROBE")
	if mode == "" {
		t.Skip("opt-in memory experiment")
	}
	n, err := strconv.Atoi(os.Getenv("BEEHIVE_ARROW_ROWS"))
	if err != nil || n <= 0 {
		t.Fatal("BEEHIVE_ARROW_ROWS must be positive")
	}
	limit := os.Getenv("BEEHIVE_ARROW_MEMORY")
	if limit == "" {
		limit = "256MB"
	}
	setup := []string{"SET threads=2", "SET memory_limit=" + sqlQuote(limit), "SET TimeZone='UTC'"}
	statement := wideArrowQuery(n)
	if source := os.Getenv("BEEHIVE_ARROW_SOURCE"); source != "" {
		statement, err = buildColdArrowSQL(&model.Query{
			Start: "2025-03-01T00:00:00Z", End: "2025-04-01T00:00:00Z",
			Filter: map[string]string{"name": "wxt.env.temp", "vsn": "W0A0"},
		}, "local-archive", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		statement = strings.Replace(statement, "iceberg_scan('local-archive')", "read_parquet("+sqlQuote(source)+")", 1)
	}
	start := time.Now()
	var rdr array.RecordReader
	if mode == "stream" {
		rdr, err = newStreamingArrowReader(context.Background(), statement, setup)
	} else if mode == "driver" {
		connector, e := duckdb.NewConnector("", nil)
		if e != nil {
			t.Fatal(e)
		}
		defer connector.Close()
		drv, e := connector.Connect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		defer drv.Close()
		conn, e := duckdb.NewArrowFromConn(drv)
		if e != nil {
			t.Fatal(e)
		}
		rdr, err = conn.QueryContext(context.Background(), "SET threads=2; SET memory_limit="+sqlQuote(limit)+"; SET TimeZone='UTC'; "+statement)
	} else {
		t.Fatal("mode must be stream or driver")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	fmt.Printf("PROBE ready_seconds=%.6f\n", time.Since(start).Seconds())
	w := ipc.NewWriter(io.Discard, ipc.WithSchema(arrowio.Schema))
	got, err := arrowio.WriteReader(w, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got != n {
		t.Fatalf("rows %d want %d", got, n)
	}
	fmt.Printf("PROBE rows=%d total_seconds=%.6f\n", got, time.Since(start).Seconds())
}

// Keep the public schema and exact metadata text while exercising the native
// streaming API with both the JSON fast path and promoted-key fallback.
// TestStreamingColdMetadataOptimization tests the streaming cold metadata optimization.
func TestStreamingColdMetadataOptimization(t *testing.T) {
	q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-02T00:00:00Z"}
	inner, err := buildColdInner(q, "local-metadata-test", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := buildColdArrowSQL(q, "local-metadata-test", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	source := `(SELECT CAST(TIMESTAMPTZ '2025-03-01' + i * INTERVAL '1 microsecond' AS TIMESTAMPTZ_NS) AS time,
  'test' AS name, (i*0.1)::DOUBLE::VARIANT AS value,
  CASE WHEN i%4=0 THEN MAP {'plugin':'raw','sensor':'quoted"text'}
       WHEN i%4=1 THEN NULL::MAP(VARCHAR,VARCHAR)
       ELSE MAP {'sensor':'中文','units':'C'} END AS meta,
  CASE WHEN i%3=0 THEN NULL::VARCHAR ELSE 'promoted' END AS plugin,
  'W0A0' AS vsn, NULL::VARCHAR AS node,NULL::VARCHAR AS host,
  NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone
  FROM range(50003) t(i))`
	statements := []string{coldArrowProjection(inner), optimized}
	readers := make([]*streamingArrowReader, 2)
	for i, statement := range statements {
		statement = strings.Replace(statement, "iceberg_scan('local-metadata-test')", source, 1)
		readers[i], err = newStreamingArrowReader(context.Background(), statement,
			[]string{"SET threads=2", "SET memory_limit='256MB'", "SET TimeZone='UTC'"})
		if err != nil {
			t.Fatal(err)
		}
		defer readers[i].Release()
		if !bindings.ResultIsStreaming(readers[i].res) {
			t.Fatal("result materialized")
		}
		if err := arrowio.SchemaCompatible(readers[i].Schema()); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for readers[0].Next() {
		if !readers[1].Next() || !array.RecordEqual(readers[0].RecordBatch(), readers[1].RecordBatch()) {
			t.Fatal("native response rows/schema/JSON changed")
		}
		count += int(readers[0].RecordBatch().NumRows())
	}
	if readers[1].Next() || readers[0].Err() != nil || readers[1].Err() != nil || count != 50003 {
		t.Fatalf("rows=%d errors=%v,%v", count, readers[0].Err(), readers[1].Err())
	}
}

// Run with the pinned native bindings in the VM/image build. Verify the actual
// streaming C API flushes a detailed profile without capturing setup SQL.
// TestStreamingColdQueryProfile tests the streaming cold query profile.
func TestStreamingColdQueryProfile(t *testing.T) {
	dir, err := NewProfileSession(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := newColdQueryProfile(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), coldQueryProfileKey{}, p)
	rdr, err := newStreamingArrowReader(ctx, wideArrowQuery(10001), []string{"SET VARIABLE diagnostic_setup='do-not-capture-this-marker'"})
	if err != nil {
		p.finish(true)
		t.Fatal(err)
	}
	var rows int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	readErr := rdr.Err()
	rdr.Release()
	p.finish(readErr != nil)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if rows != 10001 {
		t.Fatalf("rows: %d", rows)
	}
	raw, err := os.ReadFile(p.file)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("ORDER_BY")) || !bytes.Contains(raw, []byte(`"planner"`)) {
		t.Fatalf("missing detailed query profile: %s", raw)
	}
	if bytes.Contains(raw, []byte("do-not-capture-this-marker")) {
		t.Fatal("setup statement leaked into query profile")
	}
	if !p.eof {
		t.Fatal("native EOF was not recorded")
	}
}

// Test the actual linked engine and persistent settings on a new connection.
// Runtime settings intentionally require no extension download or credentials.
// TestStreamingDuckDB20Settings tests the streaming DuckDB 2.0 settings.
func TestStreamingDuckDB20Settings(t *testing.T) {
	db, err := newStreamingArrowDB(context.Background(), coldDuckDBRuntimeSetup(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 2; i++ {
		r, err := db.query(context.Background(), `SELECT version()::VARCHAR,
   current_setting('threads')::BIGINT, current_setting('async_threads')::BIGINT,
   current_setting('read_ahead_depth')::BIGINT, current_setting('arrow_output_version')::VARCHAR`, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !r.Next() {
			r.Release()
			t.Fatal("missing engine settings")
		}
		rec := r.RecordBatch()
		version := rec.Column(0).(*array.String).Value(0)
		if !strings.HasPrefix(version, "v2.0.") {
			r.Release()
			t.Fatalf("expected DuckDB 2.0, got %s", version)
		}
		if rec.Column(1).(*array.Int64).Value(0) != 2 || rec.Column(2).(*array.Int64).Value(0) != 8 || rec.Column(3).(*array.Int64).Value(0) != -1 || rec.Column(4).(*array.String).Value(0) != "1.0" {
			r.Release()
			t.Fatal("runtime settings did not persist")
		}
		r.Release()
	}
}

// Every promoted-key presence mask must retain exact JSON text, including
// empty values, null raw tags and raw/promoted collisions. No extension or
// archive fixture is required, so this remains a native CI regression.
// TestStreamingColdPromotedJSONShapes tests the streaming cold promoted JSON shapes.
func TestStreamingColdPromotedJSONShapes(t *testing.T) {
	columns := []string{
		"TIMESTAMPTZ '2025-03-01' + i * INTERVAL '1 microsecond' AS timestamp",
		"'test' AS name", "CASE WHEN i%3=0 THEN (i*0.1)::DOUBLE::VARIANT WHEN i%3=1 THEN (9007199254740993+i)::BIGINT::VARIANT ELSE 'text'::VARIANT END AS value",
		`CASE WHEN i%8=0 THEN NULL::MAP(VARCHAR,VARCHAR)
   WHEN i%8=1 THEN MAP {}::MAP(VARCHAR,VARCHAR)
   WHEN i%8=2 THEN MAP {'plugin':'raw','vsn':'raw'}
   WHEN i%8=3 THEN MAP {'job':NULL::VARCHAR,'zone':'raw'}
   WHEN i%8=4 THEN MAP {'sensor':NULL::VARCHAR,'VSN':'caps'}
   WHEN i%8=5 THEN MAP {'x':'}'}
   WHEN i%8=6 THEN MAP {'x':'{'}
   ELSE MAP {'sensor':'quoted"text','units':'中文'} END AS meta`,
	}
	for bit, tag := range promotedTags {
		columns = append(columns, fmt.Sprintf("CASE WHEN ((i//8) & %d) != 0 THEN CASE WHEN i%%5=0 THEN '' ELSE %s END END::VARCHAR AS %s", 1<<bit, sqlQuote(tag+"\"\\\n中文"), tag))
	}
	source := "SELECT " + strings.Join(columns, ",\n") + " FROM range(1024) t(i)"
	statements := []string{coldArrowProjectionWithMeta(source, coldMetaBeforeProjectionOptimization()), coldArrowProjectionWithMeta(source, coldMetaJSONExpr())}
	readers := make([]*streamingArrowReader, 2)
	for i, sql := range statements {
		var err error
		readers[i], err = newStreamingArrowReader(context.Background(), sql, []string{"SET TimeZone='UTC'", "SET threads=2", "SET memory_limit='256MB'"})
		if err != nil {
			t.Fatal(err)
		}
		defer readers[i].Release()
	}
	var rows int64
	for readers[0].Next() {
		if !readers[1].Next() || !array.RecordEqual(readers[0].RecordBatch(), readers[1].RecordBatch()) {
			t.Fatal("promoted JSON shape changed public response")
		}
		rows += readers[0].RecordBatch().NumRows()
	}
	if readers[1].Next() || readers[0].Err() != nil || readers[1].Err() != nil || rows != 1024 {
		t.Fatalf("rows=%d errors=%v,%v", rows, readers[0].Err(), readers[1].Err())
	}
}
