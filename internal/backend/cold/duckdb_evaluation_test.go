// duckdb_evaluation_test.go contains tests that evaluate DuckDB performance.
package cold

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"

	"beehive-data-api/internal/arrowio"
)

// TestColdDuckDBEvaluationProbe tests the cold DuckDB evaluation probe.
// Opt-in native probe. The caller supplies read-only local response SQL and
// an isolated output directory. No extensions or network are needed.
func TestColdDuckDBEvaluationProbe(t *testing.T) {
	root := os.Getenv("COLD_EVAL_DIR")
	if root == "" {
		t.Skip("opt-in DuckDB version comparison")
	}
	raw, err := os.ReadFile(os.Getenv("COLD_EVAL_SQL"))
	if err != nil {
		t.Fatal(err)
	}
	memory := os.Getenv("COLD_EVAL_MEMORY")
	if memory == "" {
		memory = "512MB"
	}
	ctx := context.Background()
	db, err := newStreamingArrowDB(ctx, []string{"SET threads=2", "SET memory_limit=" + sqlQuote(memory), "SET TimeZone='UTC'"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Read the engine actually linked into this process, not a package label.
	readInfo := func() map[string]string {
		t.Helper()
		r, err := db.query(ctx, `SELECT 'version' AS name, version()::VARCHAR AS value
   UNION ALL SELECT name, value FROM duckdb_settings()
   WHERE name IN ('threads','memory_limit','async_threads','read_ahead_depth','http_client_pool_capacity')`, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Release()
		info := map[string]string{}
		for r.Next() {
			batch := r.RecordBatch()
			names, values := batch.Column(0).(*array.String), batch.Column(1).(*array.String)
			for i := 0; i < names.Len(); i++ {
				info[strings.Clone(names.Value(i))] = strings.Clone(values.Value(i))
			}
		}
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		return info
	}
	info := readInfo()
	if !strings.HasPrefix(info["version"], "v2.") {
		t.Fatalf("expected DuckDB 2.0, linked %s", info["version"])
	}
	threads := os.Getenv("COLD_EVAL_ASYNC_THREADS")
	if threads == "" {
		threads = "8"
	}
	n, err := strconv.Atoi(threads)
	if err != nil || n < 1 {
		t.Fatal("positive COLD_EVAL_ASYNC_THREADS required")
	}
	r, err := db.newReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = r.execSetup([]string{fmt.Sprintf("SET async_threads=%d", n)})
	r.Release()
	if err != nil {
		t.Fatal(err)
	}
	info = readInfo()
	var p *coldQueryProfile
	if os.Getenv("COLD_EVAL_PROFILE") == "1" {
		p, err = newColdQueryProfile(ctx, root)
		if err != nil {
			t.Fatal(err)
		}
		ctx = context.WithValue(ctx, coldQueryProfileKey{}, p)
	}
	body, err := os.Create(filepath.Join(root, "body.arrows"))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	r, err = db.query(ctx, string(raw), nil)
	if err != nil {
		body.Close()
		p.finish(true)
		t.Fatal(err)
	}
	if err := arrowio.SchemaCompatible(r.Schema()); err != nil {
		r.Release()
		body.Close()
		p.finish(true)
		t.Fatal(err)
	}
	result := &coldArrowResults{rdr: r, release: func() {}, profile: p}
	first := 0.0
	rStart := time.Now()
	// Fetching one batch exposes first-result latency. Replay it through a reader
	// wrapper so the standard Zstd IPC path still writes exactly the full result.
	hasFirst := r.Next()
	if hasFirst {
		first = time.Since(start).Seconds()
	}
	wrapped := &coldEvalFirstReader{RecordReader: r, first: hasFirst}
	result.rdr = wrapped
	rows, writeErr := arrowio.WriteIPC(body, result, arrowio.CompressionZstd)
	closeErr := result.Close()
	elapsed := time.Since(start).Seconds()
	fileErr := body.Close()
	if writeErr != nil || closeErr != nil || fileErr != nil {
		t.Fatalf("write=%v result=%v file=%v", writeErr, closeErr, fileErr)
	}
	report := map[string]any{"engine": info, "rows": rows, "first_batch_seconds": first, "total_seconds": elapsed,
		"ready_seconds": rStart.Sub(start).Seconds(), "compression": "zstd", "profiled": p != nil,
		"scope": "native Go streaming + Zstd IPC to isolated local file; no Iceberg or network"}
	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "run.json"), append(out, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

// coldEvalFirstReader is the first reader for the cold DuckDB evaluation.
// Next normally advances and releases the previous native chunk. The first
type coldEvalFirstReader struct {
	array.RecordReader
	first bool
}

// Next advances the first reader.
// The first fetched batch has already been measured, so expose it once without advancing.
func (r *coldEvalFirstReader) Next() bool {
	if r.first {
		r.first = false
		return true
	}
	return r.RecordReader.Next()
}
