// compression_test.go contains the tests for the arrow compression.
package arrowio

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
)

// compressionBatchResults is the compression batch results type.
// This adapter exercises the same direct batch path used by cold results.
type compressionBatchResults struct {
	*testutil.Results
	reader array.RecordReader
}

// ArrowReader returns the arrow reader for the compression batch results.
func (r *compressionBatchResults) ArrowReader() array.RecordReader { return r.reader }

// TestArrowCompressionRoundTrip tests the arrow compression round trip.
func TestArrowCompressionRoundTrip(t *testing.T) {
	records := make([]*model.Record, 1024)
	for i := range records {
		records[i] = &model.Record{Timestamp: time.Unix(0, int64(i)), Name: "test", Value: i,
			Meta: map[string]string{"tag": strings.Repeat("metadata", 30)}}
	}
	records[0].Value, records[0].Meta = true, nil
	records[1].Value = "Unicode 🌡️ and escaped \"text\""
	records[2].Value, records[2].Meta = nil, map[string]string{}
	records[3].Value = int64(9223372036854775807)
	var plain bytes.Buffer
	baseline, _ := (&testutil.Backend{Records: records}).Query(context.Background(), nil)
	if _, err := WriteIPC(&plain, baseline, CompressionNone); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []Compression{CompressionNone, CompressionZstd, CompressionLZ4} {
		for _, native := range []bool{false, true} {
			t.Run(string(mode)+map[bool]string{false: "/records", true: "/batches"}[native], func(t *testing.T) {
				input, err := ipc.NewReader(bytes.NewReader(plain.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				defer input.Release()
				results, _ := (&testutil.Backend{Records: records}).Query(context.Background(), nil)
				if native {
					results = &compressionBatchResults{Results: &testutil.Results{}, reader: input}
				}
				var output bytes.Buffer
				n, err := WriteIPC(&output, results, mode)
				if err != nil {
					t.Fatal(err)
				}
				if n != len(records) {
					t.Fatalf("rows %d", n)
				}
				if mode != CompressionNone && output.Len() >= plain.Len() {
					t.Fatal("repetitive fixture was not compressed")
				}
				expected, err := ipc.NewReader(bytes.NewReader(plain.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				defer expected.Release()
				actual, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				defer actual.Release()
				for expected.Next() {
					if !actual.Next() || !array.RecordEqual(expected.RecordBatch(), actual.RecordBatch()) {
						t.Fatal("decoded records differ")
					}
				}
				if actual.Next() || actual.Err() != nil || expected.Err() != nil {
					t.Fatal("invalid stream termination")
				}
			})
		}
	}
	if _, err := WriteIPC(io.Discard, baseline, "invalid"); err == nil {
		t.Fatal("accepted invalid compression")
	}
}

// TestParseArrowCompression tests the parse arrow compression.
func TestParseArrowCompression(t *testing.T) {
	for raw, want := range map[string]Compression{"": CompressionZstd, "none": CompressionNone, " ZSTD ": CompressionZstd, "lz4": CompressionLZ4} {
		got, err := ParseCompression(raw)
		if err != nil || got != want {
			t.Fatalf("%q: got %q, %v", raw, got, err)
		}
	}
	if _, err := ParseCompression("gzip"); err == nil {
		t.Fatal("accepted gzip as IPC compression")
	}
}

// TestArrowCompressionProbe is a reproducible fresh-process encoder harness.
// It is skipped during ordinary tests. exp59 sets these paths in an isolated
// temporary directory, then checks the resulting IPC with the Python client.
func TestArrowCompressionProbe(t *testing.T) {
	input := os.Getenv("ARROW_PROBE_INPUT")
	if input == "" {
		t.Skip("exp59 harness only")
	}
	mode, err := ParseCompression(os.Getenv("ARROW_PROBE_MODE"))
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(input)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	reader, err := ipc.NewReader(source)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if err := SchemaCompatible(reader.Schema()); err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(os.Getenv("ARROW_PROBE_OUTPUT"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	tracked := &compressionProbeReader{RecordReader: reader, start: time.Now()}
	results := &compressionBatchResults{Results: &testutil.Results{}, reader: tracked}
	var before, after syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
		t.Fatal(err)
	}
	tracked.start = time.Now()
	n, err := WriteIPC(output, results, mode)
	elapsed := time.Since(tracked.start).Seconds()
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
		t.Fatal(err)
	}
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	stat, err := output.Stat()
	if err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"mode": mode, "rows": n, "bytes": stat.Size(), "encode_seconds": elapsed,
		"first_batch_seconds": tracked.firstBatchSeconds, "encode_cpu_seconds": cpu(after) - cpu(before)}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Clean(os.Getenv("ARROW_PROBE_REPORT")), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

// compressionProbeReader is the compression probe reader type.
// A reproducible fresh-process encoder harness.
type compressionProbeReader struct {
	array.RecordReader
	start             time.Time
	batches           int
	firstBatchSeconds float64
}

// Next advances the compression probe reader.
func (r *compressionProbeReader) Next() bool {
	// The next request for a batch follows successful serialization of the prior
	// one, so this measures completion of the first IPC batch, not just headers.
	if r.batches == 1 {
		r.firstBatchSeconds = time.Since(r.start).Seconds()
	}
	ok := r.RecordReader.Next()
	if ok {
		r.batches++
	}
	return ok
}
