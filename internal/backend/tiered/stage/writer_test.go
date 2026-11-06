// writer_test.go checks the private Parquet stage: round trip, budgets, and close errors.

package stage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
)

func writeStage(t *testing.T, rows []Row) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stage.parquet")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := New(f, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, r := range rows {
		if err = w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParquetRoundTrip(t *testing.T) {
	rows := []Row{
		{Time: 1740787200000000194, Name: "μ\n<name>", Meta: map[string]string{"b": "😀", "": "", "a": "\"\\\n"}, Kind: "uint", Value: "18446744073709551615"},
		{Time: -9223372036854775808, Name: "t", Meta: map[string]string{}, Kind: "int", Value: "-9223372036854775808"},
		{Time: 9223372036854775807, Name: "t", Meta: map[string]string{}, Kind: "null", Value: ""},
	}
	path := writeStage(t, rows)
	// Independent consumer verifies MAP annotation/levels, empty maps and exact
	// scalar text. Runs without linking the Go native DuckDB package.
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2 for independent Parquet decoding")
	}
	cmd := exec.Command(binary, "-batch", "-bail", "-json")
	cmd.Stdin = strings.NewReader("SELECT * FROM read_parquet('" + strings.ReplaceAll(path, "'", "''") + "');")
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, data)
	}
	var got []Row
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatal(err, string(data))
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("got %#v want %#v", got, rows)
	}
}

func TestEmptyAndBoundedGroups(t *testing.T) {
	for _, count := range []int{0, 1, batchRows + 1} {
		rows := make([]Row, count)
		for i := range rows {
			rows[i] = Row{Time: int64(i), Name: "t", Kind: "int", Value: "1"}
		}
		path := writeStage(t, rows)
		f, err := file.OpenParquetFile(path, false)
		if err != nil {
			t.Fatal(err)
		}
		if f.NumRows() != int64(count) {
			t.Fatal(f.NumRows(), count)
		}
		for i := 0; i < f.NumRowGroups(); i++ {
			if f.RowGroup(i).NumRows() > batchRows {
				t.Fatal("unbounded row group")
			}
		}
		want := 0
		if count > 0 {
			want = (count + batchRows - 1) / batchRows
		}
		if f.NumRowGroups() != want {
			t.Fatal("row limit", f.NumRowGroups(), want)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Byte bound must flush even when the row count is small. A single record
	// above the batch target is allowed if it still fits the request budget.
	path := writeStage(t, []Row{{Name: strings.Repeat("x", int(batchBytes)), Kind: "string"}, {Name: "next", Kind: "null"}})
	f, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.NumRowGroups() != 2 {
		t.Fatal("byte bound", f.NumRowGroups())
	}
}

func TestBudgetAndMalformedInput(t *testing.T) {
	row := Row{Time: 1, Name: "t", Kind: "uint", Value: "18446744073709551615"}
	var out bytes.Buffer
	w, err := New(&out, InputBytes(row))
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Append(row); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(row); err == nil {
		t.Fatal("compressed output bypassed input budget")
	}
	if w.InputBytes() != InputBytes(row) {
		t.Fatal(w.InputBytes())
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(row); err == nil {
		t.Fatal("append after close")
	}
	if _, err = New(io.Discard, 0); err == nil {
		t.Fatal("invalid budget")
	}
	for _, r := range []Row{{Name: string([]byte{255})}, {Meta: map[string]string{"x": string([]byte{255})}}} {
		w, err = New(io.Discard, 1024)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Append(r); err == nil {
			t.Fatal("invalid UTF8 accepted")
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

type failingOutput struct{ fail bool }

var outputError = errors.New("staging disk failure")

func (w *failingOutput) Write(p []byte) (int, error) {
	if w.fail {
		return 0, outputError
	}
	return len(p), nil
}
func TestFlushAndFooterErrors(t *testing.T) {
	for _, withRow := range []bool{false, true} {
		out := &failingOutput{}
		w, err := New(out, 1024)
		if err != nil {
			t.Fatal(err)
		}
		if withRow {
			if err = w.Append(Row{Name: "t", Kind: "null"}); err != nil {
				t.Fatal(err)
			}
		}
		out.fail = true
		if err = w.Close(); !errors.Is(err, outputError) {
			t.Fatalf("lost write/footer error: %v", err)
		}
	}
}

func BenchmarkDirectStage(b *testing.B) {
	for _, tags := range []int{0, 8} {
		b.Run(fmt.Sprintf("tags%d", tags), func(b *testing.B) {
			row := Row{Time: 1740787200000000194, Name: "wxt.env.temp", Meta: map[string]string{}, Kind: "float", Value: "11.7"}
			for i := 0; i < tags; i++ {
				row.Meta[fmt.Sprint("tag", i)] = "registry.sagecontinuum.org/example/plugin:1.0"
			}
			b.ReportAllocs()
			b.SetBytes(InputBytes(row) * 100000)
			for i := 0; i < b.N; i++ {
				w, err := New(io.Discard, 1<<30)
				if err != nil {
					b.Fatal(err)
				}
				for j := 0; j < 100000; j++ {
					if err = w.Append(row); err != nil {
						b.Fatal(err)
					}
				}
				if err = w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
