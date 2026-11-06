//go:build duckdb_arrow

// selection_native_scale_test.go runs the Go execution path on synthetic files.
// A CLI success does not prove that path.

package cold

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Exercise the native Go execution interfaces separately from the CLI: a CLI
// success does not prove the pending-streaming or database/sql path completes.
// All data are private synthetic files; no Pelican/Influx credentials are used.
func TestSpanningSelectionNativeScale(t *testing.T) {
	queries := append([]selectionScaleQuery{selectionRangeScaleQuery(t)}, selectionParquetScaleQueries(t)...)
	for _, q := range queries {
		t.Run(q.name+"/arrow", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			db, err := newStreamingArrowDB(ctx, coldDuckDBRuntimeSetup(nil))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			reader, err := db.query(ctx, q.sql, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			count := 0
			for reader.Next() {
				batch := reader.RecordBatch()
				for i := 0; i < int(batch.NumRows()); i++ {
					count++
					if int64(batch.Column(0).(*array.Timestamp).Value(i)) != q.ns || batch.Column(1).(*array.String).Value(i) != "wxt.env.temp" || batch.Column(2).(*array.String).Value(i) != q.value {
						t.Fatal("selected observation changed")
					}
				}
			}
			if err = reader.Err(); err != nil || count != 1 {
				t.Fatal("native Arrow did not complete one selected row", count, err)
			}
		})
		t.Run(q.name+"/rows", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			for _, statement := range append(coldDuckDBRuntimeSetup(nil), "SET temp_directory="+sqlQuote(t.TempDir())) {
				if _, err = db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			rows, err := db.QueryContext(ctx, q.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				var stamp time.Time
				var name, value, meta string
				if err = rows.Scan(&stamp, &name, &value, &meta); err != nil {
					t.Fatal(err)
				}
				count++
				if stamp.UnixNano() != q.ns || name != "wxt.env.temp" || value != q.value {
					t.Fatal("selected observation changed", stamp, name, value)
				}
			}
			if err = rows.Err(); err != nil || count != 1 {
				t.Fatal("native rows did not complete one selected row", count, err)
			}
		})
	}
}
