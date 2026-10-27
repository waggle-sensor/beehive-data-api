// arrow_test.go contains the tests for the cold arrow type.
package cold

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"

	"beehive-data-api/internal/arrowio"
)

// TestColdArrowProjectionSchema tests the cold arrow projection schema.
// Locks the DuckDB export to apiSchema. The
// outer select is the one buildColdArrowSQL appends to the Iceberg scan.
func TestColdArrowProjectionSchema(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	drv, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer drv.Close()
	if _, err := drv.(driver.ExecerContext).ExecContext(context.Background(), "SET TimeZone='UTC'", nil); err != nil {
		t.Fatal(err)
	}
	arrowConn, err := duckdb.NewArrowFromConn(drv)
	if err != nil {
		t.Fatal(err)
	}
	inner := `
        SELECT
            TIMESTAMPTZ '2025-03-01T00:00:00.026403Z' AS timestamp,
            'wxt.env.temp' AS name,
            11.7::DOUBLE::VARIANT AS value,
            MAP {'vsn': 'W0A0', 'plugin': 'waggle-wxt536'}::MAP(VARCHAR, VARCHAR) AS meta`
	rdr, err := arrowConn.QueryContext(context.Background(), coldArrowProjection(inner))
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	if err := arrowio.SchemaCompatible(rdr.Schema()); err != nil {
		t.Fatalf("%v\nschema: %s", err, rdr.Schema())
	}
	if !rdr.Next() {
		t.Fatal(rdr.Err())
	}
	out, err := arrowio.AdoptBatch(rdr.RecordBatch())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.NumRows() != 1 {
		t.Fatalf("rows %d", out.NumRows())
	}
	ts := out.Column(arrowio.FieldTimestamp).(*array.Timestamp)
	// The public projection preserves every stored digit.
	want := time.Date(2025, 3, 1, 0, 0, 0, 26403000, time.UTC)
	got := time.Unix(0, int64(ts.Value(0))).UTC()
	if !got.Equal(want) {
		t.Fatalf("timestamp %s, want %s", got, want)
	}
	if out.Column(arrowio.FieldName).(*array.String).Value(0) != "wxt.env.temp" {
		t.Fatal("name")
	}
	if out.Column(arrowio.FieldValue).(*array.String).Value(0) != "11.7" {
		t.Fatalf("value %s", out.Column(arrowio.FieldValue).(*array.String).Value(0))
	}
	var meta map[string]string
	if err := json.Unmarshal([]byte(out.Column(arrowio.FieldMeta).(*array.String).Value(0)), &meta); err != nil {
		t.Fatal(err)
	}
	if meta["vsn"] != "W0A0" || meta["plugin"] != "waggle-wxt536" {
		t.Fatalf("meta %#v", meta)
	}
}
