// backend_test.go contains the tests for the cold backend type.
package cold

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// TestScanColdRecord tests the scan cold record.
func TestScanColdRecord(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.Exec("SET TimeZone='UTC'")

	rows, err := db.Query(`
		SELECT
			TIMESTAMPTZ '2025-03-01T00:00:01.5Z' AS timestamp,
			'env.temperature' AS name,
			to_json(2.5::DOUBLE::VARIANT)::VARCHAR AS value,
			'{"plugin":"plug","units":"degree Celsius","vsn":"W09A"}' AS meta
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	rec, err := scanColdRecord(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Timestamp.Equal(time.Date(2025, 3, 1, 0, 0, 1, 500000000, time.UTC)) {
		t.Fatalf("timestamp %s", rec.Timestamp)
	}
	if rec.Name != "env.temperature" || string(rec.Value.(json.RawMessage)) != "2.5" {
		t.Fatalf("record %+v", rec)
	}
	if rec.Meta["plugin"] != "plug" || rec.Meta["vsn"] != "W09A" || rec.Meta["units"] != "degree Celsius" {
		t.Fatalf("meta %#v", rec.Meta)
	}
}
