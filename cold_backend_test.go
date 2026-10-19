package main

import (
	"testing"
	"time"

	"database/sql"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

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
			2.5::DOUBLE AS value_float,
			NULL::BIGINT AS value_int,
			NULL::VARCHAR AS value_str,
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
	if rec.Name != "env.temperature" || rec.Value != 2.5 {
		t.Fatalf("record %+v", rec)
	}
	if rec.Meta["plugin"] != "plug" || rec.Meta["vsn"] != "W09A" || rec.Meta["units"] != "degree Celsius" {
		t.Fatalf("meta %#v", rec.Meta)
	}
}
