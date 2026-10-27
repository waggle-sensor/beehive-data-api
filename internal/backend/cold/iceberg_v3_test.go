// iceberg_v3_test.go contains the tests for the cold Iceberg v3.
package cold

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// TestIcebergV3RoundTrip tests the Iceberg v3 round trip.
// publishes a format-version 3 table through DuckDB's
// Iceberg writer, then reads it back with iceberg_scan and no catalog. That is
// the API's reader. The table stores timestamptz_ns and one value VARIANT.
// COLD_V3_ENDPOINT is the REST catalog and COLD_V3_ROOT is the warehouse
// directory mounted at the same path inside the catalog. The test skips when
// either is unset, and it does not touch Pelican or the published v2 warehouse.
func TestIcebergV3RoundTrip(t *testing.T) {
	endpoint := os.Getenv("COLD_V3_ENDPOINT")
	root := os.Getenv("COLD_V3_ROOT")
	if endpoint == "" || root == "" {
		t.Skip("set COLD_V3_ENDPOINT and COLD_V3_ROOT to an isolated Iceberg REST warehouse")
	}

	ctx := context.Background()
	db := openV3DB(t)
	defer db.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("SET TimeZone='UTC'")
	exec("INSTALL iceberg")
	exec("LOAD iceberg")
	exec("INSTALL httpfs")
	exec("LOAD httpfs")
	exec(`ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT '` + endpoint + `', AUTHORIZATION_TYPE 'none')`)
	namespace := fmt.Sprintf("v3_test_%d", time.Now().UnixNano())
	exec("CREATE SCHEMA lake." + namespace)
	exec(`CREATE TABLE lake.` + namespace + `.obs (
		time TIMESTAMPTZ_NS,
		name VARCHAR,
		value VARIANT,
		meta MAP(VARCHAR, VARCHAR),
        plugin VARCHAR, vsn VARCHAR, node VARCHAR, host VARCHAR, task VARCHAR, job VARCHAR, zone VARCHAR
	) WITH ('format-version' = '3')`)
	exec(`INSERT INTO lake.` + namespace + `.obs (time,name,value,meta) VALUES
		(TIMESTAMPTZ_NS '2025-03-01 00:00:07.420753193+00', 'wxt.env.temp', 21.5::DOUBLE::VARIANT, MAP {'sensor': 'sht', 'units': 'C'}),
		(TIMESTAMPTZ_NS '2025-03-01 00:00:07.420753571+00', 'wxt.env.temp', 9007199254740993::BIGINT::VARIANT, MAP {'sensor': 'sht', 'units': 'C'}),
		(TIMESTAMPTZ_NS '2025-03-01 00:00:08.000000001+00', 'plugin.status', '123'::VARCHAR::VARIANT, MAP {'plugin': 'notes'}),
		(TIMESTAMPTZ_NS '2025-03-01 00:00:08.000000002+00', 'plugin.flag', true::BOOLEAN::VARIANT, MAP {})`)

	var metaPath string
	if err := db.QueryRowContext(ctx, "SELECT metadata_location FROM iceberg_load_table_response(lake."+namespace+".obs)").Scan(&metaPath); err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(root, metaPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		t.Fatalf("catalog head outside isolated root: %s", metaPath)
	}
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		FormatVersion int `json:"format-version"`
		Schemas       []struct {
			Fields []struct {
				Name string `json:"name"`
				Type any    `json:"type"`
			} `json:"fields"`
		} `json:"schemas"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		t.Fatal(err)
	}
	if header.FormatVersion != 3 {
		t.Fatalf("format-version %d in %s", header.FormatVersion, metaPath)
	}
	types := map[string]string{}
	for _, field := range header.Schemas[len(header.Schemas)-1].Fields {
		types[field.Name] = strings.ToLower(string(mustJSON(field.Type)))
	}
	if !strings.Contains(types["time"], "timestamptz_ns") || !strings.Contains(types["value"], "variant") {
		t.Fatalf("schema types: %#v", types)
	}

	// A second connection scans the metadata file only, the way the API does.
	reader := openV3DB(t)
	defer reader.Close()
	if _, err := reader.ExecContext(ctx, "SET TimeZone='UTC'"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"INSTALL iceberg", "LOAD iceberg"} {
		if _, err := reader.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := reader.QueryContext(ctx, `
		SELECT epoch_ns(time), name, to_json(value)::VARCHAR, to_json(meta)::VARCHAR
		FROM iceberg_scan(?)
		ORDER BY time`, metaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []struct {
		ns    int64
		name  string
		value string
	}{
		{1740787207420753193, "wxt.env.temp", "21.5"},
		{1740787207420753571, "wxt.env.temp", "9007199254740993"},
		{1740787208000000001, "plugin.status", `"123"`},
		{1740787208000000002, "plugin.flag", "true"},
	}
	for i, exp := range want {
		if !rows.Next() {
			t.Fatalf("row %d missing: %v", i, rows.Err())
		}
		var ns int64
		var name, value, meta string
		if err := rows.Scan(&ns, &name, &value, &meta); err != nil {
			t.Fatal(err)
		}
		if ns != exp.ns || name != exp.name || value != exp.value {
			t.Fatalf("row %d: got %d %s %s meta %s", i, ns, name, value, meta)
		}
		if ns%1000 == 0 {
			t.Fatalf("row %d lost its nanosecond remainder: %d", i, ns)
		}
		if !json.Valid([]byte(meta)) {
			t.Fatalf("row %d meta is not JSON: %s", i, meta)
		}
	}
	if rows.Next() {
		t.Fatal("extra row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	testV3HTTPResponses(t, reader, metaPath)
}

// openV3DB opens the v3 database.
func openV3DB(t *testing.T) *sql.DB {
	t.Helper()
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		for _, q := range coldDuckDBRuntimeSetup(nil) {
			if _, err := execer.ExecContext(context.Background(), q, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connector.Close() })
	db := sql.OpenDB(connector)
	t.Cleanup(func() { db.Close() })
	return db
}

// mustJSON marshals the value to JSON.
func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return []byte(err.Error())
	}
	return raw
}
