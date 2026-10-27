// sql_test.go contains the tests for the cold SQL functions.
package cold

import (
	"strings"
	"testing"
	"time"

	model "beehive-data-api/internal/query"
)

// TestBuildColdSQL tests the build cold SQL.
func TestBuildColdSQL(t *testing.T) {
	now := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	url := "https://example.test/sage/obs/metadata/00002-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.metadata.json"
	q := &model.Query{
		Start:  "2025-03-01T00:00:00Z",
		End:    "2025-03-01T00:01:00Z",
		Filter: map[string]string{"name": "wxt.env.temp", "sensor": "vaisala-wxt536"},
	}
	sql, err := buildColdSQL(q, url, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"iceberg_scan('" + url + "')",
		"time >= TIMESTAMPTZ_NS '2025-03-01T00:00:00Z'",
		"time < TIMESTAMPTZ_NS '2025-03-01T00:01:00Z'",
		"name = 'wxt.env.temp'",
		"meta['sensor'] = 'vaisala-wxt536'",
		"ORDER BY timestamp",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("sql missing %q\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "read_parquet") {
		t.Errorf("sql still reads parquet directly:\n%s", sql)
	}

	star := &model.Query{Start: q.Start, End: q.End, Filter: map[string]string{"name": "env.*"}}
	sql, err = buildColdSQL(star, url, now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "regexp_matches(name, '^env.*$')") {
		t.Errorf("star filter was not a regex:\n%s", sql)
	}

	pipe := &model.Query{Start: q.Start, End: q.End, Filter: map[string]string{"plugin": "a|b"}}
	sql, err = buildColdSQL(pipe, url, now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "regexp_matches(plugin, '^(a|b)$')") {
		t.Errorf("pipe filter was not a grouped regex:\n%s", sql)
	}

	n := 5
	head := &model.Query{Start: q.Start, End: q.End, Head: &n}
	sql, err = buildColdSQL(head, url, now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "PARTITION BY name, meta ORDER BY timestamp ASC") || !strings.Contains(sql, "rn <= 5") {
		t.Errorf("head was not per-series:\n%s", sql)
	}

	tail := &model.Query{Start: q.Start, End: q.End, Tail: &n}
	sql, err = buildColdSQL(tail, url, now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "ORDER BY timestamp DESC") {
		t.Errorf("tail did not order descending:\n%s", sql)
	}

	fn := "mean"
	if _, err := buildColdSQL(&model.Query{Start: q.Start, End: q.End, Func: &fn}, url, now); err == nil {
		t.Error("aggregation was accepted")
	}
	if _, err := buildColdSQL(&model.Query{Start: q.Start, End: q.End, Head: &n, Tail: &n}, url, now); err == nil {
		t.Error("head and tail together was accepted")
	}
	if _, err := buildColdSQL(&model.Query{Start: "-1h", End: q.End}, url, now); err == nil {
		t.Error("relative start was accepted")
	}
	open := &model.Query{Start: q.Start}
	sql, err = buildColdSQL(open, url, now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "time < TIMESTAMPTZ_NS '2025-06-01T00:00:00Z'") {
		t.Errorf("empty end did not use now:\n%s", sql)
	}

	arrowSQL, err := buildColdArrowSQL(q, url, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"CAST(timestamp AS TIMESTAMP_NS) AS timestamp",
		"to_json(value)::VARCHAR",
		"json_merge_patch",
		"iceberg_scan('" + url + "')",
	} {
		if !strings.Contains(arrowSQL, want) {
			t.Errorf("arrow sql missing %q\n%s", want, arrowSQL)
		}
	}
}
