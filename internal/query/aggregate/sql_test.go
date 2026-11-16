// sql_test.go checks window bounds and the metadata dictionary in the aggregate plan.

package aggregate

import (
	model "beehive-data-api/internal/query"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestAggregateWindowValidation(t *testing.T) {
	for _, text := range []string{"0s", "-1h", "1mo1d", "1x", "9223372036854775808ns", "99999999999999h", ""} {
		if _, err := parseAggregateWindow(text); err == nil {
			t.Errorf("accepted %q", text)
		}
	}
	for _, text := range []string{"2ns", "5m", "1h30m", "1w", "1mo", "1y", "1y2mo"} {
		if _, err := parseAggregateWindow(text); err != nil {
			t.Errorf("%s: %v", text, err)
		}
	}
	fn, window := "mean", "1ns"
	q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T00:01:00Z", Func: &fn, Window: &window}
	if _, err := BuildSQL(q, "input", time.Now()); err == nil {
		t.Fatal("unbounded expansion accepted")
	}
}

func TestCalendarWindowBoundsUseUTC(t *testing.T) {
	fn, window := "mean", "1mo"
	q := model.Query{Start: "2025-02-28T18:00:00-06:00", End: "2025-03-31T18:00:00.000000194-06:00", Func: &fn, Window: &window}
	actual, err := BuildSQL(&q, "input", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	q.Start = "2025-03-01T00:00:00Z"
	q.End = "2025-04-01T00:00:00.000000194Z"
	want, err := BuildSQL(&q, "input", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if actual != want {
		t.Fatal("calendar expansion depends on query timezone instead of UTC instants")
	}
}

func TestAggregateMetadataDictionary(t *testing.T) {
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2")
	}
	version, err := exec.Command(binary, "--version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), "v2.0.") {
		t.Fatalf("DuckDB 2.0 CLI required: %s (%v)", version, err)
	}
	type observation struct {
		meta, plugin, vsn string
		value             int
		identity          string
	}
	input := []observation{
		{`MAP {'plugin':'old','x':'a','y':'b'}`, "'p'", "'v'", 1, `{"plugin":"p","vsn":"v","x":"a","y":"b"}`},
		{`MAP {'y':'b','x':'a','plugin':'different'}`, "'p'", "'v'", 2, `{"plugin":"p","vsn":"v","x":"a","y":"b"}`},
		{`MAP {'vsn':'v','y':'b','plugin':'p','x':'a'}`, "NULL", "NULL", 3, `{"plugin":"p","vsn":"v","x":"a","y":"b"}`},
		{`MAP {'x':'other'}`, "'p'", "'v'", 4, `{"plugin":"p","vsn":"v","x":"other"}`},
		{`NULL::MAP(VARCHAR,VARCHAR)`, "'p'", "'v'", 5, `{"plugin":"p","vsn":"v"}`},
		{`MAP {}::MAP(VARCHAR,VARCHAR)`, "'p'", "'v'", 6, `{"plugin":"p","vsn":"v"}`},
		{`MAP {'plugin':'raw'}`, "NULL", "NULL", 7, `{"plugin":"raw"}`},
		// Repeated raw tuple: only its dictionary entry is deduplicated.
		{`MAP {}::MAP(VARCHAR,VARCHAR)`, "'p'", "'v'", 8, `{"plugin":"p","vsn":"v"}`},
	}
	var source []string
	groups := map[string][]int{}
	for i, row := range input {
		source = append(source, fmt.Sprintf("SELECT make_timestamp_ns(%d) AS timestamp,'sensor' AS name,%s AS meta,%s::VARCHAR AS plugin,%s::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,%d::BIGINT::VARIANT AS value", int64(1740787200000000194)+int64(i), row.meta, row.plugin, row.vsn, row.value))
		groups[row.identity] = append(groups[row.identity], row.value)
	}
	for _, fn := range []string{"count", "mean", "sum", "min", "max"} {
		for _, limit := range []string{"all", "head0", "head1", "tail1"} {
			t.Run(fn+"/"+limit, func(t *testing.T) {
				window := "1m"
				q := model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T00:02:00Z", Func: &fn, Window: &window}
				zero, one := 0, 1
				switch limit {
				case "head0":
					q.Head = &zero
				case "head1":
					q.Head = &one
				case "tail1":
					q.Tail = &one
				}
				sql, err := BuildSQL(&q, strings.Join(source, " UNION ALL "), time.Now())
				if err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(binary, "-batch", "-bail", "-json")
				cmd.Stdin = strings.NewReader("SET threads=2;" + sql)
				data, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%v: %s", err, data)
				}
				var rows []struct {
					Timestamp, Name, Meta string
					Value                 *string
				}
				if err = json.Unmarshal(data, &rows); err != nil {
					t.Fatalf("%v: %s", err, data)
				}
				if len(rows) != 2*len(groups) {
					t.Fatalf("series expansion: %d != %d", len(rows), 2*len(groups))
				}
				seen := map[string]bool{}
				for _, row := range rows {
					var meta map[string]string
					if err = json.Unmarshal([]byte(row.Meta), &meta); err != nil {
						t.Fatal(err)
					}
					canonical, _ := json.Marshal(meta)
					values, ok := groups[string(canonical)]
					key := row.Timestamp + string(canonical)
					if !ok || seen[key] || row.Name != "sensor" {
						t.Fatal("changed series or duplicate", row)
					}
					seen[key] = true
					switch limit {
					case "head0":
						values = nil
					case "head1":
						values = values[:1]
					case "tail1":
						values = values[len(values)-1:]
					}
					if strings.Contains(row.Timestamp, "00:02:00") {
						values = nil
					} else if !strings.Contains(row.Timestamp, "00:01:00") {
						t.Fatal("window label", row.Timestamp)
					}
					if len(values) == 0 && fn != "count" {
						if row.Value != nil && *row.Value != "null" {
							t.Fatal("empty aggregate changed", row)
						}
						continue
					}
					want := float64(0)
					if fn == "count" {
						want = float64(len(values))
					} else {
						want = float64(values[0])
						if fn == "sum" || fn == "mean" {
							want = 0
						}
						for _, n := range values {
							switch fn {
							case "sum", "mean":
								want += float64(n)
							case "min":
								want = math.Min(want, float64(n))
							case "max":
								want = math.Max(want, float64(n))
							}
						}
						if fn == "mean" {
							want /= float64(len(values))
						}
					}
					var actual float64
					if row.Value == nil || json.Unmarshal([]byte(*row.Value), &actual) != nil || math.Abs(actual-want) > 1e-12 {
						t.Fatal("aggregate changed", row.Value, want)
					}
				}
			})
		}
	}
}
