// reduce_test.go checks numeric wrapping, mixed kinds, and null tag presence.

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

func TestReducedAggregateRejectsObservationLimits(t *testing.T) {
	fn := "mean"
	one := 1
	for _, q := range []model.Query{{Head: &one, Func: &fn}, {Tail: &one, Func: &fn}} {
		if _, err := buildReducedAggregateSQL(&q, "", time.Now()); err == nil {
			t.Fatal("partial reduction must not precede observation limits")
		}
	}
}

func TestAggregateNumericAndTypeSemantics(t *testing.T) {
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2")
	}
	for _, tc := range []struct {
		name, values string
		count        int
		mixed        bool
	}{
		{"signed_wrap", "CASE WHEN i=0 THEN 9223372036854775807::BIGINT::VARIANT ELSE 1::BIGINT::VARIANT END", 2, false},
		{"unsigned_wrap", "CASE WHEN i=0 THEN 18446744073709551615::UBIGINT::VARIANT ELSE 1::UBIGINT::VARIANT END", 2, false},
		{"nullable_uint", "CASE WHEN i=0 THEN NULL::VARIANT ELSE 18446744073709551615::UBIGINT::VARIANT END", 2, false},
		{"all_null", "NULL::VARIANT", 2, false},
		{"float_cancellation", "CASE i WHEN 0 THEN 1e16::DOUBLE::VARIANT WHEN 1 THEN 1::DOUBLE::VARIANT ELSE (-1e16)::DOUBLE::VARIANT END", 3, false},
		{"weighted_float", "CASE WHEN i<100 THEN 1::DOUBLE::VARIANT ELSE 100::DOUBLE::VARIANT END", 101, false},
		{"mixed_numeric", "CASE WHEN i=0 THEN 1::BIGINT::VARIANT ELSE 1::DOUBLE::VARIANT END", 2, true},
		{"unsupported", "CASE WHEN i=0 THEN 'x'::VARIANT ELSE true::VARIANT END", 2, true},
	} {
		for _, fn := range []string{"count", "mean", "sum", "min", "max"} {
			t.Run(tc.name+"/"+fn, func(t *testing.T) {
				source := fmt.Sprintf("SELECT make_timestamp_ns(1740787200000000194+i) AS timestamp,'sensor' AS name,MAP {'plugin':CASE WHEN i%%2=0 THEN 'old1' ELSE 'old2' END} AS meta,'p'::VARCHAR AS plugin,'v'::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,%s AS value FROM range(%d) r(i)", tc.values, tc.count)
				window := "1m"
				q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T00:02:00Z", Func: &fn, Window: &window}
				limited := *q
				limited.Head = &tc.count // Select every observation through the live head path.
				baseline, err := buildLimitedAggregateSQL(&limited, source, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				reduced, err := BuildSQL(q, source, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				var expected []byte
				for _, sql := range []string{baseline, reduced} {
					cmd := exec.Command(binary, "-batch", "-bail", "-json")
					cmd.Stdin = strings.NewReader("SET threads=2;" + sql)
					out, err := cmd.CombinedOutput()
					if tc.mixed && fn != "count" {
						if err == nil || !strings.Contains(string(out), "unsupported or mixed aggregate input") {
							t.Fatalf("expected kind rejection: %v %s", err, out)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%v: %s", err, out)
					}
					if expected == nil {
						expected = out
						continue
					}
					var a, b []struct {
						Timestamp, Name, Meta string
						Value                 *string
					}
					if json.Unmarshal(expected, &a) != nil || json.Unmarshal(out, &b) != nil || len(a) != len(b) {
						t.Fatalf("decode/count mismatch: %s / %s", expected, out)
					}
					for i := range a {
						if a[i].Timestamp != b[i].Timestamp || a[i].Name != b[i].Name || a[i].Meta != b[i].Meta {
							t.Fatal("identity changed", a[i], b[i])
						}
						x, y := a[i].Value, b[i].Value
						if x == nil || y == nil {
							if x != y {
								t.Fatal("null changed")
							}
							continue
						}
						// Strings carry JSON scalars, so exact integer equality never goes via float64.
						if (tc.name == "weighted_float" || tc.name == "float_cancellation") || fn == "mean" {
							var xnum, ynum float64
							if json.Unmarshal([]byte(*x), &xnum) != nil || json.Unmarshal([]byte(*y), &ynum) != nil || math.Abs(xnum-ynum) > math.Max(1e-10, math.Abs(xnum)*1e-12) {
								t.Fatal("float changed", *x, *y)
							}
						} else if *x != *y {
							t.Fatal("exact value changed", *x, *y)
						}
					}
				}
			})
		}
	}
}

func TestAggregateNullKeyPresence(t *testing.T) {
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2")
	}
	source := `SELECT make_timestamp_ns(1740787200000000194+i) AS timestamp,'sensor' AS name,
 CASE WHEN i=0 THEN MAP {'plugin':NULL,'x':'a'} ELSE MAP {'x':'a'} END AS meta,
 NULL::VARCHAR AS plugin,NULL::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
 NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,1::BIGINT::VARIANT AS value FROM range(2) r(i)`
	fn := "count"
	q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T00:01:00Z", Func: &fn}
	limited := *q
	all := 2
	limited.Head = &all
	baseline, err := buildLimitedAggregateSQL(&limited, source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	reduced, err := BuildSQL(q, source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{baseline, reduced} {
		cmd := exec.Command(binary, "-batch", "-bail", "-json")
		cmd.Stdin = strings.NewReader(sql)
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, data)
		}
		var rows []struct{ Meta, Value string }
		if err = json.Unmarshal(data, &rows); err != nil {
			t.Fatal(err)
		}
		identities := map[string]bool{}
		for _, r := range rows {
			if r.Value != "1" {
				t.Fatal("distinct null/absent series merged", r)
			}
			identities[r.Meta] = true
		}
		if len(rows) != 2 || !identities[`{"plugin":null,"x":"a"}`] || !identities[`{"x":"a"}`] {
			t.Fatal("key presence changed", string(data))
		}
	}
}
