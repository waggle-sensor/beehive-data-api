// selection_scale_test.go checks large per-series limits through the DuckDB CLI.

package cold

import (
	"beehive-data-api/internal/backend/tiered"
	model "beehive-data-api/internal/query"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type selectionScaleQuery struct {
	name, sql, value string
	ns               int64
	query            *model.Query
	source           string
}

func selectionScaleCLI(t *testing.T, sql string) []byte {
	t.Helper()
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-batch", "-bail", "-json")
	cmd.Stdin = strings.NewReader("SET threads=2; SET memory_limit='3GB'; " + sql)
	start := time.Now()
	out, err := cmd.CombinedOutput()
	t.Logf("CLI elapsed %s", time.Since(start))
	if err != nil {
		t.Fatalf("%v (%v): %s", err, ctx.Err(), out)
	}
	return out
}

func selectionRangeScaleQuery(t *testing.T) selectionScaleQuery {
	t.Helper()
	const count = 521840
	cold := fmt.Sprintf(`SELECT make_timestamp_ns(1740787200000000194+i) AS timestamp,'wxt.env.temp' AS name,MAP {'sensor':'wxt'} AS meta,'P' AS plugin,'W0A0' AS vsn,'N' AS node,'H' AS host,'T' AS task,NULL::VARCHAR AS job,'Z' AS zone,11.7::DOUBLE::VARIANT AS value FROM range(%d) r(i)`, count)
	hot := fmt.Sprintf(`SELECT make_timestamp_ns(1740787200000000194+%d+i) AS timestamp,'wxt.env.temp' AS name,MAP {'sensor':'wxt','plugin':'P','vsn':'W0A0','node':'N','host':'H','task':'T','zone':'Z'} AS meta,NULL::VARCHAR AS plugin,NULL::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,11.7::DOUBLE::VARIANT AS value FROM range(%d) r(i)`, count, count)
	n := 1
	q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-02T00:00:00Z", Head: &n}
	source := cold + " UNION ALL " + hot
	sql, err := tiered.BuildSelectionSQL(q, source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return selectionScaleQuery{name: "range_head", sql: sql, value: "11.7", ns: 1740787200000000194, query: q, source: source}
}

func selectionParquetScaleQueries(t *testing.T) []selectionScaleQuery {
	t.Helper()
	dir := t.TempDir()
	coldPath, hotPath := dir+"/cold.parquet", dir+"/hot.parquet"
	setup := fmt.Sprintf(`SET TimeZone='UTC'; COPY (SELECT make_timestamp_ns(1740787200000000194+i*1000000)::TIMESTAMPTZ_NS AS time,'wxt.env.temp' AS name,MAP {'sensor':'wxt'} AS meta,'P' AS plugin,'W0A0' AS vsn,'N' AS node,'H' AS host,'T' AS task,NULL::VARCHAR AS job,'Z' AS zone,(11.7+i%%10)::DOUBLE::VARIANT AS value FROM range(521840) r(i)) TO %s (FORMAT PARQUET,ROW_GROUP_SIZE 262144);
 COPY (SELECT 1740830400000000194+i*1000000 AS time,'wxt.env.temp' AS name,MAP {'sensor':'wxt','plugin':'P','vsn':'W0A0','node':'N','host':'H','task':'T','zone':'Z'} AS meta,'float' AS kind,(11.7+i%%10)::VARCHAR AS value FROM range(521840) r(i)) TO %s (FORMAT PARQUET,ROW_GROUP_SIZE 24576);`, sqlQuote(coldPath), sqlQuote(hotPath))
	selectionScaleCLI(t, setup)
	border := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)
	var result []selectionScaleQuery
	for _, tail := range []bool{false, true} {
		n := 1
		q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-02T00:00:00Z", Filter: map[string]string{"name": "wxt.env.temp", "vsn": "W0A0"}}
		expected := selectionScaleQuery{name: "parquet_head", ns: 1740787200000000194, value: "11.7"}
		if tail {
			q.Tail = &n
			expected.name = "parquet_tail"
			expected.ns = 1740830400000000194 + 521839*1000000
			expected.value = "20.7"
		} else {
			q.Head = &n
		}
		rawQuery := *q
		rawQuery.End = border.Format(time.RFC3339Nano)
		raw, err := buildColdRawSource(&rawQuery, "fixture", "", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		source := tiered.UnionSource(raw, hotPath)
		sql, err := tiered.BuildSelectionSQL(q, source, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		expected.sql = strings.ReplaceAll(sql, "iceberg_scan('fixture')", "read_parquet("+sqlQuote(coldPath)+")")
		expected.query = q
		expected.source = strings.ReplaceAll(source, "iceberg_scan('fixture')", "read_parquet("+sqlQuote(coldPath)+")")
		result = append(result, expected)
	}
	return result
}

func checkSelectionScaleCLI(t *testing.T, q selectionScaleQuery) {
	t.Helper()
	out := selectionScaleCLI(t, "SELECT epoch_ns(timestamp) AS ns,name,value FROM ("+q.sql+");")
	var rows []struct {
		NS          int64
		Name, Value string
	}
	if err := json.Unmarshal(out, &rows); err != nil || len(rows) != 1 || rows[0].NS != q.ns || rows[0].Name != "wxt.env.temp" || rows[0].Value != q.value {
		t.Fatal("wrong selected observation", string(out), err)
	}
}
func TestSpanningSelectionScale(t *testing.T) { checkSelectionScaleCLI(t, selectionRangeScaleQuery(t)) }
func TestSpanningSelectionParquetScale(t *testing.T) {
	for _, q := range selectionParquetScaleQueries(t) {
		t.Run(q.name, func(t *testing.T) { checkSelectionScaleCLI(t, q) })
	}
}

func TestSpanningSelectionLargeLimitScale(t *testing.T) {
	for _, base := range selectionParquetScaleQueries(t) {
		t.Run(base.name, func(t *testing.T) {
			n := 1000
			q := *base.query
			first, last := int64(1740787200000000194), int64(1740787200000000194+999*1000000)
			if q.Head != nil {
				q.Head = &n
			} else {
				q.Tail = &n
				first = 1740830400000000194 + (521840-1000)*1000000
				last = 1740830400000000194 + 521839*1000000
			}
			statement, err := tiered.BuildSelectionSQL(&q, base.source, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			sql := `SELECT count(*) AS n,count(DISTINCT epoch_ns(timestamp)) AS distinct_times,min(epoch_ns(timestamp)) AS first_ns,max(epoch_ns(timestamp)) AS last_ns,
 count(*) FILTER(WHERE value::DOUBLE IS DISTINCT FROM (11.7+((epoch_ns(timestamp)-CASE WHEN epoch_ns(timestamp)<1740830400000000000 THEN 1740787200000000194 ELSE 1740830400000000194 END)//1000000)%10)::DOUBLE) AS bad_values FROM (` + statement + `);`
			out := selectionScaleCLI(t, sql)
			var rows []struct {
				N        int
				Distinct int   `json:"distinct_times"`
				First    int64 `json:"first_ns"`
				Last     int64 `json:"last_ns"`
				Bad      int   `json:"bad_values"`
			}
			if err = json.Unmarshal(out, &rows); err != nil || len(rows) != 1 || rows[0].N != 1000 || rows[0].Distinct != 1000 || rows[0].First != first || rows[0].Last != last || rows[0].Bad != 0 {
				t.Fatal("large-limit candidate changed observations", string(out), err)
			}
		})
	}
}
