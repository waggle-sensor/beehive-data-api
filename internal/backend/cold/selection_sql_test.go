// selection_sql_test.go checks spanning head/tail SQL and the raw hot union.

package cold

import (
	"beehive-data-api/internal/backend/tiered"
	model "beehive-data-api/internal/query"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSelectionValidation(t *testing.T) {
	now := time.Unix(0, 1000).UTC()
	n, negative := 1, -1
	fn, window := "mean", "1m"
	valid := model.Query{Start: time.Unix(0, 1).UTC().Format(time.RFC3339Nano), Head: &n}
	start, end, err := tiered.SelectionBounds(&valid, now)
	if err != nil || start.UnixNano() != 1 || !end.Equal(now) {
		t.Fatal(start, end, err)
	}
	for _, change := range []func(*model.Query){
		func(q *model.Query) { q.Head = nil }, func(q *model.Query) { q.Tail = &n }, func(q *model.Query) { q.Head = &negative }, func(q *model.Query) { q.Func = &fn }, func(q *model.Query) { q.Window = &window }, func(q *model.Query) { q.Start = "-1h" }, func(q *model.Query) { q.End = q.Start }, func(q *model.Query) { q.End = "3000-01-01T00:00:00Z" },
	} {
		q := valid
		change(&q)
		if _, _, err := tiered.SelectionBounds(&q, now); err == nil {
			t.Fatal("invalid selection accepted", q)
		}
	}

}

func TestSpanningSelectionSQL(t *testing.T) {
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2")
	}
	// Cold rows carry raw metadata/promoted collisions. Hot rows have merged
	// metadata. Empty/null maps and different key orders must share identities.
	source := `SELECT make_timestamp_ns(ns) AS timestamp,name,meta,plugin,NULL::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,value FROM (VALUES
 (100::BIGINT,'t',MAP {'vsn':'A','sensor':'s','plugin':'stale'},'P','18446744073709551615'::DECIMAL(20,0)::VARIANT),
 (200,'t',MAP {'sensor':'s','plugin':'stale','vsn':'A'},'P','-9223372036854775808'::BIGINT::VARIANT),
 (300,'t',MAP {'plugin':'P','vsn':'A','sensor':'s'},NULL,TRUE::VARIANT),
 (400,'t',MAP {'sensor':'s','vsn':'A','plugin':'P'},NULL,'string'::VARIANT),
 (90,'t',MAP {'vsn':'B','sensor':'s'},'P',1.25::DOUBLE::VARIANT),
 (410,'t',MAP {'plugin':'P','vsn':'B','sensor':'s'},NULL,NULL::VARIANT),
 (110,'cold',MAP {}::MAP(VARCHAR,VARCHAR),NULL,1::VARIANT),
 (210,'cold',MAP {}::MAP(VARCHAR,VARCHAR),NULL,2::VARIANT),
 (350,'hot',MAP {'vsn':'H'},NULL,3::VARIANT),
 (450,'hot',MAP {'vsn':'H'},NULL,4::VARIANT),
 (500,'empty',NULL::MAP(VARCHAR,VARCHAR),NULL,5::VARIANT),
 (600,'empty',MAP {}::MAP(VARCHAR,VARCHAR),NULL,6::VARIANT)
 ) AS input(ns,name,meta,plugin,value)`
	type row struct {
		NS    int64
		Name  string
		Meta  map[string]string
		Value json.RawMessage
	}
	expected := []row{
		{100, "t", map[string]string{"plugin": "P", "vsn": "A", "sensor": "s"}, json.RawMessage("18446744073709551615")},
		{200, "t", map[string]string{"plugin": "P", "vsn": "A", "sensor": "s"}, json.RawMessage("-9223372036854775808")},
		{300, "t", map[string]string{"plugin": "P", "vsn": "A", "sensor": "s"}, json.RawMessage("true")},
		{400, "t", map[string]string{"plugin": "P", "vsn": "A", "sensor": "s"}, json.RawMessage(`"string"`)},
		{90, "t", map[string]string{"plugin": "P", "vsn": "B", "sensor": "s"}, json.RawMessage("1.25")},
		{410, "t", map[string]string{"plugin": "P", "vsn": "B", "sensor": "s"}, json.RawMessage("null")},
		{110, "cold", map[string]string{}, json.RawMessage("1")}, {210, "cold", map[string]string{}, json.RawMessage("2")},
		{350, "hot", map[string]string{"vsn": "H"}, json.RawMessage("3")}, {450, "hot", map[string]string{"vsn": "H"}, json.RawMessage("4")},
		{500, "empty", map[string]string{}, json.RawMessage("5")}, {600, "empty", map[string]string{}, json.RawMessage("6")},
	}
	identity := func(r row) string { b, _ := json.Marshal(r.Meta); return r.Name + string(b) }
	for _, tail := range []bool{false, true} {
		for _, limit := range []int{0, 1, 2, 20} {
			q := &model.Query{Start: time.Unix(0, 1).UTC().Format(time.RFC3339Nano), End: time.Unix(0, 1000).UTC().Format(time.RFC3339Nano)}
			if tail {
				q.Tail = &limit
			} else {
				q.Head = &limit
			}
			sql, err := tiered.BuildSelectionSQL(q, source, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-batch", "-bail", "-json")
			cmd.Stdin = strings.NewReader("SELECT epoch_ns(timestamp) AS ns,name,meta,value FROM (" + sql + ");")
			data, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%v: %s", err, data)
			}
			var wire []struct {
				NS                int64
				Name, Meta, Value string
			}
			if len(strings.TrimSpace(string(data))) > 0 {
				if err = json.Unmarshal(data, &wire); err != nil {
					t.Fatal(err, string(data))
				}
			}
			got := []row{}
			for _, r := range wire {
				value := r.Value
				if value == "" {
					value = "null"
				}
				v := row{NS: r.NS, Name: r.Name, Value: json.RawMessage(value)}
				if err = json.Unmarshal([]byte(r.Meta), &v.Meta); err != nil {
					t.Fatal(err)
				}
				got = append(got, v)
			}
			groups := map[string][]row{}
			for _, r := range expected {
				key := identity(r)
				groups[key] = append(groups[key], r)
			}
			want := []row{}
			for _, group := range groups {
				sort.Slice(group, func(i, j int) bool { return group[i].NS < group[j].NS })
				n := limit
				if n > len(group) {
					n = len(group)
				}
				if tail {
					want = append(want, group[len(group)-n:]...)
				} else {
					want = append(want, group[:n]...)
				}
			}
			sort.Slice(want, func(i, j int) bool { return want[i].NS < want[j].NS })
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("tail=%t limit=%d\ngot %#v\nwant %#v", tail, limit, got, want)
			}
		}
	}
}

func TestSpanningSelectionRawUnion(t *testing.T) {
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2")
	}
	root := t.TempDir()
	coldPath, hotPath := root+"/cold.parquet", root+"/hot.parquet"
	run := func(sql string) []byte {
		t.Helper()
		cmd := exec.Command(binary, "-batch", "-bail", "-json")
		cmd.Stdin = strings.NewReader(sql)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return out
	}
	run("SET TimeZone='UTC'; COPY (SELECT make_timestamp_ns(ns)::TIMESTAMPTZ_NS AS time,'t' AS name,ns::VARIANT AS value,MAP {'vsn':'A'} AS meta,'P' AS plugin,NULL::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone FROM (VALUES (100::BIGINT),(200),(300),(400)) r(ns)) TO " + sqlQuote(coldPath) + " (FORMAT PARQUET); COPY (SELECT ns AS time,'t' AS name,MAP {'plugin':'P','vsn':'A'} AS meta,'uint' AS kind,ns::VARCHAR AS value FROM (VALUES (300::BIGINT),(500)) r(ns)) TO " + sqlQuote(hotPath) + " (FORMAT PARQUET);")
	border := time.Unix(0, 300).UTC()
	for _, tail := range []bool{false, true} {
		n := 2
		q := &model.Query{Start: time.Unix(0, 1).UTC().Format(time.RFC3339Nano), End: time.Unix(0, 1000).UTC().Format(time.RFC3339Nano), Filter: map[string]string{"name": "t"}}
		if tail {
			q.Tail = &n
		} else {
			q.Head = &n
		}
		raw := *q
		raw.End = border.Format(time.RFC3339Nano)
		source, err := buildColdRawSource(&raw, "fixture.metadata.json", "", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		sql, err := tiered.BuildSelectionSQL(q, tiered.UnionSource(source, hotPath), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		sql = strings.ReplaceAll(sql, "iceberg_scan('fixture.metadata.json')", "read_parquet("+sqlQuote(coldPath)+")")
		out := run("SET TimeZone='UTC'; SELECT epoch_ns(timestamp) AS ns,value FROM (" + sql + ");")
		var got []struct {
			NS    int64
			Value string
		}
		if err = json.Unmarshal(out, &got); err != nil {
			t.Fatal(err, string(out))
		}
		want := []int64{100, 200}
		if tail {
			want = []int64{300, 500}
		}
		if len(got) != 2 || got[0].NS != want[0] || got[1].NS != want[1] {
			t.Fatalf("wrong per-series/border selection: %s", out)
		}
		for _, r := range got {
			if r.Value != strconv.FormatInt(r.NS, 10) {
				t.Fatal("value changed", r)
			}
		}
	}
}
