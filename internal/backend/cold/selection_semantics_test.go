// selection_semantics_test.go checks that raw tag aliases merge into one public series.

package cold

import (
	"beehive-data-api/internal/backend/tiered"
	model "beehive-data-api/internal/query"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

// This oracle merges raw metadata in Go and chooses observations per public
// series. It does not keep an alternative SQL selector in the API package.
func TestSpanningSelectionCanonicalAliases(t *testing.T) {
	source := `SELECT make_timestamp_ns(1740787200000000194+i*1000003) AS timestamp,
 CASE WHEN i%7=0 THEN 'other' ELSE 't' END AS name,
 CASE i%8
 WHEN 0 THEN MAP {'vsn':v,'sensor':'s','plugin':'old'}
 WHEN 1 THEN MAP {'plugin':'older','sensor':'s','vsn':v}
 WHEN 2 THEN MAP {'sensor':'s','plugin':'P','vsn':v}
 WHEN 3 THEN MAP {'plugin':'P','vsn':v,'sensor':'s'}
 WHEN 4 THEN NULL::MAP(VARCHAR,VARCHAR)
 WHEN 5 THEN MAP {}::MAP(VARCHAR,VARCHAR)
 WHEN 6 THEN MAP {'sensor':NULL::VARCHAR}
 ELSE MAP {'vsn':v,'sensor':NULL::VARCHAR,'plugin':'obsolete'} END AS meta,
 CASE WHEN i%8 IN (2,3) THEN NULL::VARCHAR ELSE 'P' END AS plugin,
 CASE WHEN i%8 IN (4,5,6) THEN v ELSE NULL::VARCHAR END AS vsn,
 NULL::VARCHAR AS node,NULL::VARCHAR AS host,NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone,
 CASE i%6 WHEN 0 THEN 18446744073709551615::UBIGINT::VARIANT
 WHEN 1 THEN '-9223372036854775808'::BIGINT::VARIANT
 WHEN 2 THEN TRUE::VARIANT WHEN 3 THEN 'text'::VARIANT
 WHEN 4 THEN NULL::VARIANT ELSE (i/7.0)::DOUBLE::VARIANT END AS value
 FROM (SELECT i,CAST((i//7)%3 AS VARCHAR) AS v FROM range(2048) r(i))`

	type observation struct {
		NS                int64
		Name, Value, Meta string
	}
	var raw []struct {
		observation
		Plugin, VSN, Node, Host, Task, Job, Zone *string
	}
	out := selectionScaleCLI(t, "SELECT epoch_ns(timestamp) AS ns,name,to_json(value)::VARCHAR AS value,to_json(meta)::VARCHAR AS meta,plugin,vsn,node,host,task,job,zone FROM ("+source+") ORDER BY timestamp;")
	if err := json.Unmarshal(out, &raw); err != nil {
		t.Fatal(err)
	}
	groups := map[string][]observation{}
	for _, row := range raw {
		meta := map[string]*string{}
		if row.Meta != "" {
			if err := json.Unmarshal([]byte(row.Meta), &meta); err != nil {
				t.Fatal(err)
			}
		}
		if meta == nil {
			meta = map[string]*string{}
		}
		for key, value := range map[string]*string{"plugin": row.Plugin, "vsn": row.VSN, "node": row.Node, "host": row.Host, "task": row.Task, "job": row.Job, "zone": row.Zone} {
			if value != nil {
				meta[key] = value
			}
		}
		canonical, err := json.Marshal(meta)
		if err != nil {
			t.Fatal(err)
		}
		selected := row.observation
		selected.Meta = string(canonical)
		key := selected.Name + selected.Meta
		groups[key] = append(groups[key], selected)
	}
	for _, tail := range []bool{false, true} {
		for _, n := range []int{0, 1, 2, 10, 1000} {
			t.Run(fmt.Sprintf("tail_%t_n_%d", tail, n), func(t *testing.T) {
				expected := []observation{}
				for _, group := range groups {
					count := n
					if count > len(group) {
						count = len(group)
					}
					if tail {
						expected = append(expected, group[len(group)-count:]...)
					} else {
						expected = append(expected, group[:count]...)
					}
				}
				sort.Slice(expected, func(i, j int) bool { return expected[i].NS < expected[j].NS })
				q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-02T00:00:00Z"}
				if tail {
					q.Tail = &n
				} else {
					q.Head = &n
				}
				statement, err := tiered.BuildSelectionSQL(q, source, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				result := selectionScaleCLI(t, "SELECT epoch_ns(timestamp) AS ns,name,value,meta FROM ("+statement+");")
				actual := []observation{}
				if err = json.Unmarshal(result, &actual); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(expected, actual) {
					t.Fatalf("selection differs from independent Go oracle: expected %d rows, got %d", len(expected), len(actual))
				}
			})
		}
	}
}
