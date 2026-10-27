// projection_probe_test.go contains tests that probe the cold projection.
package cold

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	model "beehive-data-api/internal/query"
)

// coldMetaBeforeProjectionOptimization returns the cold meta before projection optimization.
// Frozen expression before the exp67 change, evaluated on DuckDB 2.0. This is
// an experiment reference, not support for an older engine or public schema.
func coldMetaBeforeProjectionOptimization() string {
	keys, pairs := make([]string, len(promotedTags)), make([]string, len(promotedTags))
	for i, tag := range promotedTags {
		keys[i] = sqlQuote(tag)
		pairs[i] = sqlQuote(tag) + ", " + tag
	}
	return fmt.Sprintf(`CASE WHEN list_has_any(map_keys(meta), [%s])
 THEN to_json(map_concat(COALESCE(meta, MAP {}::MAP(VARCHAR, VARCHAR)), %s))::VARCHAR
 ELSE json_merge_patch(to_json(COALESCE(meta, MAP {}::MAP(VARCHAR, VARCHAR))),
 json_merge_patch('{}', json_object(%s)))::VARCHAR END`, strings.Join(keys, ", "), promotedTagMap(), strings.Join(pairs, ", "))
}

// TestColdProjectionProbe tests the cold projection probe.
func TestColdProjectionProbe(t *testing.T) {
	root := os.Getenv("COLD_PROJECTION_PROBE_DIR")
	if root == "" {
		t.Skip("exp67 native projection experiment only")
	}
	raw, err := os.ReadFile(os.Getenv("COLD_PROJECTION_QUERIES"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name  string
		Query model.Query
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range cases {
		source, _, err := coldResponseSource(&c.Query, "__SAGE_METADATA__", now)
		if err != nil {
			t.Fatal(err)
		}
		oldMeta := coldMetaBeforeProjectionOptimization()
		if c.Query.Head != nil || c.Query.Tail != nil {
			oldMeta = "to_json(meta)::VARCHAR"
		}
		baseline := coldArrowProjectionWithMeta(source, oldMeta)
		candidate, err := buildColdArrowSQL(&c.Query, "__SAGE_METADATA__", now)
		if err != nil {
			t.Fatal(err)
		}
		statements := map[string]string{"baseline": baseline, "optimized": candidate, "meta_before": oldMeta}
		// Diagnostic outputs change values deliberately. Compare operator work;
		// their elapsed time also includes changed sort payloads and IPC sizes.
		statements["without_meta"] = coldArrowProjectionWithMeta(source, "'{}'::VARCHAR")
		statements["without_value"] = strings.Replace(candidate, coldProjectionValueSQL(), "'null'::VARCHAR", 1)
		statements["without_both"] = strings.Replace(statements["without_meta"], coldProjectionValueSQL(), "'null'::VARCHAR", 1)
		if statements["without_value"] == candidate {
			t.Fatal("could not isolate value projection")
		}
		if c.Query.Head == nil && c.Query.Tail == nil {
			_, currentMeta, err := coldResponseSource(&c.Query, "__SAGE_METADATA__", now)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"full", "without_meta", "without_value", "without_both"} {
				statements["cost_"+mode] = coldProjectionCostSQL(source, currentMeta,
					mode == "without_value" || mode == "without_both", mode == "without_meta" || mode == "without_both")
			}
			statements["cardinality"] = fmt.Sprintf(`SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,
    'metadata cardinality'::VARCHAR AS name,
    to_json(struct_pack(rows:=count(*),metadata_sets:=count(DISTINCT struct_pack(meta:=meta,
    plugin:=plugin,vsn:=vsn,node:=node,host:=host,task:=task,job:=job,zone:=zone))))::VARCHAR AS value,
    '{}'::VARCHAR AS meta FROM (%s)`, source)
		}
		for mode, sql := range statements {
			if err := os.WriteFile(filepath.Join(root, c.Name+"-"+mode+".sql"), []byte(sql), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// coldProjectionValueSQL returns the cold projection value SQL.
// Match the production projection exactly, so the ablation cannot silently
// become a full query if its SQL changes. This is diagnostic test code only.
func coldProjectionValueSQL() string {
	return "to_json(value)::VARCHAR"
}

// coldProjectionCostSQL returns the cold projection cost SQL.
// Diagnostic only: hash every raw field in every mode to keep input column
// requirements fixed. Tiny aggregate output avoids response sort/IPC payload
// differences. Measures JSON formation plus checksum hashing, not pure JSON CPU.
func coldProjectionCostSQL(source, meta string, omitValue, omitMeta bool) string {
	value := coldProjectionValueSQL()
	if omitValue {
		value = "'null'::VARCHAR"
	}
	if omitMeta {
		meta = "'{}'::VARCHAR"
	}
	guard := "hash(timestamp,name,value,meta," + strings.Join(promotedTags, ",") + ")"
	return fmt.Sprintf(`SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,
 'projection cost diagnostic'::VARCHAR AS name,
 to_json(struct_pack(rows:=count(*),source_hash_sum:=sum(%s)::VARCHAR,
 source_hash_xor:=bit_xor(%s)::VARCHAR,value_hash_sum:=sum(hash(%s))::VARCHAR,
 meta_hash_sum:=sum(hash(%s))::VARCHAR))::VARCHAR AS value,
 '{}'::VARCHAR AS meta FROM (%s)`, guard, guard, value, meta, source)
}

// TestColdProjectionDiagnostics tests the cold projection diagnostics.
func TestColdProjectionDiagnostics(t *testing.T) {
	q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-04-01T00:00:00Z"}
	source, meta, err := coldResponseSource(q, "pinned.metadata.json", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, omitted := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
		statement := coldProjectionCostSQL(source, meta, omitted[0], omitted[1])
		for _, want := range []string{"hash(timestamp,name,value,meta,plugin,vsn,node,host,task,job,zone)", "source_hash_sum", "source_hash_xor", "count(*)", "iceberg_scan('pinned.metadata.json')"} {
			if !strings.Contains(statement, want) {
				t.Fatalf("missing %q", want)
			}
		}
		if strings.Contains(statement, "ORDER BY") {
			t.Fatal("checksum diagnostic must not sort")
		}
	}
}
