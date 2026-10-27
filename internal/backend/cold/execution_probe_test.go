// execution_probe_test.go contains tests that probe the cold execution.
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

// TestColdExecutionProbe tests the cold execution probe.
// Export actual baseline/current SQL for exp63's DuckDB execution profiles.
// Baseline retains the original metadata MAP assembly, not a second hand-written
// interpretation of response semantics. No network access in this probe.
func TestColdExecutionProbe(t *testing.T) {
	root := os.Getenv("COLD_EXECUTION_PROBE_DIR")
	if root == "" {
		t.Skip("exp63 profiling/semantic harness only")
	}
	var cases []struct {
		Name  string
		Query model.Query
	}
	raw, err := os.ReadFile(os.Getenv("COLD_EXECUTION_QUERIES"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range cases {
		inner, err := buildColdInner(&c.Query, "__SAGE_METADATA__", now)
		if err != nil {
			t.Fatal(err)
		}
		optimized, err := buildColdArrowSQL(&c.Query, "__SAGE_METADATA__", now)
		if err != nil {
			t.Fatal(err)
		}
		ndjson, err := buildColdSQL(&c.Query, "__SAGE_METADATA__", now)
		if err != nil {
			t.Fatal(err)
		}
		delayed := coldArrowProjection("SELECT * FROM (" + inner + ") ORDER BY timestamp")
		// Leave only the inner sort, so EXPLAIN can measure late JSON formatting.
		delayed = strings.TrimSuffix(strings.TrimSpace(delayed), "ORDER BY timestamp")
		statements := map[string]string{
			"baseline": coldArrowProjection(inner), "optimized": optimized, "delayed_json": delayed,
			"baseline_ndjson":  fmt.Sprintf("SELECT timestamp,name,to_json(value)::VARCHAR AS value,to_json(meta)::VARCHAR AS meta FROM (%s) ORDER BY timestamp", inner),
			"optimized_ndjson": ndjson,
		}
		for mode, sql := range statements {
			if err := os.WriteFile(filepath.Join(root, c.Name+"-"+mode+".sql"), []byte(sql), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
