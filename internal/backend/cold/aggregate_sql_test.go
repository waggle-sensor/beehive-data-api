// aggregate_sql_test.go checks the aggregate statement against an independent oracle.

package cold

import (
	"beehive-data-api/internal/backend/tiered"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/query/aggregate"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAggregateCompilerOracle(t *testing.T) {
	manifestPath, binary := os.Getenv("COLD_AGGREGATION_MANIFEST"), os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if manifestPath == "" || binary == "" {
		t.Skip("set COLD_AGGREGATION_MANIFEST and COLD_AGGREGATION_DUCKDB2")
	}
	version, err := exec.Command(binary, "--version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), "v2.0.") {
		t.Fatalf("DuckDB 2.0 CLI required: %s (%v)", version, err)
	}
	type expected struct {
		Time  int64             `json:"time"`
		Name  string            `json:"name"`
		Meta  map[string]string `json:"meta"`
		Value json.RawMessage   `json:"value"`
		Kind  string            `json:"kind"`
	}
	var manifest struct {
		Root, Metadata string
		Cases          []struct {
			ID       string
			Request  model.Query
			Border   string
			Expected []expected
		}
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	var header struct {
		Properties map[string]string `json:"properties"`
	}
	raw, err = os.ReadFile(strings.TrimPrefix(manifest.Metadata, "file://"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &header); err != nil {
		t.Fatal(err)
	}
	encoding := header.Properties["sage.value.encoding"]
	run := func(sql string) []byte {
		t.Helper()
		cmd := exec.Command(binary, "-batch", "-bail", "-json")
		cmd.Stdin = strings.NewReader("LOAD iceberg;" + sql)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("native SQL: %v\n%s\n%s", err, out, sql)
		}
		return out
	}
	stage := t.TempDir()
	for _, tc := range manifest.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			sourceQuery := tc.Request
			var hotPath string
			if strings.HasSuffix(tc.ID, "-split") {
				input := filepath.Join(manifest.Root, "data", strings.TrimSuffix(tc.ID, "-split")+"-hot.parquet")
				path := filepath.Join(stage, tc.ID+".parquet")
				run("COPY (SELECT time_ns AS time,name,meta,kind,text AS value FROM read_parquet(" + sqlQuote(input) + ")) TO " + sqlQuote(path) + " (FORMAT PARQUET);")
				border, err := time.Parse(time.RFC3339Nano, tc.Border)
				if err != nil {
					t.Fatal(err)
				}
				sourceQuery.End = border.UTC().Format(time.RFC3339Nano)
				hotPath = path
			}
			source, err := buildColdRawSource(&sourceQuery, manifest.Metadata, encoding, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if hotPath != "" {
				source = tiered.UnionSource(source, hotPath)
			}
			sql, err := aggregate.BuildSQL(&tc.Request, source, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			data := run("SELECT epoch_ns(timestamp) AS time,name,value,meta FROM (" + sql + ");")
			var got []struct {
				Time  int64
				Name  string
				Value *string
				Meta  string
			}
			if err = json.Unmarshal(data, &got); err != nil {
				t.Fatalf("%v: %s", err, data)
			}
			if len(got) != len(tc.Expected) {
				t.Fatalf("rows %d != %d", len(got), len(tc.Expected))
			}
			// Order by canonical identity so ties do not inherit an engine's MAP order.
			actual := map[string]*string{}
			identity := func(stamp int64, name string, meta map[string]string) string {
				b, _ := json.Marshal(meta)
				return fmt.Sprint(stamp, "/", name, "/", string(b))
			}
			for _, row := range got {
				var meta map[string]string
				if err = json.Unmarshal([]byte(row.Meta), &meta); err != nil {
					t.Fatal(err)
				}
				key := identity(row.Time, row.Name, meta)
				if _, ok := actual[key]; ok {
					t.Fatal("duplicate aggregate identity")
				}
				actual[key] = row.Value
			}
			for _, want := range tc.Expected {
				text, ok := actual[identity(want.Time, want.Name, want.Meta)]
				if !ok {
					t.Fatal("missing aggregate identity")
				}
				if string(want.Value) == "null" {
					if text != nil && *text != "null" {
						t.Fatal("null changed")
					}
					continue
				}
				if text == nil {
					t.Fatal("value became null")
				}
				if want.Kind == "float" {
					var a, b float64
					if json.Unmarshal([]byte(*text), &a) != nil || json.Unmarshal(want.Value, &b) != nil || math.Abs(a-b) > math.Max(1e-10, 1e-12*math.Abs(b)) {
						t.Fatalf("float %s != %s", *text, want.Value)
					}
				} else {
					a, okA := new(big.Int).SetString(*text, 10)
					b, okB := new(big.Int).SetString(string(want.Value), 10)
					if !okA || !okB || a.Cmp(b) != 0 {
						t.Fatalf("exact integer %s != %s", *text, want.Value)
					}
				}
			}
		})
	}
}
