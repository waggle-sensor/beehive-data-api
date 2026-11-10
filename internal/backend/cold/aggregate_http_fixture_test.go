// aggregate_http_fixture_test.go supports the actual production HTTP tests.

package cold

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"beehive-data-api/internal/arrowio"
	model "beehive-data-api/internal/query"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type aggregationFixtureRecord struct {
	Time  int64             `json:"time"`
	Name  string            `json:"name"`
	Meta  map[string]string `json:"meta"`
	Value json.RawMessage   `json:"value"`
	Kind  string            `json:"kind"`
}
type aggregationFixtureCase struct {
	ID       string                     `json:"id"`
	Request  model.Query                `json:"request"`
	Border   string                     `json:"border"`
	Expected []aggregationFixtureRecord `json:"expected"`
}
type aggregationFixtureManifest struct {
	Root     string                   `json:"root"`
	Metadata string                   `json:"metadata"`
	Cases    []aggregationFixtureCase `json:"cases"`
}
type aggregationFixtureTransport func(*http.Request) (*http.Response, error)

func (f aggregationFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func aggregationFixtureDecode(t *testing.T, format string, body []byte) []aggregationFixtureRecord {
	t.Helper()
	var output []aggregationFixtureRecord
	if format == "arrow" {
		reader, err := ipc.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Release()
		if !reader.Schema().Equal(arrowio.Schema) {
			t.Fatal("public Arrow schema changed", reader.Schema())
		}
		for reader.Next() {
			record := reader.RecordBatch()
			for i := 0; i < int(record.NumRows()); i++ {
				row := aggregationFixtureRecord{Time: int64(record.Column(0).(*array.Timestamp).Value(i)), Name: record.Column(1).(*array.String).Value(i), Value: json.RawMessage("null")}
				value := record.Column(2).(*array.String)
				if !value.IsNull(i) {
					row.Value = json.RawMessage(value.Value(i))
				}
				if err := json.Unmarshal([]byte(record.Column(3).(*array.String).Value(i)), &row.Meta); err != nil {
					t.Fatal(err)
				}
				output = append(output, row)
			}
		}
		if err := reader.Err(); err != nil {
			t.Fatal(err)
		}
	} else {
		decoder := json.NewDecoder(bytes.NewReader(body))
		for {
			var row struct {
				Timestamp time.Time         `json:"timestamp"`
				Name      string            `json:"name"`
				Value     json.RawMessage   `json:"value"`
				Meta      map[string]string `json:"meta"`
			}
			err := decoder.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			output = append(output, aggregationFixtureRecord{Time: row.Timestamp.UnixNano(), Name: row.Name, Value: row.Value, Meta: row.Meta})
		}
	}
	return output
}
func aggregationFixtureCompare(t *testing.T, got, want []aggregationFixtureRecord) {
	t.Helper()
	key := func(row aggregationFixtureRecord) string {
		meta, _ := json.Marshal(row.Meta)
		return fmt.Sprintf("%020d/%s/%s", row.Time, row.Name, meta)
	}
	sort.Slice(got, func(i, j int) bool { return key(got[i]) < key(got[j]) })
	want = append([]aggregationFixtureRecord(nil), want...)
	sort.Slice(want, func(i, j int) bool { return key(want[i]) < key(want[j]) })
	if len(got) != len(want) {
		t.Fatalf("rows %d != %d", len(got), len(want))
	}
	for i, a := range got {
		b := want[i]
		if a.Time != b.Time || a.Name != b.Name || !reflect.DeepEqual(a.Meta, b.Meta) {
			t.Fatalf("identity differs: %#v / %#v", a, b)
		}
		if bytes.Equal(b.Value, []byte("null")) {
			if !bytes.Equal(a.Value, []byte("null")) {
				t.Fatal("null changed", string(a.Value))
			}
			continue
		}
		if b.Kind == "int" || b.Kind == "uint" {
			left, ok := new(big.Int).SetString(string(a.Value), 10)
			if !ok {
				t.Fatal("integer narrowed/noninteger JSON", string(a.Value))
			}
			right, ok := new(big.Int).SetString(string(b.Value), 10)
			if !ok || left.Cmp(right) != 0 {
				t.Fatal("integer changed", string(a.Value), string(b.Value))
			}
		} else if b.Kind == "string" || b.Kind == "bool" {
			var left, right any
			if json.Unmarshal(a.Value, &left) != nil || json.Unmarshal(b.Value, &right) != nil || !reflect.DeepEqual(left, right) {
				t.Fatal("scalar changed", string(a.Value), string(b.Value))
			}
		} else {
			var left, right float64
			if json.Unmarshal(a.Value, &left) != nil || json.Unmarshal(b.Value, &right) != nil || math.Abs(left-right) > math.Max(1e-10, 1e-12*math.Abs(right)) {
				t.Fatal("float changed", string(a.Value), string(b.Value))
			}
		}
	}
}

// headTailFixtureCases derives an independent observation oracle from original
// fixture rows, rather than using aggregate outputs or SQL to choose limits.
func headTailFixtureCases(t *testing.T, cases []aggregationFixtureCase, rows []aggregationFixtureRecord) []aggregationFixtureCase {
	t.Helper()
	seen := map[string]bool{}
	var result []aggregationFixtureCase
	for _, tc := range cases {
		key := tc.Request.Start + "/" + tc.Request.End + "/" + tc.Request.Filter["name"] + "/" + tc.Border
		if seen[key] {
			continue
		}
		seen[key] = true
		if len(tc.Request.Filter) != 1 {
			t.Fatal("head/tail fixture expects an exact name filter")
		}
		start, e1 := time.Parse(time.RFC3339Nano, tc.Request.Start)
		end, e2 := time.Parse(time.RFC3339Nano, tc.Request.End)
		if e1 != nil || e2 != nil {
			t.Fatal("invalid fixture bounds")
		}
		groups := map[string][]aggregationFixtureRecord{}
		for _, row := range rows {
			if row.Name != tc.Request.Filter["name"] || row.Time < start.UnixNano() || row.Time >= end.UnixNano() {
				continue
			}
			tags, _ := json.Marshal(row.Meta)
			series := row.Name + string(tags)
			groups[series] = append(groups[series], row)
		}
		placement := "split"
		for _, p := range []string{"cold_only", "hot_only"} {
			if strings.HasSuffix(tc.ID, "-"+p) {
				placement = p
			}
		}
		for _, tail := range []bool{false, true} {
			for _, n := range []int{0, 1, 2, 1000} {
				q := tc.Request
				q.Func = nil
				q.Window = nil
				q.Head = nil
				q.Tail = nil
				limit := n
				mode := "head"
				if tail {
					q.Tail = &limit
					mode = "tail"
				} else {
					q.Head = &limit
				}
				var expected []aggregationFixtureRecord
				for _, group := range groups {
					ordered := append([]aggregationFixtureRecord(nil), group...)
					sort.Slice(ordered, func(i, j int) bool { return ordered[i].Time < ordered[j].Time })
					count := n
					if count > len(ordered) {
						count = len(ordered)
					}
					if tail {
						expected = append(expected, ordered[len(ordered)-count:]...)
					} else {
						expected = append(expected, ordered[:count]...)
					}
				}
				result = append(result, aggregationFixtureCase{ID: fmt.Sprintf("selection-%d-%s%d-%s", len(seen), mode, n, placement), Request: q, Border: tc.Border, Expected: expected})
			}
		}
	}
	return result
}
