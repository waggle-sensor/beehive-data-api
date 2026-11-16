// staging_test.go checks the private hot Parquet stage: omitted early rows,
// preserved kinds, and closure when setup or a write fails.

package tiered

import (
	"beehive-data-api/internal/backend/tiered/stage"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/query/aggregate"
	"beehive-data-api/internal/testutil"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type uncertainHotClose struct{ testutil.Results }

func (r *uncertainHotClose) Close() error {
	r.Closed = true
	return errors.New("unconfirmed source close")
}

func readHotStage(t *testing.T, path string) []stage.Row {
	t.Helper()
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2 for independent Parquet decoding")
	}
	cmd := exec.Command(binary, "-batch", "-bail", "-json")
	cmd.Stdin = strings.NewReader("SELECT time,name,meta,kind,value FROM read_parquet(" + aggregate.Quote(path) + ",hive_partitioning=false);")
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, data)
	}
	var rows []stage.Row
	if err = json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err, string(data))
	}
	return rows
}

func TestHotAggregateStageOmitsRowsBeforeSlice(t *testing.T) {
	profile := &recordingStageProfile{phases: map[string]float64{}}
	ctx := model.WithQueryProfile(context.Background(), profile)
	var err error
	start := time.Unix(0, 0).UTC()
	end := start.Add(10 * time.Minute).Add(194)
	q := &model.Query{Start: start.Format(time.RFC3339Nano), End: end.Format(time.RFC3339Nano)}
	inside := time.Unix(0, 60000000195).UTC()
	rows := &testutil.Results{Records: []*model.Record{
		{Timestamp: time.Unix(0, -299999999806).UTC(), Name: "exp86.negative", Value: 4.0},
		{Timestamp: inside, Name: "exp86.negative", Value: 8.0},
	}}
	file := filepath.Join(t.TempDir(), "rows.parquet")
	if err := (&Backend{}).writeHotStage(ctx, rows, file, q); err != nil {
		t.Fatal(err)
	}
	staged := readHotStage(t, file)
	if len(staged) != 1 || staged[0].Time != inside.UnixNano() || staged[0].Value != "8" {
		t.Fatal(staged)
	}
	if profile.phases["aggregate_hot_rows"] != 1 || profile.phases["aggregate_input_bytes"] != float64(stage.InputBytes(staged[0])) {
		t.Fatal("staging diagnostics must count only eligible rows and uncompressed input", profile.phases)
	}
	late := &testutil.Results{Records: []*model.Record{{Timestamp: end, Name: "exp86.negative", Value: 8.0}}}
	if err = (&Backend{}).writeHotStage(context.Background(), late, filepath.Join(t.TempDir(), "late.parquet"), q); err == nil {
		t.Fatal("row at the exclusive end was staged")
	}
}

func TestHotAggregateSetupMarksUncertainClose(t *testing.T) {
	r := &uncertainHotClose{}
	q := &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T00:01:00Z"}
	err := (&Backend{}).writeHotStage(context.Background(), r, filepath.Join(t.TempDir(), "hot.parquet"), q)
	if !r.Closed || !errors.Is(err, model.ErrResourceClosureUncertain) {
		t.Fatal("unsafe grant-release signal", err)
	}
}

func TestHotAggregateStagePreservesKindsAndCloses(t *testing.T) {
	start := time.Unix(1740787200, 194).UTC()
	q := &model.Query{Start: start.Format(time.RFC3339Nano), End: start.Add(time.Minute).Format(time.RFC3339Nano)}
	records := []*model.Record{}
	for _, value := range []any{uint64(18446744073709551615), int64(6), 6.0, "6", true, nil} {
		records = append(records, &model.Record{Timestamp: start, Name: "t", Value: value})
	}
	rows := &testutil.Results{Records: records}
	b := &Backend{}
	file := filepath.Join(t.TempDir(), "rows.parquet")
	if err := b.writeHotStage(context.Background(), rows, file, q); err != nil {
		t.Fatal(err)
	}
	if !rows.Closed {
		t.Fatal("hot results not closed")
	}
	rows = &testutil.Results{Records: records}
	b.TieredMaxStagingBytes = 1
	if err := b.writeHotStage(context.Background(), rows, filepath.Join(t.TempDir(), "bounded.parquet"), q); err == nil || !rows.Closed {
		t.Fatal("budget or hot ownership failed", err)
	}
	var kinds []string
	for _, row := range readHotStage(t, file) {
		kinds = append(kinds, row.Kind)
		if row.Kind == "uint" && row.Value != "18446744073709551615" {
			t.Fatal("uint rounded")
		}
		if row.Meta == nil {
			t.Fatal("nil metadata not normalized")
		}
	}
	if strings.Join(kinds, ",") != "uint,int,float,string,bool,null" {
		t.Fatal(kinds)
	}
}

func TestHotAggregateStageFailureClosesSource(t *testing.T) {
	start := time.Unix(1740787200, 194).UTC()
	q := &model.Query{Start: start.Format(time.RFC3339Nano), End: start.Add(time.Minute).Format(time.RFC3339Nano)}
	failure := errors.New("source iteration failed")
	for _, name := range []string{"cancelled", "source", "invalid-value", "outside-range", "file-exists"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rows := &testutil.Results{Records: []*model.Record{{Timestamp: start, Name: "t", Value: 1.0}}}
			path := filepath.Join(t.TempDir(), "hot.parquet")
			switch name {
			case "cancelled":
				cancel()
			case "source":
				rows.ErrValue = failure
			case "invalid-value":
				rows.Records[0].Value = math.Inf(1)
			case "outside-range":
				rows.Records[0].Timestamp = start.Add(time.Minute)
			case "file-exists":
				if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := (&Backend{}).writeHotStage(ctx, rows, path, q)
			if err == nil || !rows.Closed {
				t.Fatal("failure or source closure lost", err)
			}
			if name == "source" && !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if name == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if name == "file-exists" {
				data, e := os.ReadFile(path)
				if e != nil || string(data) != "existing" {
					t.Fatal("existing file overwritten", e)
				}
			}
		})
	}
}

func TestNativeHotStageCodec(t *testing.T) {
	binary := os.Getenv("COLD_AGGREGATION_DUCKDB2")
	if binary == "" {
		t.Skip("set COLD_AGGREGATION_DUCKDB2")
	}
	work := t.TempDir()
	output := filepath.Join(work, "hot.parquet")
	start := time.Unix(0, 1740787200000000194).UTC()
	hotRows := &testutil.Results{Records: []*model.Record{
		{Timestamp: start, Name: "t", Meta: map[string]string{"vsn": "a", "sensor": "test"}, Value: uint64(18446744073709551615)},
		{Timestamp: start.Add(1), Name: "t", Value: int64(-9223372036854775808)},
		{Timestamp: start.Add(2), Name: "t", Value: nil},
	}}
	if err := (&Backend{}).writeHotStage(context.Background(), hotRows, output, &model.Query{Start: start.Format(time.RFC3339Nano), End: start.Add(time.Minute).Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-batch", "-bail", "-json")
	cmd.Stdin = strings.NewReader("SELECT time,kind,value FROM read_parquet(" + aggregate.Quote(output) + ") ORDER BY time;")
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, data)
	}
	var rows []struct {
		Time        int64
		Kind, Value string
	}
	if err = json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err, string(data))
	}
	if len(rows) != 3 || rows[0].Time != 1740787200000000194 || rows[0].Kind != "uint" || rows[0].Value != "18446744073709551615" || rows[1].Value != "-9223372036854775808" || rows[2].Kind != "null" {
		t.Fatalf("staging coerced native source: %s", data)
	}
}

type recordingStageProfile struct {
	phases map[string]float64
	failed bool
}

func (*recordingStageProfile) Enabled() bool               { return true }
func (*recordingStageProfile) Clock() time.Time            { return time.Now() }
func (p *recordingStageProfile) Value(k string, v float64) { p.phases[k] = v }
func (p *recordingStageProfile) Duration(k string, t time.Time) {
	p.phases[k] = time.Since(t).Seconds()
}
func (p *recordingStageProfile) Mark(k string)      { p.phases[k] = 1 }
func (p *recordingStageProfile) Finish(failed bool) { p.failed = failed }
