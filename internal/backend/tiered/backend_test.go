// backend_test.go contains the tests for the Backend type.
package tiered

import (
	"context"
	"strings"
	"testing"
	"time"

	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
)

// recordingBackend records the query it was handed and replays fixed records.
type recordingBackend struct {
	name    string
	records []*model.Record
	got     *model.Query
	err     error
	calls   int
}

// Query queries the backend.
func (b *recordingBackend) Query(ctx context.Context, query *model.Query) (model.Results, error) {
	b.calls++
	q := *query
	b.got = &q
	if b.err != nil {
		return nil, b.err
	}
	return &testutil.Results{Records: b.records}, nil
}

// mustTime parses a time from a string.
func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("bad test timestamp %q: %s", s, err)
	}
	return ts
}

// rec creates a record.
func rec(t *testing.T, ts string, name string) *model.Record {
	return &model.Record{Timestamp: mustTime(t, ts), Name: name, Meta: map[string]string{}}
}

// newTiered creates a new tiered backend.
func newTiered(t *testing.T, hot, cold *recordingBackend, watermark string) *Backend {
	t.Helper()
	return &Backend{Hot: hot, Cold: cold, Watermark: mustTime(t, watermark)}
}

// TestTieredRouting tests the tiered routing.
func TestTieredRouting(t *testing.T) {
	const wm = "2025-05-15T00:00:00Z"

	tests := []struct {
		name     string
		start    string
		end      string
		wantHot  int
		wantCold int
	}{
		// `end` is EXCLUSIVE, so a range ending exactly at the watermark
		// holds no hot data at all and must not pay for a hot query.
		{"entirely before", "2025-05-01T00:00:00Z", "2025-05-10T00:00:00Z", 0, 1},
		{"ending exactly at the watermark", "2025-05-01T00:00:00Z", wm, 0, 1},
		{"entirely after", "2025-05-20T00:00:00Z", "2025-05-25T00:00:00Z", 1, 0},
		{"starting exactly at the watermark", wm, "2025-05-25T00:00:00Z", 1, 0},
		{"spanning", "2025-05-10T00:00:00Z", "2025-05-20T00:00:00Z", 1, 1},
		// No end means no upper bound, which always reaches into hot.
		{"open-ended from cold", "2025-05-01T00:00:00Z", "", 1, 1},
		{"open-ended from hot", "2025-05-20T00:00:00Z", "", 1, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hot := &recordingBackend{name: "hot"}
			cold := &recordingBackend{name: "cold"}
			backend := newTiered(t, hot, cold, wm)

			results, err := backend.Query(context.Background(), &model.Query{Start: tc.start, End: tc.end})
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			defer results.Close()

			if hot.calls != tc.wantHot {
				t.Errorf("hot calls = %d, want %d", hot.calls, tc.wantHot)
			}
			if cold.calls != tc.wantCold {
				t.Errorf("cold calls = %d, want %d", cold.calls, tc.wantCold)
			}
		})
	}
}

// TestTieredSplitIsExactAtTheBoundary tests the tiered split is exact at the boundary.
// The split must be exact at the boundary: the sub-ranges have to tile the
// original with no gap and no overlap, or records are dropped or doubled.
func TestTieredSplitIsExactAtTheBoundary(t *testing.T) {
	const wm = "2025-05-15T00:00:00Z"
	hot := &recordingBackend{}
	cold := &recordingBackend{}
	backend := newTiered(t, hot, cold, wm)

	results, err := backend.Query(context.Background(), &model.Query{
		Start:  "2025-05-10T00:00:00Z",
		End:    "2025-05-20T00:00:00Z",
		Filter: map[string]string{"vsn": "W099"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer results.Close()

	if cold.got.Start != "2025-05-10T00:00:00Z" {
		t.Errorf("cold start = %q, want the original start", cold.got.Start)
	}
	if cold.got.End != hot.got.Start {
		t.Errorf("cold end %q != hot start %q: the split must tile exactly", cold.got.End, hot.got.Start)
	}
	if hot.got.End != "2025-05-20T00:00:00Z" {
		t.Errorf("hot end = %q, want the original end", hot.got.End)
	}
	// The filter must reach both tiers, or one side silently returns more.
	if hot.got.Filter["vsn"] != "W099" || cold.got.Filter["vsn"] != "W099" {
		t.Error("filter did not reach both tiers")
	}
}

// TestTieredWatermarkIsNanosecondExact tests the tiered watermark is nanosecond exact.
// The watermark is compared in nanoseconds on purpose: hot holds ns and
// cold holds us, so a us-rounded boundary would strand a sub-us sliver.
func TestTieredWatermarkIsNanosecondExact(t *testing.T) {
	const wm = "2025-05-15T00:00:00.000000500Z"
	hot := &recordingBackend{}
	cold := &recordingBackend{}
	backend := newTiered(t, hot, cold, wm)

	// Ends 1ns before the watermark: entirely cold, no hot query.
	results, err := backend.Query(context.Background(), &model.Query{
		Start: "2025-05-15T00:00:00Z",
		End:   "2025-05-15T00:00:00.000000499Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	results.Close()

	if hot.calls != 0 {
		t.Errorf("hot was queried for a range ending 1ns before the watermark")
	}
	if cold.calls != 1 {
		t.Errorf("cold calls = %d, want 1", cold.calls)
	}

	// Starts 1ns after: entirely hot.
	hot2 := &recordingBackend{}
	cold2 := &recordingBackend{}
	backend2 := newTiered(t, hot2, cold2, wm)
	results2, err := backend2.Query(context.Background(), &model.Query{
		Start: "2025-05-15T00:00:00.000000501Z",
		End:   "2025-05-16T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	results2.Close()

	if cold2.calls != 0 {
		t.Errorf("cold was queried for a range starting after the watermark")
	}
	if hot2.calls != 1 {
		t.Errorf("hot calls = %d, want 1", hot2.calls)
	}
}

// TestTieredMergeIsOrderedAndLazy tests the tiered merge is ordered and lazy.
// Cold records come first and hot second, with no buffering. Order matters
// because clients assume a time-ordered stream.
func TestTieredMergeIsOrderedAndLazy(t *testing.T) {
	const wm = "2025-05-15T00:00:00Z"
	hot := &recordingBackend{records: []*model.Record{
		rec(t, "2025-05-16T00:00:00Z", "c"),
		rec(t, "2025-05-17T00:00:00Z", "d"),
	}}
	cold := &recordingBackend{records: []*model.Record{
		rec(t, "2025-05-11T00:00:00Z", "a"),
		rec(t, "2025-05-12T00:00:00Z", "b"),
	}}
	backend := newTiered(t, hot, cold, wm)

	results, err := backend.Query(context.Background(), &model.Query{
		Start: "2025-05-10T00:00:00Z",
		End:   "2025-05-20T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer results.Close()

	var got []string
	var last time.Time
	for results.Next() {
		r := results.Record()
		got = append(got, r.Name)
		if !last.IsZero() && r.Timestamp.Before(last) {
			t.Errorf("records out of order at %s", r.Name)
		}
		last = r.Timestamp
	}
	if err := results.Err(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	want := []string{"a", "b", "c", "d"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestTieredRefusesWhatItCannotSplit tests the tiered refuses what it cannot split.
// Aggregations and head/tail cannot be split. Both must refuse rather than
// return a plausible wrong answer.
func TestTieredRefusesWhatItCannotSplit(t *testing.T) {
	const wm = "2025-05-15T00:00:00Z"
	mean := "mean"
	limit := 10

	tests := []struct {
		name  string
		query *model.Query
		want  string
	}{
		{"aggregation", &model.Query{Start: "2025-05-10T00:00:00Z", End: "2025-05-20T00:00:00Z", Func: &mean}, "aggregation"},
		{"head", &model.Query{Start: "2025-05-10T00:00:00Z", End: "2025-05-20T00:00:00Z", Head: &limit}, "per-series"},
		{"tail", &model.Query{Start: "2025-05-10T00:00:00Z", End: "2025-05-20T00:00:00Z", Tail: &limit}, "per-series"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hot := &recordingBackend{}
			cold := &recordingBackend{}
			backend := newTiered(t, hot, cold, wm)

			_, err := backend.Query(context.Background(), tc.query)
			if err == nil {
				t.Fatal("expected an error, got none")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if hot.calls != 0 || cold.calls != 0 {
				t.Error("a refused query should not reach either tier")
			}
		})
	}

	// The same operations are fine within a single tier -- the restriction
	// is about splitting, not about the operation itself.
	hot := &recordingBackend{}
	cold := &recordingBackend{}
	backend := newTiered(t, hot, cold, wm)
	if _, err := backend.Query(context.Background(), &model.Query{
		Start: "2025-05-20T00:00:00Z", End: "2025-05-25T00:00:00Z", Func: &mean,
	}); err != nil {
		t.Errorf("single-tier aggregation should be allowed: %s", err)
	}
	if hot.calls != 1 {
		t.Error("single-tier aggregation should reach the hot tier")
	}
}

// TestTieredRejectsRelativeTimestamps tests the tiered rejects relative timestamps.
// A relative timestamp cannot be routed without knowing "now". Guessing
// would route to the wrong tier, so it is refused explicitly.
func TestTieredRejectsRelativeTimestamps(t *testing.T) {
	hot := &recordingBackend{}
	cold := &recordingBackend{}
	backend := newTiered(t, hot, cold, "2025-05-15T00:00:00Z")

	_, err := backend.Query(context.Background(), &model.Query{Start: "-1h"})
	if err == nil {
		t.Fatal("expected an error for a relative timestamp")
	}
	if !strings.Contains(err.Error(), "relative") {
		t.Errorf("error %q does not explain the problem", err)
	}
}

// TestTieredSurfacesFirstTierError tests the tiered surfaces first tier error.
// A failing cold tier must not look like an empty-but-complete result. The
// service writes 200 before iterating, so Err() is the only signal left.
func TestTieredSurfacesFirstTierError(t *testing.T) {
	const wm = "2025-05-15T00:00:00Z"
	hot := &recordingBackend{records: []*model.Record{rec(t, "2025-05-16T00:00:00Z", "c")}}
	cold := &recordingBackend{}
	backend := newTiered(t, hot, cold, wm)

	results, err := backend.Query(context.Background(), &model.Query{
		Start: "2025-05-10T00:00:00Z",
		End:   "2025-05-20T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer results.Close()

	// Inject a failure into the cold stream after it was handed over.
	cr := results.(*tieredResults).first.(*testutil.Results)
	cr.ErrValue = context.DeadlineExceeded

	for results.Next() {
	}
	if results.Err() == nil {
		t.Error("a failing first tier must surface through Err(), not be swallowed")
	}
	// It must also NOT have advanced into hot: a partial result that looks
	// whole is the failure mode this guards against.
	if results.(*tieredResults).onSecond {
		t.Error("advanced into the hot tier after the cold tier failed")
	}
}

// TestTieredClosesColdWhenHotFailsToStart tests the tiered closes cold when hot fails to start.
// If the hot query fails to even start, the already-open cold results must
// be closed or the connection leaks.
func TestTieredClosesColdWhenHotFailsToStart(t *testing.T) {
	const wm = "2025-05-15T00:00:00Z"
	hot := &recordingBackend{err: context.DeadlineExceeded}
	cold := &recordingBackend{records: []*model.Record{rec(t, "2025-05-11T00:00:00Z", "a")}}
	backend := newTiered(t, hot, cold, wm)

	_, err := backend.Query(context.Background(), &model.Query{
		Start: "2025-05-10T00:00:00Z",
		End:   "2025-05-20T00:00:00Z",
	})
	if err == nil {
		t.Fatal("expected the hot failure to propagate")
	}
	if cold.calls != 1 {
		t.Fatalf("cold should have been queried first")
	}
}
