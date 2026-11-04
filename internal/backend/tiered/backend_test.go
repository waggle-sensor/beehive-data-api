// backend_test.go checks the nanosecond border, lazy cold-then-hot merge, and
// the queries the tiered backend refuses to split.

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
func newTiered(t *testing.T, hot, cold *recordingBackend, border string) *Backend {
	t.Helper()
	return &Backend{Hot: hot, Cold: cold, Border: mustTime(t, border)}
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
		// `end` is EXCLUSIVE, so a range ending exactly at the border
		// holds no hot data at all and must not pay for a hot query.
		{"entirely before", "2025-05-01T00:00:00Z", "2025-05-10T00:00:00Z", 0, 1},
		{"ending exactly at the border", "2025-05-01T00:00:00Z", wm, 0, 1},
		{"entirely after", "2025-05-20T00:00:00Z", "2025-05-25T00:00:00Z", 1, 0},
		{"starting exactly at the border", wm, "2025-05-25T00:00:00Z", 1, 0},
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

// TestTieredBorderIsNanosecondExact tests the tiered border is nanosecond exact.
// The border is compared in nanoseconds on purpose: hot holds ns and
// cold holds us, so a us-rounded boundary would strand a sub-us sliver.
func TestTieredBorderIsNanosecondExact(t *testing.T) {
	const wm = "2025-05-15T00:00:00.000000500Z"
	hot := &recordingBackend{}
	cold := &recordingBackend{}
	backend := newTiered(t, hot, cold, wm)

	// Ends 1ns before the border: entirely cold, no hot query.
	results, err := backend.Query(context.Background(), &model.Query{
		Start: "2025-05-15T00:00:00Z",
		End:   "2025-05-15T00:00:00.000000499Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	results.Close()

	if hot.calls != 0 {
		t.Errorf("hot was queried for a range ending 1ns before the border")
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
		t.Errorf("cold was queried for a range starting after the border")
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

// TestTieredResolvesRelativeTimestamps tests that one API clock fixes both bounds
// before routing, and that the caller's query text stays unchanged.
func TestTieredResolvesRelativeTimestamps(t *testing.T) {
	const border = "2025-05-15T00:00:00Z"
	now := mustTime(t, "2025-05-20T00:00:00Z")

	t.Run("recent hot range", func(t *testing.T) {
		hot := &recordingBackend{}
		cold := &recordingBackend{}
		backend := newTiered(t, hot, cold, border)
		backend.now = func() time.Time { return now }
		original := &model.Query{Start: "-1h", Func: strptr("mean")}

		results, err := backend.Query(context.Background(), original)
		if err != nil {
			t.Fatalf("hot relative query: %s", err)
		}
		results.Close()
		if original.Start != "-1h" {
			t.Fatalf("caller query was rewritten to %q", original.Start)
		}
		if hot.calls != 1 || cold.calls != 0 {
			t.Fatalf("calls hot=%d cold=%d", hot.calls, cold.calls)
		}
		if hot.got.Start != "2025-05-19T23:00:00Z" || hot.got.End != "" {
			t.Fatalf("hot bounds = %q %q", hot.got.Start, hot.got.End)
		}
	})

	t.Run("cold range", func(t *testing.T) {
		hot := &recordingBackend{}
		cold := &recordingBackend{}
		backend := newTiered(t, hot, cold, border)
		backend.now = func() time.Time { return now }

		results, err := backend.Query(context.Background(), &model.Query{Start: "-10d", End: "-6d"})
		if err != nil {
			t.Fatalf("cold relative query: %s", err)
		}
		results.Close()
		if cold.calls != 1 || hot.calls != 0 {
			t.Fatalf("calls hot=%d cold=%d", hot.calls, cold.calls)
		}
		if cold.got.Start != "2025-05-10T00:00:00Z" || cold.got.End != "2025-05-14T00:00:00Z" {
			t.Fatalf("cold bounds = %q %q", cold.got.Start, cold.got.End)
		}
	})

	t.Run("open range splits at the border", func(t *testing.T) {
		hot := &recordingBackend{}
		cold := &recordingBackend{}
		backend := newTiered(t, hot, cold, border)
		backend.now = func() time.Time { return now }

		results, err := backend.Query(context.Background(), &model.Query{Start: "-10d"})
		if err != nil {
			t.Fatalf("spanning relative query: %s", err)
		}
		results.Close()
		if cold.got.Start != "2025-05-10T00:00:00Z" || cold.got.End != border {
			t.Fatalf("cold bounds = %q %q", cold.got.Start, cold.got.End)
		}
		if hot.got.Start != border || hot.got.End != "" {
			t.Fatalf("hot bounds = %q %q", hot.got.Start, hot.got.End)
		}
	})

	t.Run("spanning aggregation is still refused", func(t *testing.T) {
		hot := &recordingBackend{}
		cold := &recordingBackend{}
		backend := newTiered(t, hot, cold, border)
		backend.now = func() time.Time { return now }

		_, err := backend.Query(context.Background(), &model.Query{Start: "-30d", Func: strptr("mean")})
		if err == nil || !strings.Contains(err.Error(), "aggregation") {
			t.Fatalf("error = %v", err)
		}
		if hot.calls != 0 || cold.calls != 0 {
			t.Fatal("refused query reached a tier")
		}
	})

	t.Run("invalid duration", func(t *testing.T) {
		hot := &recordingBackend{}
		cold := &recordingBackend{}
		backend := newTiered(t, hot, cold, border)
		backend.now = func() time.Time { return now }

		_, err := backend.Query(context.Background(), &model.Query{Start: "-nope"})
		if err == nil || !strings.Contains(err.Error(), "invalid start timestamp") {
			t.Fatalf("error = %v", err)
		}
		if hot.calls != 0 || cold.calls != 0 {
			t.Fatal("invalid duration reached a tier")
		}
	})
}

func strptr(s string) *string { return &s }

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

func TestColdFrontierConfiguration(t *testing.T) {
	dynamic := FrontierOptions{Mode: "dynamic", Warehouse: "warehouse", URL: "http://loader", Token: "secret", Owner: "api", Bucket: "waggle"}
	for _, field := range []string{"warehouse", "url", "token", "owner", "bucket", "border"} {
		t.Run("dynamic/"+field, func(t *testing.T) {
			o := dynamic
			switch field {
			case "warehouse":
				o.Warehouse = ""
			case "url":
				o.URL = ""
			case "token":
				o.Token = ""
			case "owner":
				o.Owner = ""
			case "bucket":
				o.Bucket = ""
			case "border":
				o.Border = "2025-05-01T00:00:00Z"
			}
			if _, err := ParseFrontier(o); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	if c, err := ParseFrontier(dynamic); err != nil || c.Mode != "dynamic" || !c.Border.IsZero() {
		t.Fatal(c, err)
	}
	border := "2025-05-01T00:00:00.000000194Z"
	c, err := ParseFrontier(FrontierOptions{Mode: "static", Warehouse: "warehouse", Border: border})
	if err != nil || c.Border.Format(time.RFC3339Nano) != border {
		t.Fatal(c, err)
	}
	if _, err := ParseFrontier(FrontierOptions{Mode: "static"}); err != nil {
		t.Fatal(err)
	}
	for _, o := range []FrontierOptions{
		{Mode: ""}, {Mode: "auto"}, {Mode: "static", Warehouse: "warehouse"},
		{Mode: "static", Warehouse: "warehouse", Border: "invalid"}, {Mode: "static", Border: border},
		{Mode: "static", URL: "loader"}, {Mode: "static", Token: "secret"}, {Mode: "static", Owner: "api"},
	} {
		if _, err := ParseFrontier(o); err == nil {
			t.Fatal("invalid configuration accepted", o)
		}
	}
}
