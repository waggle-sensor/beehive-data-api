// coordination_test.go checks managed reads against a fake coordinator.
// A grant stays until the result closes, a failed acquire does not fall
// back to the static border, and routing uses the pinned frontier.

package tiered

import (
	model "beehive-data-api/internal/query"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// transport is an HTTP round tripper backed by one function.
type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// result is a finished result stream. Close records that the caller released it.
type result struct {
	closed bool
	err    error
}

func (r *result) Next() bool            { return false }
func (r *result) Record() *model.Record { return nil }
func (r *result) Err() error            { return nil }
func (r *result) Close() error          { r.closed = true; return r.err }

// inner records the query it received and the cold metadata on that context.
type inner struct {
	metadata string
	res      *result
	fail     bool
	ctx      context.Context
	got      *model.Query
}

func (b *inner) Query(ctx context.Context, query *model.Query) (model.Results, error) {
	b.ctx = ctx
	copied := *query
	b.got = &copied
	b.metadata = model.ColdMetadata(ctx)
	if b.fail {
		return nil, errors.New("query failed")
	}
	return b.res, nil
}

// fixture serves one successful acquire, a failing renew, and one release.
// The acquired frontier is 2025-03-02 and coverage starts on 2025-03-01.
// Renewal cannot extend that deadline. Release is refused until the inner
// result closes, unless the query was never going to return that result.
func fixture(t *testing.T) (*Backend, *inner, *int) {
	t.Helper()
	released := new(int)
	base := "https://origin/sage/cold-storage-archive-test/warehouse"
	in := &inner{res: &result{}}
	b := &Backend{Hot: in, Cold: in, Coordinator: &Coordinator{URL: "https://coordinator", Token: "secret", Bucket: "waggle", Owner: "pod", Warehouse: base}}
	b.Coordinator.Client = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatal("missing auth")
		}
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if strings.HasSuffix(r.URL.Path, "renew") {
			return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable")), Header: make(http.Header)}, nil
		}
		payload := `{"ok":true}`
		if strings.HasSuffix(r.URL.Path, "acquire") {
			raw, _ := json.Marshal(View{ExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339Nano), ID: body["id"], Warehouse: base, Metadata: base + "/sage/obs/metadata/head.metadata.json", Frontier: "2025-03-02T00:00:00Z", CoverageStart: "2025-03-01T00:00:00Z"})
			payload = string(raw)
		} else {
			if !in.res.closed && !in.fail {
				t.Fatal("read grant released before result close")
			}
			*released++
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(payload)), Header: make(http.Header)}, nil
	})}
	return b, in, released
}

// TestReadViewPinsMetadataUntilResultCloses keeps the grant through the
// query and releases it once. A second close does not release again. The
// wrapper still exposes the single child stream used by native Arrow.
func TestReadViewPinsMetadataUntilResultCloses(t *testing.T) {
	b, in, released := fixture(t)
	r, e := b.Query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T01:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasSuffix(in.metadata, "head.metadata.json") || *released != 0 {
		t.Fatal(in.metadata, *released)
	}
	parts, ok := r.(model.CompositeResults)
	if !ok || len(parts.Parts()) != 1 {
		t.Fatal("native Arrow stream wrapper lost")
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	r.Close()
	if *released != 1 {
		t.Fatal(*released)
	}
}

// TestQueryFailureReleasesAndCloseFailureHolds releases the grant when the
// query itself fails, and holds it when closing the result fails.
func TestQueryFailureReleasesAndCloseFailureHolds(t *testing.T) {
	b, in, released := fixture(t)
	in.fail = true
	if _, e := b.Query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z"}); e == nil || *released != 1 {
		t.Fatal(e, *released)
	}
	b, in, released = fixture(t)
	in.res.err = errors.New("close uncertain")
	r, e := b.Query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Close(); e == nil || *released != 0 {
		t.Fatal(e, *released)
	}
}

// TestCoordinatorFailureNeverFallsBack rejects the query when acquire
// cannot reach the coordinator.
func TestCoordinatorFailureNeverFallsBack(t *testing.T) {
	b, _, _ := fixture(t)
	b.Coordinator.Client = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })}
	if _, e := b.Query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z"}); e == nil {
		t.Fatal("fell back to static border")
	}
}

// TestDynamicFrontierUsesTieredRouting splits on the grant's frontier.
// The backend's static border is ignored. Cold is strictly before the
// frontier and hot starts at it.
func TestDynamicFrontierUsesTieredRouting(t *testing.T) {
	for _, tc := range []struct {
		name, start, end string
		hot, cold        bool
	}{
		{"cold", "2025-03-01T00:00:00Z", "2025-03-02T00:00:00Z", false, true},
		{"hot", "2025-03-02T00:00:00Z", "2025-03-03T00:00:00Z", true, false},
		{"spanning", "2025-03-01T00:00:00Z", "2025-03-03T00:00:00Z", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, in, released := fixture(t)
			in.fail = true // This test replaces fixture result ownership with two tiers.
			hot, cold := &recordingBackend{}, &recordingBackend{}
			b.Hot, b.Cold = hot, cold
			b.Border = time.Now() // Dynamic routing must ignore the static boundary.
			r, err := b.Query(context.Background(), &model.Query{Start: tc.start, End: tc.end})
			if err != nil {
				t.Fatal(err)
			}
			if (hot.calls != 0) != tc.hot || (cold.calls != 0) != tc.cold {
				t.Fatal("wrong tier", hot.calls, cold.calls)
			}
			if tc.hot && tc.cold && (hot.got.Start != "2025-03-02T00:00:00Z" || cold.got.End != "2025-03-02T00:00:00Z") {
				t.Fatal("split did not use pinned frontier")
			}
			if err := r.Close(); err != nil || *released != 1 {
				t.Fatal(err, *released)
			}
		})
	}
}

// TestReadDeadlineCancelsQuery cancels the query context when the grant
// expires and leaves the grant held until Close.
func TestReadDeadlineCancelsQuery(t *testing.T) {
	b, in, released := fixture(t)
	original := b.Coordinator.Client.Transport
	b.Coordinator.Client.Transport = transport(func(req *http.Request) (*http.Response, error) {
		resp, err := original.RoundTrip(req)
		if err == nil && strings.HasSuffix(req.URL.Path, "acquire") {
			var view View
			if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			view.ExpiresAt = time.Now().Add(100 * time.Millisecond).Format(time.RFC3339Nano)
			raw, _ := json.Marshal(view)
			resp.Body = io.NopCloser(strings.NewReader(string(raw)))
		}
		return resp, err
	})
	r, err := b.Query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-in.ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("managed query was not canceled")
	}
	if !errors.Is(r.Err(), context.DeadlineExceeded) || r.Next() {
		t.Fatal("expired result remained usable", r.Err())
	}
	if *released != 0 {
		t.Fatal("grant released before resource closure")
	}
	if err := r.Close(); err != nil || *released != 1 {
		t.Fatal(err, *released)
	}
}

// TestInvalidReadDeadlineNeverStartsQuery rejects an empty, unparseable,
// or already expired deadline before either tier is queried, then releases
// the grant because no result was opened.
func TestInvalidReadDeadlineNeverStartsQuery(t *testing.T) {
	for _, deadline := range []string{"", "invalid", time.Now().Add(-time.Hour).Format(time.RFC3339Nano)} {
		t.Run(deadline, func(t *testing.T) {
			b, in, released := fixture(t)
			original := b.Coordinator.Client.Transport
			b.Coordinator.Client.Transport = transport(func(req *http.Request) (*http.Response, error) {
				resp, err := original.RoundTrip(req)
				if err == nil && strings.HasSuffix(req.URL.Path, "acquire") {
					var view View
					json.NewDecoder(resp.Body).Decode(&view)
					resp.Body.Close()
					view.ExpiresAt = deadline
					raw, _ := json.Marshal(view)
					resp.Body = io.NopCloser(strings.NewReader(string(raw)))
				}
				return resp, err
			})
			// fixture permits release without a resource close when no query started.
			in.fail = true
			if _, err := b.Query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z"}); err == nil {
				t.Fatal("invalid deadline accepted")
			}
			if in.ctx != nil || *released != 1 {
				t.Fatal("query started or grant leaked", *released)
			}
		})
	}
}

// TestDynamicRelativeStartResolvesBeforeCoverage turns a relative start
// into an absolute instant before the coverage check. A start before
// coverage is rejected and does not reach a tier.
func TestDynamicRelativeStartResolvesBeforeCoverage(t *testing.T) {
	b, in, released := fixture(t)
	b.now = func() time.Time { return time.Date(2025, 3, 3, 0, 0, 0, 0, time.UTC) }
	results, err := b.Query(context.Background(), &model.Query{Start: "-1h"})
	if err != nil {
		t.Fatal(err)
	}
	if in.got == nil || in.got.Start != "2025-03-02T23:00:00Z" {
		t.Fatalf("routed start = %#v", in.got)
	}
	if err = results.Close(); err != nil || *released != 1 {
		t.Fatal(err, *released)
	}

	b, in, released = fixture(t)
	b.now = func() time.Time { return time.Date(2025, 3, 3, 0, 0, 0, 0, time.UTC) }
	in.fail = true
	if _, err = b.Query(context.Background(), &model.Query{Start: "-100d"}); err == nil || !strings.Contains(err.Error(), "coverage") {
		t.Fatal(err)
	}
	if in.got != nil || *released != 1 {
		t.Fatal("coverage rejection routed or leaked", in.got, *released)
	}
}

func TestSetupUncertainClosureWaitsForLeaseExpiry(t *testing.T) {
	b, _, released := fixture(t)
	_, err := b.Coordinator.query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T01:00:00Z"}, func(context.Context, *model.Query, time.Time) (model.Results, error) {
		return nil, model.ErrResourceClosureUncertain
	})
	if !errors.Is(err, model.ErrResourceClosureUncertain) || *released != 0 {
		t.Fatal("uncertain setup released a live grant", err, *released)
	}
}
