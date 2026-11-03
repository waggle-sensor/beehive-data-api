// coordination_test.go checks that a managed read keeps its snapshot until
// close, and that a coordinator failure does not fall back to an unpinned query.

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

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type result struct {
	closed bool
	err    error
}

func (r *result) Next() bool            { return false }
func (r *result) Record() *model.Record { return nil }
func (r *result) Err() error            { return nil }
func (r *result) Close() error          { r.closed = true; return r.err }

type inner struct {
	metadata string
	res      *result
	fail     bool
	ctx      context.Context
}

func (b *inner) Query(ctx context.Context, _ *model.Query) (model.Results, error) {
	b.ctx = ctx
	b.metadata = model.ColdMetadata(ctx)
	if b.fail {
		return nil, errors.New("query failed")
	}
	return b.res, nil
}
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
func TestCoordinatorFailureNeverFallsBack(t *testing.T) {
	b, _, _ := fixture(t)
	b.Coordinator.Client = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })}
	if _, e := b.Query(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z"}); e == nil {
		t.Fatal("fell back to static border")
	}
}

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
