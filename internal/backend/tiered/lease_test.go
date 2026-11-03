// lease_test.go checks that renewal can outlive the first deadline without
// changing the pinned snapshot, and that a hung renewal still expires.

package tiered

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRenewingLeaseOutlivesInitialExpiry(t *testing.T) {
	expiry := time.Now().Add(300 * time.Millisecond)
	view := View{ID: "read", Warehouse: "warehouse", Metadata: "pinned", Frontier: "frontier", CoverageStart: "coverage", Generation: 4}
	var renewals atomic.Int32
	b := &Coordinator{URL: "https://coordinator", Token: "secret", Bucket: "waggle"}
	b.Client = &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1/reads/renew" || req.Header.Get("Authorization") != "Bearer secret" {
			t.Error("invalid renewal request")
		}
		var body map[string]string
		json.NewDecoder(req.Body).Decode(&body)
		if body["owner"] != "api" || body["id"] != view.ID || body["bucket"] != "waggle" {
			t.Error(body)
		}
		renewals.Add(1)
		next := view
		next.ExpiresAt = time.Now().Add(300 * time.Millisecond).Format(time.RFC3339Nano)
		raw, _ := json.Marshal(next)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})}
	ctx, stop := b.readLease(context.Background(), "api", view, expiry)
	defer stop()
	timer := time.NewTimer(time.Until(expiry.Add(150 * time.Millisecond)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatal("healthy lease ended at original timeout", context.Cause(ctx))
	case <-timer.C:
	}
	if renewals.Load() < 2 {
		t.Fatal("expected periodic renewals", renewals.Load())
	}
	stop()
	if ctx.Err() == nil {
		t.Fatal("stopping did not cancel query")
	}
}

func TestHungRenewalCannotOutliveConfirmedLease(t *testing.T) {
	started := make(chan struct{}, 1)
	b := &Coordinator{URL: "https://coordinator", Client: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		started <- struct{}{}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}}
	ctx, stop := b.readLease(context.Background(), "api", View{ID: "read"}, time.Now().Add(150*time.Millisecond))
	defer stop()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("renewal never started")
	}
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatal(context.Cause(ctx))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("hung renewal prevented expiry")
	}
}

func TestRenewalCannotChangeSnapshot(t *testing.T) {
	b := &Coordinator{URL: "https://coordinator", Client: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		raw, _ := json.Marshal(View{ID: "read", Metadata: "different-snapshot", ExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339Nano)})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})}}
	ctx, stop := b.readLease(context.Background(), "api", View{ID: "read", Metadata: "pinned"}, time.Now().Add(300*time.Millisecond))
	defer stop()
	select {
	case <-ctx.Done():
		if context.Cause(ctx).Error() != "invalid renewed read lease" {
			t.Fatal(context.Cause(ctx))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("changed snapshot accepted")
	}
}
