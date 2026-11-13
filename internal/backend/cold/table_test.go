package cold

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	model "beehive-data-api/internal/query"
)

type tableTransport func(*http.Request) (*http.Response, error)

func (f tableTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestReadHintUsesConfiguredTable(t *testing.T) {
	identity, err := model.NewTableIdentity("telemetry", "readings")
	if err != nil {
		t.Fatal(err)
	}
	backend := &Backend{Lakehouse: "https://origin/sage/cold/lakehouse", Table: identity,
		Client: &http.Client{Transport: tableTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path != "/sage/cold/lakehouse/telemetry/readings/metadata/version-hint.text" {
				t.Fatal(r.URL)
			}
			if r.Header.Get("Authorization") != "Bearer fixture" {
				t.Fatal("missing token")
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("head")), Header: make(http.Header)}, nil
		})}}
	if stem, err := backend.readHint(context.Background(), "fixture"); err != nil || stem != "head" {
		t.Fatal(stem, err)
	}
}
