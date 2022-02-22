package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"
)

func TestQueryResponse(t *testing.T) {
	records := []*Record{
		{
			Timestamp: time.Date(2021, 1, 1, 10, 0, 0, 0, time.UTC),
			Name:      "sys.uptime",
			Value:     100321,
			Meta: map[string]string{
				"node":   "0000000000000001",
				"plugin": "status:1.0.2",
			},
		},
		{
			Timestamp: time.Date(2022, 1, 1, 10, 30, 0, 0, time.UTC),
			Name:      "env.temp.htu21d",
			Value:     2.3,
			Meta: map[string]string{
				"node":   "0000000000000001",
				"plugin": "metsense:1.0.2",
			},
		},
		{
			Timestamp: time.Date(2023, 2, 1, 10, 45, 0, 0, time.UTC),
			Name:      "raw.htu21d",
			Value:     "234124123",
			Meta: map[string]string{
				"node":   "0000000000000002",
				"plugin": "metsense:1.0.2",
			},
		},
	}

	svc := &Service{
		Backend: &DummyBackend{records},
	}

	body := bytes.NewBufferString(`{
		"start": "-4h"
	}`)

	r := httptest.NewRequest("POST", "/", body)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()

	assertStatusCode(t, resp, http.StatusOK)

	scanner := bufio.NewScanner(resp.Body)

	// check that output from server is just newline separated json records in same order
	for _, record := range records {
		if !scanner.Scan() {
			t.Fatalf("expected response for record %v", record)
		}
		b1, _ := json.Marshal(record)
		b2 := scanner.Bytes()
		if !bytes.Equal(b1, b2) {
			t.Fatalf("records don't match\nexpect: %s\noutput: %s", b1, b2)
		}
	}
}

func TestQueryDisallowedField(t *testing.T) {
	svc := &Service{
		Backend: &DummyBackend{},
	}

	body := bytes.NewBufferString(`{
		"start": "-4h",
		"filters": {
			"node": "node123"
		}
	}`)

	r := httptest.NewRequest("POST", "/", body)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()

	assertStatusCode(t, resp, http.StatusBadRequest)
	assertReadBody(t, resp, []byte(`error: failed to parse query: json: unknown field "filters"
`))
}

func TestContentDispositionHeader(t *testing.T) {
	svc := &Service{
		Backend: &DummyBackend{},
	}

	body := bytes.NewBufferString(`{
		"start": "-4h"
	}`)

	r := httptest.NewRequest("POST", "/", body)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()

	pattern := regexp.MustCompile("attachment; filename=\"sage-download-(.+).ndjson\"")

	s := resp.Header.Get("Content-Disposition")

	if !pattern.MatchString(s) {
		t.Fatalf("response must proper Content-Disposition header. got %q", s)
	}
}

func TestBadMeta(t *testing.T) {
	svc := &Service{
		Backend: &DummyBackend{},
	}

	r := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{
		"start": "-4h", "filter": {"meta.vsn":"123"}
	}`))

	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()

	assertStatusCode(t, resp, http.StatusInternalServerError)
	assertReadBody(t, resp, []byte("error: must provide a request body\n"))
}

func TestNoPayload(t *testing.T) {
	svc := &Service{
		Backend: &DummyBackend{},
	}

	r := httptest.NewRequest("POST", "/", nil)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()

	assertStatusCode(t, resp, http.StatusBadRequest)
	assertReadBody(t, resp, []byte(`error: must provide a request body
`))
}

func assertStatusCode(t *testing.T, resp *http.Response, want int) {
	if resp.StatusCode != want {
		t.Fatalf("invalid status code. want: %d got: %d", want, resp.StatusCode)
	}
}

func assertReadBody(t *testing.T, resp *http.Response, want []byte) {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, b) {
		t.Fatalf("invalid body. want: %q got: %q", want, b)
	}
}
