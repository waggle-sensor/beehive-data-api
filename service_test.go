package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
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

	svc := NewService(&ServiceConfig{
		Backend: &DummyBackend{records},
	})

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

func TestValidQuery(t *testing.T) {
	testcases := map[string]struct {
		body  string
		valid bool
		resp  string
	}{
		"Valid1":         {`{"start": "-4h"}`, true, ""},
		"Valid2":         {`{"start": "-4h", "filter": {"node": "node123", "vsn": "W123"}}`, true, ""},
		"Empty":          {``, false, "error: no query provided\n"},
		"NoStart":        {`{}`, false, "error: failed to parse query: missing start field\n"},
		"GoodFilterKey1": {`{"start": "-4h", "filter": {"meta": "W123"}}`, true, ""},
		"GoodFilterKey2": {`{"start": "-4h", "filter": {"meta_tag": "W123"}}`, true, ""},
		"GoodFilterKey3": {`{"start": "-4h", "filter": {"meta2": "W123"}}`, true, ""},
		"GoodFilterKey4": {`{"start": "-4h", "filter": {"_meta": "W123"}}`, true, ""},
		"BadFilterKey1":  {`{"start": "-4h", "filter": {"meta.vsn": "W123"}}`, false, "error: failed to parse query: invalid filter key: \"meta.vsn\"\n"},
		"BadFilterKey2":  {`{"start": "-4h", "filter": {"meta-vsn": "W123"}}`, false, "error: failed to parse query: invalid filter key: \"meta-vsn\"\n"},
		"BadFilterKey3":  {`{"start": "-4h", "filter": {"1meta": "W123"}}`, false, "error: failed to parse query: invalid filter key: \"1meta\"\n"},
		"BadField":       {`{"start": "-4h", "unknown": "val"}`, false, "error: failed to parse query: json: unknown field \"unknown\"\n"},
		"EOF":            {`{"start": "-4h",`, false, "error: failed to parse query: unexpected EOF\n"},
		"BadJSON":        {`{"start": "-4h",}`, false, "error: failed to parse query: invalid character '}' looking for beginning of object key string\n"},
		"Wildcard1":      {`{"start": "-4h", "filter": {"host": ".*nxcore.*"}}`, true, ""},
		// TODO(sean) since we are mocking out influxdb during testing, we are not detecting the following case
		// correctly. we should move towards testing against the real services.
		"Wildcard2": {`{"start": "-4h", "filter": {"plugin": "waggle/plugin-iio.*"}}`, true, ""},
	}

	svc := NewService(&ServiceConfig{
		Backend: &DummyBackend{},
	})

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", bytes.NewBufferString(tc.body))
			w := httptest.NewRecorder()
			svc.ServeHTTP(w, r)
			resp := w.Result()

			if tc.valid {
				assertStatusCode(t, resp, http.StatusOK)
			} else {
				assertStatusCode(t, resp, http.StatusBadRequest)
			}
			assertReadBody(t, resp, []byte(tc.resp))
		})
	}
}

func TestContentDispositionHeader(t *testing.T) {
	svc := NewService(&ServiceConfig{
		Backend: &DummyBackend{},
	})

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

func TestRequestSizeLimit(t *testing.T) {
	svc := NewService(&ServiceConfig{
		Backend: &DummyBackend{},
	})

	body := bytes.NewBufferString(fmt.Sprintf(`{
		"start": "-4h",
		"filter": {
			"uhoh": "%s"
		}
	}`, strings.Repeat("x", 4096)))

	r := httptest.NewRequest("POST", "/", body)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 400 {
		t.Fatalf("service should have rejected large request. got response %s", resp.Status)
	}

	want := "error: query is too large - must be <1KB\n"

	if string(respBody) != want {
		t.Fatalf("unexpected response body.\ngot %q.\nwant %q", respBody, want)
	}
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
