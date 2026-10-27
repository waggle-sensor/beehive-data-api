// query_test.go contains the tests for the Service type.
package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"beehive-data-api/internal/arrowio"
	"beehive-data-api/internal/backend/tiered"
	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/testutil"
)

// TestQueryResponse tests the query response.
func TestQueryResponse(t *testing.T) {
	records := []*model.Record{
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
		Backend: &testutil.Backend{Records: records},
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

// TestValidQuery tests the valid query.
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
		Backend: &testutil.Backend{},
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

// TestContentDispositionHeader tests the content disposition header.
func TestContentDispositionHeader(t *testing.T) {
	svc := NewService(&ServiceConfig{
		Backend: &testutil.Backend{},
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

// TestRequestSizeLimit tests the request size limit.
func TestRequestSizeLimit(t *testing.T) {
	svc := NewService(&ServiceConfig{
		Backend: &testutil.Backend{},
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

// TestArrowQueryResponse tests the arrow query response.
func TestArrowQueryResponse(t *testing.T) {
	records := []*model.Record{
		{
			Timestamp: time.Date(2021, 1, 1, 10, 0, 0, 0, time.UTC),
			Name:      "sys.uptime",
			Value:     100321,
			Meta:      map[string]string{"node": "0000000000000001", "plugin": "status:1.0.2"},
		},
		{
			Timestamp: time.Date(2022, 1, 1, 10, 30, 0, 0, time.UTC),
			Name:      "env.temp.htu21d",
			Value:     2.3,
			Meta:      map[string]string{"node": "0000000000000001"},
		},
		{
			Timestamp: time.Date(2023, 2, 1, 10, 45, 0, 0, time.UTC),
			Name:      "raw.htu21d",
			Value:     "234124123",
			Meta:      map[string]string{},
		},
		{
			Timestamp: time.Date(2024, 3, 1, 0, 0, 0, 1, time.UTC),
			Name:      "sys.flag",
			Value:     true,
			Meta:      nil,
		},
	}
	svc := NewService(&ServiceConfig{Backend: &testutil.Backend{Records: records}})
	r := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"start": "-4h"}`))
	r.Header.Set("Accept", arrowio.MIME+`, application/x-ndjson;q=0.1`)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()
	assertStatusCode(t, resp, http.StatusOK)
	if got := resp.Header.Get("Content-Type"); got != arrowio.MIME {
		t.Fatalf("content type %q", got)
	}
	if !regexp.MustCompile(`attachment; filename="sage-download-(.+).arrows"`).MatchString(resp.Header.Get("Content-Disposition")) {
		t.Fatalf("content disposition %q", resp.Header.Get("Content-Disposition"))
	}

	rdr, err := ipc.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	if err := arrowio.SchemaCompatible(rdr.Schema()); err != nil {
		t.Fatal(err)
	}
	if !rdr.Next() {
		t.Fatal(rdr.Err())
	}
	rec := rdr.RecordBatch()
	if rec.NumRows() != int64(len(records)) {
		t.Fatalf("rows %d", rec.NumRows())
	}
	names := rec.Column(arrowio.FieldName).(*array.String)
	values := rec.Column(arrowio.FieldValue).(*array.String)
	if names.Value(0) != "sys.uptime" || values.Value(0) != "100321" {
		t.Fatalf("row 0 name=%s value=%s", names.Value(0), values.Value(0))
	}
	if names.Value(1) != "env.temp.htu21d" || values.Value(1) != "2.3" {
		t.Fatalf("row 1 value=%s", values.Value(1))
	}
	if values.Value(2) != `"234124123"` {
		t.Fatalf("row 2 value=%s", values.Value(2))
	}
	if values.Value(3) != "true" {
		t.Fatalf("row 3 value=%s", values.Value(3))
	}
	metas := rec.Column(arrowio.FieldMeta).(*array.String)
	var meta map[string]string
	if err := json.Unmarshal([]byte(metas.Value(0)), &meta); err != nil {
		t.Fatal(err)
	}
	if meta["node"] != "0000000000000001" || meta["plugin"] != "status:1.0.2" {
		t.Fatalf("row 0 meta %#v", meta)
	}
	if metas.Value(2) != "{}" {
		t.Fatalf("row 2 meta %s", metas.Value(2))
	}
	if !metas.IsNull(3) {
		t.Fatalf("row 3 meta %s", metas.Value(3))
	}
	if rdr.Next() {
		t.Fatal("extra batch")
	}
}

// TestArrowTieredResponse tests the arrow tiered response.
func TestArrowTieredResponse(t *testing.T) {
	wm := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	cold := &model.Record{
		Timestamp: time.Date(2025, 4, 30, 23, 0, 0, 0, time.UTC),
		Name:      "cold.m",
		Value:     1.5,
		Meta:      map[string]string{"vsn": "W0A0"},
	}
	hot := &model.Record{
		Timestamp: wm,
		Name:      "hot.m",
		Value:     false,
		Meta:      map[string]string{"vsn": "W0A0"},
	}
	svc := NewService(&ServiceConfig{Backend: &tiered.Backend{
		Cold:      &testutil.Backend{Records: []*model.Record{cold}},
		Hot:       &testutil.Backend{Records: []*model.Record{hot}},
		Watermark: wm,
	}})
	r := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"start":"2025-04-30T00:00:00Z","end":"2025-05-02T00:00:00Z"}`))
	r.Header.Set("Accept", arrowio.MIME)
	w := httptest.NewRecorder()
	svc.ServeHTTP(w, r)
	resp := w.Result()
	assertStatusCode(t, resp, http.StatusOK)
	rdr, err := ipc.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	var names []string
	for rdr.Next() {
		col := rdr.RecordBatch().Column(arrowio.FieldName).(*array.String)
		for i := 0; i < col.Len(); i++ {
			names = append(names, col.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "cold.m,hot.m" {
		t.Fatalf("names %v", names)
	}
}

// fixedResultsBackend is a backend that returns a fixed results.
type fixedResultsBackend struct{ result model.Results }

// Query queries the backend.
func (b fixedResultsBackend) Query(context.Context, *model.Query) (model.Results, error) {
	return b.result, nil
}

// TestArrowLateErrorAbortsResponse tests the arrow late error aborts response.
func TestArrowLateErrorAbortsResponse(t *testing.T) {
	// Emit a complete batch before failing, as a streaming sort can do.
	rows := make([]*model.Record, arrowio.BatchRows)
	for i := range rows {
		rows[i] = &model.Record{Timestamp: time.Unix(0, int64(i)), Name: "m", Value: 1}
	}
	results := &testutil.Results{Records: rows, ErrValue: fmt.Errorf("late query error")}
	svc := NewService(&ServiceConfig{Backend: fixedResultsBackend{results}})
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"start":"2025-03-01T00:00:00Z"}`))
	r.Header.Set("Accept", arrowio.MIME)
	w := httptest.NewRecorder()
	defer func() {
		if got := recover(); got != http.ErrAbortHandler {
			t.Fatalf("abort=%v, want http.ErrAbortHandler", got)
		}
		if !results.Closed {
			t.Fatal("results were not closed on abort")
		}
		if w.Body.Len() == 0 {
			t.Fatal("test never emitted a batch")
		}
	}()
	svc.ServeHTTP(w, r)
}

// assertStatusCode asserts the status code of the response.
func assertStatusCode(t *testing.T, resp *http.Response, want int) {
	if resp.StatusCode != want {
		t.Fatalf("invalid status code. want: %d got: %d", want, resp.StatusCode)
	}
}

// assertReadBody asserts the body of the response.
func assertReadBody(t *testing.T, resp *http.Response, want []byte) {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, b) {
		t.Fatalf("invalid body. want: %q got: %q", want, b)
	}
}
