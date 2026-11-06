// query_test.go checks the Flux script, including the bucket override and rejected shapes.

package hot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"

	model "beehive-data-api/internal/query"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	influxquery "github.com/influxdata/influxdb-client-go/v2/api/query"
)

func TestAggregateRawInputRejectsUnexpectedField(t *testing.T) {
	record := influxquery.NewFluxRecord(0, map[string]interface{}{"_measurement": "t", "_field": "other", "_value": int64(1)})
	strict := &influxResults{validateValueField: true}
	if _, err := strict.convertToAPIRecord(record); err == nil {
		t.Fatal("unexpected field silently merged into aggregate")
	}
	ordinary := &influxResults{}
	if _, err := ordinary.convertToAPIRecord(record); err != nil {
		t.Fatal("ordinary hot field behavior changed", err)
	}
}

// init initializes the test environment.
func init() {
	log.SetOutput(io.Discard)
}

// strptr converts a string to a pointer.
func strptr(s string) *string {
	return &s
}

// intptr converts an integer to a pointer.
func intptr(x int) *int {
	return &x
}

// TestBuildFluxQuery tests the build flux query.
func TestBuildFluxQuery(t *testing.T) {
	testcases := map[string]struct {
		Query      *model.Query
		Expect     string
		ShouldFail bool
	}{
		"StartRange": {
			Query: &model.Query{
				Start: "-4h",
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h)`,
		},
		"Bucket": {
			Query: &model.Query{
				Bucket: strptr("downsampled"),
				Start:  "-4h",
				Tail:   intptr(3),
			},
			Expect: `from(bucket:"downsampled") |> range(start:-4h) |> tail(n:3)`,
		},
		"InvalidBucket": {
			Query: &model.Query{
				Bucket: strptr("_badbucket"),
				Start:  "-4h",
				Tail:   intptr(3),
			},
			Expect:     ``,
			ShouldFail: true,
		},
		"StartEnd": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h)`,
		},
		"StartEndTail": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Tail:  intptr(3),
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> tail(n:3)`,
		},
		"StartEndHead": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Head:  intptr(3),
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> limit(n:3)`,
		},
		"ExactFilter": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"node": "0000000000000001",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r.node == "0000000000000001")`,
		},
		"ExactFilterMultiple": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"node": "0000000000000001",
					"vsn":  "W001",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r.node == "0000000000000001" and r.vsn == "W001")`,
		},
		"RegexpFilter": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"name": "env.temp.*",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r._measurement =~ /^env.temp.*$/)`,
		},
		"RegexpOr": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"name": "env.temp.*",
					"vsn":  "W001|W002",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r._measurement =~ /^env.temp.*$/ and r.vsn =~ /^(W001|W002)$/)`,
		},
		"RegexpEscape": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"plugin": "docker.io/waggle/plugin-iio.*",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r.plugin =~ /^docker.io\/waggle\/plugin-iio.*$/)`,
		},
		"Combined1": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Tail:  intptr(123),
				Filter: map[string]string{
					"name":   "env.temp.*",
					"vsn":    "V001",
					"sensor": "es.*",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r._measurement =~ /^env.temp.*$/ and r.sensor =~ /^es.*$/ and r.vsn == "V001") |> tail(n:123)`,
		},
		"Combined2": {
			Query: &model.Query{
				Start: "-4h",
				End:   "-2h",
				Tail:  intptr(123),
				Filter: map[string]string{
					"name":   "env.temp.*",
					"vsn":    "V001|W123",
					"sensor": "es.*",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r._measurement =~ /^env.temp.*$/ and r.sensor =~ /^es.*$/ and r.vsn =~ /^(V001|W123)$/) |> tail(n:123)`,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			s, err := buildFluxQuery("mybucket", tc.Query)

			if tc.ShouldFail {
				if err == nil {
					t.Fatal(fmt.Printf("expected error"))
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if s != tc.Expect {
					t.Fatalf("flux query expected:\nexpect: %s\noutput: %s", s, tc.Expect)
				}
			}
		})
	}
}

// TestBuildFluxBadQuery tests the build flux bad query.
func TestBuildFluxBadQuery(t *testing.T) {
	testcases := []*model.Query{
		{
			Start: "-4h",
			Filter: map[string]string{
				"name": "); drop bucket",
			},
		},
		{
			Start: "); danger",
		},
		{
			End: "); danger",
		},
		{
			Head: intptr(3),
			Tail: intptr(3),
		},
	}

	for _, query := range testcases {
		_, err := buildFluxQuery("mybucket", query)
		if err == nil {
			t.Fatalf("expected error for %#v", query)
		}
	}
}

type failingCSVReader struct{ err error }

func (reader failingCSVReader) Read([]byte) (int, error) { return 0, reader.err }

func TestInfluxIterationErrorIsNotSuccessfulEOF(t *testing.T) {
	for _, failure := range []error{errors.New("lost Influx response"), context.Canceled} {
		results := &influxResults{results: api.NewQueryTableResult(io.NopCloser(failingCSVReader{failure}))}
		if results.Next() {
			t.Fatal("failed stream produced a row")
		}
		if !errors.Is(results.Err(), failure) {
			t.Fatal("SDK iteration failure was hidden", results.Err())
		}
		conversion := errors.New("record conversion failure")
		results.err = conversion
		if results.Err() != conversion {
			t.Fatal("conversion error precedence changed")
		}
	}
}

type rawSDKTransport func(*http.Request) (*http.Response, error)

func (f rawSDKTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRawAggregationSDKRetainsNativeTypes(t *testing.T) {
	csv := `#datatype,string,long,dateTime:RFC3339,unsignedLong,string,string,string
,result,table,_time,_value,_field,_measurement,vsn
,_result,0,2025-03-01T00:00:00.000000194Z,18446744073709551615,value,t,A
#datatype,string,long,dateTime:RFC3339,double,string,string,string
,result,table,_time,_value,_field,_measurement,vsn
,_result,1,2025-03-01T00:00:00.000000195Z,6.0,value,t,A
#datatype,string,long,dateTime:RFC3339,long,string,string,string
,result,table,_time,_value,_field,_measurement,vsn
,_result,2,2025-03-01T00:00:00.000000196Z,6,value,t,A
#datatype,string,long,dateTime:RFC3339,boolean,string,string,string
,result,table,_time,_value,_field,_measurement,vsn
,_result,3,2025-03-01T00:00:00.000000197Z,false,value,t,A
#datatype,string,long,dateTime:RFC3339,string,string,string,string
,result,table,_time,_value,_field,_measurement,vsn
,_result,4,2025-03-01T00:00:00.000000198Z,6,value,t,A
`
	client := influxdb2.NewClientWithOptions("https://fixture", "fixture", influxdb2.DefaultOptions().SetHTTPClient(&http.Client{Transport: rawSDKTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(csv))}, nil
	})}))
	defer client.Close()
	backend := Backend{Client: client, Org: "fixture", Bucket: "fixture"}
	result, err := backend.QueryRawSource(context.Background(), &model.Query{Start: "2025-03-01T00:00:00Z", End: "2025-03-01T00:01:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	expected := []any{uint64(18446744073709551615), float64(6), int64(6), false, "6"}
	index := 0
	for result.Next() {
		if index >= len(expected) {
			t.Fatal("extra record")
		}
		row := result.Record()
		if row.Value != expected[index] || row.Meta["vsn"] != "A" || row.Timestamp.UnixNano() != 1740787200000000194+int64(index) {
			t.Fatalf("native record changed: %+v (%T)", row, row.Value)
		}
		index++
	}
	if result.Err() != nil || index != len(expected) {
		t.Fatal(index, result.Err())
	}
}
