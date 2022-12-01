package main

import (
	"fmt"
	"io"
	"log"
	"testing"
)

func init() {
	log.SetOutput(io.Discard)
}

func strptr(s string) *string {
	return &s
}

func intptr(x int) *int {
	return &x
}

func TestBuildFluxQuery(t *testing.T) {
	testcases := map[string]struct {
		Query      *Query
		Expect     string
		ShouldFail bool
	}{
		"StartRange": {
			Query: &Query{
				Start: "-4h",
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h)`,
		},
		"Bucket": {
			Query: &Query{
				Bucket: strptr("downsampled"),
				Start:  "-4h",
				Tail:   intptr(3),
			},
			Expect: `from(bucket:"downsampled") |> range(start:-4h) |> tail(n:3)`,
		},
		"InvalidBucket": {
			Query: &Query{
				Bucket: strptr("_badbucket"),
				Start:  "-4h",
				Tail:   intptr(3),
			},
			Expect:     ``,
			ShouldFail: true,
		},
		"StartEnd": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h)`,
		},
		"StartEndTail": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
				Tail:  intptr(3),
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> tail(n:3)`,
		},
		"StartEndHead": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
				Head:  intptr(3),
			},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> limit(n:3)`,
		},
		"ExactFilter": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"node": "0000000000000001",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r.node == "0000000000000001")`,
		},
		"ExactFilterMultiple": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"node": "0000000000000001",
					"vsn":  "W001",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r.node == "0000000000000001" and r.vsn == "W001")`,
		},
		"RegexpFilter": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"name": "env.temp.*",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r._measurement =~ /^env.temp.*$/)`,
		},
		"RegexpOr": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"name": "env.temp.*",
					"vsn":  "W001|W002",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r._measurement =~ /^env.temp.*$/ and r.vsn =~ /^(W001|W002)$/)`,
		},
		"RegexpEscape": {
			Query: &Query{
				Start: "-4h",
				End:   "-2h",
				Filter: map[string]string{
					"plugin": "docker.io/waggle/plugin-iio.*",
				}},
			Expect: `from(bucket:"mybucket") |> range(start:-4h,stop:-2h) |> filter(fn: (r) => r.plugin =~ /^docker.io\/waggle\/plugin-iio.*$/)`,
		},
		"Combined1": {
			Query: &Query{
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
			Query: &Query{
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

func TestBuildFluxBadQuery(t *testing.T) {
	testcases := []*Query{
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
