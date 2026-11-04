// duration_test.go checks Flux duration resolution before tier routing.
// A relative bound becomes one UTC instant. An absolute RFC3339 string is
// left untouched. Calendar months and years clamp to the last valid day.

package tiered

import (
	"testing"
	"time"
)

// TestParseFluxDuration resolves durations against a fixed instant.
// Repeated units add, and a missing sign is positive. A signed string that
// is not a duration is an error. An unsigned one is left for the absolute parser.
func TestParseFluxDuration(t *testing.T) {
	now := time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		in   string
		want string
	}{
		{"-1h", "2025-05-19T23:00:00Z"},
		{"-1h30m", "2025-05-19T22:30:00Z"},
		{"-1h1h", "2025-05-19T22:00:00Z"},
		{"+1h", "2025-05-20T01:00:00Z"},
		{"1h", "2025-05-20T01:00:00Z"},
		{"-2d", "2025-05-18T00:00:00Z"},
		{"-1w", "2025-05-13T00:00:00Z"},
		{"-1mo", "2025-04-20T00:00:00Z"},
		{"0s", "2025-05-20T00:00:00Z"},
		{"-1500ms", "2025-05-19T23:59:58.5Z"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, relative, err := resolveBound(tc.in, now)
			if err != nil || !relative || got.Format(time.RFC3339Nano) != tc.want {
				t.Fatalf("got %s relative=%v err=%v, want %s", got.Format(time.RFC3339Nano), relative, err, tc.want)
			}
		})
	}

	// March 31 and February 29 move back to the last day of the shorter month.
	clamped, relative, err := resolveBound("-1mo", time.Date(2025, 3, 31, 12, 0, 0, 0, time.UTC))
	if err != nil || !relative || clamped.Format(time.RFC3339Nano) != "2025-02-28T12:00:00Z" {
		t.Fatalf("month clamp = %s relative=%v err=%v", clamped.Format(time.RFC3339Nano), relative, err)
	}
	year, relative, err := resolveBound("-1y", time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC))
	if err != nil || !relative || year.Format(time.RFC3339Nano) != "2023-02-28T00:00:00Z" {
		t.Fatalf("year clamp = %s relative=%v err=%v", year.Format(time.RFC3339Nano), relative, err)
	}

	// An RFC3339 bound is not a duration, so resolution reports a zero time.
	absolute, relative, err := resolveBound("2025-05-20T00:00:00Z", now)
	if err != nil || relative || !absolute.IsZero() {
		t.Fatal("absolute bound was treated as a duration", absolute, relative, err)
	}
	// Signed garbage must fail. Unsigned garbage stays unresolved.
	for _, bad := range []string{"-nope", "1.5h", "-", "-1", "1h30"} {
		_, relative, err := resolveBound(bad, now)
		if relative || ((bad[0] == '+' || bad[0] == '-') && err == nil) {
			t.Fatalf("%q was accepted relative=%v err=%v", bad, relative, err)
		}
	}
}
