// size_test.go checks that the byte charge still matches the former JSON line.

package stage

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"
)

func TestInputBytesMatchesFormerBudget(t *testing.T) {
	strings := []string{"", "abc", "<>&", "\"\\\b\f\n\r\t\x00\x1f", "😀é\u2028\u2029", string([]byte{0xff, 0xfe}), "�"}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		b := make([]byte, rng.Intn(256))
		_, _ = rng.Read(b)
		strings = append(strings, string(b))
	}
	for i, s := range strings {
		r := Row{Time: math.MinInt64 + int64(i), Name: s, Meta: map[string]string{s: s, "vsn": "W000"}, Kind: "string", Value: s}
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := InputBytes(r), int64(len(data)+1); got != want {
			t.Fatalf("case %d: got %d want %d (%q)", i, got, want, data)
		}
	}
	for _, r := range []Row{{Meta: map[string]string{}}, {Time: math.MaxInt64, Meta: map[string]string{}, Kind: "uint", Value: "18446744073709551615"}} {
		data, _ := json.Marshal(r)
		if InputBytes(r) != int64(len(data)+1) {
			t.Fatal(string(data), InputBytes(r))
		}
	}
	// The production path always normalized nil to an empty map.
	if InputBytes(Row{}) != InputBytes(Row{Meta: map[string]string{}}) {
		t.Fatal("nil map charge")
	}
}
