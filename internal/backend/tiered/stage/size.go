// size.go charges each staged row as if it were still one JSON line.
// The Parquet file is smaller; the request budget stays on the old measure.

package stage

import (
	"strconv"
	"unicode/utf8"
)

// Row retains nanosecond instants and exact scalar text, including uint64.
type Row struct {
	Time  int64             `json:"time"`
	Name  string            `json:"name"`
	Meta  map[string]string `json:"meta"`
	Kind  string            `json:"kind"`
	Value string            `json:"value"`
}

// InputBytes charges the former JSON-encoded row plus newline without encoding
// it. This preserves the uncompressed request limit when output is compressed.
// Nil metadata is charged as the normalized empty map.
func InputBytes(r Row) int64 {
	n := int64(len(`{"time":`) + len(strconv.FormatInt(r.Time, 10)) + len(`,"name":`) + len(`,"meta":{`) + len(`},"kind":`) + len(`,"value":`) + len("}\n"))
	n += jsonStringBytes(r.Name) + jsonStringBytes(r.Kind) + jsonStringBytes(r.Value)
	first := true
	for k, v := range r.Meta {
		if !first {
			n++
		}
		first = false
		n += jsonStringBytes(k) + 1 + jsonStringBytes(v)
	}
	return n
}

// jsonStringBytes counts the bytes in one JSON string.
func jsonStringBytes(s string) int64 {
	n := int64(2)
	for len(s) > 0 {
		c := s[0]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t':
				n += 2
			case c < 0x20 || c == '<' || c == '>' || c == '&':
				n += 6
			default:
				n++
			}
			s = s[1:]
			continue
		}
		r, size := utf8.DecodeRuneInString(s)
		if (r == utf8.RuneError && size == 1) || r == '\u2028' || r == '\u2029' {
			n += 6
		} else {
			n += int64(size)
		}
		s = s[size:]
	}
	return n
}
