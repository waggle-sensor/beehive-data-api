// Package aggregate plans SQL that reduces one raw hot and cold union.
// Per-tier aggregates are never combined. A window label is the window end,
// clipped to the caller's end.
package aggregate

import (
	"fmt"
	"strings"
	"time"
)

// promotedTags are the identity columns stored beside meta. A tag present in
// both places uses the promoted column when the public map is rebuilt.
var promotedTags = []string{"plugin", "vsn", "node", "host", "task", "job", "zone"}

// PromotedTags returns a copy so callers cannot change the shared order.
func PromotedTags() []string { return append([]string(nil), promotedTags...) }

// sqlQuote wraps one SQL string literal.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// promotedTagMap builds the public metadata contribution of non-null promoted tags.
func promotedTagMap() string {
	keys, vals := make([]string, len(promotedTags)), make([]string, len(promotedTags))
	for i, tag := range promotedTags {
		keys[i], vals[i] = sqlQuote(tag), tag
	}
	return fmt.Sprintf("map_from_entries(list_filter(list_zip([%s], [%s]), lambda x : x[2] IS NOT NULL))",
		strings.Join(keys, ", "), strings.Join(vals, ", "))
}

// parseColdTime accepts RFC3339. An empty string uses fallback. A relative
// form is rejected here; the tiered router resolves those before planning.
func parseColdTime(s string, fallback time.Time) (time.Time, error) {
	if s == "" {
		return fallback, nil
	}
	if s[0] == '-' || s[0] == '+' {
		return time.Time{}, fmt.Errorf("relative timestamps are not supported by the cold backend; use an absolute RFC3339 timestamp")
	}
	return time.Parse(time.RFC3339, s)
}

// Quote, PromotedTagMap, and ParseTime are the pieces other packages share
// when they build a raw union around this plan.
func Quote(s string) string                                     { return sqlQuote(s) }
func PromotedTagMap() string                                    { return promotedTagMap() }
func ParseTime(s string, fallback time.Time) (time.Time, error) { return parseColdTime(s, fallback) }
