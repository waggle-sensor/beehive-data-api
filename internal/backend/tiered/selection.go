// selection.go keeps the first or last observations of each public series
// across both tiers. The limit is per series, not a global row cap.

package tiered

import (
	"fmt"
	"strings"
	"time"

	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/query/aggregate"
)

// SelectionBounds requires exactly one of head or tail, an absolute start,
// and an end after that start. An omitted end uses now.
func SelectionBounds(query *model.Query, now time.Time) (time.Time, time.Time, error) {
	if query.Func != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("standalone selection cannot have an aggregation function")
	}
	if query.Window != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("window cannot be used without aggregation function")
	}
	if (query.Head == nil) == (query.Tail == nil) {
		return time.Time{}, time.Time{}, fmt.Errorf("specify exactly one of head or tail")
	}
	limit := query.Head
	if query.Tail != nil {
		limit = query.Tail
	}
	if *limit < 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("head/tail must be >= 0")
	}
	start, err := aggregate.ParseTime(query.Start, time.Time{})
	if err != nil || start.IsZero() {
		return start, time.Time{}, fmt.Errorf("selection requires an absolute start")
	}
	end, err := aggregate.ParseTime(query.End, now)
	if err != nil || !end.After(start) {
		return start, end, fmt.Errorf("selection end must be after start")
	}
	for _, t := range []time.Time{start, end} {
		if !t.Equal(time.Unix(0, t.UnixNano())) {
			return start, end, fmt.Errorf("selection bound outside nanosecond timestamp range")
		}
	}
	return start.UTC(), end.UTC(), nil
}

// BuildSelectionSQL first prunes each raw tag tuple to N observations, then
// canonicalizes metadata and selects the final N per public series. Raw aliases
// may merge into one series, so the final window is required. A discarded raw
// point already has N earlier/later points in its own canonical series.
// The materialized producer has one consumer; metadata work follows pruning.
func BuildSelectionSQL(query *model.Query, source string, now time.Time) (string, error) {
	if _, _, err := SelectionBounds(query, now); err != nil {
		return "", err
	}
	limit, order := query.Head, "ASC"
	if query.Tail != nil {
		limit, order = query.Tail, "DESC"
	}
	keys := append([]string{"name", "meta"}, aggregate.PromotedTags()...)
	rawRanked := fmt.Sprintf("SELECT *,row_number() OVER (PARTITION BY %s ORDER BY timestamp %s) AS __raw_rank FROM (%s)", strings.Join(keys, ","), order, source)
	candidates := fmt.Sprintf("SELECT * EXCLUDE(__raw_rank) FROM (%s) WHERE __raw_rank<=%d", rawRanked, *limit)
	meta := "map_from_entries(list_sort(map_entries(map_concat(COALESCE(meta,MAP {}::MAP(VARCHAR,VARCHAR))," + aggregate.PromotedTagMap() + "))))"
	canonical := "SELECT CAST(timestamp AS TIMESTAMP_NS) AS timestamp,name,value," + meta + " AS meta FROM (SELECT * FROM raw_candidates)"
	ranked := fmt.Sprintf("SELECT *,row_number() OVER (PARTITION BY name,meta ORDER BY timestamp %s) AS rn FROM (%s)", order, canonical)
	selected := fmt.Sprintf("SELECT * EXCLUDE(rn) FROM (%s) WHERE rn<=%d", ranked, *limit)
	return "WITH raw_candidates AS MATERIALIZED (" + candidates + ") SELECT timestamp,name,to_json(value)::VARCHAR AS value,to_json(meta)::VARCHAR AS meta FROM (" + selected + ") ORDER BY timestamp,name,meta", nil
}
