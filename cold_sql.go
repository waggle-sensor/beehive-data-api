package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Promoted tag columns on the Iceberg `obs` table. Every other filter key
// lives in the `meta` map. Same split cold_backend.py uses.
var promotedTags = []string{"plugin", "vsn", "node", "host", "task", "job", "zone"}

// isPromotedTag checks if a field is a promoted tag.
func isPromotedTag(field string) bool {
	for _, t := range promotedTags {
		if field == t {
			return true
		}
	}
	return false
}

// sqlQuote quotes a string for use in a SQL statement.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// buildColdSQL compiles a Beehive query into one DuckDB statement that reads
// an Iceberg metadata file directly. DuckDB plans the manifests and the
// Parquet files itself; nothing is loaded into Arrow first.
func buildColdSQL(query *Query, metadataURL string, now time.Time) (string, error) {
	if query.Func != nil {
		return "", fmt.Errorf("aggregations are not supported against cold storage")
	}
	if query.Window != nil {
		return "", fmt.Errorf("window cannot be used without aggregation function")
	}
	if query.Head != nil && query.Tail != nil {
		return "", fmt.Errorf("head and tail cannot both be specified")
	}
	if strings.Contains(metadataURL, "'") {
		return "", fmt.Errorf("metadata url contains a quote")
	}

	start, err := parseColdTime(query.Start, time.Time{})
	if err != nil {
		return "", fmt.Errorf("invalid start timestamp %q: %w", query.Start, err)
	}
	if start.IsZero() {
		return "", fmt.Errorf("missing start field")
	}
	end, err := parseColdTime(query.End, now)
	if err != nil {
		return "", fmt.Errorf("invalid end timestamp %q: %w", query.End, err)
	}

	preds := []string{
		fmt.Sprintf("time >= TIMESTAMPTZ %s", sqlQuote(start.UTC().Format(time.RFC3339Nano))),
		fmt.Sprintf("time <  TIMESTAMPTZ %s", sqlQuote(end.UTC().Format(time.RFC3339Nano))),
	}
	fields := make([]string, 0, len(query.Filter))
	for field := range query.Filter {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		pred, err := filterToSQL(field, query.Filter[field])
		if err != nil {
			return "", err
		}
		preds = append(preds, pred)
	}

	keys := make([]string, len(promotedTags))
	vals := make([]string, len(promotedTags))
	for i, t := range promotedTags {
		keys[i] = sqlQuote(t)
		vals[i] = t
	}
	promotedMap := fmt.Sprintf(
		"map_from_entries(list_filter(list_zip([%s], [%s]), x -> x[2] IS NOT NULL))",
		strings.Join(keys, ", "), strings.Join(vals, ", "))

	inner := fmt.Sprintf(`
        SELECT
            time AS timestamp,
            name,
            value_float::DOUBLE AS value_float,
            value_int::BIGINT AS value_int,
            value_str::VARCHAR AS value_str,
            map_concat(COALESCE(meta, MAP {}::MAP(VARCHAR, VARCHAR)),
                       %s) AS meta
        FROM iceberg_scan(%s)
        WHERE %s`,
		promotedMap, sqlQuote(metadataURL), strings.Join(preds, " AND "))

	if query.Head != nil || query.Tail != nil {
		n := query.Head
		order := "ASC"
		if query.Tail != nil {
			n = query.Tail
			order = "DESC"
		}
		if *n < 0 {
			return "", fmt.Errorf("head/tail must be >= 0")
		}
		inner = fmt.Sprintf(`
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY name, meta ORDER BY timestamp %s
                ) AS rn
                FROM (%s)
            ) WHERE rn <= %d`, order, inner, *n)
	}

	return fmt.Sprintf(`
        SELECT timestamp, name, value_float, value_int, value_str, to_json(meta) AS meta
        FROM (%s)
        ORDER BY timestamp`, inner), nil
}

// parseColdTime parses a time string into a time.Time.
func parseColdTime(s string, fallback time.Time) (time.Time, error) {
	if s == "" {
		return fallback, nil
	}
	if s[0] == '-' || s[0] == '+' {
		return time.Time{}, fmt.Errorf("relative timestamps are not supported by the cold backend; use an absolute RFC3339 timestamp")
	}
	return time.Parse(time.RFC3339, s)
}

// filterToSQL converts a filter field and pattern into a SQL predicate.
func filterToSQL(field, pattern string) (string, error) {
	if !isValidFilterString(field) {
		return "", fmt.Errorf("invalid filter field name %q", field)
	}
	if !isValidFilterString(pattern) {
		return "", fmt.Errorf("invalid filter field pattern %q", pattern)
	}
	col := "name"
	switch {
	case field == "name":
		col = "name"
	case isPromotedTag(field):
		col = field
	default:
		col = fmt.Sprintf("meta[%s]", sqlQuote(field))
	}
	switch {
	case strings.Contains(pattern, "|"):
		return fmt.Sprintf("regexp_matches(%s, %s)", col, sqlQuote("^("+pattern+")$")), nil
	case strings.Contains(pattern, "*"):
		return fmt.Sprintf("regexp_matches(%s, %s)", col, sqlQuote("^"+pattern+"$")), nil
	default:
		return fmt.Sprintf("%s = %s", col, sqlQuote(pattern)), nil
	}
}
