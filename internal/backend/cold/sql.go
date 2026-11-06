// sql.go builds the cold SELECT. Filters, time bounds, and the metadata shape
// are fixed here so the scanner can assume one row layout.

package cold

import (
	"fmt"
	"sort"
	"strings"
	"time"

	model "beehive-data-api/internal/query"
	"beehive-data-api/internal/query/aggregate"
)

// uintEncoding names snapshots whose uint64 values are stored as decimals.
const uintEncoding = "variant-uint64-decimal-v1"

// Promoted tag columns on the Iceberg `obs` table. Every other filter key
// lives in the `meta` map. Same split cold_backend.py uses.
var promotedTags = aggregate.PromotedTags()

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
	return aggregate.Quote(s)
}

// buildColdSQL compiles a Beehive query into one DuckDB statement that reads
// an Iceberg metadata file directly. DuckDB plans the manifests and the
// Parquet files itself; nothing is loaded into Arrow first. meta is JSON text
// because database/sql scans a string.
func buildColdSQL(query *model.Query, metadataURL string, now time.Time) (string, error) {
	inner, meta, err := coldResponseSource(query, metadataURL, now)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
        SELECT timestamp, name, to_json(value)::VARCHAR AS value, %s AS meta
        FROM (%s)
        ORDER BY timestamp`, meta, inner), nil
}

// Ordinary extracts can combine metadata directly as JSON without building a
// MAP per row. Head/tail still use the merged MAP for per-series partitioning.
func coldResponseSource(query *model.Query, metadataURL string, now time.Time) (string, string, error) {
	raw := query.Head == nil && query.Tail == nil
	inner, err := buildColdSource(query, metadataURL, now, raw)
	meta := "to_json(meta)::VARCHAR"
	if raw {
		meta = coldMetaJSONExpr()
	}
	return inner, meta, err
}

// buildColdArrowSQL is the same scan projected into the Arrow response schema.
// Metadata stays JSON text, the same shape the newline-JSON statement sorts.
// Serialize before ordering so the sort does not carry nested MAP payloads.
// timestamp is nanoseconds so a tier-spanning stream has one timestamp type.
// The projection keeps every stored digit. Microsecond Iceberg values
// therefore end in zeros; nanosecond Parquet keeps its remainder.
func buildColdArrowSQL(query *model.Query, metadataURL string, now time.Time) (string, error) {
	inner, meta, err := coldResponseSource(query, metadataURL, now)
	if err != nil {
		return "", err
	}
	return coldArrowProjectionWithMeta(inner, meta), nil
}

// coldArrowProjectionWithMeta is the outer select of an Arrow cold query with meta.
func coldArrowProjectionWithMeta(inner, meta string) string {
	return fmt.Sprintf(`
        SELECT
            CAST(timestamp AS TIMESTAMP_NS) AS timestamp,
            name,
            to_json(value)::VARCHAR AS value,
            %s AS meta
        FROM (%s)
        ORDER BY timestamp`, meta, inner)
}

// promotedTagMap preserves the historical key ordering and null handling.
func promotedTagMap() string {
	return aggregate.PromotedTagMap()
}

// coldMetaJSONExpr emits exactly the existing JSON object. Promoted JSON omits
// null values before applying them to raw tags; this preserves raw tags when
// the corresponding promoted column is absent.
// Raw collisions use the original MAP expression to preserve even key order.
// The name is distinct from coldMetaJSON, which parses a JSON string.
func coldMetaJSONExpr() string {
	keys := make([]string, len(promotedTags))
	for i, tag := range promotedTags {
		keys[i] = sqlQuote(tag)
	}
	return fmt.Sprintf(`CASE WHEN list_has_any(map_keys(meta), [%s])
  THEN to_json(map_concat(COALESCE(meta, MAP {}::MAP(VARCHAR, VARCHAR)), %s))::VARCHAR
  ELSE json_merge_patch(to_json(COALESCE(meta, MAP {}::MAP(VARCHAR, VARCHAR))),
       %s)::VARCHAR END`,
		strings.Join(keys, ", "), promotedTagMap(), coldPromotedJSONExpr())
}

// Avoid parsing a JSON object just to strip nulls when its shape already tells
// us which keys exist. These predicates are checked for every row; sparse or
// unusual metadata still uses the general null-stripping expression. Raw-key
// collisions continue through the original MAP path in coldMetaJSONExpr.
func coldPromotedJSONExpr() string {
	object := func(tags []string) string {
		pairs := make([]string, len(tags))
		for i, tag := range tags {
			pairs[i] = sqlQuote(tag) + ", " + tag
		}
		return "json_object(" + strings.Join(pairs, ", ") + ")"
	}
	var core, minimalAbsent []string
	for _, tag := range promotedTags {
		if tag != "job" {
			core = append(core, tag)
		}
		if tag != "plugin" && tag != "vsn" {
			minimalAbsent = append(minimalAbsent, tag)
		}
	}
	present := make([]string, len(core))
	for i, tag := range core {
		present[i] = tag + " IS NOT NULL"
	}
	absent := make([]string, len(minimalAbsent))
	for i, tag := range minimalAbsent {
		absent[i] = tag + " IS NULL"
	}
	all := object(promotedTags)
	return fmt.Sprintf(`CASE
  WHEN %s THEN CASE WHEN job IS NULL THEN %s ELSE %s END
  WHEN plugin IS NOT NULL AND vsn IS NOT NULL AND %s THEN %s
  ELSE json_merge_patch('{}', %s) END`, strings.Join(present, " AND "),
		object(core), all, strings.Join(absent, " AND "), object([]string{"plugin", "vsn"}), all)
}

// buildColdSource builds the cold source for the Iceberg scan.
func buildColdSource(query *model.Query, metadataURL string, now time.Time, rawMeta bool) (string, error) {
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

	// v3 stores timezone-aware nanoseconds. Parse bounds directly at that
	// precision; widening TIMESTAMPTZ after parsing would already lose digits.
	instant := "time"
	bound := func(op, value string) string {
		return fmt.Sprintf("%s %s TIMESTAMPTZ_NS %s", instant, op, sqlQuote(value))
	}
	preds := []string{
		bound(">=", start.UTC().Format(time.RFC3339Nano)),
		bound("<", end.UTC().Format(time.RFC3339Nano)),
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

	meta := "map_concat(COALESCE(meta, MAP {}::MAP(VARCHAR, VARCHAR)), " + promotedTagMap() + ") AS meta"
	if rawMeta {
		meta = "meta, " + strings.Join(promotedTags, ", ")
	}

	inner := fmt.Sprintf(`
        SELECT
            %s AS timestamp,
            name,
            value,
            %s
        FROM iceberg_scan(%s)
        WHERE %s`,
		fmt.Sprintf("CAST(%s AT TIME ZONE 'UTC' AS TIMESTAMP_NS)", instant), meta, sqlQuote(metadataURL), strings.Join(preds, " AND "))

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

	return inner, nil
}

// aggregationValue restores a uint64 VARIANT when the snapshot used decimal
// storage. Any other encoding leaves the stored value unchanged.
func aggregationValue(value, encoding string) string {
	if encoding != uintEncoding {
		return value
	}
	return "CASE WHEN variant_typeof(" + value + ") IN ('DECIMAL(20, 0)','DECIMAL(38, 0)') THEN CASE WHEN " + value + "::DECIMAL(38,0) BETWEEN 0 AND 18446744073709551615 THEN " + value + "::UBIGINT::VARIANT ELSE error('invalid encoded uint64') END ELSE " + value + " END"
}

// buildColdAggregateSQL scans this tier's raw rows and reduces them with the shared plan.
func buildColdAggregateSQL(query *model.Query, metadata, encoding string, now time.Time) (string, error) {
	source, err := buildColdRawSource(query, metadata, encoding, now)
	if err != nil {
		return "", err
	}
	return aggregate.BuildSQL(query, source, now)
}

// buildColdRawSource exposes filtered cold rows without reducing observations.
func buildColdRawSource(query *model.Query, metadata, encoding string, now time.Time) (string, error) {
	raw := *query
	raw.Func = nil
	raw.Window = nil
	raw.Head = nil
	raw.Tail = nil
	cold, err := buildColdSource(&raw, metadata, now, true)
	if err != nil {
		return "", err
	}
	source := "SELECT timestamp,name,meta," + strings.Join(promotedTags, ",") + "," + aggregationValue("value", encoding) + " AS value FROM (" + cold + ")"
	return source, nil
}

// parseColdTime delegates to the shared absolute-time parser.
func parseColdTime(s string, fallback time.Time) (time.Time, error) {
	return aggregate.ParseTime(s, fallback)
}

// filterToSQL converts a filter field and pattern into a SQL predicate.
func filterToSQL(field, pattern string) (string, error) {
	if !model.IsValidFilterString(field) {
		return "", fmt.Errorf("invalid filter field name %q", field)
	}
	if !model.IsValidFilterString(pattern) {
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
