// sql.go plans windowed and limited aggregates over one raw union.
// Head and tail select observations before that reduction.

package aggregate

import (
	model "beehive-data-api/internal/query"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// maxAggregateRows caps both the number of public series and the windows
// expanded for one series. The product of those two is the result budget.
const maxAggregateRows int64 = 1_000_000

// windowToken matches one integer and one Flux unit. The matches must cover
// the whole string, so a gap or a trailing fragment is rejected.
var windowToken = regexp.MustCompile(`([0-9]+)(ns|us|ms|mo|s|m|h|d|w|y)`)

// aggregateWindow is either a fixed nanosecond width or a count of calendar months.
type aggregateWindow struct{ nanos, months int64 }

// parseAggregateWindow accepts repeated units from one family.
// A year counts as twelve months. Calendar units cannot mix with fixed units.
func parseAggregateWindow(text string) (aggregateWindow, error) {
	var window aggregateWindow
	if text == "" {
		return window, fmt.Errorf("aggregation window is empty")
	}
	cursor := 0
	units := map[string]int64{"ns": 1, "us": 1000, "ms": 1000000, "s": 1000000000, "m": 60e9, "h": 3600e9, "d": 86400e9, "w": 604800e9, "mo": 1, "y": 12}
	for _, match := range windowToken.FindAllStringSubmatchIndex(text, -1) {
		if match[0] != cursor {
			return window, fmt.Errorf("unsupported aggregation window %q", text)
		}
		cursor = match[1]
		magnitude, err := strconv.ParseInt(text[match[2]:match[3]], 10, 64)
		if err != nil {
			return window, err
		}
		unit := text[match[4]:match[5]]
		factor := units[unit]
		target := &window.nanos
		if unit == "mo" || unit == "y" {
			target = &window.months
		}
		if magnitude > (math.MaxInt64-*target)/factor {
			return window, fmt.Errorf("aggregation window overflow")
		}
		*target += magnitude * factor
	}
	if cursor != len(text) || (window.nanos == 0 && window.months == 0) || window.months > 120000 {
		return window, fmt.Errorf("unsupported aggregation window %q", text)
	}
	if window.nanos != 0 && window.months != 0 {
		return window, fmt.Errorf("aggregation window cannot mix calendar and fixed units")
	}
	return window, nil
}

// Bounds checks the function, the limits, and the absolute nanosecond range.
// An omitted end uses now. A window is parsed here so a bad interval fails
// before either tier is read.
func Bounds(query *model.Query, now time.Time) (time.Time, time.Time, error) {
	if query.Func == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("aggregation function missing")
	}
	switch *query.Func {
	case "count", "mean", "sum", "min", "max":
	default:
		return time.Time{}, time.Time{}, fmt.Errorf("unsupported aggregation function %q", *query.Func)
	}
	if query.Head != nil && query.Tail != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("head and tail cannot both be specified")
	}
	for _, n := range []*int{query.Head, query.Tail} {
		if n != nil && *n < 0 {
			return time.Time{}, time.Time{}, fmt.Errorf("head/tail must be >= 0")
		}
	}
	start, err := parseColdTime(query.Start, time.Time{})
	if err != nil || start.IsZero() {
		return start, time.Time{}, fmt.Errorf("aggregation requires an absolute start")
	}
	end, err := parseColdTime(query.End, now)
	if err != nil || !end.After(start) {
		return start, end, fmt.Errorf("aggregation end must be after start")
	}
	start, end = start.UTC(), end.UTC()
	for _, t := range []time.Time{start, end} {
		if !t.Equal(time.Unix(0, t.UnixNano())) {
			return start, end, fmt.Errorf("aggregation bound outside nanosecond timestamp range")
		}
	}
	if query.Window != nil {
		if _, _, _, err := aggregateBoundaries(query, start, end); err != nil {
			return start, end, err
		}
	}
	return start, end, nil
}

// floorDiv performs a floor division.
// floorDiv rounds toward negative infinity. Go's / truncates toward zero.
func floorDiv(a, b int64) int64 {
	quotient := a / b
	if a%b < 0 {
		quotient--
	}
	return quotient
}

// Window keys use exact epoch nanoseconds or months from January 1970. Labels
// are window ends clipped to the original request end (including nanoseconds).
func aggregateBoundaries(query *model.Query, start, end time.Time) (string, string, int64, error) {
	if query.Window == nil {
		return "0", "SELECT 0::BIGINT AS bucket," + strconv.FormatInt(start.UnixNano(), 10) + "::BIGINT AS stop_ns", 1, nil
	}
	window, err := parseAggregateWindow(*query.Window)
	if err != nil {
		return "", "", 0, err
	}
	var first, last int64
	expression := ""
	if window.nanos > 0 {
		first = floorDiv(start.UnixNano(), window.nanos)
		last = floorDiv(end.UnixNano()-1, window.nanos)
		n := new(big.Int).Sub(big.NewInt(last), big.NewInt(first))
		n.Add(n, big.NewInt(1))
		if !n.IsInt64() || n.Int64() > maxAggregateRows {
			return "", "", 0, fmt.Errorf("aggregation window expansion exceeds %d rows", maxAggregateRows)
		}
		expression = fmt.Sprintf("(epoch_ns(timestamp)//%d - CASE WHEN epoch_ns(timestamp)%%%d<0 THEN 1 ELSE 0 END)", window.nanos, window.nanos)
		return expression, fmt.Sprintf("SELECT i AS bucket,least(%d::HUGEINT,(i::HUGEINT+1)*%d)::BIGINT AS stop_ns FROM range(%d,%d) w(i)", end.UnixNano(), window.nanos, first, last+1), n.Int64(), nil
	}
	month := func(t time.Time) int64 { return int64(t.Year()*12+int(t.Month())-1) - 1970*12 }
	first = floorDiv(month(start), window.months)
	last = floorDiv(month(end.Add(-time.Nanosecond)), window.months)
	if last-first+1 > maxAggregateRows {
		return "", "", 0, fmt.Errorf("aggregation window expansion exceeds budget")
	}
	floored := "make_timestamp(epoch_ns(timestamp)//1000 - CASE WHEN epoch_ns(timestamp)%1000<0 THEN 1 ELSE 0 END)"
	index := fmt.Sprintf("(year(%s)::BIGINT*12+month(%s)-1-23640)", floored, floored)
	expression = fmt.Sprintf("(%s//%d - CASE WHEN %s%%%d<0 THEN 1 ELSE 0 END)", index, window.months, index, window.months)
	var rows []string
	for i := first; i <= last; i++ {
		absolute := int64(1970*12) + (i+1)*window.months
		year := floorDiv(absolute, 12)
		month := absolute - year*12 + 1
		stop := time.Date(int(year), time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		if end.Before(stop) {
			stop = end
		}
		rows = append(rows, fmt.Sprintf("(%d,%d)", i, stop.UnixNano()))
	}
	return expression, "SELECT * FROM (VALUES " + strings.Join(rows, ",") + ") w(bucket,stop_ns)", last - first + 1, nil
}

// aggregateKind classifies one VARIANT as null, int, uint, float, or other.
func aggregateKind(value string) string {
	return "CASE WHEN " + value + " IS NULL THEN 'null' " +
		"WHEN variant_typeof(" + value + ")='INT64' THEN 'int' WHEN variant_typeof(" + value + ")='UINT64' THEN 'uint' WHEN variant_typeof(" + value + ")='DOUBLE' THEN 'float' ELSE 'other' END"
}

// wrappedAggregate sums int or uint values in HUGEINT, then wraps into the
// source width. Mean divides that wrapped total by the count of the same kind.
func wrappedAggregate(functionName, kind, native string) string {
	total := "sum(CASE WHEN kind=" + sqlQuote(kind) + " THEN value::" + native + "::HUGEINT END)"
	wrapped := "(((" + total + ")%18446744073709551616)+18446744073709551616)%18446744073709551616"
	if kind == "int" {
		wrapped = "(((((" + total + ")+9223372036854775808)%18446744073709551616)+18446744073709551616)%18446744073709551616)-9223372036854775808"
	}
	value := "CAST((" + wrapped + ") AS " + native + ")"
	if functionName == "mean" {
		value += "::DOUBLE/NULLIF(count(CASE WHEN kind=" + sqlQuote(kind) + " THEN value END),0)"
	}
	return "(" + value + ")::VARIANT"
}

// aggregateValueSQL returns one VARIANT expression.
// Count includes nulls. Sum, mean, min, and max reject mixed or non-numeric
// kinds, and a non-finite float fails the statement.
func aggregateValueSQL(functionName string) string {
	value := "count(*)::BIGINT::VARIANT"
	if functionName != "count" {
		var branches []string
		for _, pair := range [][2]string{{"int", "BIGINT"}, {"uint", "UBIGINT"}, {"float", "DOUBLE"}} {
			kind, native := pair[0], pair[1]
			expression := ""
			if kind != "float" && (functionName == "sum" || functionName == "mean") {
				expression = wrappedAggregate(functionName, kind, native)
			} else {
				op := functionName
				if functionName == "mean" {
					op = "avg"
					native = "DOUBLE"
				}
				expression = "CAST(" + op + "(CASE WHEN kind=" + sqlQuote(kind) + " THEN value::" + native + " END) AS " + native + ")::VARIANT"
				if kind == "float" {
					number := "CAST(" + op + "(CASE WHEN kind='float' THEN value::DOUBLE END) AS DOUBLE)"
					expression = "CASE WHEN " + number + " IS NULL OR isfinite(" + number + ") THEN " + number + "::VARIANT ELSE error('nonfinite aggregate result') END"
				}
			}
			branches = append(branches, "WHEN "+sqlQuote(kind)+" THEN "+expression)
		}
		value = "CASE WHEN count(*) FILTER(WHERE kind NOT IN ('int','uint','float','null'))>0 OR count(DISTINCT kind) FILTER(WHERE kind!='null')>1 THEN error('unsupported or mixed aggregate input') ELSE CASE min(kind) FILTER(WHERE kind!='null') " + strings.Join(branches, " ") + " END END"
	}
	return value
}

// Limits select individual observations before aggregation. Other aggregates
// reduce canonical grouping tuples before buffering to bound the working set.
func BuildSQL(query *model.Query, source string, now time.Time) (string, error) {
	if query.Head != nil || query.Tail != nil {
		return buildLimitedAggregateSQL(query, source, now)
	}
	return buildReducedAggregateSQL(query, source, now)
}

// Preserve raw tag tuples until the dictionary: merging promoted tags and
// sorting MAP entries is done once per distinct tuple, never per observation.
// Distinct tuples can resolve to the same public series (key order, overrides,
// null versus empty metadata), so only canonical identities receive series IDs.
func buildLimitedAggregateSQL(query *model.Query, source string, now time.Time) (string, error) {
	if query.Head == nil && query.Tail == nil {
		return "", fmt.Errorf("observation-selection plan requires head or tail")
	}
	start, end, err := Bounds(query, now)
	if err != nil {
		return "", err
	}
	bucket, boundaries, n, err := aggregateBoundaries(query, start, end)
	if err != nil {
		return "", err
	}
	limit, order := query.Head, "ASC"
	if query.Tail != nil {
		limit, order = query.Tail, "DESC"
	}
	selected := fmt.Sprintf("SELECT * EXCLUDE(rn) FROM (SELECT *,row_number() OVER(PARTITION BY series_id ORDER BY timestamp %s) AS rn FROM filtered) WHERE rn<=%d", order, *limit)

	functionName := *query.Func
	value := aggregateValueSQL(functionName)
	outputValue := "a.v"
	if functionName == "count" {
		outputValue = "coalesce(a.v,0::BIGINT::VARIANT)"
	}
	join := "SELECT s.name,s.meta,w.stop_ns," + outputValue + " AS v FROM series s CROSS JOIN boundaries w LEFT JOIN aggregated a ON a.series_id=s.series_id AND a.bucket=w.bucket"
	if query.Window == nil && (functionName == "min" || functionName == "max") {
		join += " WHERE a.n IS NOT NULL"
	}
	keys := append([]string{"name", "meta"}, promotedTags...)
	metadata := "map_concat(COALESCE(meta,MAP {}::MAP(VARCHAR,VARCHAR))," + promotedTagMap() + ")"
	var matches, columns []string
	for _, key := range keys {
		matches = append(matches, "r."+key+" IS NOT DISTINCT FROM d."+key)
		columns = append(columns, "c."+key)
	}
	return fmt.Sprintf(`WITH raw AS MATERIALIZED (%s),
 tag_tuples AS MATERIALIZED (SELECT DISTINCT %s FROM raw),
 canonical_tags AS MATERIALIZED (SELECT *,map_from_entries(list_sort(map_entries(%s))) AS series_meta FROM tag_tuples),
 series_raw AS MATERIALIZED (SELECT row_number() OVER () AS series_id,name,series_meta AS meta FROM (SELECT DISTINCT name,series_meta FROM canonical_tags)),
 series AS (SELECT * FROM series_raw WHERE (SELECT CASE WHEN count(*)>%d THEN error('aggregation result expansion exceeds budget') ELSE true END FROM series_raw)),
 tag_dictionary AS MATERIALIZED (SELECT %s,s.series_id FROM canonical_tags c JOIN series_raw s ON c.name IS NOT DISTINCT FROM s.name AND c.series_meta=s.meta),
 filtered AS MATERIALIZED (SELECT r.timestamp,r.value,d.series_id,%s AS kind FROM raw r JOIN tag_dictionary d ON %s),selected AS (%s),
 grouped AS (SELECT *,%s AS bucket FROM selected),
 boundaries AS (%s),aggregated AS (SELECT series_id,bucket,count(*) AS n,%s AS v FROM grouped GROUP BY series_id,bucket),output AS (%s)
 SELECT make_timestamp_ns(stop_ns) AS timestamp,name,to_json(v)::VARCHAR AS value,to_json(meta)::VARCHAR AS meta FROM output ORDER BY timestamp,name,meta`,
		source, strings.Join(keys, ","), metadata, maxAggregateRows/n, strings.Join(columns, ","), aggregateKind("r.value"), strings.Join(matches, " AND "), selected, bucket, boundaries, value, join), nil
}
