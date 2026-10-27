// query.go contains the code for the hot backend query.
package hot

import (
	"fmt"
	model "beehive-data-api/internal/query"
	"sort"
	"strings"
)

// buildFluxQuery builds a Flux query string for InfluxDB from a bucket name and Query
// for the hot backend.
func buildFluxQuery(bucket string, query *model.Query) (string, error) {
	// override bucket if part of query
	if query.Bucket != nil {
		bucket = *query.Bucket
	}

	// we assume buckets starting with _ are private
	if strings.HasPrefix(bucket, "_") {
		return "", fmt.Errorf("not authorized to access bucket %q", bucket)
	}

	// start query out with data bucket
	parts := []string{
		fmt.Sprintf(`from(bucket:"%s")`, bucket),
	}

	// add range subquery, if not empty
	rangeSubquery, err := buildRangeSubquery(query)
	if err != nil {
		return "", err
	}
	if rangeSubquery != "" {
		parts = append(parts, rangeSubquery)
	}

	// add filter subquery, if not empty
	filterSubquery, err := buildFilterSubquery(query)
	if err != nil {
		return "", err
	}
	if filterSubquery != "" {
		parts = append(parts, filterSubquery)
	}

	if query.Head != nil && query.Tail != nil {
		return "", fmt.Errorf("head and tail cannot both be specified")
	}

	// add head subquery if included
	if query.Head != nil {
		parts = append(parts, fmt.Sprintf("limit(n:%d)", *query.Head))
	}

	// add tail subquery if included
	if query.Tail != nil {
		parts = append(parts, fmt.Sprintf("tail(n:%d)", *query.Tail))
	}

	// ensure window is not used without aggregation function
	if query.Window != nil && query.Func == nil {
		return "", fmt.Errorf("window cannot be used without aggregation function")
	}

	// add aggregation subqueries if included
	if query.Func != nil && query.Window != nil {
		parts = append(parts, fmt.Sprintf("aggregateWindow(every: %s, fn: %s)", *query.Window, *query.Func))
	} else if query.Func != nil {
		fn := *query.Func
		switch fn {
		case "mean":
			parts = append(parts, "mean()")
		case "min":
			parts = append(parts, "min()")
		case "max":
			parts = append(parts, "max()")
		case "sum":
			parts = append(parts, "sum()")
		case "count":
			parts = append(parts, "count()")
		default:
			return "", fmt.Errorf("unsupported function")
		}
	}

	return strings.Join(parts, " |> "), nil
}

// buildRangeSubquery builds the range subquery for the influx query.
// for the hot backend.
func buildRangeSubquery(query *model.Query) (string, error) {
	var parts []string
	if !model.IsValidFilterString(query.Start) {
		return "", fmt.Errorf("invalid start timestamp %q", query.Start)
	}
	if !model.IsValidFilterString(query.End) {
		return "", fmt.Errorf("invalid end timestamp %q", query.End)
	}
	if query.Start != "" {
		parts = append(parts, "start:"+query.Start)
	}
	if query.End != "" {
		parts = append(parts, "stop:"+query.End)
	}
	if len(parts) > 0 {
		return fmt.Sprintf(`range(%s)`, strings.Join(parts, ",")), nil
	}
	return "", nil
}

// Waggle and InfluxDB use slightly different field names in at least one case, so
// we keep a map to document and translate between them when needed.
var fieldRenameMap = map[string]string{
	"name": "_measurement",
}

// buildFilterSubquery builds the filter subquery for the influx query.
// for the hot backend.
func buildFilterSubquery(query *model.Query) (string, error) {
	var parts []string

	for field, pattern := range query.Filter {
		if !model.IsValidFilterString(field) {
			return "", fmt.Errorf("invalid filter field name %q", field)
		}
		if !model.IsValidFilterString(pattern) {
			return "", fmt.Errorf("invalid filter field pattern %q", pattern)
		}

		// rename field, if needed
		if s, ok := fieldRenameMap[field]; ok {
			field = s
		}

		// handle wildcard or exact match. (this may not actually be an optimization)
		// TODO(sean) use regexp.QuoteMeta instead of manually using ReplaceAll.
		switch {
		case strings.Contains(pattern, "|"):
			parts = append(parts, fmt.Sprintf("r.%s =~ /^(%s)$/", field, strings.ReplaceAll(pattern, "/", "\\/")))
		case strings.Contains(pattern, "*"):
			parts = append(parts, fmt.Sprintf("r.%s =~ /^%s$/", field, strings.ReplaceAll(pattern, "/", "\\/")))
		default:
			parts = append(parts, fmt.Sprintf("r.%s == \"%s\"", field, pattern))
		}
	}

	if len(parts) > 0 {
		sort.Strings(parts)
		return fmt.Sprintf(`filter(fn: (r) => %s)`, strings.Join(parts, " and ")), nil
	}

	return "", nil
}
