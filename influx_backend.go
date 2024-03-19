package main

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	influxdb2query "github.com/influxdata/influxdb-client-go/v2/api/query"
)

// InfluxBackend implements a backend to InfluxDB.
type InfluxBackend struct {
	Client influxdb2.Client
	Org    string
	Bucket string
}

// Query converts and makes a query to an Influx backend.
func (backend *InfluxBackend) Query(ctx context.Context, query *Query) (Results, error) {
	fluxQuery, err := buildFluxQuery(backend.Bucket, query)
	if err != nil {
		return nil, err
	}

	queryAPI := backend.Client.QueryAPI(backend.Org)

	results, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, err
	}

	return &influxResults{results: results, usedAggFunc: query.Func != nil}, nil
}

type influxResults struct {
	results     *api.QueryTableResult
	record      *Record
	err         error
	usedAggFunc bool
}

func (r *influxResults) Err() error {
	return r.err
}

func (r *influxResults) Close() error {
	return r.results.Close()
}

func (r *influxResults) Record() *Record {
	return r.record
}

func (r *influxResults) Next() bool {
	if !r.results.Next() {
		return false
	}
	r.record, r.err = r.convertToAPIRecord(r.results.Record())
	return r.err == nil
}

func (r *influxResults) convertToAPIRecord(rec *influxdb2query.FluxRecord) (*Record, error) {
	apirec := &Record{}

	name, ok := rec.Values()["_measurement"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid measurement name type")
	}

	apirec.Name = name
	if r.usedAggFunc {
		// TODO(sean) Think about the right timestamp to provide for aggregations, particularly if we plan on supporting any windowing.
		apirec.Timestamp = rec.Start()
	} else {
		apirec.Timestamp = rec.Time()
	}
	apirec.Value = rec.Values()["_value"]
	apirec.Meta = buildMetaFromRecord(rec)
	return apirec, nil
}

func buildMetaFromRecord(rec *influxdb2query.FluxRecord) map[string]string {
	meta := make(map[string]string)

	for k, v := range rec.Values() {
		// skip influxdb internal fields (convention is to start with _)
		if strings.HasPrefix(k, "_") {
			continue
		}
		// skip influxdb "leaky" fields (table and result don't use above
		// convention but still leak internal details about query)
		if k == "table" || k == "result" {
			continue
		}
		// only include string types in meta
		s, ok := v.(string)
		if !ok {
			continue
		}
		meta[k] = s
	}

	return meta
}

// buildFluxQuery builds a Flux query string for InfluxDB from a bucket name and Query
func buildFluxQuery(bucket string, query *Query) (string, error) {
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

	// add func subquery if included
	if query.Func != nil {
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

func buildRangeSubquery(query *Query) (string, error) {
	var parts []string
	if !isValidFilterString(query.Start) {
		return "", fmt.Errorf("invalid start timestamp %q", query.Start)
	}
	if !isValidFilterString(query.End) {
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

func buildFilterSubquery(query *Query) (string, error) {
	var parts []string

	for field, pattern := range query.Filter {
		if !isValidFilterString(field) {
			return "", fmt.Errorf("invalid filter field name %q", field)
		}
		if !isValidFilterString(pattern) {
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

var validQueryStringRE = regexp.MustCompile("^[A-Za-z0-9+-_.*:| ]*$")

func isValidFilterString(s string) bool {
	return validQueryStringRE.MatchString(s)
}
