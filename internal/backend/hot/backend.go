// backend.go reads the hot tier from Influx. A bucket whose name starts with
// "_" is rejected. An aggregation without a window reports the range start
// instead of each point's time.

package hot

import (
	"context"
	"fmt"
	"strings"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	influxdb2query "github.com/influxdata/influxdb-client-go/v2/api/query"

	model "beehive-data-api/internal/query"
)

// Backend implements a backend to InfluxDB for the hot backend.
type Backend struct {
	Client influxdb2.Client
	Org    string
	Bucket string
}

// Query converts and makes a query to an Influx backend.
func (backend *Backend) Query(ctx context.Context, query *model.Query) (model.Results, error) {
	return backend.query(ctx, query, false)
}

// QueryRawSource validates the managed value-field contract for aggregate
// and spanning head/tail staging.
func (backend *Backend) QueryRawSource(ctx context.Context, query *model.Query) (model.Results, error) {
	return backend.query(ctx, query, true)
}

// query runs one Flux query. validateField rejects any field other than value
// so a staged aggregate cannot mix Flux fields into one series.
func (backend *Backend) query(ctx context.Context, query *model.Query, validateField bool) (model.Results, error) {
	fluxQuery, err := buildFluxQuery(backend.Bucket, query)
	if err != nil {
		return nil, err
	}

	queryAPI := backend.Client.QueryAPI(backend.Org)

	results, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, err
	}

	return &influxResults{results: results, validateValueField: validateField, useStartTimestamp: query.Func != nil && query.Window == nil}, nil
}

// influxResults is the results for the influx backend.
type influxResults struct {
	results            *api.QueryTableResult
	record             *model.Record
	err                error
	useStartTimestamp  bool
	validateValueField bool
}

// Err returns the error for the influx results.
func (r *influxResults) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.results.Err()
}

// Close closes the influx results.
func (r *influxResults) Close() error {
	return r.results.Close()
}

// Record returns the record for the influx results.
func (r *influxResults) Record() *model.Record {
	return r.record
}

// Next returns the next record for the influx results.
func (r *influxResults) Next() bool {
	if !r.results.Next() {
		return false
	}
	r.record, r.err = r.convertToAPIRecord(r.results.Record())
	return r.err == nil
}

// convertToAPIRecord maps one Flux row onto the public record. The measurement
// name and _value are required to have their Flux types.
func (r *influxResults) convertToAPIRecord(rec *influxdb2query.FluxRecord) (*model.Record, error) {
	if r.validateValueField && rec.Values()["_field"] != "value" {
		return nil, fmt.Errorf("aggregation requires the single value-field contract")
	}
	apirec := &model.Record{}

	name, ok := rec.Values()["_measurement"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid measurement name type")
	}

	apirec.Name = name
	if r.useStartTimestamp {
		// TODO(sean) Think about the right timestamp to provide for aggregations, particularly if we plan on supporting any windowing.
		apirec.Timestamp = rec.Start()
	} else {
		apirec.Timestamp = rec.Time()
	}
	apirec.Value = rec.Values()["_value"]
	apirec.Meta = buildMetaFromRecord(rec)
	return apirec, nil
}

// buildMetaFromRecord copies Flux tags into the public meta map. Internal
// fields, and the leaked table and result columns, are left out.
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
