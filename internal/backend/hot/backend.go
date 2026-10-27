// backend.go contains the Backend type and its associated functions for the hot backend.
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
	fluxQuery, err := buildFluxQuery(backend.Bucket, query)
	if err != nil {
		return nil, err
	}

	queryAPI := backend.Client.QueryAPI(backend.Org)

	results, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, err
	}

	return &influxResults{results: results, useStartTimestamp: query.Func != nil && query.Window == nil}, nil
}

// influxResults is the results for the influx backend.
type influxResults struct {
	results           *api.QueryTableResult
	record            *model.Record
	err               error
	useStartTimestamp bool
}

// Err returns the error for the influx results.
func (r *influxResults) Err() error {
	return r.err
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

// convertToAPIRecord converts the influx record to an API record.
func (r *influxResults) convertToAPIRecord(rec *influxdb2query.FluxRecord) (*model.Record, error) {
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

// buildMetaFromRecord builds the meta from the influx record.
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
