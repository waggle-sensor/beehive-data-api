// backend.go is an in-memory query.Backend that replays a fixed record list so tests can exercise handlers without hot or cold backends.
package testutil

import (
	"context"

	model "beehive-data-api/internal/query"
)

// Backend implements a test backend which replays a list of test records for the testutil backend.
type Backend struct {
	Records []*model.Record
}

// Query provides the test records through Results for the testutil backend.
func (backend *Backend) Query(ctx context.Context, query *model.Query) (model.Results, error) {
	return &Results{Records: backend.Records}, nil
}

// Results is the results for the dummy backend.
type Results struct {
	Records  []*model.Record
	ErrValue error
	Closed   bool
	record   *model.Record
}

// Err returns the error for the dummy results.
func (r *Results) Err() error {
	return r.ErrValue
}

// Close closes the dummy results.
func (r *Results) Close() error {
	r.Closed = true
	return nil
}

// Record returns the record for the dummy results.
func (r *Results) Record() *model.Record {
	return r.record
}

// Next returns the next record for the dummy results.
func (r *Results) Next() bool {
	if len(r.Records) == 0 {
		return false
	}
	r.record = r.Records[0]
	r.Records = r.Records[1:]
	return true
}
