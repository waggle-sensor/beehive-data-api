// types.go is the query contract: the request body, a measurement record, and
// the Backend and Results interfaces every store implements.

package query

import (
	"context"
	"errors"
	"time"
)

// ErrResourceClosureUncertain keeps an acquired read grant until lease expiry
// when setup fails and its raw source cannot confirm closure.
var ErrResourceClosureUncertain = errors.New("query resource closure uncertain")

// Backend defines an interface for query backends.
type Backend interface {
	Query(context.Context, *Query) (Results, error)
}

// Results defines an interface for query result sets.
type Results interface {
	Err() error
	Close() error
	Next() bool
	Record() *Record
}

// Query holds an SDR query body.
type Query struct {
	Bucket *string           `json:"bucket,omitempty"`
	Start  string            `json:"start,omitempty"`
	End    string            `json:"end,omitempty"`
	Head   *int              `json:"head,omitempty"`
	Tail   *int              `json:"tail,omitempty"`
	Func   *string           `json:"experimental_func,omitempty"`
	Window *string           `json:"experimental_window,omitempty"`
	Filter map[string]string `json:"filter"`
}

// Record holds an SDR API record.
type Record struct {
	Timestamp time.Time         `json:"timestamp"`
	Name      string            `json:"name"`
	Value     interface{}       `json:"value"`
	Meta      map[string]string `json:"meta"`
}

// CompositeResults exposes ordered streams for encoders that consume native
// batches. The enclosing Results retains responsibility for closing the parts.
type CompositeResults interface {
	Results
	Parts() []Results
}

// RawSourceBackend reads raw records and validates the single value-field
// contract for aggregates and spanning head/tail. Other extracts retain their
// existing field behavior.
type RawSourceBackend interface {
	QueryRawSource(context.Context, *Query) (Results, error)
}

// PlannedSourceBackend exposes an authenticated, filtered cold scan to a caller's
// SQL plan. It does not read hot storage or own cross-tier scratch files.
type PlannedSourceBackend interface {
	ValidateSourceBucket(*Query) error
	BeginQueryProfile(context.Context) (context.Context, QueryProfile)
	QuerySource(context.Context, *Query, bool, func(string, time.Time) (string, error)) (Results, error)
}

// QueryProfile shares one engine profile across tiered staging and execution.
type QueryProfile interface {
	Enabled() bool
	Clock() time.Time
	Duration(string, time.Time)
	Value(string, float64)
	Mark(string)
	Finish(bool)
}

type queryProfileKey struct{}

// WithQueryProfile attaches the profile shared by hot staging and cold execution.
func WithQueryProfile(ctx context.Context, profile QueryProfile) context.Context {
	return context.WithValue(ctx, queryProfileKey{}, profile)
}

// QueryProfileFromContext returns the attached profile, or one that records nothing.
func QueryProfileFromContext(ctx context.Context) QueryProfile {
	if profile, ok := ctx.Value(queryProfileKey{}).(QueryProfile); ok {
		return profile
	}
	return NoQueryProfile{}
}

// NoQueryProfile records nothing. It is used when profiling is off.
type NoQueryProfile struct{}

func (NoQueryProfile) Enabled() bool              { return false }
func (NoQueryProfile) Clock() time.Time           { return time.Time{} }
func (NoQueryProfile) Duration(string, time.Time) {}
func (NoQueryProfile) Value(string, float64)      {}
func (NoQueryProfile) Mark(string)                {}
func (NoQueryProfile) Finish(bool)                {}
