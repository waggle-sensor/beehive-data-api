package main

import (
	"context"
	"fmt"
	"time"
)

// TieredBackend routes a query across a hot and a cold tier.
//
// The watermark is a single instant: everything strictly before it lives in
// cold storage, everything at or after it lives in hot. It is held in
// NANOSECONDS, deliberately. Hot keeps nanosecond resolution (InfluxDB is
// the real-time tier) while cold is microsecond (pyiceberg cannot store ns),
// so a microsecond-rounded watermark would leave a sub-microsecond sliver at
// the cut that is either served twice or not at all. Comparing in ns makes
// the boundary exact.
//
// A consequence worth stating plainly: a tier-spanning result legitimately
// carries MIXED precision -- ns-stamped records from hot, us-stamped from
// cold. That is the migration's visible signature, not a defect.
type TieredBackend struct {
	Hot       Backend
	Cold      Backend
	Watermark time.Time
}

// Query routes to one tier, or splits across both and merges.
func (backend *TieredBackend) Query(ctx context.Context, query *Query) (Results, error) {
	start, err := parseQueryTime(query.Start, time.Time{})
	if err != nil {
		return nil, fmt.Errorf("invalid start timestamp %q: %w", query.Start, err)
	}
	// An empty end means "no upper bound", which always reaches into hot.
	end, err := parseQueryTime(query.End, maxTime)
	if err != nil {
		return nil, fmt.Errorf("invalid end timestamp %q: %w", query.End, err)
	}

	// `end` is EXCLUSIVE, matching the wire protocol. So a query ending
	// exactly at the watermark is entirely cold, and one starting exactly
	// at it is entirely hot.
	coldOnly := !end.After(backend.Watermark)
	hotOnly := !start.Before(backend.Watermark)

	switch {
	case coldOnly:
		return backend.Cold.Query(ctx, query)
	case hotOnly:
		return backend.Hot.Query(ctx, query)
	}

	// Spanning the watermark from here on.

	// Aggregations cannot be split. A mean of two per-tier means is wrong
	// without the counts to weight them, and silently returning a plausible
	// wrong number is worse than refusing.
	if query.Func != nil {
		return nil, fmt.Errorf("aggregation %q spans the hot/cold boundary at %s and cannot be computed across tiers; narrow the range to one side",
			*query.Func, backend.Watermark.Format(time.RFC3339Nano))
	}

	// head/tail are PER SERIES in Flux, not global -- they are table
	// operations. Concatenating each tier's per-series head would return up
	// to 2n records per series, which is a wrong answer rather than a
	// degraded one. Reapplying the limit during the merge would require
	// tracking every series seen, which is unbounded at this cardinality
	// (one seeded day alone has ~905k series). Refusing is the honest
	// option and keeps the semantics the client already relies on.
	if query.Head != nil || query.Tail != nil {
		return nil, fmt.Errorf("head/tail spans the hot/cold boundary at %s; these are per-series limits and cannot be merged across tiers; narrow the range to one side",
			backend.Watermark.Format(time.RFC3339Nano))
	}

	coldQuery := *query
	coldQuery.End = backend.Watermark.Format(time.RFC3339Nano)

	hotQuery := *query
	hotQuery.Start = backend.Watermark.Format(time.RFC3339Nano)

	coldResults, err := backend.Cold.Query(ctx, &coldQuery)
	if err != nil {
		return nil, err
	}

	hotResults, err := backend.Hot.Query(ctx, &hotQuery)
	if err != nil {
		coldResults.Close()
		return nil, err
	}

	// Cold first: it holds the older half, and the two ranges are disjoint
	// at the watermark, so concatenation preserves time order without
	// buffering either side.
	return &tieredResults{first: coldResults, second: hotResults}, nil
}

// maxTime stands in for "no upper bound" on a query with no end.
var maxTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

// parseQueryTime parses the subset of timestamp forms the wire protocol
// accepts. An empty string yields the supplied default.
//
// Relative forms ("-1h") are not resolved here: a tiered query using one
// cannot be routed without knowing "now", and quietly guessing would route
// to the wrong tier. They are rejected instead, with a message that says so.
func parseQueryTime(s string, fallback time.Time) (time.Time, error) {
	if s == "" {
		return fallback, nil
	}
	if s[0] == '-' || s[0] == '+' {
		return time.Time{}, fmt.Errorf("relative timestamps are not supported by the tiered backend; use an absolute RFC3339 timestamp")
	}
	return time.Parse(time.RFC3339, s)
}

// tieredResults concatenates two disjoint, individually ordered streams.
//
// Lazy by construction: `second` is not touched until `first` is exhausted,
// so a large result set never lands in memory.
type tieredResults struct {
	first    Results
	second   Results
	onSecond bool
	err      error
}

// Err returns the error from the last operation.
func (r *tieredResults) Err() error {
	if r.err != nil {
		return r.err
	}
	if err := r.first.Err(); err != nil {
		return err
	}
	return r.second.Err()
}

// Close closes the results and releases the database connection.
func (r *tieredResults) Close() error {
	// Close both even if the first fails, or the second's connection leaks.
	firstErr := r.first.Close()
	secondErr := r.second.Close()
	if firstErr != nil {
		return firstErr
	}
	return secondErr
}

// Record returns the current row as a Record.
func (r *tieredResults) Record() *Record {
	if r.onSecond {
		return r.second.Record()
	}
	return r.first.Record()
}

// Next advances the results to the next row.
func (r *tieredResults) Next() bool {
	if !r.onSecond {
		if r.first.Next() {
			return true
		}
		// Do not advance into the second tier if the first failed -- that
		// would turn a partial failure into a result set that looks whole.
		// The service writes 200 and the headers before iterating, so a
		// mid-stream error cannot change the status code; the only honest
		// signal left is to stop and report through Err().
		if err := r.first.Err(); err != nil {
			r.err = err
			return false
		}
		r.onSecond = true
	}
	return r.second.Next()
}
