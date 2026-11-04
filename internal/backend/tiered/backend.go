// backend.go splits one query at a nanosecond border. Cold is read first and
// hot continues only when the window crosses the border. Relative bounds are
// resolved once before that decision. Aggregations, head, and tail are refused
// when the resolved window spans the border.

package tiered

import (
	"context"
	"fmt"
	"strings"
	"time"

	model "beehive-data-api/internal/query"
)

// Backend routes static and dynamically coordinated queries across hot/cold tiers.
//
// In static mode Border is the boundary; dynamic mode uses Coordinator.
// The boundary is a single instant: everything strictly before it lives in
// cold storage, everything at or after it lives in hot. It is held in
// NANOSECONDS so v3 cold and hot rows meet at exactly one half-open boundary.
// Both tiers preserve nanosecond timestamps.
type Backend struct {
	Hot         model.Backend
	Cold        model.Backend
	Border      time.Time
	Coordinator *Coordinator
	now         func() time.Time // the clock used to resolve relative bounds; tests pin it
}

// clock is the instant both relative bounds are measured from.
func (backend *Backend) clock() time.Time {
	if backend.now != nil {
		return backend.now()
	}
	return time.Now()
}

// Query resolves relative bounds, then routes to one tier or splits across both.
// The caller's query is not modified. Dynamic coverage sees the resolved start.
func (backend *Backend) Query(ctx context.Context, query *model.Query) (model.Results, error) {
	resolved, err := resolveRelativeBounds(query, backend.clock())
	if err != nil {
		return nil, err
	}
	if backend.Coordinator != nil {
		return backend.Coordinator.query(ctx, resolved, backend.queryAt)
	}
	return backend.queryAt(ctx, resolved, backend.Border)
}

// queryAt routes using the request's chosen boundary; it never mutates shared configuration.
func (backend *Backend) queryAt(ctx context.Context, query *model.Query, border time.Time) (model.Results, error) {
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
	// exactly at the border is entirely cold, and one starting exactly
	// at it is entirely hot.
	coldOnly := !end.After(border)
	hotOnly := !start.Before(border)

	switch {
	case coldOnly:
		return backend.Cold.Query(ctx, query)
	case hotOnly:
		return backend.Hot.Query(ctx, query)
	}

	// Spanning the border from here on.

	// Aggregations cannot be split. A mean of two per-tier means is wrong
	// without the counts to weight them, and silently returning a plausible
	// wrong number is worse than refusing.
	if query.Func != nil {
		return nil, fmt.Errorf("aggregation %q spans the hot/cold boundary at %s and cannot be computed across tiers; narrow the range to one side",
			*query.Func, border.Format(time.RFC3339Nano))
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
			border.Format(time.RFC3339Nano))
	}

	coldQuery := *query
	coldQuery.End = border.Format(time.RFC3339Nano)

	hotQuery := *query
	hotQuery.Start = border.Format(time.RFC3339Nano)

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
	// at the border, so concatenation preserves time order without
	// buffering either side.
	return &tieredResults{first: coldResults, second: hotResults}, nil
}

// maxTime stands in for "no upper bound" on a query with no end.
var maxTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

// parseQueryTime parses an absolute bound. An empty string yields the supplied default.
// Relative bounds are rewritten to absolute timestamps before this is called.
func parseQueryTime(s string, fallback time.Time) (time.Time, error) {
	if s == "" {
		return fallback, nil
	}
	return time.Parse(time.RFC3339, s)
}

// tieredResults concatenates two disjoint, individually ordered streams.
//
// Lazy by construction: `second` is not touched until `first` is exhausted,
// so a large result set never lands in memory.
type tieredResults struct {
	first    model.Results
	second   model.Results
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
func (r *tieredResults) Record() *model.Record {
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

// Parts exposes the ordered child streams without transferring their ownership.
func (r *tieredResults) Parts() []model.Results { return []model.Results{r.first, r.second} }

// FrontierOptions holds startup settings for static or dynamic cold routing.
type FrontierOptions struct {
	Mode, Warehouse, Border, URL, Token, Owner, Bucket string
}

// FrontierConfig is the validated frontier mode and optional static boundary.
type FrontierConfig struct {
	Mode   string
	Border time.Time
}

// ParseFrontier validates startup settings without opening either storage tier.
func ParseFrontier(options FrontierOptions) (FrontierConfig, error) {
	config := FrontierConfig{Mode: strings.ToLower(strings.TrimSpace(options.Mode))}
	if config.Mode != "static" && config.Mode != "dynamic" {
		return config, fmt.Errorf("cold.frontier must be static or dynamic")
	}
	if config.Mode == "dynamic" {
		if options.Warehouse == "" || options.URL == "" || options.Token == "" || options.Owner == "" || options.Bucket == "" {
			return config, fmt.Errorf("dynamic cold.frontier requires cold.warehouse, hot-to-cold-loader.url, hot-to-cold-loader.reader-token, hot-to-cold-loader.reader-owner and influxdb.bucket")
		}
		if options.Border != "" {
			return config, fmt.Errorf("cold.border must be unset with dynamic cold.frontier")
		}
		return config, nil
	}
	if options.URL != "" || options.Token != "" || options.Owner != "" {
		return config, fmt.Errorf("hot-to-cold-loader settings require dynamic cold.frontier")
	}
	if options.Warehouse == "" {
		if options.Border != "" {
			return config, fmt.Errorf("cold.border requires cold.warehouse")
		}
		return config, nil // Default hot-only deployment.
	}
	var err error
	config.Border, err = time.Parse(time.RFC3339Nano, options.Border)
	if err != nil {
		return config, fmt.Errorf("static cold.frontier requires an RFC3339 cold.border: %w", err)
	}
	return config, nil
}
