package cold

import (
	model "beehive-data-api/internal/query"
)

import "time"

// buildColdInner forces merged-MAP metadata for regression comparisons and
// historical execution probes. Live queries use coldResponseSource instead.
func buildColdInner(query *model.Query, metadataURL string, now time.Time) (string, error) {
	return buildColdSource(query, metadataURL, now, false)
}

// coldArrowProjection serializes an already assembled metadata MAP. Tests use
// this baseline to check the optimized projection's output and streaming.
func coldArrowProjection(inner string) string {
	return coldArrowProjectionWithMeta(inner, "to_json(meta)::VARCHAR")
}
