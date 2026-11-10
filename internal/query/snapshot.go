// snapshot.go carries the managed metadata URL from grant acquisition to the
// cold backend. An empty value means the query has no pinned snapshot.

package query

import "context"

type metadataKey struct{}

// WithColdMetadata pins the managed snapshot chosen before tier routing.
func WithColdMetadata(ctx context.Context, metadata string) context.Context {
	return context.WithValue(ctx, metadataKey{}, metadata)
}

// ColdMetadata is the pinned metadata URL, or empty when the context has none.
func ColdMetadata(ctx context.Context) string {
	value, _ := ctx.Value(metadataKey{}).(string)
	return value
}
