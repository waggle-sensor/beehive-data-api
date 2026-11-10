// encoding.go reads the snapshot property that says whether uint64 values were
// stored as decimals. Only the last inspected snapshot is cached.

package cold

import (
	model "beehive-data-api/internal/query"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// ValidateSourceBucket rejects an override that names a different bucket.
// Cold storage has one warehouse, so the override must match the configured hot bucket.
func (backend *Backend) ValidateSourceBucket(query *model.Query) error {
	if query.Bucket != nil && (backend.SourceBucket == "" || *query.Bucket != backend.SourceBucket) {
		return fmt.Errorf("cold query does not cover requested bucket")
	}
	return nil
}

// Only aggregate requests need kind restoration. Extracts serialize the stored
// decimal directly, so their scan/projection path and wire schema stay intact.
func (backend *Backend) aggregateEncoding(ctx context.Context, metadata, token string) (string, error) {
	backend.encodingMu.Lock()
	defer backend.encodingMu.Unlock()
	if backend.encodingURL == metadata {
		return backend.encodingValue, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadata, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := backend.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return "", fmt.Errorf("aggregation metadata returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (32<<20)+1))
	if err != nil {
		return "", err
	}
	if len(data) > 32<<20 {
		return "", fmt.Errorf("aggregation metadata exceeds 32 MiB")
	}
	var header struct {
		Properties map[string]string `json:"properties"`
	}
	if err = json.Unmarshal(data, &header); err != nil {
		return "", err
	}
	encoding := header.Properties["sage.value.encoding"]
	if encoding != "" && encoding != uintEncoding {
		return "", fmt.Errorf("unsupported cold value encoding %q", encoding)
	}
	// Metadata URLs identify immutable snapshots. Avoid an unbounded cache over
	// archival generations: retain only the last inspected snapshot.
	backend.encodingURL, backend.encodingValue = metadata, encoding
	return encoding, nil
}
