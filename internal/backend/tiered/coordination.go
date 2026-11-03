// coordination.go pins one frontier and snapshot for a query and owns the
// read grant until the results are closed.

package tiered

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	model "beehive-data-api/internal/query"
)

// View is one acquired read. Frontier, metadata, coverage, and generation stay
// fixed for that grant. Renewal may move ExpiresAt later and nothing else.
type View struct {
	ExpiresAt     string `json:"expires_at"`
	ID            string `json:"id"`
	Warehouse     string `json:"warehouse"`
	Metadata      string `json:"metadata"`
	Frontier      string `json:"frontier"`
	CoverageStart string `json:"coverage_start"`
	Generation    int64  `json:"generation"`
}

// Coordinator configures dynamic frontier discovery for the tiered backend.
type Coordinator struct {
	URL, Token, Bucket, Owner, Warehouse string
	Client                               *http.Client
	ownerOnce                            sync.Once
	sessionOwner                         string
	ownerError                           error
}

// ownerSession appends one process UUID to the configured owner. Later calls
// return that same owner.
func (coordinator *Coordinator) ownerSession() (string, error) {
	coordinator.ownerOnce.Do(func() {
		value := make([]byte, 16)
		if _, coordinator.ownerError = rand.Read(value); coordinator.ownerError == nil {
			coordinator.sessionOwner = coordinator.Owner + "/" + hex.EncodeToString(value)
			log.Printf("managed archive reader owner=%s", coordinator.sessionOwner)
		}
	})
	return coordinator.sessionOwner, coordinator.ownerError
}

// call posts one JSON command and does not follow redirects. A response body
// is capped at 1 MiB and is decoded only when out is non-nil.
func (coordinator *Coordinator) call(ctx context.Context, path string, in any, out any) error {
	raw, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(coordinator.URL, "/")+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+coordinator.Token)
	req.Header.Set("Content-Type", "application/json")
	client := coordinator.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("archive coordination HTTP %d", resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
	}
	return nil
}

// release drops the grant on its own timeout, so a canceled query can still release it.
func (coordinator *Coordinator) release(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return coordinator.call(ctx, "/v1/reads/release", map[string]string{"id": id, "bucket": coordinator.Bucket}, nil)
}

// query acquires a pinned view, rejects a start before coverage, and routes at
// the pinned frontier. A failure releases the grant. The returned results
// release it from Close, and only after the child results close cleanly.
func (coordinator *Coordinator) query(ctx context.Context, query *model.Query, route func(context.Context, *model.Query, time.Time) (model.Results, error)) (model.Results, error) {
	if query.Bucket != nil && *query.Bucket != coordinator.Bucket {
		return nil, errors.New("managed archive does not cover the requested bucket")
	}
	owner, err := coordinator.ownerSession()
	if err != nil {
		return nil, err
	}
	randomID := make([]byte, 16)
	if _, err := rand.Read(randomID); err != nil {
		return nil, err
	}
	requestID := hex.EncodeToString(randomID)
	var view View
	if err := coordinator.call(ctx, "/v1/reads/acquire", map[string]string{"id": requestID, "owner": owner, "bucket": coordinator.Bucket}, &view); err != nil {
		log.Printf("managed read acquire unresolved owner=%s id=%s", owner, requestID)
		return nil, err
	}
	fail := func(err error) (model.Results, error) { return nil, errors.Join(err, coordinator.release(requestID)) }
	if view.ID != requestID || view.Warehouse != coordinator.Warehouse {
		return fail(errors.New("managed warehouse/read identity mismatch"))
	}
	frontier, err := time.Parse(time.RFC3339Nano, view.Frontier)
	if err != nil {
		return fail(err)
	}
	coverage, err := time.Parse(time.RFC3339Nano, view.CoverageStart)
	if err != nil {
		return fail(err)
	}
	start, err := time.Parse(time.RFC3339Nano, query.Start)
	if err != nil {
		return fail(err)
	}
	if start.Before(coverage) {
		return fail(fmt.Errorf("managed coverage starts at %s", view.CoverageStart))
	}
	if frontier.Before(coverage) {
		return fail(errors.New("invalid managed coverage frontier"))
	}
	if start.Before(frontier) {
		metadataURL, err := url.Parse(view.Metadata)
		warehouseURL, warehouseErr := url.Parse(coordinator.Warehouse)
		if err != nil || warehouseErr != nil || view.Metadata == "" || metadataURL.Scheme != warehouseURL.Scheme || metadataURL.Host != warehouseURL.Host || metadataURL.RawQuery != "" || metadataURL.Fragment != "" || metadataURL.RawPath != "" || metadataURL.User != nil || !strings.HasPrefix(metadataURL.Path, warehouseURL.Path+"/sage/obs/metadata/") || strings.Contains(metadataURL.Path, "..") || !strings.HasSuffix(metadataURL.Path, ".metadata.json") {
			return fail(errors.New("invalid managed metadata URL"))
		}
	}
	deadline, err := time.Parse(time.RFC3339Nano, view.ExpiresAt)
	if err != nil || !time.Now().Before(deadline) {
		return fail(errors.New("missing, invalid or expired managed read deadline"))
	}
	readCtx, cancel := coordinator.readLease(ctx, owner, view, deadline)
	results, err := route(model.WithColdMetadata(readCtx, view.Metadata), query, frontier)
	if err != nil {
		cancel()
		return fail(err)
	}
	if err = context.Cause(readCtx); err != nil {
		cancel()
		if closeErr := results.Close(); closeErr != nil {
			return nil, errors.Join(err, closeErr) // grant expires even if closure is uncertain
		}
		return fail(err)
	}
	return &grantedResults{Results: results, ctx: readCtx, cancel: cancel, release: func() error { return coordinator.release(requestID) }}, nil
}

type grantedResults struct {
	model.Results
	ctx      context.Context
	cancel   context.CancelFunc
	release  func() error
	once     sync.Once
	closeErr error
}

// Parts preserves the native Arrow path through this ownership wrapper.
func (r *grantedResults) Parts() []model.Results { return []model.Results{r.Results} }

// Next stops when the lease context ends, before asking the child for another row.
func (r *grantedResults) Next() bool {
	if r.ctx.Err() != nil {
		return false
	}
	return r.Results.Next()
}

// Err joins a lease cancellation with the child error.
func (r *grantedResults) Err() error { return errors.Join(context.Cause(r.ctx), r.Results.Err()) }

// Close stops the lease, closes the child results, then releases the grant.
// A second close returns the first result. An uncertain child close keeps the grant.
func (r *grantedResults) Close() error {
	r.once.Do(func() {
		r.cancel()
		r.closeErr = r.Results.Close()
		if r.closeErr == nil {
			r.closeErr = r.release()
		}
	})
	return r.closeErr
}
