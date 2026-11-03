// lease.go keeps a managed read alive until the caller cancels it. A renewal
// may extend the deadline and must not change the pinned snapshot. A hung
// renewal still ends the query at the last confirmed expiry.

package tiered

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// readLease renews independently of row/batch iteration, including query setup.
// Its watchdog cancels at the last confirmed expiry even if a renewal hangs.
// The returned cancel stops renewal and waits for that goroutine to finish.
func (coordinator *Coordinator) readLease(parent context.Context, owner string, view View, expiry time.Time) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	var mu sync.Mutex
	var watchdog *time.Timer
	mu.Lock()
	watchdog = time.AfterFunc(time.Until(expiry), func() {
		mu.Lock()
		defer mu.Unlock()
		if remaining := time.Until(expiry); remaining > 0 {
			watchdog.Reset(remaining)
		} else {
			cancel(context.DeadlineExceeded)
		}
	})
	mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { mu.Lock(); watchdog.Stop(); mu.Unlock() }()
		retry := false
		for {
			mu.Lock()
			confirmedExpiry := expiry
			mu.Unlock()
			interval := time.Until(confirmedExpiry) / 3
			if interval > time.Minute {
				interval = time.Minute
			}
			if retry && interval > time.Second {
				interval = time.Second
			}
			if interval <= 0 {
				cancel(context.DeadlineExceeded)
				return
			}
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			requestCtx, end := context.WithDeadline(ctx, confirmedExpiry)
			requestCtx, timeout := context.WithTimeout(requestCtx, 30*time.Second)
			var renewed View
			err := coordinator.call(requestCtx, "/v1/reads/renew", map[string]string{"id": view.ID, "owner": owner, "bucket": coordinator.Bucket}, &renewed)
			timeout()
			end()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				if !retry {
					log.Printf("managed read renewal failed; retrying before lease expiry: %v", err)
				}
				retry = true
				continue
			}
			next, err := time.Parse(time.RFC3339Nano, renewed.ExpiresAt)
			if err != nil || renewed.ID != view.ID || renewed.Warehouse != view.Warehouse || renewed.Metadata != view.Metadata || renewed.Frontier != view.Frontier || renewed.CoverageStart != view.CoverageStart || renewed.Generation != view.Generation || next.Before(confirmedExpiry) {
				cancel(errors.New("invalid renewed read lease"))
				return
			}
			mu.Lock()
			// A delayed response cannot revive an already expired local query.
			if ctx.Err() != nil || !time.Now().Before(expiry) {
				cancel(context.DeadlineExceeded)
				mu.Unlock()
				return
			}
			expiry = next
			watchdog.Reset(time.Until(expiry))
			mu.Unlock()
			retry = false
		}
	}()
	return ctx, func() { cancel(context.Canceled); <-done }
}
