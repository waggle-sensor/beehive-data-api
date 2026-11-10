// request_queue.go bounds how many queries run at once. A caller that cannot
// enter before the timeout should reject the request.

package httpapi

import "time"

// RequestQueue is a fixed number of slots. Timeout is how long Enter waits.
type RequestQueue struct {
	ch      chan struct{}
	timeout time.Duration
}

// NewRequestQueue reserves size slots and uses timeout for each Enter.
func NewRequestQueue(size int, timeout time.Duration) *RequestQueue {
	return &RequestQueue{
		ch:      make(chan struct{}, size),
		timeout: timeout,
	}
}

// Enter takes one slot. It returns false when the timeout elapses first.
func (q *RequestQueue) Enter() bool {
	select {
	case q.ch <- struct{}{}:
		return true
	case <-time.After(q.timeout):
	}
	return false
}

// Leave returns one slot. It blocks if it is not paired with a successful Enter.
func (q *RequestQueue) Leave() {
	<-q.ch
}
