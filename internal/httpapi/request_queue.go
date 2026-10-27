// request_queue.go contains the RequestQueue type and its associated functions for the public HTTP API.
package httpapi

import "time"

// RequestQueue is the queue for the requests.
type RequestQueue struct {
	ch      chan struct{}
	timeout time.Duration
}

// NewRequestQueue creates a new RequestQueue.
func NewRequestQueue(size int, timeout time.Duration) *RequestQueue {
	return &RequestQueue{
		ch:      make(chan struct{}, size),
		timeout: timeout,
	}
}

// Enter enters the request queue.
func (q *RequestQueue) Enter() bool {
	select {
	case q.ch <- struct{}{}:
		return true
	case <-time.After(q.timeout):
	}
	return false
}

// Leave leaves the request queue.
func (q *RequestQueue) Leave() {
	<-q.ch
}
