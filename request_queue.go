package main

import "time"

type RequestQueue struct {
	ch      chan struct{}
	timeout time.Duration
}

func NewRequestQueue(size int, timeout time.Duration) *RequestQueue {
	return &RequestQueue{
		ch:      make(chan struct{}, size),
		timeout: timeout,
	}
}

func (q *RequestQueue) Enter() bool {
	select {
	case q.ch <- struct{}{}:
		return true
	case <-time.After(q.timeout):
	}
	return false
}

func (q *RequestQueue) Leave() {
	<-q.ch
}
