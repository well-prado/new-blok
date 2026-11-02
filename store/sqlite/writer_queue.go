package sqlite

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// writerQueue hands one turn at a time to its callers, first come first
// served (#214). Arrival is the moment a caller is appended to the waiting
// list under the mutex, so it is observable (see acquire's enqueued hook)
// and does not depend on goroutine scheduling.
type writerQueue struct {
	mu      sync.Mutex
	held    bool
	waiting list.List // of *writerWaiter, oldest first
}

type writerWaiter struct {
	turn    chan struct{} // closed when the turn is handed over
	granted bool
}

// heldTurns reports how many turns are held: zero or one.
func (q *writerQueue) heldTurns() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.held {
		return 1
	}
	return 0
}

// acquire waits for the turn for up to timeout and returns its release. A
// caller that finds the turn free and nobody waiting takes it at once.
// Otherwise it joins the waiting list and then calls enqueued, if set, so a
// test knows the caller has arrived. On expiry it returns timedOut true; on
// cancellation, the context's error.
func (q *writerQueue) acquire(ctx context.Context, timeout time.Duration, enqueued func()) (release func(), timedOut bool, err error) {
	q.mu.Lock()
	if !q.held && q.waiting.Len() == 0 {
		q.held = true
		q.mu.Unlock()
		return q.release, false, nil
	}
	waiter := &writerWaiter{turn: make(chan struct{})}
	element := q.waiting.PushBack(waiter)
	q.mu.Unlock()
	if enqueued != nil {
		enqueued()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-waiter.turn:
		return q.release, false, nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		timedOut = true
	}
	q.mu.Lock()
	if waiter.granted {
		// The turn was handed over as the wait ended: pass it on.
		q.mu.Unlock()
		q.release()
		return nil, timedOut, err
	}
	q.waiting.Remove(element)
	q.mu.Unlock()
	return nil, timedOut, err
}

// release gives the turn to the oldest waiter, or frees it.
func (q *writerQueue) release() {
	q.mu.Lock()
	defer q.mu.Unlock()
	front := q.waiting.Front()
	if front == nil {
		q.held = false
		return
	}
	waiter := q.waiting.Remove(front).(*writerWaiter)
	waiter.granted = true
	close(waiter.turn)
}
