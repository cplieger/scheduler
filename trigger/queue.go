package trigger

import (
	"errors"
	"sync"
	"time"
)

// --- Run queue ---
//
// The daemon is the single owner of job execution: every trigger — the
// built-in ticker and each socket client — submits a Job here, and one
// executor goroutine serves them strictly in order. FIFO with no coalescing:
// every accepted request gets its own run and its own true result (a queued
// trigger's payload replays exactly; nothing is merged, deferred, or replayed
// with the wrong arguments). The shape assumes idempotent runs, so a
// back-to-back duplicate from a trigger burst costs only time.

// Queue rejection errors. Their messages travel the wire verbatim as the
// rejection Reason a waiting client logs, so they are part of the trigger
// contract.
var (
	// ErrClosed rejects submissions once Close has stopped admission.
	ErrClosed = errors.New("scheduler is shutting down")
	// ErrFull rejects submissions while the queue is at capacity — honest
	// backpressure, never unbounded queueing.
	ErrFull = errors.New("run queue is full")
)

// Outcome is a Job's final result.
type Outcome struct {
	// Reason explains a not-OK outcome that isn't a plain job failure
	// (cancelled by shutdown, a failed preflight), or annotates an OK outcome
	// that carries a caveat.
	Reason string
	// Duration is the elapsed execution time; zero when the job never ran.
	Duration time.Duration
	// OK is the run's outcome.
	OK bool
}

// Job is one queued run request. The executor signals lifecycle through it:
// Start the moment the run begins, then Finish with the single result —
// exactly once per accepted job, from the run itself or from shutdown
// cancellation.
type Job[P any] struct {
	started chan struct{}
	result  chan Outcome
	// Payload carries the request's arguments (the app's own type; an
	// argless daemon uses an empty struct).
	Payload P
	// Trigger labels the run's origin in logs: TriggerExternal for socket
	// requests; apps use their own labels (startup, interval) for ticker
	// jobs.
	Trigger string
}

// TriggerExternal is the Trigger label the Server stamps on socket-submitted
// jobs.
const TriggerExternal = "external"

// NewJob builds a job for the given trigger label and payload.
func NewJob[P any](trigger string, payload P) *Job[P] {
	return &Job[P]{
		Payload: payload,
		Trigger: trigger,
		started: make(chan struct{}),
		// Buffered so the executor never blocks on a departed waiter.
		result: make(chan Outcome, 1),
	}
}

// Start marks the moment the executor begins the run; the started channel
// closes and the Server relays EventStarted. Call at most once.
func (j *Job[P]) Start() {
	close(j.started)
}

// Started is closed by the executor the moment the run begins. A job
// cancelled before starting delivers its result without ever starting.
func (j *Job[P]) Started() <-chan struct{} {
	return j.started
}

// Finish delivers the job's single result. Exactly one Finish per accepted
// job; the buffered result channel means the caller never blocks on a
// departed waiter.
func (j *Job[P]) Finish(out Outcome) {
	j.result <- out
}

// Result receives the job's exactly-one outcome — from the run itself, or
// from shutdown cancellation.
func (j *Job[P]) Result() <-chan Outcome {
	return j.result
}

// Queue is the bounded FIFO between triggers and the executor. Submission is
// non-blocking: a full or closed queue rejects immediately. The channel is
// the queue; the executor is its only receiver.
type Queue[P any] struct {
	jobs   chan *Job[P]
	mu     sync.Mutex
	closed bool
}

// NewQueue builds a queue holding at most capacity pending jobs. Size it for
// the realistic trigger set (a periodic job plus a trigger burst), not for
// storage: a client hitting a full queue is rejected immediately with a clear
// reason rather than queued unboundedly.
func NewQueue[P any](capacity int) *Queue[P] {
	return &Queue[P]{jobs: make(chan *Job[P], capacity)}
}

// Submit enqueues j, failing fast with ErrFull or ErrClosed. An accepted job
// is guaranteed exactly one result. The send is non-blocking and happens
// under the mutex, so it can never race Close's channel close.
func (q *Queue[P]) Submit(j *Job[P]) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	select {
	case q.jobs <- j:
		return nil
	default:
		return ErrFull
	}
}

// Jobs is the executor's receive source: range over it until Close drains it.
func (q *Queue[P]) Jobs() <-chan *Job[P] {
	return q.jobs
}

// Close stops admission and closes the channel, letting the executor's range
// loop drain the already-queued jobs (the executor cancels each once shutdown
// is signalled) and terminate. Idempotent; called at shutdown.
func (q *Queue[P]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.jobs)
}
