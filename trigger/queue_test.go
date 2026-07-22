package trigger

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestQueue_FIFOOrder pins the queue's core contract: jobs come out in
// submission order (every trigger gets its own run, strictly in order).
func TestQueue_FIFOOrder(t *testing.T) {
	t.Parallel()
	q := NewQueue[testPayload](4)
	a := NewJob(TriggerExternal, testPayload{Repos: []string{"a/a"}})
	b := NewJob(TriggerExternal, testPayload{Repos: []string{"b/b"}})
	c := NewJob("interval", testPayload{})
	for _, j := range []*Job[testPayload]{a, b, c} {
		if err := q.Submit(j); err != nil {
			t.Fatalf("Submit() = %v, want nil", err)
		}
	}
	q.Close()
	var got []*Job[testPayload]
	for j := range q.Jobs() {
		got = append(got, j)
	}
	want := []*Job[testPayload]{a, b, c}
	if len(got) != len(want) {
		t.Fatalf("drained %d jobs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("job %d out of order", i)
		}
	}
}

// TestQueue_FullRejectsImmediately pins the bounded-backpressure contract:
// a full queue rejects with ErrFull instead of blocking the trigger.
func TestQueue_FullRejectsImmediately(t *testing.T) {
	t.Parallel()
	q := NewQueue[struct{}](1)
	if err := q.Submit(NewJob(TriggerExternal, struct{}{})); err != nil {
		t.Fatalf("first Submit() = %v, want nil", err)
	}
	if err := q.Submit(NewJob(TriggerExternal, struct{}{})); !errors.Is(err, ErrFull) {
		t.Errorf("Submit() on a full queue = %v, want ErrFull", err)
	}
}

// TestQueue_ClosedRejectsSubmissions pins the shutdown-admission contract.
func TestQueue_ClosedRejectsSubmissions(t *testing.T) {
	t.Parallel()
	q := NewQueue[struct{}](4)
	q.Close()
	if err := q.Submit(NewJob(TriggerExternal, struct{}{})); !errors.Is(err, ErrClosed) {
		t.Errorf("Submit() after Close = %v, want ErrClosed", err)
	}
	q.Close() // idempotent: a second close must not panic
}

// TestQueue_ConcurrentSubmitAndCloseIsSafe hammers Submit against Close
// under the race detector: the mutex serializes them, so no send can hit a
// closed channel (the panic the naive design allowed).
func TestQueue_ConcurrentSubmitAndCloseIsSafe(t *testing.T) {
	t.Parallel()
	for range 50 {
		q := NewQueue[struct{}](2)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				_ = q.Submit(NewJob(TriggerExternal, struct{}{}))
			})
		}
		q.Close()
		wg.Wait()
		for range q.Jobs() { // drain only
		}
	}
}

// TestJob_ExactlyOneResultNeverBlocksExecutor pins the exactly-one-result
// delivery contract: Finish is non-blocking even with no receiver (buffered),
// and the single result reaches a late waiter intact.
func TestJob_ExactlyOneResultNeverBlocksExecutor(t *testing.T) {
	t.Parallel()
	j := NewJob(TriggerExternal, testPayload{Repos: []string{"o/r"}})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		j.Finish(Outcome{OK: true, Duration: 42 * time.Millisecond})
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Finish blocked with no waiter; the result channel must be buffered")
	}
	out := <-j.Result()
	if !out.OK || out.Duration != 42*time.Millisecond {
		t.Errorf("Result() = %+v, want the finished outcome", out)
	}
}
