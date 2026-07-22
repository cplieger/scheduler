package trigger

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestExecute_RunsJobsInOrderWithOwnResults pins the helper's core contract:
// each accepted job is started, run through the callback with its own trigger
// label and payload, and finished with exactly the outcome the callback
// returned, in submission order.
func TestExecute_RunsJobsInOrderWithOwnResults(t *testing.T) {
	t.Parallel()
	q := NewQueue[testPayload](4)
	a := NewJob(TriggerExternal, testPayload{Repos: []string{"a/a"}})
	b := NewJob("interval", testPayload{})
	for _, j := range []*Job[testPayload]{a, b} {
		if err := q.Submit(j); err != nil {
			t.Fatalf("Submit() = %v, want nil", err)
		}
	}
	q.Close()

	var seen []string
	Execute(t.Context(), q, func(_ context.Context, trigger string, p testPayload) Outcome {
		seen = append(seen, trigger)
		return Outcome{OK: true, Reason: trigger, Duration: time.Duration(len(p.Repos))}
	})

	if len(seen) != 2 || seen[0] != TriggerExternal || seen[1] != "interval" {
		t.Fatalf("callback saw triggers %v, want [external interval] in order", seen)
	}
	for _, tc := range []struct {
		job  *Job[testPayload]
		want Outcome
	}{
		{a, Outcome{OK: true, Reason: TriggerExternal, Duration: 1}},
		{b, Outcome{OK: true, Reason: "interval", Duration: 0}},
	} {
		select {
		case <-tc.job.Started():
		default:
			t.Error("job was run but Started() is not closed")
		}
		if got := <-tc.job.Result(); got != tc.want {
			t.Errorf("Result() = %+v, want %+v", got, tc.want)
		}
		select {
		case extra := <-tc.job.Result():
			t.Errorf("second result %+v delivered, want exactly one", extra)
		default:
		}
	}
}

// TestExecute_CancelledContextFinishesWithoutStarting pins the shutdown
// drain: a job received after the context is done is finished with
// CancelledReason without ever starting, and the callback never runs.
func TestExecute_CancelledContextFinishesWithoutStarting(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	q := NewQueue[struct{}](1)
	j := NewJob(TriggerExternal, struct{}{})
	if err := q.Submit(j); err != nil {
		t.Fatalf("Submit() = %v, want nil", err)
	}
	q.Close()

	Execute(ctx, q, func(context.Context, string, struct{}) Outcome {
		t.Error("callback ran for a job cancelled before start")
		return Outcome{}
	})

	select {
	case <-j.Started():
		t.Error("Started() closed for a cancelled job; it must never start")
	default:
	}
	got := <-j.Result()
	want := Outcome{OK: false, Reason: CancelledReason}
	if got != want {
		t.Errorf("Result() = %+v, want %+v", got, want)
	}
}

// TestExecute_PanickingCallbackStillDeliversResult pins the stranded-waiter
// guard: a panicking callback still yields the job's exactly-one result (OK
// false, the panic value in the Reason) before the panic propagates out of
// the executor with its original value.
func TestExecute_PanickingCallbackStillDeliversResult(t *testing.T) {
	t.Parallel()
	q := NewQueue[struct{}](1)
	j := NewJob(TriggerExternal, struct{}{})
	if err := q.Submit(j); err != nil {
		t.Fatalf("Submit() = %v, want nil", err)
	}
	q.Close()

	propagated := make(chan any, 1)
	go func() {
		defer func() { propagated <- recover() }()
		Execute(t.Context(), q, func(context.Context, string, struct{}) Outcome {
			panic("boom")
		})
	}()

	select {
	case v := <-propagated:
		if v != "boom" {
			t.Errorf("propagated panic = %v, want the original value \"boom\"", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Execute swallowed the callback panic; it must fail fast after delivering the result")
	}

	select {
	case got := <-j.Result():
		if got.OK || !strings.Contains(got.Reason, "boom") {
			t.Errorf("Result() = %+v, want OK=false with the panic value in the Reason", got)
		}
	default:
		t.Fatal("no result delivered for the panicked job; the waiter would be stranded")
	}
	select {
	case extra := <-j.Result():
		t.Errorf("second result %+v delivered, want exactly one", extra)
	default:
	}
}
