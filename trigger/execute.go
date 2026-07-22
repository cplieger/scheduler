package trigger

import (
	"context"
	"fmt"
)

// CancelledReason is the Outcome.Reason Execute delivers for a job it
// cancels instead of running because the context was already done. Like
// ErrClosed and ErrFull, the string travels the wire verbatim to the waiting
// client, so it is part of the trigger contract.
const CancelledReason = "cancelled: scheduler shutting down"

// Execute is the executor loop for the common daemon shape: it receives jobs
// from q strictly in order and runs each through the run callback, owning the
// Start/Finish lifecycle so the exactly-one-result contract is structural
// rather than conventional — the callback returns the job's Outcome (timing
// its own Duration) and cannot forget or double-deliver it. Mutual exclusion
// is this loop: it is the queue's single receiver, so nothing else starts a
// job.
//
// ctx is the daemon's shutdown context, and it governs admission, not the
// in-flight run: a job received after ctx is done is finished with
// CancelledReason without ever starting (the shutdown drain of already-queued
// jobs), while a run already in flight sees the cancellation only through the
// ctx passed to the callback and decides for itself. Execute blocks until
// Close drains the queue, so a caller can treat its return as the executor's
// drain.
//
// If the callback panics, the in-flight job's single result is still
// delivered (OK false, the panic value as the Reason) before the panic
// propagates, so a synchronously waiting client is never stranded by a
// crashing daemon taking the socket down after the fact; the executor itself
// still fails fast.
//
// Policy stays in the app, per the package contract: what the run does, how
// outcomes map to health, and every log line live in the callback. A daemon
// whose executor needs different mechanics — running jobs outside the
// shutdown context (context.WithoutCancel), halting admission on an
// app-specific state, a different cancellation vocabulary — keeps its
// hand-written loop; Execute is opt-in.
func Execute[P any](ctx context.Context, q *Queue[P], run func(ctx context.Context, trigger string, payload P) Outcome) {
	for j := range q.Jobs() {
		if ctx.Err() != nil {
			j.Finish(Outcome{OK: false, Reason: CancelledReason})
			continue
		}
		executeOne(ctx, j, run)
	}
}

// executeOne runs a single job under the exactly-one-Finish guarantee: the
// normal path delivers the callback's outcome, and the panic path delivers a
// failure outcome before re-panicking (the runtime reports the original
// panic value with both traces).
func executeOne[P any](ctx context.Context, j *Job[P], run func(ctx context.Context, trigger string, payload P) Outcome) {
	j.Start()
	defer func() {
		if v := recover(); v != nil {
			j.Finish(Outcome{OK: false, Reason: fmt.Sprintf("panic: %v", v)})
			panic(v)
		}
	}()
	j.Finish(run(ctx, j.Trigger, j.Payload))
}
