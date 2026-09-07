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
// from q strictly in order and runs each through run, owning the Start/Finish
// lifecycle so the exactly-one-result contract is structural rather than
// conventional. Mutual exclusion is this loop, the queue's single receiver.
// ctx governs ADMISSION, not the in-flight run: a job received after ctx is
// done finishes with CancelledReason without ever starting, while a run already
// in flight sees cancellation only through the ctx passed to run. Execute blocks
// until Close drains the queue, so its return IS the executor's drain.
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
