package scheduler

import (
	"context"
	"math/rand/v2"
	"time"
)

// Job is one unit of scheduled work. It receives the loop's context, which is
// cancelled when RunLoop is asked to stop; a job that must run to completion
// past a shutdown signal should derive its own context (context.WithoutCancel)
// internally. A Job reports its outcome through its own closure (setting a
// health marker, logging); RunLoop does not inspect a return value.
type Job func(ctx context.Context)

// LoopOptions configures RunLoop. Interval must be positive; Jitter and
// FireOnStart are optional.
type LoopOptions struct {
	// Interval is the gap between ticks. It must be positive; RunLoop returns
	// immediately otherwise (the built-in mode ParseInterval selects always
	// carries a positive interval).
	Interval time.Duration
	// Jitter spreads each tick uniformly across ±(Jitter × Interval) so
	// restarts across many instances do not synchronize into a thundering herd on a
	// shared upstream. It is a fraction in [0, 1); 0 disables jitter. The
	// startup fire is never jittered.
	Jitter float64
	// FireOnStart runs the job immediately as the first iteration, before the
	// first interval elapses, so a freshly-deployed container does work at once
	// instead of waiting a full interval.
	FireOnStart bool
}

// RunLoop runs job on a schedule until ctx is cancelled. The job runs
// sequentially in the loop, so two invocations never overlap in-process; guard
// cross-process overlap (an external trigger racing the loop) with TryLock
// inside the job. RunLoop blocks until ctx is cancelled and the in-flight job
// (if any) returns, so a caller can treat its return as a completed drain.
//
// RunLoop covers the built-in scheduling mode only. A one-shot (ModeOnce) job
// is run directly by the caller; an idle (ModeExternal) container simply waits
// on ctx.Done. RunLoop returns immediately if Interval is not positive.
func RunLoop(ctx context.Context, job Job, opts LoopOptions) {
	if opts.Interval <= 0 {
		return
	}

	delay := JitteredDelay(opts.Interval, opts.Jitter)
	if opts.FireOnStart {
		delay = 0
	}
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			// A fired timer and a cancelled context can both be ready in the
			// same iteration (select picks a ready case at random), so re-check
			// the stop request before launching a run that would race the drain.
			if ctx.Err() != nil {
				return
			}
			job(ctx)
		}
		delay = JitteredDelay(opts.Interval, opts.Jitter)
	}
}

// JitteredDelay returns a delay drawn uniformly from
// [interval−fraction×interval, interval+fraction×interval). It is the pure core
// of RunLoop's jitter, exported so the ±band can be tested directly and reused.
// A non-positive fraction or interval returns interval unchanged.
func JitteredDelay(interval time.Duration, fraction float64) time.Duration {
	if fraction <= 0 || interval <= 0 {
		return interval
	}
	if fraction > 1 {
		fraction = 1
	}
	spread := time.Duration(fraction * float64(interval))
	span := max(2*spread, 1)
	// #nosec G404 -- scheduling jitter, not a security-sensitive value.
	return interval - spread + time.Duration(rand.Int64N(int64(span)))
}
