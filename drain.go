package scheduler

import (
	"context"
	"time"
)

// DefaultDrainPoll is a reasonable poll interval for WaitForDrain: runs take
// seconds to minutes, so a sub-second poll keeps the post-completion shutdown
// delay negligible without busy-waiting.
const DefaultDrainPoll = 500 * time.Millisecond

// WaitForDrain blocks until no run holds the lock at path (the in-flight run
// finished, or its process died — flock releases on exit), or until maxWait
// elapses or ctx is cancelled. It returns true if the run drained and false
// otherwise. A failure probing the lock (an I/O error on the lock path) is
// treated conservatively as "not drained" and also returns false. It is the
// external-mode shutdown drain: when runs are triggered
// out-of-band (a separate docker-exec process the daemon cannot wait() on), the
// daemon polls the shared lock instead so a redeploy does not tear down a run
// mid-flight. maxWait should be one run's own maximum lifetime; the container's
// stop grace period is the real outer bound. A non-positive poll uses
// DefaultDrainPoll.
func WaitForDrain(ctx context.Context, path string, poll, maxWait time.Duration) bool {
	if poll <= 0 {
		poll = DefaultDrainPoll
	}

	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		inFlight, err := InFlight(path)
		if err != nil {
			return false
		}
		if !inFlight {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}
