// Package scheduler is the scheduling scaffold shared by several
// containerized job runners (docker-fclones-scheduler, docker-rsync-scheduler,
// docker-renovate-scheduler, pg-autodump, github-scout, seadex-scout).
//
// It provides small, orthogonal primitives — not a framework — that a
// composition root wires together:
//
//   - ParseInterval turns a *_INTERVAL environment value into a Schedule (a
//     cadence plus a Mode: built-in, external, or once), applying the
//     standard off/disabled/0 sentinel and fallback rules.
//   - RunLoop drives the built-in mode: a startup fire plus a jittered interval
//     ticker that drains on context cancellation. JitteredDelay is its pure,
//     testable core.
//   - TryLock / Unlock / InFlight / ReadHolder are an advisory flock(2) overlap
//     guard so a run and an out-of-band trigger never execute two jobs at once;
//     Latch is the bare single-bit cross-process marker for one-off signals
//     such as a shutdown/drain latch (a trigger arriving mid-run is queued by
//     Exclusive, below).
//   - Exclusive composes the pieces above into packaged cross-process run
//     coalescing for whole cycles: at most one cycle runs at a time across
//     processes, a requester that finds a run in flight queues a rerun request
//     (bounded, no blocked waiters) or skips its tick, and the runner executes
//     the queued demand when the current run finishes — continuing through job
//     errors by default (a queued request is owed a run, succeed or fail;
//     WithStopOnError opts out) and retiring at a rerun cap that defers a
//     storm's residue to the next run. WithGate wires the composition root's
//     shutdown signal (a context, a drain Latch) in front of every run start,
//     so a stop request is never followed by a fresh run.
//   - SlotFile is the storage mechanism under Exclusive's counter — a
//     single-slot byte payload mutated by flock'd read-modify-write
//     transactions — exported for apps whose coalescing state carries a
//     payload (the payload's meaning and merge/claim policy stay app-side).
//   - WaitForDrain polls the lock so a daemon can wait out an externally
//     triggered run before exiting on shutdown.
//   - NewCommandRunner builds context-cancellable subprocesses that shut down
//     gracefully (SIGTERM with a grace period before SIGKILL).
//
// The package is deliberately silent about what a job does, how health is
// signaled, and how logging is configured; those belong to the consuming app
// (see the companion health library for the marker pattern). It carries no
// runtime dependencies beyond the standard library, and its flock-based
// primitives are Unix-only.
package scheduler
