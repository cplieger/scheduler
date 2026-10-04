# Overlap guard and run coalescing

This page covers how scheduler stops two runs of one job from overlapping when they start from different processes, such as the daemon's timer and a `docker exec` trigger. It is for developers whose job can start from more than one entry point.

## The overlap guard

`TryLock(path)` takes a non-blocking `flock(2)` lock on a file, creating the file when it is absent, and `Unlock` releases it. The lock serializes runs within one process, such as a startup run racing a tick, and across processes, such as a `docker exec` trigger racing the loop. When another holder owns the lock, `TryLock` returns `ok == false` with no error.

Each acquisition writes the current time into the file. `ReadHolder(path)` reads it back, so a contender can report how long the current holder has run. That value is for logs only and never affects locking.

Put the lock file where untrusted local users cannot write, such as a container-private directory, never a shared host `/tmp`. The file is opened following symlinks, so a planted symlink would be overwritten.

With `TryLock` alone, a trigger that arrives mid-run is refused. `Exclusive` queues it as a rerun instead.

## Run coalescing across processes

`Exclusive` runs at most one cycle at a time per app instance, across every entry point. Entry points include the daemon's tick and a subcommand that an operator or an external scheduler runs with `docker exec`. A request that arrives during a run does not wait. It records a rerun request in a counter file and returns at once, and the active runner runs the queued request when its current run finishes.

The queue holds 1 request by default, set with `WithQueueCapacity`. Requests beyond it are discarded, because the queued rerun already starts after they arrived.

`Run` is queue mode, for callers whose request must be met by a run that starts after it arrives. `RunOrSkip` is skip mode, for timer ticks. When a run is in flight, it logs a warning and skips the tick, because the next tick brings fresh results.

```go
ex := scheduler.NewExclusive("/config", logger)

// Daemon: RunLoop ticks use skip mode. A busy lock means the job is already
// running, and the next tick provides freshness, so never queue a tick.
scheduler.RunLoop(ctx, func(ctx context.Context) {
	_, _ = ex.RunOrSkip(func() error { return runCycle(ctx) })
}, scheduler.LoopOptions{Interval: sched.Interval, FireOnStart: true})

// Poll subcommand, run by an operator or an external scheduler, uses queue
// mode: the request must be met by a run that starts after it arrived.
outcome, err := ex.Run(func() error { return runCycle(ctx) })
switch outcome {
case scheduler.OutcomeQueued, scheduler.OutcomeDiscarded:
	os.Exit(0) // the in-flight runner covers this request; nothing to wait for
default:
	if err != nil {
		os.Exit(1)
	}
}
```

`OutcomeQueued` and `OutcomeDiscarded` are success for the requesting process, so it can log and exit 0. The outcome stays meaningful beside an error:

- `OutcomeRan` with an error means the job ran and failed. Job errors are joined across reruns.
- `OutcomeNone` with an error means the lock or queue file failed, and nothing ran or queued.
- `OutcomeQueued` with an error means the request was recorded but the check after it failed. The queued request still stands.

The lock is a `flock(2)` on `cycle.lock` in the directory, so the kernel releases it when the holding process dies. A crashed run never blocks the scheduler, and a queue counter left by a crash is cleared at the next acquisition. `Pending` reports the queued request count, and `ReadHolder` on `ExclusiveLockName` reports when the current cycle started.

The directory must exist. Both files stay in it for good, because deleting a locked file would let a second opener lock a different file. The same symlink caution as `TryLock` applies to the directory.

## Deferral rules

Three rules are deliberate. A deferred request stays in the counter for the next run. Only a crash or an I/O failure that tears the counter write can drop it, and the next scheduled run then covers that demand.

- A failed run does not stop queued demand. Each queued request is owed a run, succeed or fail, up to the rerun cap below.
- `WithGate(func() bool)` puts your shutdown signal, usually a check of the shutdown context's `Err`, in front of every run start. A closed gate makes a first run return `OutcomeGated`, and queued demand behind it is deferred. A run in flight is never interrupted. The gate is checked before each new run starts, and requests still queue while it is closed.
- One holder runs at most 8 queued reruns per acquisition. Past that cap it stops and defers the rest, so a constant stream of triggers cannot hold one runner forever.

## Log lines

`NewExclusive` takes a `*slog.Logger`, and a nil logger falls back to `slog.Default()`. The message text is stable, so alert rules can match it, and a change to any message is a breaking change.

| Message | Level | When |
| --- | --- | --- |
| `cycle lock busy; queued rerun request` | INFO | `Run` queued a request behind a run in flight |
| `cycle lock busy; rerun already queued; discarding request` | INFO | `Run` found the queue full and discarded the request |
| `cycle lock busy; skipping tick` | WARN | `RunOrSkip` found a run in flight |
| `running queued cycle request` | INFO | A holder starts a queued rerun. The `attempt` attribute counts from 1 |
| `stale queued-run marker cleared at startup` | WARN | A fresh holder found a counter left by a crash |
| `rerun cap reached; deferring queued demand` | WARN | A holder ran 8 reruns and left the rest queued |
| `cycle gate closed; skipping run` | INFO | The gate was closed when a run was about to start |
| `cycle gate closed; deferring queued demand` | INFO | The gate closed with requests still queued |

## Custom coalescing state

The counter under the queue is exported as `SlotFile`. It stores one payload of bytes, shared across processes through one file, and changes it by read-modify-write transactions under a short exclusive `flock` on the file itself. Build on it when your coalescing state needs more than a count.

`SlotFile` owns only the transaction. It creates the file on first use, takes a blocking lock, skips the write when the bytes are unchanged and never deletes a live slot. What the bytes mean, how concurrent requests merge and when a request counts as served are your policy. Your parser must read torn or unreadable bytes as the zero value, because a crash mid-write can tear the slot.
