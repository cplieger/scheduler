# scheduler

[![Go Reference](https://pkg.go.dev/badge/github.com/cplieger/scheduler.svg)](https://pkg.go.dev/github.com/cplieger/scheduler)
[![Go version](https://img.shields.io/github/go-mod/go-version/cplieger/scheduler)](https://github.com/cplieger/scheduler/blob/main/go.mod)
[![Test coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/scheduler/badges/coverage.json)](https://github.com/cplieger/scheduler/actions/workflows/coverage.yml)
[![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/scheduler/badges/mutation.json)](https://github.com/cplieger/scheduler/issues?q=label%3Agremlins-tracker)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cplieger/scheduler/badge)](https://scorecard.dev/viewer/?uri=github.com/cplieger/scheduler)

> Scheduling scaffold for containerized job runners

A standalone Go library of small, composable primitives for a container that
runs a job on an interval or an external trigger: interval parsing with the
standard sentinels, a startup-plus-ticker run loop with jitter, an advisory
`flock` overlap guard, a graceful shutdown drain, and a SIGTERM-graceful
subprocess runner. Standard library only (test dependency:
`pgregory.net/rapid`). Unix-only (the overlap guard is `flock(2)`).

It is a toolbox, not a framework: each primitive is independent, and the
composition root wires the ones it needs. The library says nothing about what a
job does, how health is signaled, or how logging is configured — those stay in
the app (health is the companion library for the marker pattern).

## Install

```sh
go get github.com/cplieger/scheduler/v2@latest
```

## Usage

A typical composition root reads an interval variable, picks a mode, and drives
the job — guarding overlap and shutting down gracefully:

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cplieger/scheduler/v2"
)

const lockPath = "/tmp/.myjob.lock"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sched := scheduler.ParseInterval(os.Getenv("JOB_INTERVAL"), 6*time.Hour,
		scheduler.WithName("JOB_INTERVAL"))

	switch sched.Mode {
	case scheduler.ModeBuiltin:
		// Fire once now, then every interval (with ±10% jitter), draining on SIGTERM.
		scheduler.RunLoop(ctx, runPass, scheduler.LoopOptions{
			Interval:    sched.Interval,
			FireOnStart: true,
			Jitter:      0.10,
		})
	case scheduler.ModeExternal:
		// Idle: runs are triggered out-of-band (an Ofelia docker-exec of a
		// one-shot subcommand). On shutdown, wait out an in-flight external run.
		<-ctx.Done()
		scheduler.WaitForDrain(context.Background(), lockPath, scheduler.DefaultDrainPoll, 10*time.Minute)
	case scheduler.ModeOnce:
		runPass(ctx) // run exactly once, then exit
	}
}

// run builds context-cancellable subprocesses that get SIGTERM (not SIGKILL)
// on shutdown, with a grace period before the kill.
var run = scheduler.NewCommandRunner(scheduler.DefaultGrace)

func runPass(ctx context.Context) {
	lock, ok, err := scheduler.TryLock(lockPath)
	if err != nil {
		return // could not acquire; mark unhealthy in a real app
	}
	if !ok {
		return // another run already in flight — the overlap guard skips this one
	}
	defer lock.Unlock()

	cmd := run(ctx, "rsync", "-a", "/src/", "remote:/dst/")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	_ = cmd.Run()
}
```

### Interval parsing

`ParseInterval` applies the standard sentinel and fallback rules to a
`*_INTERVAL` environment value and returns a `Schedule` (cadence + `Mode`):

| Raw value | Result |
| --- | --- |
| `"30m"`, `"1h30m"` (positive Go duration) | `ModeBuiltin`, that cadence (clamped by `WithBounds`) |
| `""` (unset) | `ModeBuiltin`, the default cadence |
| `"off"`, `"disabled"` (case-insensitive) | `ModeExternal` |
| `"0"`, `"0s"` (zero) | `ModeExternal`, or `ModeOnce` with `WithZeroAsOnce()` |
| `"-1h"` (negative) | `ModeBuiltin` at default + a warning (a likely typo) |
| `"banana"` (unparseable) | `ModeBuiltin` at default + a warning |

Options: `WithZeroAsOnce()` (treat a zero duration as run-once), `WithBounds(low, high)`
(clamp a positive cadence), `WithName(env)` (name the variable in warnings),
`WithIntervalLogger(l)` (route warnings to a specific logger; defaults to `slog.Default()`),
`WithRedactedValue()` (keep the supplied raw value out of every warning — use when the
interval passes through secret-capable config expansion, where a config typo could place
an expanded secret in the field; plain env-var reads should keep the default echo,
it is useful diagnostics).

### Overlap guard and coalescing

`TryLock` / `Unlock` serialize runs across both the in-process loop and an
out-of-band `docker exec` trigger. `InFlight` probes whether a run holds the
lock; `ReadHolder` reads how long it has held it (observability only). A
trigger that arrives mid-run is not dropped: `Exclusive` (next section) queues
it as a coalesced rerun.

`Latch` is the bare single-bit cross-process marker (present on disk means
raised). Use it for one-off signals between processes that cannot signal each
other, such as a daemon raising a shutdown/drain latch on `SIGTERM` that a
separate `docker exec` child observes with `Raised()` and drains on — and wire
that latch into `Exclusive` through `WithGate` so queued reruns stop launching
once shutdown is signalled.

### Run coalescing across processes

`Exclusive` packages the lock + queue pattern into cross-process run coalescing
for a whole app: at most one cycle runs at a time per instance, across every
entry point (the resident daemon's tick, a `poll` subcommand exec'd by an
operator or an external scheduler). A request that arrives while a cycle runs
is queued — without a blocked process: the requester records a rerun request in
a counter file and exits immediately, and the active runner executes the queued
demand when the current run finishes. Requests beyond the queue capacity
(default 1, set with `WithQueueCapacity`) are discarded, because the queued
rerun already guarantees a run starts after they arrived.

The two entry points pair as queue mode for demand-driven callers and skip mode
for time-driven ticks:

```go
ex := scheduler.NewExclusive("/config", logger)

// Daemon: RunLoop ticks use skip mode — a busy lock means the job is already
// running, and the next tick provides freshness; never queue a tick.
scheduler.RunLoop(ctx, func(ctx context.Context) {
	_, _ = ex.RunOrSkip(func() error { return runCycle(ctx) })
}, scheduler.LoopOptions{Interval: sched.Interval, FireOnStart: true})

// Poll subcommand (exec'd by an operator or an external scheduler): queue
// mode — the request must be satisfied by a run that starts after it arrived.
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

The lock is a `flock(2)` (`cycle.lock` in the directory), so the kernel
releases it when the holding process dies — a crashed run never wedges the
scheduler, and a queue counter orphaned by a crash is cleared at the next
acquisition. `Pending` reports the queued-request count for observability, and
`ReadHolder` on `ExclusiveLockName` reports how long the current cycle has run.

Three policy edges are deliberate, and all deferral is demand-preserving (the
queue counter survives; the next run satisfies it):

- A failed run does not stop queued demand by default — each queued request is
  owed a run, succeed or fail. `WithStopOnError()` opts into the opposite:
  after a failed run the holder retires (warning `cycle failed; deferring
  queued demand`) instead of hammering a failing job.
- `WithGate(func() bool)` puts the composition root's shutdown signal (a
  context, a drain latch) in front of every run start: a gated initial run
  returns `OutcomeGated` (`cycle gate closed; skipping run`), and queued
  demand behind a closed gate defers (`cycle gate closed; deferring queued
  demand`) — an in-flight run is never interrupted, and a stop request is
  never followed by a fresh run.
- A holder executes at most 8 queued reruns per acquisition: past that cap it
  retires (warning `rerun cap reached; deferring queued demand`), so a
  relentless trigger source cannot pin one holder indefinitely. Each rerun's
  `running queued cycle request` line carries an `attempt` ordinal for log
  attribution.

The storage under the queue counter is exported as `SlotFile`: a single-slot
byte payload shared across processes through one file, mutated by atomic
read-modify-write transactions under a short exclusive `flock` on the file
itself. Build on it when your coalescing state needs a payload the counter
cannot carry — docker-renovate-scheduler records *which repos* a queued
trigger wants, so a full-fleet request queued behind a scoped run reruns
unscoped. The bytes' meaning, how concurrent demands merge, and when recorded
demand counts as served are deliberately the caller's parser and policy;
`SlotFile` owns only the transaction (create-on-first-use, blocking lock,
skip-if-unchanged write, never unlink a live slot).

## API

- `Mode` — `ModeBuiltin`, `ModeExternal`, `ModeOnce` (implements `fmt.Stringer`).
- `Schedule` — `{Interval, Mode}` returned by `ParseInterval`.
- `ParseInterval(raw string, def time.Duration, opts ...IntervalOption) Schedule`.
- `WithZeroAsOnce()`, `WithBounds(low, high)`, `WithName(name)`, `WithIntervalLogger(l)`, `WithRedactedValue()` — interval options.
- `Job` — `func(ctx context.Context)`, one unit of scheduled work.
- `LoopOptions` — `{Interval, Jitter, FireOnStart}`.
- `RunLoop(ctx, job, opts)` — sequential startup-plus-ticker loop; drains on cancellation.
- `JitteredDelay(interval, fraction) time.Duration` — the pure ±band jitter core.
- `Lock`, `TryLock(path) (*Lock, bool, error)`, `(*Lock).Unlock()`.
- `InFlight(path) (bool, error)`, `ReadHolder(path) (time.Time, bool)`.
- `Latch`, `NewLatch(path)`, `.Raise() error`, `.Raised() bool`, `.Clear()` — the bare single-bit cross-process marker, used for one-off signals such as a shutdown/drain latch.
- `Exclusive`, `NewExclusive(dir, logger, opts...)`, `.Run(job) (Outcome, error)` (queue mode), `.RunOrSkip(job) (Outcome, error)` (skip mode), `.Pending() (int, error)` — cross-process run coalescing.
- `WithQueueCapacity(n)`, `WithGate(func() bool)`, `WithStopOnError()` — Exclusive options: queue depth (default 1), a pre-run shutdown gate, and fail-fast rerun deferral.
- `SlotFile`, `NewSlotFile(path)`, `.Mutate(fn func(before []byte) []byte) ([]byte, error)` — the flock'd single-slot read-modify-write transaction behind Exclusive's counter, exported for app-defined coalescing payloads.
- `Outcome` — `OutcomeRan`, `OutcomeRanQueued`, `OutcomeQueued`, `OutcomeDiscarded`, `OutcomeSkipped`, `OutcomeGated`, `OutcomeNone` (implements `fmt.Stringer`).
- `ExclusiveLockName`, `ExclusiveQueueName` — the file names Exclusive maintains inside its directory.
- `WaitForDrain(ctx, path, poll, maxWait) bool`, `DefaultDrainPoll`.
- `CommandRunner`, `NewCommandRunner(grace) CommandRunner`, `DefaultGrace`.

## Unsupported by design

These are deliberate non-goals, not a TODO list. The library is one cohesive
concept — schedule a container job, guard its overlap, run and drain it — and
stays small on purpose. It complements the other cplieger libraries rather than
absorbing them.

| Feature | Rationale |
| --- | --- |
| Logging setup (slog handler, UTC time attr) | The composition root owns logging. The library logs interval warnings through `slog.Default()` (or `WithIntervalLogger`) and `Exclusive`'s coalescing lines through its injected logger (nil falls back to `slog.Default()`); it never configures a handler. |
| Health signaling | `Set(healthy)` is the app's call inside its job. Use the companion [`health`](https://github.com/cplieger/health) library for the marker; the two compose. |
| What a job does / its outcome type | `Job` is `func(ctx)`. Exit codes, health flips, and log lines are the app's policy, wired inside the closure. |
| Cron expressions / calendar schedules | This is interval + external-trigger scheduling. For `0 2 * * *` semantics, use an external scheduler (Ofelia, cron) in `ModeExternal`. |
| Distributed / multi-node coordination | The `flock` guard is single-host. Cross-node leader election is a different abstraction (a lease store), out of scope. |
| Concurrent in-process runs | `RunLoop` is sequential by design (two runs never overlap in-process); the `flock` guards the cross-process case. Run a job concurrently yourself if you must. |
| Retry / backoff of a failed run | Retrying outbound work belongs to [`httpx`](https://github.com/cplieger/httpx); a failed pass is reported by the job and retried on the next tick or trigger. |

## Disclaimer

This project is built with care and follows security best practices, but it is
intended for personal / self-hosted use. No guarantees of fitness for production
environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude Opus](https://www.anthropic.com/claude)
and [Kiro](https://kiro.dev). The human maintainer defines architecture,
supervises implementation, and makes all final decisions.

## License

GPL-3.0 — see [LICENSE](LICENSE).
