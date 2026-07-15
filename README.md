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
go get github.com/cplieger/scheduler@latest
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

	"github.com/cplieger/scheduler"
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
lock; `ReadHolder` reads how long it has held it (observability only). For a
trigger that arrives mid-run, `RerunFlag` coalesces any number of overlapping
triggers into exactly one queued rerun:

```go
flag := scheduler.NewRerunFlag("/tmp/.myjob.rerun")
for {
	flag.Clear()      // clear before the run so only triggers during it queue a rerun
	runOnce(ctx)
	if !flag.Pending() {
		break
	}
}
```

`RerunFlag` is a coalescing specialization of `Latch`, the bare single-bit
cross-process marker (present on disk means raised). Use a `Latch` directly for
one-off signals between processes that cannot signal each other, such as a
daemon raising a shutdown/drain latch on `SIGTERM` that a separate `docker exec`
child observes with `Raised()` and drains on.

### Run coalescing across processes

`Exclusive` packages the lock + queue pattern into Renovate's run-coalescing
model for a whole app: at most one cycle runs at a time per instance, across
every entry point (the resident daemon's tick, a `poll` subcommand exec'd by an
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
- `RerunFlag`, `NewRerunFlag(path)`, `.Set()`, `.Pending() bool`, `.Clear()`.
- `Latch`, `NewLatch(path)`, `.Raise() error`, `.Raised() bool`, `.Clear()` — the bare single-bit cross-process marker behind `RerunFlag`, used directly for one-off signals such as a shutdown/drain latch.
- `Exclusive`, `NewExclusive(dir, logger, opts...)`, `.Run(job) (Outcome, error)` (queue mode), `.RunOrSkip(job) (Outcome, error)` (skip mode), `.Pending() (int, error)` — cross-process run coalescing.
- `WithQueueCapacity(n)` — Exclusive option: how many rerun requests may queue (default 1).
- `Outcome` — `OutcomeRan`, `OutcomeRanQueued`, `OutcomeQueued`, `OutcomeDiscarded`, `OutcomeSkipped`, `OutcomeNone` (implements `fmt.Stringer`).
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
| Logging setup (slog handler, UTC time attr) | The composition root owns logging. The library logs interval warnings through `slog.Default()` (or `WithIntervalLogger`), a best-effort rerun-flag write-failure warning through `slog.Default()`, and `Exclusive`'s coalescing lines through its injected logger (nil falls back to `slog.Default()`); it never configures a handler. |
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
