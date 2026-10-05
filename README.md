# scheduler

[![Go Reference](https://pkg.go.dev/badge/github.com/cplieger/scheduler/v4.svg)](https://pkg.go.dev/github.com/cplieger/scheduler/v4) [![Go version](https://img.shields.io/github/go-mod/go-version/cplieger/scheduler)](https://github.com/cplieger/scheduler/blob/main/go.mod) [![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/scheduler/badges/mutation.json)](https://github.com/cplieger/scheduler/issues?q=label%3Agremlins-tracker)

scheduler lets your Go container run its job on an interval or an outside trigger, with no overlapping runs and a clean shutdown.

It replaces the interval parsing, ticker loop, lock-file handling and graceful child-process shutdown you would otherwise write in each job runner's `main`. It uses only the standard library at run time, needs Go 1.27.1 or later, works on Unix systems only because its locks use `flock(2)`, and is licensed under Apache-2.0.

## Why use it

scheduler is built for a long-running container with one job, such as a backup, sync or polling daemon.

- `ParseInterval` reads a setting such as `JOB_INTERVAL` as a cadence, external-trigger mode or run-once mode.
- `RunLoop` runs one job at a time on every tick, plus once at start with `FireOnStart`, and waits for it on shutdown.
- `Stamp` records the last run on a volume, so a recreated container keeps its schedule and runs at start only when due.
- `TryLock` lets a `docker exec` trigger skip its run during a timer run, and `Exclusive` queues it instead.
- `NewCommandRunner` stops a child process with SIGTERM, then SIGKILL after a default 5-second grace.
- With the `trigger` package, one daemon runs every job and each trigger waits for its own run's result.

Consider [robfig/cron](https://github.com/robfig/cron) if you need cron expressions. It parses the standard cron format, with an optional seconds field and per-schedule time zones.

## Install

```sh
go get github.com/cplieger/scheduler/v4@latest
```

## Usage

A typical `main` reads the interval setting, picks a mode, and runs each pass under the overlap lock:

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cplieger/scheduler/v4"
)

const lockPath = "/tmp/.myjob.lock"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sched := scheduler.ParseInterval(os.Getenv("JOB_INTERVAL"), 6*time.Hour,
		scheduler.WithName("JOB_INTERVAL"))

	switch sched.Mode {
	case scheduler.ModeBuiltin:
		// Fire once now, then every interval with ±10% jitter, draining on SIGTERM.
		scheduler.RunLoop(ctx, runPass, scheduler.LoopOptions{
			Interval:    sched.Interval,
			FireOnStart: true,
			Jitter:      0.10,
		})
	case scheduler.ModeExternal:
		// Idle: runs arrive from outside, for example a docker exec of a
		// one-shot subcommand, and the lock keeps them from overlapping.
		<-ctx.Done()
	case scheduler.ModeOnce:
		runPass(ctx) // run exactly once, then exit
	}
}

// run builds context-cancellable subprocesses that get SIGTERM on shutdown
// and SIGKILL only after the grace period.
var run = scheduler.NewCommandRunner(scheduler.DefaultGrace)

func runPass(ctx context.Context) {
	lock, ok, err := scheduler.TryLock(lockPath)
	if err != nil {
		return // could not acquire; mark unhealthy in a real app
	}
	if !ok {
		return // another run is in flight; the overlap guard skips this one
	}
	defer lock.Unlock()

	cmd := run(ctx, "rsync", "-a", "/src/", "remote:/dst/")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	_ = cmd.Run()
}
```

In `ModeExternal`, an external scheduler such as [Ofelia](https://github.com/mcuadros/ofelia) starts each run with `docker exec`. The package examples on [pkg.go.dev](https://pkg.go.dev/github.com/cplieger/scheduler/v4#pkg-examples) show `ParseInterval`, `TryLock`, `Exclusive` and `Stamp` on their own, and `go test` keeps them true. Pass `WithRedactedValue(true)` when the interval setting can hold an expanded secret, so `ParseInterval` warnings never echo its value. The trigger server never logs request payloads, because a forwarded environment can carry secrets.

- [Scheduling a job](docs/scheduling.md) covers every interval value, restart-aware startup runs and subprocess shutdown.
- [Overlap guard and run coalescing](docs/coordination.md) covers `TryLock`, `Exclusive` and `SlotFile`, for a run that starts from more than one process.
- [Trigger broker](docs/trigger.md) covers the `trigger` package, where each trigger gets its own run and result.

## API

- Interval parsing: `ParseInterval`, `Schedule`, `Mode` and the `With*` interval options.
- Run loop: `RunLoop`, `LoopOptions`, `Job` and `JitteredDelay`.
- Restart record: `Stamp`, `RunRecord` and `FailurePolicy`.
- Overlap guard: `TryLock`, `Lock` and `ReadHolder`.
- Run coalescing: `Exclusive` with `WithQueueCapacity` and `WithGate`, `Outcome`, and `SlotFile`.
- Subprocesses: `CommandRunner`, `NewCommandRunner` and `DefaultGrace`.
- Package `trigger`: `Queue`, `Job`, `Execute`, `Listen`, `Server`, `Submit`, `Event` and their error values.

The full reference is on pkg.go.dev for [scheduler](https://pkg.go.dev/github.com/cplieger/scheduler/v4) and [trigger](https://pkg.go.dev/github.com/cplieger/scheduler/v4/trigger).

## Unsupported by design

These are deliberate non-goals, which keep the library small.

| Feature | Rationale |
| --- | --- |
| Logging setup | Your `main` owns logging. The library logs through `slog.Default()` or a logger you pass, and never configures a handler. |
| Health signaling | Set health inside your job, with the companion [health](https://github.com/cplieger/health) library. |
| What a job does and its result type | `Job` is `func(ctx)`. Exit codes, health and log lines are your policy. |
| Cron expressions and calendar schedules | Run an external scheduler such as Ofelia or cron in `ModeExternal`. |
| Coordination across hosts | The `flock` guard works on one host. Leader election across nodes needs a lease store. |
| Concurrent runs in one process | `RunLoop` runs one job at a time by design. Run jobs concurrently yourself if you must. |
| Retry and backoff of a failed run | Retrying outbound calls belongs to [httpx](https://github.com/cplieger/httpx). A failed pass is retried on the next tick or trigger. |

## Documentation

- [Scheduling a job](docs/scheduling.md) covers every interval value, the run loop, restart-aware startup runs and subprocess shutdown.
- [Overlap guard and run coalescing](docs/coordination.md) covers `TryLock`, `Exclusive` and `SlotFile`, for a job that starts from more than one process.
- [Trigger broker](docs/trigger.md) covers the `trigger` package, for a daemon that runs every job itself.

## Contributing

Issues and pull requests are welcome. The [shared contributing rules](https://github.com/cplieger/.github/blob/main/CONTRIBUTING.md) apply.

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude](https://claude.com), [GPT](https://openai.com), and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

Apache-2.0. See [LICENSE](LICENSE).
