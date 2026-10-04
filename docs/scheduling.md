# Scheduling a job

This page covers how scheduler reads an interval setting, runs the job loop, keeps a schedule across restarts and stops subprocesses. It is for developers wiring the library into a job runner's `main`.

## Interval values

`ParseInterval` reads a raw `*_INTERVAL` value and returns a `Schedule` with a cadence and a `Mode`. Leading and trailing spaces are ignored.

| Raw value | Result |
| --- | --- |
| `"30m"`, `"1h30m"` (positive Go duration) | `ModeBuiltin` at that cadence, clamped by `WithBounds` |
| `""` (unset) | `ModeBuiltin` at the default cadence |
| `"off"`, `"disabled"` (case-insensitive across ASCII letters only) | `ModeExternal` |
| `"0"`, `"0s"` (zero) | `ModeExternal`, or `ModeOnce` with `WithZeroAsOnce(true)` |
| `"-1h"` (negative) | `ModeBuiltin` at the default, with a warning for a likely typo |
| `"banana"` (unparseable) | `ModeBuiltin` at the default, with a warning |

The default must be positive, and `ParseInterval` panics otherwise. A `ModeBuiltin` result therefore always carries a positive interval that is safe to pass to `time.NewTicker`.

## Interval options

- `WithZeroAsOnce(true)` makes a zero duration select `ModeOnce`. `false` keeps the default, `ModeExternal`.
- `WithBounds(low, high)` clamps a positive cadence and logs a warning when it changes the value. A non-positive bound is ignored, so `WithBounds(time.Minute, 0)` sets only a floor. Bounds given in the wrong order are swapped.
- `WithName(env)` names the variable in warnings. It defaults to `interval`.
- `WithIntervalLogger(l)` sends warnings to a specific logger instead of `slog.Default()`.
- `WithRedactedValue(true)` keeps the raw value out of every warning. `false` keeps the default, which echoes it.

When a bool option is passed more than once, the last one wins. Pass `WithRedactedValue(true)` when the interval passes through config expansion that can hold secrets, where a typo could place an expanded secret in the field. A plain environment variable read should keep the default echo, because the value helps diagnose a typo.

## The run loop

`RunLoop` covers `ModeBuiltin`. For `ModeOnce`, call the job directly. For `ModeExternal`, wait on `ctx.Done()` while runs come from outside.

`RunLoop` runs one job at a time, so two runs never overlap in one process. It returns once the context is cancelled and the running job has returned, so its return means the drain is complete. It returns at once when `Interval` is not positive. A panic in a job is not recovered and ends the process. A `Job` returns nothing, so it reports its own result, for example by setting a health marker or writing a log line.

`LoopOptions` has four fields:

- `Interval` is the gap between ticks.
- `Jitter` spreads each tick across plus or minus that fraction of the interval, so instances that restart together do not all reach a shared upstream at once. `0.10` spreads ticks by 10 percent, and `0` turns jitter off.
- `FireOnStart` runs the job at once, before the first interval passes. The startup run is never jittered.
- `FirstDelay` replaces the first tick's delay. It is never jittered, and `FireOnStart` overrides it.

`JitteredDelay(interval, fraction)` is the jitter calculation on its own, for code and tests that need the same spread.

## Restart-aware startup runs

`FireOnStart: true` runs the job as soon as the container starts. A container that is recreated often, such as one that tracks a fast-moving upstream image, then repeats work the previous container finished minutes earlier. `Stamp` records when a scheduled run last completed, in a file on a persisted volume, so the startup run happens only when it is due:

```go
stamp := scheduler.NewStamp("/data/.myjob-last-run")

due := stamp.Due(sched.Interval, time.Now(), scheduler.RetryFailed)
scheduler.RunLoop(ctx, runPass, scheduler.LoopOptions{
	Interval:    sched.Interval,
	FireOnStart: due, // fire at boot only when no recent run survived the restart
	// Phase the first tick from the previous run, not from boot: a run 30
	// minutes old on a 1h interval means the next tick lands in 30 minutes.
	FirstDelay: stamp.Remaining(sched.Interval, time.Now(), scheduler.RetryFailed),
})
```

Your job records each completed scheduled pass and its result with `stamp.Record(ok)`. You decide which runs count, and a manually triggered, scoped run usually does not. `Record` has a single writer by contract and takes no lock. The `FailurePolicy` argument says what a failed last run means:

- `RetryFailed` means only a fresh successful run skips the startup run. A restart after a failure runs again at once, so an operator who fixes a bad setting and recreates the container gets immediate feedback.
- `CountFailed` means any completed run holds its slot, and the interval ticker retries. Use it for jobs whose failed passes are expensive enough that a restart must not repeat them early.

A missing, torn or unreadable record reads as due, so a damaged file costs one extra startup run and never a skipped schedule. Without a persisted volume, the file is lost on every recreate, and every recreated container runs the job at start.

`Remaining` is the time left in the current period. It is zero exactly when `Due` is true, and at most one interval. Passed as `LoopOptions.FirstDelay`, it schedules the next run one interval after the previous run instead of one interval after boot. Restarts then neither add runs nor delay the schedule.

`Due` and `Remaining` panic on a `FailurePolicy` other than these two. Keep the stamp file where untrusted local users cannot write, because it is created following symlinks.

## Subprocesses

`NewCommandRunner(grace)` returns a `CommandRunner` that builds context-cancellable commands. When the context is cancelled, the child gets SIGTERM instead of the SIGKILL that `os/exec` sends by default, and SIGKILL follows only after `grace`. A non-positive grace uses `DefaultGrace`, which is 5 seconds. Set `Stdout` and `Stderr` on the returned command before you run it.

Both signals reach the child only, never the processes it forked. If your child forks, as a package manager or a shell pipeline does, set `SysProcAttr.Setpgid` on the returned command and replace its `Cancel` with a signal to the process group.
