# Contributing to scheduler

Notes on the primitives, the design contract, and the local workflow. Most of
the guidance is about keeping the library a small, composable toolbox rather
than letting it grow into a framework.

## The toolbox, not a framework

`scheduler` is a standard-library-only Go package (one test-only dependency,
`pgregory.net/rapid`) of independent primitives a containerized job runner
composes. Nothing here owns the run loop's outer control flow, the health
signal, or the logger — the composition root does. Each primitive is usable on
its own:

- **`ParseInterval`** — a `*_INTERVAL` value → `Schedule{Interval, Mode}`,
  applying the shared off/disabled/0 sentinel and fallback rules.
- **`RunLoop` / `JitteredDelay`** — the built-in mode's startup-fire + jittered
  ticker; sequential, so runs never overlap in-process. `JitteredDelay` is the
  pure ±band core, split out so it can be property-tested directly.
- **`TryLock` / `Unlock` / `ReadHolder`** — the `flock(2)` overlap guard and
  its holder-age probe.
- **`Exclusive`** — cross-process run coalescing over the lock + a queue
  counter: `Run` (queue mode, demand-driven callers) and `RunOrSkip` (skip
  mode, loop ticks), with a bounded rerun queue (`WithQueueCapacity`).
- **`SlotFile`** — the flock'd single-slot read-modify-write transaction
  under `Exclusive`'s counter, exported for app-defined coalescing payloads.
- **`NewCommandRunner`** — context-cancellable subprocesses that get SIGTERM
  (not SIGKILL) plus a grace period.
- **The `trigger` subpackage** — the single-owner broker for daemons where
  PID 1 owns every run: bounded FIFO `Queue`, owner-only unix-socket
  `Server`, synchronous `Submit` client, and the opt-in `Execute` loop that
  owns the `Start`/`Finish` lifecycle.

The three scheduling modes a consumer selects on `Schedule.Mode`:

- `ModeBuiltin` → `RunLoop` (a positive interval).
- `ModeExternal` → idle on `ctx.Done`, runs triggered out-of-band (guarded by
  `TryLock`, or owned outright via the `trigger` subpackage).
- `ModeOnce` → call the job directly, then exit.

## Unsupported by design — a binding contract

The "[Unsupported by design](README.md#unsupported-by-design)" table in
`README.md` lists deliberate non-features. A PR that adds one — a cron-expression
parser, a health registry, a built-in logger, cross-node leader election,
concurrent in-process runs, run-level retry/backoff — will be declined
regardless of quality, because each belongs either in the composition root or in
a different library (`health`, `httpx`). If you think a non-goal should change,
open an issue first.

The other half of the contract is scope discipline in the other direction:
resist folding a consumer's app-specific policy into a primitive. `ParseInterval`
gained `WithBounds` and `WithZeroAsOnce` because two consumers genuinely needed
them; it will not grow a knob per consumer. A primitive earns a new option only
when a real app benefits, never for symmetry or completeness.

## Public API

The whole surface is small; keep it that way.

- `Mode` (`ModeBuiltin`/`ModeExternal`/`ModeOnce`), `Schedule`, `ParseInterval`,
  and the interval options `WithZeroAsOnce` / `WithBounds` / `WithName` /
  `WithIntervalLogger` / `WithRedactedValue`.
- `Job`, `LoopOptions`, `RunLoop`, `JitteredDelay`.
- `Lock`, `TryLock`, `(*Lock).Unlock`, `ReadHolder`.
- `Exclusive`, `NewExclusive`, `.Run` / `.RunOrSkip` / `.Pending`, the
  `WithQueueCapacity` / `WithGate` / `WithStopOnError` options, `Outcome`,
  and the `ExclusiveLockName` / `ExclusiveQueueName` file-name constants. Its
  coalescing log messages are a pinned contract — tests assert the exact
  text, and consumers alert on them in Loki; changing one is a breaking
  change.
- `SlotFile`, `NewSlotFile`, `.Mutate`.
- `CommandRunner`, `NewCommandRunner`, `DefaultGrace`.
- Subpackage `trigger`: `Queue[P]` / `NewQueue` / `.Submit` / `.Jobs` /
  `.Close` (`ErrFull`, `ErrClosed`), `Job[P]` / `NewJob` / `.Start` /
  `.Started` / `.Finish` / `.Result`, `Outcome`, `TriggerExternal`,
  `Execute` / `CancelledReason`, `Listen`, `Server[P]` / `.Serve` / `.Wait`,
  `Event` (`EventQueued` / `EventStarted` / `EventDone`), `Submit`
  (`ErrUnreachable`, `ErrSend`, `ErrConnectionLost`), `DialTimeout`. The
  rejection errors and `CancelledReason` travel the wire verbatim, so their
  text is part of the trigger contract.

`ParseInterval` logs its fallback warnings through `slog.Default()` unless
`WithIntervalLogger` is passed; there is no package-level logger and no
import-time side effect. The `flock` primitives are Unix-only.

## Local development

The module targets the Go version pinned in `go.mod` (use that toolchain or
newer; `GOTOOLCHAIN=auto` fetches it).

```sh
go build ./...
go test ./...
go test -race ./...
```

Run with `-race` before pushing: the `RunLoop` and drain tests exercise
goroutines and context cancellation, and the lock tests contend across file
descriptors.

### Linting and formatting

Lint config is `.golangci.yaml` (golangci-lint v2), synced from `cplieger/ci`.
`golangci-lint run` reports unformatted files as issues, so format before
pushing. `sloglint` is kv-only — log with key/value pairs.

```sh
golangci-lint run
golangci-lint fmt
```

### Fuzzing

`FuzzParseInterval` (in `interval_fuzz_test.go`) is the untrusted-input boundary
— it asserts the parser never panics, always returns a defined `Mode`, and never
yields a built-in schedule with a non-positive interval (which `time.NewTicker`
panics on — a consumer may pass a built-in interval straight to it, so
`ParseInterval` is the sole gate; `RunLoop` itself also guards defensively). Run
it with a time budget, and add a seed for any new parsing edge:

```sh
go test -run='^$' -fuzz=FuzzParseInterval -fuzztime=30s .
```

### Mutation testing

`.gremlins.yaml` configures [Gremlins](https://gremlins.dev) (synced from
`cplieger/ci`; change it upstream). Run it locally to check new tests kill
mutants:

```sh
gremlins unleash .
```

## Test layout

Tests live beside the code, split by intent — match the right file when adding
cases:

- `interval_test.go` — `ParseInterval` table (modes, bounds, warnings) and
  `Mode.String`; `interval_fuzz_test.go` — `FuzzParseInterval`.
- `loop_test.go` — `RunLoop` fire-on-start / repeated ticks / drain / guards,
  and the `rapid` property that `JitteredDelay` stays within its ±band.
- `lock_test.go` — mutual exclusion and `ReadHolder`.
- `exclusive_test.go` — `Exclusive` queue/skip modes: acquire, queue, discard,
  capacity bounds, consume loop, stale-marker clear, error joining, the
  pinned log contract, a no-overlap hammer, and the crash-release proof (a
  re-exec'd child holding the lock is SIGKILLed and the flock must die with
  it).
- `command_test.go` — runner construction, default grace, and the SIGTERM-on-cancel
  proof (a child that traps TERM and exits 42).
- `trigger/*_test.go` — the broker: queue admission and FIFO order, the
  `Execute` lifecycle guarantees (exactly one result, cancel-before-start,
  panic delivery), socket hygiene, wire protocol, and client error mapping.
- `example_test.go` — runnable `Example` functions that double as docs; keep
  their `// Output:` blocks correct. `bench_test.go` — allocation/throughput
  benchmarks. `helpers_test.go` — shared test helpers.

## Commits and PRs

Branch from `main`, keep changes focused with tests, and open a PR. This account
uses [Conventional Commits](https://www.conventionalcommits.org/) parsed by
git-cliff (`cliff.toml`) to build release notes, so the commit type drives the
version bump: `feat:`, `fix:`, `sec:`, and `chore:`/`docs:`/`refactor:`/`test:`
(no release). Write the subject as the changelog line a consumer would read.

## Conduct & security

By participating you agree to the org-wide
[Code of Conduct](https://github.com/cplieger/.github/blob/main/CODE_OF_CONDUCT.md).
Report security issues through the
[security policy](https://github.com/cplieger/.github/blob/main/SECURITY.md) —
never in a public issue.
