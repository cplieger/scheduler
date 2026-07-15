package scheduler

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// Names of the two files Exclusive maintains inside its directory. They are
// exported so a consumer can point observability tooling at them — for example
// ReadHolder(filepath.Join(dir, ExclusiveLockName)) to report how long the
// current cycle has been running.
const (
	// ExclusiveLockName is the flock(2) file serializing cycle runs.
	ExclusiveLockName = "cycle.lock"
	// ExclusiveQueueName is the counter file holding the number of queued
	// rerun requests.
	ExclusiveQueueName = "cycle.queued"
)

// Outcome reports what an Exclusive.Run or RunOrSkip call did with the
// request. It stays meaningful alongside a non-nil error: check the error
// first, then the outcome — OutcomeRan with an error means the job ran and
// failed, while OutcomeNone with an error means an infrastructure failure
// prevented the request from running or queueing at all.
type Outcome int

const (
	// OutcomeNone is the zero value: the request produced neither a run nor a
	// queue entry. It is returned only with an infrastructure error (the lock
	// or queue file could not be used).
	OutcomeNone Outcome = iota
	// OutcomeRan means this call acquired the cycle lock and ran the job.
	OutcomeRan
	// OutcomeRanQueued means this call ran the job and then executed at least
	// one queued rerun request on top of it.
	OutcomeRanQueued
	// OutcomeQueued means a cycle was already in flight, so this request was
	// queued; the active runner executes it when the current run finishes.
	OutcomeQueued
	// OutcomeDiscarded means a cycle was in flight and the rerun queue was
	// already full; the request was dropped because the queued rerun(s)
	// already guarantee a run starts after this request arrived.
	OutcomeDiscarded
	// OutcomeSkipped means a cycle was in flight and the caller chose skip
	// mode (RunOrSkip): the tick was dropped without queueing.
	OutcomeSkipped
)

// Compile-time assertion: Outcome implements fmt.Stringer.
var _ fmt.Stringer = OutcomeNone

// String returns the lowercase outcome name for logging.
func (o Outcome) String() string {
	switch o {
	case OutcomeNone:
		return "none"
	case OutcomeRan:
		return "ran"
	case OutcomeRanQueued:
		return "ran-queued"
	case OutcomeQueued:
		return "queued"
	case OutcomeDiscarded:
		return "discarded"
	case OutcomeSkipped:
		return "skipped"
	default:
		return "unknown"
	}
}

// ExclusiveOption configures NewExclusive.
type ExclusiveOption func(*Exclusive)

// WithQueueCapacity sets how many rerun requests may queue while a cycle is
// running (the default is 1, Renovate's coalescing model: one queued rerun
// satisfies all demand that arrived before it starts). Each queued request is
// consumed by exactly one rerun, so a capacity of n allows up to n back-to-back
// reruns to accumulate. A value below 1 is treated as 1.
func WithQueueCapacity(n int) ExclusiveOption {
	return func(e *Exclusive) {
		if n >= 1 {
			e.capacity = n
		}
	}
}

// Exclusive coordinates cycle runs across processes so that at most one runs
// at a time, with a small queue of pending rerun requests instead of blocked
// waiters. It is the cross-process analogue of RerunFlag's in-run coalescing,
// for callers that are themselves short-lived processes (a poll subcommand
// exec'd by an operator or an external scheduler racing the resident daemon).
//
// The mechanics: a runner holds a flock(2) on dir/cycle.lock for the whole
// job; a requester that finds the lock busy increments the counter in
// dir/cycle.queued and exits immediately (never blocking for the job's
// duration); the runner consumes the counter at job end, rerunning once per
// queued request until none remain. Because the lock is an flock, the kernel
// releases it when the holding process dies — there is no stale-lock state —
// and a queue counter orphaned by a crash is cleared at the next acquisition
// (the run about to start satisfies the demand it recorded).
//
// Both files live in dir and are created on first use; they are never
// deleted (clearing the queue writes a zero count — unlinking a locked file
// would let a concurrent opener land on a different inode and break mutual
// exclusion). Place dir where untrusted local users cannot write, per the
// same symlink-following caveat as TryLock and Latch.
type Exclusive struct {
	logger   *slog.Logger
	dir      string
	capacity int
}

// NewExclusive returns an Exclusive coordinating runs through lock and queue
// files inside dir (which must exist). A nil log falls back to slog.Default()
// at call time. Options: WithQueueCapacity.
func NewExclusive(dir string, log *slog.Logger, opts ...ExclusiveOption) *Exclusive {
	e := &Exclusive{dir: dir, logger: log, capacity: 1}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// log resolves the logger lazily so a nil-constructed Exclusive follows the
// process default at call time (matching the library's other slog fallbacks).
func (e *Exclusive) log() *slog.Logger {
	if e.logger != nil {
		return e.logger
	}
	return slog.Default()
}

func (e *Exclusive) lockPath() string  { return filepath.Join(e.dir, ExclusiveLockName) }
func (e *Exclusive) queuePath() string { return filepath.Join(e.dir, ExclusiveQueueName) }

// Run executes job under the cycle lock, queueing the request if a run is
// already in flight (queue mode — for demand-driven callers such as a poll
// subcommand, where the caller's request must be satisfied by a run that
// starts after it arrived).
//
//   - Lock free: run the job now, then execute any rerun requests queued
//     during it (OutcomeRan, or OutcomeRanQueued if reruns happened).
//   - Lock busy, queue below capacity: record a rerun request and return
//     immediately (OutcomeQueued) — the active runner executes it when the
//     current run finishes. The requester never blocks for the job's duration.
//   - Lock busy, queue full: drop the request (OutcomeDiscarded) — the queued
//     rerun(s) already guarantee a run starts after this request arrived.
//
// The returned error carries the job's own error(s) when it ran (joined
// across reruns), or the infrastructure error that prevented the request from
// being recorded (OutcomeNone). Queued and Discarded outcomes are success for
// the requesting process: log-and-exit-0 is the intended caller behavior.
func (e *Exclusive) Run(job func() error) (Outcome, error) {
	lock, ok, err := TryLock(e.lockPath())
	if err != nil {
		return OutcomeNone, err
	}
	if ok {
		return e.runHolding(lock, acquireFresh, job)
	}

	before, err := e.mutateQueue(func(n int) int {
		if n >= e.capacity {
			return n
		}
		return n + 1
	})
	if err != nil {
		return OutcomeNone, err
	}
	if before >= e.capacity {
		e.log().Info("cycle lock busy; rerun already queued; discarding request",
			"dir", e.dir, "pending", before, "capacity", e.capacity)
		return OutcomeDiscarded, nil
	}

	// Re-probe: the runner may have finished between the failed TryLock and
	// the enqueue, in which case nobody is left to consume the request until
	// the next tick. Taking the lock now turns this requester into the runner
	// serving its own demand; a busy lock means an active runner whose
	// end-of-job consume loop (or post-release re-check) picks the request up.
	relock, ok, err := TryLock(e.lockPath())
	if err != nil {
		return OutcomeQueued, err
	}
	if !ok {
		e.log().Info("cycle lock busy; queued rerun request",
			"dir", e.dir, "pending", before+1, "capacity", e.capacity)
		return OutcomeQueued, nil
	}
	return e.runHolding(relock, acquireReprobe, job)
}

// RunOrSkip executes job under the cycle lock, skipping when a run is already
// in flight (skip mode — for time-driven callers such as a RunLoop tick, where
// the next tick provides freshness and queueing would only pile on the process
// already doing the work):
//
//	scheduler.RunLoop(ctx, func(ctx context.Context) {
//		_, _ = ex.RunOrSkip(func() error { return runCycle(ctx) })
//	}, opts)
//
// A skipped tick logs a warning (the job is overrunning its interval) and
// returns (OutcomeSkipped, nil). When the lock is free it behaves exactly like
// Run's acquired path, including executing queued rerun requests at job end.
func (e *Exclusive) RunOrSkip(job func() error) (Outcome, error) {
	lock, ok, err := TryLock(e.lockPath())
	if err != nil {
		return OutcomeNone, err
	}
	if !ok {
		e.log().Warn("cycle lock busy; skipping tick", "dir", e.dir)
		return OutcomeSkipped, nil
	}
	return e.runHolding(lock, acquireFresh, job)
}

// Pending reports the number of currently queued rerun requests. It is
// observability-only: the value can change the moment it is read.
func (e *Exclusive) Pending() (int, error) {
	return e.mutateQueue(func(n int) int { return n })
}

// acquireKind tells runHolding how the cycle lock was obtained, which decides
// what the acquisition step does with the queue counter.
type acquireKind int

const (
	// acquireFresh: a normal acquisition (the lock was free on first probe).
	// Any pre-existing queued count is demand orphaned by a crash or by a
	// requester losing the post-release race; the run about to start satisfies
	// it, so it is cleared without extra runs, with a warning.
	acquireFresh acquireKind = iota
	// acquireReprobe: a requester enqueued its own request and then won the
	// lock on the re-probe. Its queue entry is consumed silently — the run
	// about to start IS that request.
	acquireReprobe
	// acquireHandoff: the runner released the lock, then noticed a request
	// that slipped in behind its final consume check and re-acquired. The
	// entry is consumed as a live queued rerun (logged as such).
	acquireHandoff
)

// runHolding drives the holder side: the acquisition step on the queue
// counter, the first job run, the consume loop (one rerun per queued request),
// release, and the post-release re-check that closes the enqueue-after-final-
// check race. lock must be held; runHolding releases it. Job errors and any
// queue infrastructure errors are joined into the returned error; the Outcome
// reflects what actually ran regardless.
func (e *Exclusive) runHolding(lock *Lock, kind acquireKind, job func() error) (Outcome, error) {
	outcome := OutcomeRan
	var errs []error
	for {
		reran, holdErr := e.holdCycle(kind, job)
		if reran || kind == acquireHandoff {
			outcome = OutcomeRanQueued
		}
		if holdErr != nil {
			errs = append(errs, holdErr)
		}
		lock.Unlock()

		// Post-release re-check: a request enqueued between the consume loop's
		// last empty read and the Unlock above would otherwise sit until the
		// next tick. If demand is pending and the lock is still free, take it
		// back and serve the request; a busy lock means another runner owns
		// the demand now.
		pending, pendErr := e.Pending()
		if pendErr != nil {
			errs = append(errs, pendErr)
			break
		}
		if pending == 0 {
			break
		}
		relock, ok, lockErr := TryLock(e.lockPath())
		if lockErr != nil {
			errs = append(errs, lockErr)
			break
		}
		if !ok {
			break
		}
		lock, kind = relock, acquireHandoff
	}
	return outcome, errors.Join(errs...)
}

// consumeAtAcquisition settles the queue counter for a fresh, re-probe, or
// handoff acquisition (see acquireKind) before the job runs.
func (e *Exclusive) consumeAtAcquisition(kind acquireKind) error {
	switch kind {
	case acquireFresh:
		before, err := e.mutateQueue(func(int) int { return 0 })
		if err != nil {
			return err
		}
		if before > 0 {
			e.log().Warn("stale queued-run marker cleared at startup",
				"dir", e.dir, "pending", before)
		}
	case acquireReprobe, acquireHandoff:
		if _, err := e.dequeueOne(); err != nil {
			return err
		}
		if kind == acquireHandoff {
			e.log().Info("running queued cycle request", "dir", e.dir)
		}
	}
	return nil
}

// holdCycle performs one held stretch: the acquisition step for kind, the job,
// and the consume loop. It does not release the lock. reran reports whether at
// least one queued rerun was executed within this stretch.
func (e *Exclusive) holdCycle(kind acquireKind, job func() error) (reran bool, err error) {
	var errs []error
	if acqErr := e.consumeAtAcquisition(kind); acqErr != nil {
		errs = append(errs, acqErr)
	}

	if jobErr := job(); jobErr != nil {
		errs = append(errs, jobErr)
	}

	for {
		had, dqErr := e.dequeueOne()
		if dqErr != nil {
			errs = append(errs, dqErr)
			break
		}
		if !had {
			break
		}
		e.log().Info("running queued cycle request", "dir", e.dir)
		reran = true
		if jobErr := job(); jobErr != nil {
			errs = append(errs, jobErr)
		}
	}
	return reran, errors.Join(errs...)
}

// dequeueOne consumes one queued request, reporting whether one was pending.
func (e *Exclusive) dequeueOne() (had bool, err error) {
	before, err := e.mutateQueue(func(n int) int {
		if n > 0 {
			return n - 1
		}
		return n
	})
	return before > 0, err
}

// mutateQueue applies fn to the queued-request counter under a short exclusive
// flock on the counter file itself and returns the count fn saw. The critical
// section is a read-modify-write of a few bytes — microseconds, never the
// job's duration — so a blocking lock keeps requesters effectively
// non-blocking while making the counter race-free across processes. Torn or
// garbage content (a crash mid-write) parses as zero, which self-heals: the
// worst case is dropped rerun demand that the next scheduled run satisfies.
func (e *Exclusive) mutateQueue(fn func(int) int) (before int, err error) {
	f, err := os.OpenFile(e.queuePath(), os.O_CREATE|os.O_RDWR, 0o644) // #nosec G304 -- caller-supplied trusted queue path
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	if lockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); lockErr != nil {
		return 0, lockErr
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()

	raw, err := io.ReadAll(io.LimitReader(f, 64))
	if err != nil {
		return 0, err
	}
	before = parseCount(string(raw))
	after := fn(before)
	if after == before {
		return before, nil
	}
	if truncErr := f.Truncate(0); truncErr != nil {
		return before, truncErr
	}
	if _, writeErr := f.WriteAt([]byte(strconv.Itoa(after)+"\n"), 0); writeErr != nil {
		return before, writeErr
	}
	return before, nil
}

// parseCount interprets the queue counter's content, treating anything
// unreadable (empty file, torn write, garbage) as zero.
func parseCount(raw string) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		return 0
	}
	return n
}
