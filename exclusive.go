package scheduler

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
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
// failed, OutcomeNone with an error means an infrastructure failure prevented
// the request from running or queueing at all, and OutcomeQueued with an
// error means the request WAS recorded but the post-enqueue re-probe failed
// (the queued demand still stands; a current or next runner consumes it).
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
	// queued; the active runner executes it when the current run finishes (or,
	// if that runner retires at its rerun cap, the next run to acquire the
	// lock satisfies it).
	OutcomeQueued
	// OutcomeDiscarded means a cycle was in flight and the rerun queue was
	// already full; the request was dropped because the queued rerun(s)
	// already guarantee a run starts after this request arrived.
	OutcomeDiscarded
	// OutcomeSkipped means a cycle was in flight and the caller chose skip
	// mode (RunOrSkip): the tick was dropped without queueing.
	OutcomeSkipped
	// OutcomeGated means the run gate (WithGate) was closed when the runner
	// was about to start the job: nothing ran, and the queue counter was left
	// untouched (a request this call had already queued on the re-probe path
	// stays queued; the next run satisfies it).
	OutcomeGated
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
	case OutcomeGated:
		return "gated"
	default:
		return "unknown"
	}
}

// maxCoalescedReruns caps how many queued rerun requests one holder executes
// per acquisition (across its consume loops and post-release handoffs) before
// retiring, so a relentless trigger source cannot pin a single holder
// re-acquiring and rerunning indefinitely. Demand still pending at the cap
// stays recorded in the counter file and is satisfied by the next run to
// acquire the lock. The value mirrors docker-renovate-scheduler's
// maxCoalescedReruns, the consumer policy this packaging is derived from.
const maxCoalescedReruns = 8

// ExclusiveOption configures NewExclusive.
type ExclusiveOption func(*Exclusive)

// WithQueueCapacity sets how many rerun requests may queue while a cycle is
// running (the default is 1, the single-slot coalescing model: one queued
// rerun satisfies all demand that arrived before it starts). Each queued
// request is consumed by exactly one rerun, so a capacity of n allows up to n
// back-to-back reruns to accumulate. A value below 1 is treated as 1.
func WithQueueCapacity(n int) ExclusiveOption {
	return func(e *Exclusive) {
		if n >= 1 {
			e.capacity = n
		}
	}
}

// WithGate installs a pre-run gate consulted every time the runner is about
// to START the job: on acquisition, before each queued rerun, and before each
// post-release handoff. A false return stops runs from starting — an initial
// run is skipped (OutcomeGated) and queued demand is deferred to the next run
// — while the in-flight run is never interrupted. Wire it to the composition
// root's shutdown signal (a context's Err, a cross-process drain latch) so a
// stop request is never followed by a fresh run. Requests are still recorded
// while the gate is closed: the gate decides what runs, not what queues.
func WithGate(gate func() bool) ExclusiveOption {
	return func(e *Exclusive) { e.gate = gate }
}

// WithStopOnError makes a failed job run stop the consume loop and any
// further handoffs for that call: remaining queued demand is deferred to the
// next run instead of being executed against a job that just failed ("don't
// hammer a failing job"). The default keeps queue mode's demand-satisfaction
// contract: every queued request is owed a run, succeed or fail.
func WithStopOnError() ExclusiveOption {
	return func(e *Exclusive) { e.stopOnError = true }
}

// Exclusive coordinates cycle runs across processes so that at most one runs
// at a time, with a small queue of pending rerun requests instead of blocked
// waiters — packaged cross-process run coalescing for callers that are
// themselves short-lived processes (a poll subcommand exec'd by an operator or
// an external scheduler racing the resident daemon).
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
// A holder executes at most maxCoalescedReruns (8) queued reruns per
// acquisition; demand still pending past that cap is deferred — it stays in
// the counter file and the next acquisition's run satisfies it — so a
// relentless trigger source cannot pin one holder indefinitely. Two options
// bound the holder further: WithGate stops runs from starting once the
// composition root's shutdown signal trips, and WithStopOnError stops rerun
// consumption after a failed run. Deferral is always demand-preserving.
//
// Both files live in dir and are created on first use; they are never
// deleted (clearing the queue writes a zero count — unlinking a locked file
// would let a concurrent opener land on a different inode and break mutual
// exclusion). Place dir where untrusted local users cannot write, per the
// same symlink-following caveat as TryLock and Latch.
type Exclusive struct {
	logger      *slog.Logger
	gate        func() bool
	dir         string
	capacity    int
	stopOnError bool
}

// NewExclusive returns an Exclusive coordinating runs through lock and queue
// files inside dir (which must exist). A nil log falls back to slog.Default()
// at call time. Options: WithQueueCapacity, WithGate, WithStopOnError.
func NewExclusive(dir string, log *slog.Logger, opts ...ExclusiveOption) *Exclusive {
	e := &Exclusive{dir: dir, logger: log, capacity: 1}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// gateOpen reports whether a run may start; a nil gate is always open.
func (e *Exclusive) gateOpen() bool {
	return e.gate == nil || e.gate()
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
// being recorded (OutcomeNone). OutcomeQueued may also accompany an error:
// the request was recorded but the post-enqueue re-probe failed — the demand
// stands, and a current or next runner consumes it. Queued and Discarded
// outcomes are success for the requesting process: log-and-exit-0 is the
// intended caller behavior.
//
// A failed run does not stop queued demand by default: each queued request is
// owed a run that starts after it arrived, succeed or fail, so the consume
// loop continues through job errors (bounded by the rerun cap). WithStopOnError
// opts into the opposite policy — a failed run defers remaining demand to the
// next run. WithGate stops runs from STARTING once the composition root's
// shutdown (or other) signal trips: a gated initial run returns OutcomeGated,
// and demand queued behind a closed gate waits for the next run.
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
//
// A shared rerun budget (maxCoalescedReruns) spans the whole call: every run
// executed for queued demand — a consume-loop rerun or a post-release handoff
// — draws it down, and an exhausted budget retires the holder instead of
// re-acquiring, deferring whatever demand remains to the next run. The gate
// (WithGate) is consulted before the initial run and again before every
// rerun/handoff; stop-on-error (WithStopOnError) retires the holder after a
// failed run.
func (e *Exclusive) runHolding(lock *Lock, kind acquireKind, job func() error) (Outcome, error) {
	if !e.gateOpen() {
		lock.Unlock()
		e.log().Info("cycle gate closed; skipping run", "dir", e.dir)
		return OutcomeGated, nil
	}
	outcome := OutcomeRan
	var errs []error
	st := &runState{budget: maxCoalescedReruns}
	for {
		reran, holdErr := e.holdCycle(kind, job, st)
		if reran || kind == acquireHandoff {
			outcome = OutcomeRanQueued
		}
		if holdErr != nil {
			errs = append(errs, holdErr)
		}
		lock.Unlock()

		relock, again, reErr := e.reacquireForPending(st)
		if reErr != nil {
			errs = append(errs, reErr)
		}
		if !again {
			break
		}
		lock, kind = relock, acquireHandoff
	}
	return outcome, errors.Join(errs...)
}

// runState carries one Run/RunOrSkip call's holder-side accounting: the
// remaining rerun budget and whether stop-on-error tripped.
type runState struct {
	budget  int
	stopped bool
}

// attempt is the 1-based ordinal of the queued rerun that was just charged
// against the budget, for log attribution.
func (st *runState) attempt() int { return maxCoalescedReruns - st.budget }

// reacquireForPending is runHolding's post-release re-check: a request
// enqueued between the consume loop's last empty read and the Unlock would
// otherwise sit until the next tick, so if demand is pending and the lock is
// still free, the holder takes it back for a handoff stretch. A busy lock
// means another runner owns the demand now. Three conditions retire the
// holder instead, deferring the demand — it stays in the counter file and the
// next run to take the lock satisfies it — each logged exactly once, here:
// stop-on-error tripped, the gate closed, or the rerun budget ran out.
func (e *Exclusive) reacquireForPending(st *runState) (relock *Lock, again bool, err error) {
	pending, err := e.Pending()
	if err != nil || pending == 0 {
		return nil, false, err
	}
	switch {
	case st.stopped:
		e.log().Warn("cycle failed; deferring queued demand",
			"dir", e.dir, "pending", pending)
	case !e.gateOpen():
		e.log().Info("cycle gate closed; deferring queued demand",
			"dir", e.dir, "pending", pending)
	case st.budget <= 0:
		e.log().Warn("rerun cap reached; deferring queued demand",
			"dir", e.dir, "pending", pending, "cap", maxCoalescedReruns)
	default:
		l, ok, lockErr := TryLock(e.lockPath())
		if lockErr != nil {
			return nil, false, lockErr
		}
		if !ok {
			return nil, false, nil
		}
		return l, true, nil
	}
	return nil, false, nil
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
	}
	return nil
}

// holdCycle performs one held stretch: the acquisition step for kind, the job,
// and the consume loop. It does not release the lock. reran reports whether at
// least one queued rerun was executed within this stretch. st carries the
// holder-wide accounting: a handoff stretch's initial run and every
// consume-loop rerun draw the budget down one each, a failing job under
// stop-on-error trips st.stopped, and the consume loop stops without dequeuing
// once the budget is spent, the gate closes, or the stop tripped (the demand
// stays recorded for the next run; reacquireForPending logs the deferral).
func (e *Exclusive) holdCycle(kind acquireKind, job func() error, st *runState) (reran bool, err error) {
	var errs []error
	if acqErr := e.consumeAtAcquisition(kind); acqErr != nil {
		errs = append(errs, acqErr)
	}
	if kind == acquireHandoff {
		st.budget--
		e.log().Info("running queued cycle request", "dir", e.dir, "attempt", st.attempt())
	}

	errs = e.runJobOnce(job, st, errs)

	for st.budget > 0 && !st.stopped && e.gateOpen() {
		had, dqErr := e.dequeueOne()
		if dqErr != nil {
			errs = append(errs, dqErr)
			break
		}
		if !had {
			break
		}
		reran = true
		st.budget--
		e.log().Info("running queued cycle request", "dir", e.dir, "attempt", st.attempt())
		errs = e.runJobOnce(job, st, errs)
	}
	return reran, errors.Join(errs...)
}

// runJobOnce executes the job once, appending its error to errs and tripping
// the stop-on-error state when that policy is configured.
func (e *Exclusive) runJobOnce(job func() error, st *runState, errs []error) []error {
	if jobErr := job(); jobErr != nil {
		errs = append(errs, jobErr)
		if e.stopOnError {
			st.stopped = true
		}
	}
	return errs
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

// mutateQueue applies fn to the queued-request counter and returns the count
// fn saw. The counter rides SlotFile's flock'd read-modify-write transaction,
// so a blocking lock keeps requesters effectively non-blocking (the critical
// section is one short line) while making the counter race-free across
// processes. Torn or garbage content (a crash mid-write) parses as zero,
// which self-heals: the worst case is dropped rerun demand that the next
// scheduled run satisfies.
func (e *Exclusive) mutateQueue(fn func(int) int) (before int, err error) {
	raw, err := NewSlotFile(e.queuePath()).Mutate(func(cur []byte) []byte {
		n := parseCount(string(cur))
		after := fn(n)
		if after == n {
			return cur
		}
		return []byte(strconv.Itoa(after) + "\n")
	})
	return parseCount(string(raw)), err
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
