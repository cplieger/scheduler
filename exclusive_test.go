package scheduler

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// captureLogger returns a logger writing slog text lines into the returned
// buffer, for asserting the pinned Exclusive log contract.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// assertLogged fails the test unless the buffer contains a log record with
// exactly the pinned message text (slog's text handler quotes multi-word
// messages, so the assertion pins the full msg attribute).
func assertLogged(t *testing.T, buf *bytes.Buffer, msg string) {
	t.Helper()
	want := "msg=" + quoteMsg(msg)
	if !strings.Contains(buf.String(), want) {
		t.Errorf("log = %q, want it to contain %s", buf.String(), want)
	}
}

// assertNotLogged fails the test if the buffer contains the pinned message.
func assertNotLogged(t *testing.T, buf *bytes.Buffer, msg string) {
	t.Helper()
	if strings.Contains(buf.String(), "msg="+quoteMsg(msg)) {
		t.Errorf("log = %q, must not contain message %q", buf.String(), msg)
	}
}

// quoteMsg quotes a message the way slog's text handler renders multi-word
// values, so assertions pin the exact contract text.
func quoteMsg(msg string) string {
	return fmt.Sprintf("%q", msg)
}

func TestExclusiveRunFree(t *testing.T) {
	t.Parallel()
	logger, buf := captureLogger()
	e := NewExclusive(t.TempDir(), logger)

	runs := 0
	out, err := e.Run(func() error {
		runs++
		return nil
	})
	if err != nil {
		t.Fatalf("Run err = %v, want nil", err)
	}
	if out != OutcomeRan {
		t.Errorf("Run outcome = %s, want ran", out)
	}
	if runs != 1 {
		t.Errorf("job ran %d times, want 1", runs)
	}
	if buf.Len() != 0 {
		t.Errorf("uncontended Run logged %q, want no output", buf.String())
	}
}

func TestExclusiveRunJobErrorPassthrough(t *testing.T) {
	t.Parallel()
	jobErr := errors.New("cycle failed")
	e := NewExclusive(t.TempDir(), silentLogger())

	out, err := e.Run(func() error { return jobErr })
	if !errors.Is(err, jobErr) {
		t.Errorf("Run err = %v, want the job's own error", err)
	}
	if out != OutcomeRan {
		t.Errorf("Run outcome = %s, want ran (the job did run, and failed)", out)
	}
}

func TestExclusiveRunQueuesWhileBusy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	e := NewExclusive(dir, logger)

	holder, ok, err := TryLock(filepath.Join(dir, ExclusiveLockName))
	if err != nil || !ok {
		t.Fatalf("seed TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}

	runs := 0
	out, err := e.Run(func() error { runs++; return nil })
	if err != nil {
		t.Fatalf("busy Run err = %v, want nil", err)
	}
	if out != OutcomeQueued {
		t.Errorf("busy Run outcome = %s, want queued", out)
	}
	if runs != 0 {
		t.Errorf("job ran %d times while queued, want 0", runs)
	}
	assertLogged(t, buf, "cycle lock busy; queued rerun request")

	if pending, perr := e.Pending(); perr != nil || pending != 1 {
		t.Errorf("Pending = (%d, %v), want (1, nil)", pending, perr)
	}
	holder.Unlock()

	// The holder above was a bare lock, not an Exclusive runner, so the queued
	// request is orphaned demand: the next acquisition clears it (the run
	// about to start satisfies it) with the stale-marker warning, and runs
	// exactly once.
	buf.Reset()
	out, err = e.Run(func() error { runs++; return nil })
	if err != nil {
		t.Fatalf("post-release Run err = %v, want nil", err)
	}
	if out != OutcomeRan {
		t.Errorf("post-release Run outcome = %s, want ran (clear, not rerun)", out)
	}
	if runs != 1 {
		t.Errorf("job ran %d times, want exactly 1 (cleared marker must not rerun)", runs)
	}
	assertLogged(t, buf, "stale queued-run marker cleared at startup")
	if pending, perr := e.Pending(); perr != nil || pending != 0 {
		t.Errorf("Pending after clear = (%d, %v), want (0, nil)", pending, perr)
	}
}

func TestExclusiveDiscardAtCapacity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	e := NewExclusive(dir, logger)

	holder, ok, err := TryLock(filepath.Join(dir, ExclusiveLockName))
	if err != nil || !ok {
		t.Fatalf("seed TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer holder.Unlock()

	if out, rerr := e.Run(failIfRun(t)); rerr != nil || out != OutcomeQueued {
		t.Fatalf("first busy Run = (%s, %v), want (queued, nil)", out, rerr)
	}
	out, rerr := e.Run(failIfRun(t))
	if rerr != nil || out != OutcomeDiscarded {
		t.Fatalf("second busy Run = (%s, %v), want (discarded, nil)", out, rerr)
	}
	assertLogged(t, buf, "cycle lock busy; rerun already queued; discarding request")

	if pending, perr := e.Pending(); perr != nil || pending != 1 {
		t.Errorf("Pending = (%d, %v), want (1, nil) (discard must not grow the queue)", pending, perr)
	}
}

// failIfRun returns a job that fails the test if executed.
func failIfRun(t *testing.T) func() error {
	t.Helper()
	return func() error {
		t.Error("job ran, want it not to run")
		return nil
	}
}

func TestExclusiveConsumeLoop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	e := NewExclusive(dir, logger)
	requester := NewExclusive(dir, logger)

	runs := 0
	out, err := e.Run(func() error {
		runs++
		if runs == 1 {
			// A request arriving mid-run queues (capacity 1) and is executed
			// by this runner's consume loop right after the current run.
			qOut, qErr := requester.Run(failIfRun(t))
			if qErr != nil || qOut != OutcomeQueued {
				t.Errorf("mid-run Run = (%s, %v), want (queued, nil)", qOut, qErr)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run err = %v, want nil", err)
	}
	if out != OutcomeRanQueued {
		t.Errorf("Run outcome = %s, want ran-queued", out)
	}
	if runs != 2 {
		t.Errorf("job ran %d times, want 2 (initial + one queued rerun)", runs)
	}
	assertLogged(t, buf, "running queued cycle request")
	if !strings.Contains(buf.String(), "attempt=1") {
		t.Errorf("log = %q, want rerun attribution attempt=1", buf.String())
	}
	if pending, perr := e.Pending(); perr != nil || pending != 0 {
		t.Errorf("Pending after drain = (%d, %v), want (0, nil)", pending, perr)
	}
}

func TestExclusiveQueueCapacity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := NewExclusive(dir, silentLogger(), WithQueueCapacity(3))
	requester := NewExclusive(dir, silentLogger(), WithQueueCapacity(3))

	runs := 0
	out, err := e.Run(func() error {
		runs++
		if runs == 1 {
			// Two mid-run requests both fit in the capacity-3 queue and each
			// gets its own rerun.
			for i := range 2 {
				qOut, qErr := requester.Run(failIfRun(t))
				if qErr != nil || qOut != OutcomeQueued {
					t.Errorf("mid-run request %d = (%s, %v), want (queued, nil)", i, qOut, qErr)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run err = %v, want nil", err)
	}
	if out != OutcomeRanQueued {
		t.Errorf("Run outcome = %s, want ran-queued", out)
	}
	if runs != 3 {
		t.Errorf("job ran %d times, want 3 (initial + two queued reruns)", runs)
	}
}

func TestExclusiveQueueCapacityDiscardBound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := NewExclusive(dir, silentLogger(), WithQueueCapacity(2))

	holder, ok, err := TryLock(filepath.Join(dir, ExclusiveLockName))
	if err != nil || !ok {
		t.Fatalf("seed TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer holder.Unlock()

	wantOutcomes := []Outcome{OutcomeQueued, OutcomeQueued, OutcomeDiscarded, OutcomeDiscarded}
	for i, want := range wantOutcomes {
		out, rerr := e.Run(failIfRun(t))
		if rerr != nil || out != want {
			t.Errorf("busy Run %d = (%s, %v), want (%s, nil)", i, out, rerr, want)
		}
	}
	if pending, perr := e.Pending(); perr != nil || pending != 2 {
		t.Errorf("Pending = (%d, %v), want (2, nil)", pending, perr)
	}
}

func TestExclusiveCapacityBelowOneClampsToOne(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := NewExclusive(dir, silentLogger(), WithQueueCapacity(0))

	holder, ok, err := TryLock(filepath.Join(dir, ExclusiveLockName))
	if err != nil || !ok {
		t.Fatalf("seed TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer holder.Unlock()

	if out, rerr := e.Run(failIfRun(t)); rerr != nil || out != OutcomeQueued {
		t.Errorf("first busy Run = (%s, %v), want (queued, nil) (capacity clamps to 1)", out, rerr)
	}
	if out, rerr := e.Run(failIfRun(t)); rerr != nil || out != OutcomeDiscarded {
		t.Errorf("second busy Run = (%s, %v), want (discarded, nil)", out, rerr)
	}
}

func TestExclusiveRunOrSkipBusy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	e := NewExclusive(dir, logger)

	holder, ok, err := TryLock(filepath.Join(dir, ExclusiveLockName))
	if err != nil || !ok {
		t.Fatalf("seed TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer holder.Unlock()

	out, err := e.RunOrSkip(failIfRun(t))
	if err != nil {
		t.Fatalf("busy RunOrSkip err = %v, want nil", err)
	}
	if out != OutcomeSkipped {
		t.Errorf("busy RunOrSkip outcome = %s, want skipped", out)
	}
	assertLogged(t, buf, "cycle lock busy; skipping tick")

	// Skip mode never queues: the tick is dropped, not recorded.
	if pending, perr := e.Pending(); perr != nil || pending != 0 {
		t.Errorf("Pending after skip = (%d, %v), want (0, nil)", pending, perr)
	}
}

func TestExclusiveRunOrSkipFree(t *testing.T) {
	t.Parallel()
	e := NewExclusive(t.TempDir(), silentLogger())
	runs := 0
	out, err := e.RunOrSkip(func() error { runs++; return nil })
	if err != nil || out != OutcomeRan || runs != 1 {
		t.Errorf("free RunOrSkip = (%s, %v) with %d runs, want (ran, nil) with 1", out, err, runs)
	}
}

func TestExclusiveRunOrSkipDrainsQueue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	e := NewExclusive(dir, logger)
	requester := NewExclusive(dir, logger)

	runs := 0
	out, err := e.RunOrSkip(func() error {
		runs++
		if runs == 1 {
			if qOut, qErr := requester.Run(failIfRun(t)); qErr != nil || qOut != OutcomeQueued {
				t.Errorf("mid-tick Run = (%s, %v), want (queued, nil)", qOut, qErr)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunOrSkip err = %v, want nil", err)
	}
	if out != OutcomeRanQueued {
		t.Errorf("RunOrSkip outcome = %s, want ran-queued (ticks execute queued demand too)", out)
	}
	if runs != 2 {
		t.Errorf("job ran %d times, want 2", runs)
	}
	assertLogged(t, buf, "running queued cycle request")
}

func TestExclusiveGateClosedSkipsRun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	// Seed a queued request: the gated call must not consume it (the run it
	// is owed never starts) and must not clear it as stale.
	if err := os.WriteFile(filepath.Join(dir, ExclusiveQueueName), []byte("1\n"), 0o644); err != nil {
		t.Fatalf("seeding queue counter: %v", err)
	}
	e := NewExclusive(dir, logger, WithGate(func() bool { return false }))

	out, err := e.Run(failIfRun(t))
	if err != nil {
		t.Fatalf("gated Run err = %v, want nil", err)
	}
	if out != OutcomeGated {
		t.Errorf("gated Run outcome = %s, want gated", out)
	}
	assertLogged(t, buf, "cycle gate closed; skipping run")
	assertNotLogged(t, buf, "stale queued-run marker cleared at startup")
	if pending, perr := e.Pending(); perr != nil || pending != 1 {
		t.Errorf("Pending after gated run = (%d, %v), want (1, nil)", pending, perr)
	}

	// RunOrSkip honours the gate identically, and the lock is released (a
	// follow-up ungated runner acquires it and clears the seeded demand).
	if out, serr := e.RunOrSkip(failIfRun(t)); serr != nil || out != OutcomeGated {
		t.Errorf("gated RunOrSkip = (%s, %v), want (gated, nil)", out, serr)
	}
	open := NewExclusive(dir, silentLogger())
	runs := 0
	if out, rerr := open.Run(func() error { runs++; return nil }); rerr != nil || out != OutcomeRan || runs != 1 {
		t.Errorf("ungated Run after gated calls = (%s, %v) with %d runs, want (ran, nil) with 1", out, rerr, runs)
	}
}

func TestExclusiveGateClosureDefersQueuedDemand(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	var gateOpen atomic.Bool
	gateOpen.Store(true)
	e := NewExclusive(dir, logger, WithGate(gateOpen.Load))
	requester := NewExclusive(dir, silentLogger())

	runs := 0
	out, err := e.Run(func() error {
		runs++
		if qOut, qErr := requester.Run(failIfRun(t)); qErr != nil || qOut != OutcomeQueued {
			t.Errorf("mid-run Run = (%s, %v), want (queued, nil)", qOut, qErr)
		}
		gateOpen.Store(false) // shutdown lands during the pass
		return nil
	})
	if err != nil {
		t.Fatalf("Run err = %v, want nil", err)
	}
	if out != OutcomeRan {
		t.Errorf("Run outcome = %s, want ran (the queued rerun must not execute)", out)
	}
	if runs != 1 {
		t.Errorf("job ran %d times, want 1 (gate closed before the rerun)", runs)
	}
	assertLogged(t, buf, "cycle gate closed; deferring queued demand")
	if pending, perr := e.Pending(); perr != nil || pending != 1 {
		t.Errorf("Pending = (%d, %v), want (1, nil) (deferred demand must survive)", pending, perr)
	}
}

func TestExclusiveStopOnErrorDefersQueuedDemand(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	e := NewExclusive(dir, logger, WithStopOnError())
	requester := NewExclusive(dir, silentLogger())

	jobErr := errors.New("cycle failed")
	runs := 0
	out, err := e.Run(func() error {
		runs++
		if qOut, qErr := requester.Run(failIfRun(t)); qErr != nil || qOut != OutcomeQueued {
			t.Errorf("mid-run Run = (%s, %v), want (queued, nil)", qOut, qErr)
		}
		return jobErr
	})
	if !errors.Is(err, jobErr) {
		t.Errorf("Run err = %v, want the job's own error", err)
	}
	if out != OutcomeRan {
		t.Errorf("Run outcome = %s, want ran (the job ran once and failed)", out)
	}
	if runs != 1 {
		t.Errorf("job ran %d times, want 1 (stop-on-error must not rerun)", runs)
	}
	assertLogged(t, buf, "cycle failed; deferring queued demand")
	if pending, perr := e.Pending(); perr != nil || pending != 1 {
		t.Errorf("Pending = (%d, %v), want (1, nil) (deferred demand must survive)", pending, perr)
	}

	// Without the option, the same shape keeps the default consume-through
	// contract (covered in depth by TestExclusiveJobErrorsJoinedAcrossReruns).
}

func TestExclusiveRerunCapDefersDemand(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	e := NewExclusive(dir, logger)
	requester := NewExclusive(dir, logger)

	// The job re-queues demand on every run (a relentless trigger source):
	// without the cap the holder would rerun forever. It must execute the
	// initial run plus exactly maxCoalescedReruns reruns, then retire with the
	// residual request deferred in the counter file.
	runs := 0
	out, err := e.Run(func() error {
		runs++
		if runs <= 1+maxCoalescedReruns {
			if qOut, qErr := requester.Run(failIfRun(t)); qErr != nil || qOut != OutcomeQueued {
				t.Errorf("mid-run request at run %d = (%s, %v), want (queued, nil)", runs, qOut, qErr)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run err = %v, want nil", err)
	}
	if out != OutcomeRanQueued {
		t.Errorf("Run outcome = %s, want ran-queued", out)
	}
	if want := 1 + maxCoalescedReruns; runs != want {
		t.Errorf("job ran %d times, want %d (initial + capped reruns)", runs, want)
	}
	assertLogged(t, buf, "rerun cap reached; deferring queued demand")
	if pending, perr := e.Pending(); perr != nil || pending != 1 {
		t.Errorf("Pending after cap = (%d, %v), want (1, nil) (deferred demand must survive)", pending, perr)
	}

	// The deferred demand is satisfied by the next acquisition: it clears the
	// counter (the run about to start covers it) and runs exactly once.
	buf.Reset()
	postRuns := 0
	out, err = e.Run(func() error { postRuns++; return nil })
	if err != nil {
		t.Fatalf("post-cap Run err = %v, want nil", err)
	}
	if out != OutcomeRan || postRuns != 1 {
		t.Errorf("post-cap Run = (%s, %d runs), want (ran, 1)", out, postRuns)
	}
	assertLogged(t, buf, "stale queued-run marker cleared at startup")
	if pending, perr := e.Pending(); perr != nil || pending != 0 {
		t.Errorf("Pending after post-cap run = (%d, %v), want (0, nil)", pending, perr)
	}
}

func TestExclusiveGarbageCounterParsesAsZero(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logger, buf := captureLogger()
	if err := os.WriteFile(filepath.Join(dir, ExclusiveQueueName), []byte("banana\n"), 0o644); err != nil {
		t.Fatalf("seeding garbage counter: %v", err)
	}

	e := NewExclusive(dir, logger)
	runs := 0
	out, err := e.Run(func() error { runs++; return nil })
	if err != nil || out != OutcomeRan || runs != 1 {
		t.Errorf("Run over garbage counter = (%s, %v) with %d runs, want (ran, nil) with 1", out, err, runs)
	}
	// Garbage parses as zero pending, so no stale-marker warning fires.
	assertNotLogged(t, buf, "stale queued-run marker cleared at startup")
}

func TestExclusiveJobErrorsJoinedAcrossReruns(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := NewExclusive(dir, silentLogger())
	requester := NewExclusive(dir, silentLogger())

	firstErr := errors.New("initial cycle failed")
	rerunErr := errors.New("queued rerun failed")
	runs := 0
	out, err := e.Run(func() error {
		runs++
		if runs == 1 {
			if qOut, qErr := requester.Run(failIfRun(t)); qErr != nil || qOut != OutcomeQueued {
				t.Errorf("mid-run Run = (%s, %v), want (queued, nil)", qOut, qErr)
			}
			return firstErr
		}
		return rerunErr
	})
	if out != OutcomeRanQueued {
		t.Errorf("Run outcome = %s, want ran-queued", out)
	}
	if !errors.Is(err, firstErr) || !errors.Is(err, rerunErr) {
		t.Errorf("Run err = %v, want both the initial and the rerun error joined", err)
	}
}

func TestExclusiveInfraError(t *testing.T) {
	t.Parallel()
	// A directory that does not exist makes the lock file uncreatable.
	e := NewExclusive(filepath.Join(t.TempDir(), "missing-dir"), silentLogger())

	out, err := e.Run(failIfRun(t))
	if err == nil {
		t.Error("Run on an uncreatable dir err = nil, want an error")
	}
	if out != OutcomeNone {
		t.Errorf("Run outcome = %s, want none (nothing ran, nothing queued)", out)
	}

	out, err = e.RunOrSkip(failIfRun(t))
	if err == nil {
		t.Error("RunOrSkip on an uncreatable dir err = nil, want an error")
	}
	if out != OutcomeNone {
		t.Errorf("RunOrSkip outcome = %s, want none", out)
	}
}

// TestExclusiveNilLoggerUsesDefault pins the nil-logger fallback. It swaps
// slog.Default() globally, so it must NOT call t.Parallel().
func TestExclusiveNilLoggerUsesDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	dir := t.TempDir()
	holder, ok, err := TryLock(filepath.Join(dir, ExclusiveLockName))
	if err != nil || !ok {
		t.Fatalf("seed TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer holder.Unlock()

	e := NewExclusive(dir, nil)
	if out, serr := e.RunOrSkip(failIfRun(t)); serr != nil || out != OutcomeSkipped {
		t.Fatalf("RunOrSkip = (%s, %v), want (skipped, nil)", out, serr)
	}
	assertLogged(t, &buf, "cycle lock busy; skipping tick")
}

func TestExclusiveNoOverlapHammer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	var inFlight, overlaps, executions atomic.Int32
	job := func() error {
		if inFlight.Add(1) != 1 {
			overlaps.Add(1)
		}
		executions.Add(1)
		time.Sleep(200 * time.Microsecond)
		inFlight.Add(-1)
		return nil
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			e := NewExclusive(dir, silentLogger(), WithQueueCapacity(2))
			for range 25 {
				if _, err := e.Run(job); err != nil {
					t.Errorf("hammer Run err = %v, want nil", err)
				}
			}
		})
	}
	wg.Wait()

	if got := overlaps.Load(); got != 0 {
		t.Errorf("observed %d overlapping executions, want 0 (mutual exclusion)", got)
	}
	if got := executions.Load(); got < 1 {
		t.Errorf("executions = %d, want at least 1", got)
	}
	// Queued demand is consumed by reruns and handoffs up to each holder's
	// rerun cap; a holder retiring at the cap may defer residue, which the
	// next acquisition clears (the run that starts then satisfies it). One
	// final run therefore leaves the counter at zero.
	e := NewExclusive(dir, silentLogger())
	if _, err := e.Run(func() error { return nil }); err != nil {
		t.Fatalf("final drain Run err = %v, want nil", err)
	}
	if pending, err := e.Pending(); err != nil || pending != 0 {
		t.Errorf("Pending after drain = (%d, %v), want (0, nil)", pending, err)
	}
}

// TestExclusiveCrashHelper is the child half of the crash-release test: it
// acquires the cycle lock, prints HELD, and sleeps until killed. It only runs
// when re-executed by TestExclusiveCrashReleasesLock with the env marker set.
func TestExclusiveCrashHelper(t *testing.T) {
	if os.Getenv("SCHEDULER_EXCLUSIVE_CRASH_HELPER") != "1" {
		t.Skip("helper process for TestExclusiveCrashReleasesLock")
	}
	lock, ok, err := TryLock(filepath.Join(os.Getenv("SCHEDULER_EXCLUSIVE_CRASH_DIR"), ExclusiveLockName))
	if err != nil || !ok {
		fmt.Println("ACQUIRE-FAILED")
		return
	}
	defer lock.Unlock() // never reached: the parent SIGKILLs this process
	fmt.Println("HELD")
	time.Sleep(time.Minute)
}

func TestExclusiveCrashReleasesLock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Re-exec this test binary as a lock-holding child, then SIGKILL it: the
	// kernel must release the flock with the process, so a crashed run can
	// never wedge the scheduler (no stale-lock cleanup logic exists by design).
	cmd := exec.Command(os.Args[0], "-test.run=^TestExclusiveCrashHelper$", "-test.v") // #nosec G204 -- re-executes this test binary
	cmd.Env = append(os.Environ(),
		"SCHEDULER_EXCLUSIVE_CRASH_HELPER=1",
		"SCHEDULER_EXCLUSIVE_CRASH_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting helper: %v", err)
	}

	held := false
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "HELD") {
			held = true
			break
		}
		if strings.Contains(scanner.Text(), "ACQUIRE-FAILED") {
			break
		}
	}
	if !held {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("helper never reported holding the lock")
	}

	if _, ok, terr := TryLock(filepath.Join(dir, ExclusiveLockName)); terr != nil || ok {
		t.Errorf("TryLock while helper holds = (ok=%v, err=%v), want (false, nil)", ok, terr)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("killing helper: %v", err)
	}
	_ = cmd.Wait()

	lock, ok, err := TryLock(filepath.Join(dir, ExclusiveLockName))
	if err != nil || !ok {
		t.Fatalf("TryLock after helper death = (ok=%v, err=%v), want (true, nil) (flock must die with the process)", ok, err)
	}
	lock.Unlock()
}

func TestOutcomeString(t *testing.T) {
	t.Parallel()
	cases := map[Outcome]string{
		OutcomeNone:      "none",
		OutcomeRan:       "ran",
		OutcomeRanQueued: "ran-queued",
		OutcomeQueued:    "queued",
		OutcomeDiscarded: "discarded",
		OutcomeSkipped:   "skipped",
		OutcomeGated:     "gated",
		Outcome(99):      "unknown",
	}
	for outcome, want := range cases {
		if got := outcome.String(); got != want {
			t.Errorf("Outcome(%d).String() = %q, want %q", int(outcome), got, want)
		}
	}
}
