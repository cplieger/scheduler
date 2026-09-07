package scheduler

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// RunRecord is one completed scheduled run: when it completed and whether it
// succeeded.
type RunRecord struct {
	Time time.Time
	OK   bool
}

// FailurePolicy selects how Due treats a last run that completed but failed.
// It is a required parameter so every consumer states the choice explicitly.
type FailurePolicy int

const (
	// RetryFailed treats a failed run as leaving the schedule stale, so a
	// restart after a failure fires the startup run again: an operator who
	// fixes a bad configuration and recreates the container gets immediate
	// feedback.
	RetryFailed FailurePolicy = iota
	// CountFailed treats any completed run as holding its slot, leaving the
	// retry to the interval ticker. For jobs whose failed passes are
	// themselves expensive enough that a restart must not repeat them early.
	CountFailed
)

// Stamp records when a scheduled run last completed, and whether it
// succeeded, in a single file. On storage that outlives the process it lets a
// composition root skip RunLoop's startup fire and phase the first tick from
// the previous run; see ExampleStamp. A record lost or torn by a crash reads
// as unknown, and unknown reads as due, so corruption costs one extra startup
// run and never a skipped schedule. Keep the file where untrusted local users
// cannot write: it is created following symlinks and replaced in place.
type Stamp struct {
	path string
}

// NewStamp returns a Stamp backed by the file at path. The file is created on
// the first Record call; a missing file reads as no run ever recorded.
func NewStamp(path string) *Stamp {
	return &Stamp{path: path}
}

// Record replaces the stamp with the current time and ok as the last
// completed run. Single writer by contract: no lock, no fsync.
func (s *Stamp) Record(ok bool) error {
	outcome := "failed"
	if ok {
		outcome = "ok"
	}
	line := time.Now().UTC().Format(time.RFC3339Nano) + " " + outcome + "\n"
	return os.WriteFile(s.path, []byte(line), 0o600)
}

// Last reads the most recent record. known is false when no run was ever
// recorded, or the record is unreadable, torn or malformed — all of which a
// consumer treats as due.
func (s *Stamp) Last() (rec RunRecord, known bool) {
	f, err := os.Open(s.path) // #nosec G304 -- caller-supplied trusted stamp path
	if err != nil {
		return RunRecord{}, false
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, 96)
	n, rerr := f.ReadAt(buf, 0)
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return RunRecord{}, false
	}
	fields := strings.Fields(string(buf[:n]))
	if len(fields) != 2 {
		return RunRecord{}, false
	}
	t, perr := time.Parse(time.RFC3339Nano, fields[0])
	if perr != nil {
		return RunRecord{}, false
	}
	switch fields[1] {
	case "ok":
		return RunRecord{Time: t, OK: true}, true
	case "failed":
		return RunRecord{Time: t, OK: false}, true
	default:
		return RunRecord{}, false
	}
}

// qualifying returns the record Due and Remaining judge: an unknown record
// never qualifies, and RetryFailed discounts a failure. Panics on a
// FailurePolicy outside the declared constants.
func (s *Stamp) qualifying(policy FailurePolicy) (RunRecord, bool) {
	if policy != RetryFailed && policy != CountFailed {
		panic(fmt.Sprintf("scheduler: unknown FailurePolicy %d", policy))
	}
	rec, known := s.Last()
	if !known || (policy == RetryFailed && !rec.OK) {
		return RunRecord{}, false
	}
	return rec, true
}

// Due reports whether a startup run is due: no run qualifies under policy, or
// the last one completed at least interval ago. now is a parameter so caller
// and tests share one clock. A future-dated record (a restored volume, a
// stepped clock) reads as not due until now catches up; a non-positive
// interval is always due. Panics on an unknown FailurePolicy.
func (s *Stamp) Due(interval time.Duration, now time.Time, policy FailurePolicy) bool {
	rec, ok := s.qualifying(policy)
	if !ok || interval <= 0 {
		return true
	}
	return now.Sub(rec.Time) >= interval
}

// Remaining reports the time left until the next run is due: interval minus
// the qualifying record's age, floored at zero and capped at interval, so a
// future-dated record costs one full period at most. Remaining is zero exactly
// when Due is true, which is what lets one pair of calls drive both the startup
// fire and the first tick's phase. Panics like Due.
func (s *Stamp) Remaining(interval time.Duration, now time.Time, policy FailurePolicy) time.Duration {
	rec, ok := s.qualifying(policy)
	if !ok || interval <= 0 {
		return 0
	}
	rem := interval - now.Sub(rec.Time)
	switch {
	case rem <= 0:
		return 0
	case rem > interval:
		return interval
	default:
		return rem
	}
}
