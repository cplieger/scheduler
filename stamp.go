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
	// RetryFailed treats a failed run as leaving the schedule stale: Due
	// reports true unless the last recorded run both succeeded and is younger
	// than the interval. A restart after a failure fires the startup run
	// again, so an operator who fixes a bad configuration and recreates the
	// container gets immediate feedback instead of waiting out an interval.
	RetryFailed FailurePolicy = iota
	// CountFailed treats any completed run as holding its schedule slot: only
	// the record's age decides Due, and the interval ticker owns the retry
	// after a failure. For jobs whose failed passes are themselves expensive
	// enough that a restart must not repeat them early.
	CountFailed
)

// Stamp records when a scheduled run last completed, and whether it
// succeeded, in a single file. Place the file on storage that outlives the
// process (a persisted volume) and it survives a container recreate, which
// lets a composition root skip RunLoop's startup fire when the previous
// container ran recently:
//
//	due := stamp.Due(interval, time.Now(), scheduler.RetryFailed)
//	scheduler.RunLoop(ctx, job, scheduler.LoopOptions{Interval: interval, FireOnStart: due})
//
// Which runs get recorded is the caller's policy: a full scheduled pass
// counts, while a manually triggered or scoped run typically does not,
// because it does not answer the freshness question a startup fire exists
// for. Where the file lives is also the caller's: on storage that does not
// persist, the file never survives a recreate and Due degrades to always
// true, the unconditional startup fire.
//
// One process writes the stamp (the owner of the schedule); Record neither
// locks nor fsyncs. A record lost to a crash, or torn by one, reads as
// unknown, and unknown reads as due, so corruption costs at most one extra
// startup run. Like a TryLock lock file, place the stamp in a directory not
// writable by untrusted local users: it is created following symlinks and
// replaced in place.
type Stamp struct {
	path string
}

// NewStamp returns a Stamp backed by the file at path. The file is created on
// the first Record call; a missing file reads as no run ever recorded.
func NewStamp(path string) *Stamp {
	return &Stamp{path: path}
}

// Record replaces the stamp with the current time and ok as the last
// completed run. Plain truncate-and-write: no lock (single writer by
// contract) and no fsync, because losing a record to a crash is fail-safe —
// an unknown record reads as due.
func (s *Stamp) Record(ok bool) error {
	outcome := "failed"
	if ok {
		outcome = "ok"
	}
	line := time.Now().UTC().Format(time.RFC3339Nano) + " " + outcome + "\n"
	return os.WriteFile(s.path, []byte(line), 0o600)
}

// Last reads the most recent record. known is false when no run was ever
// recorded, the file is unreadable, or the record is torn or malformed — the
// conservative reading, since a consumer treats an unknown record as due.
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

// Due reports whether a startup run is due: no run is known, or the policy
// discounts the last one, or it completed at least interval ago. now is a
// parameter so the caller and its tests share one clock. A record dated in
// the future (a restored volume, a stepped clock) reads as not due until now
// catches up, bounded by the next interval tick; a non-positive interval is
// always due. Due panics on a FailurePolicy outside the declared constants —
// a programmer error, caught at first boot.
func (s *Stamp) Due(interval time.Duration, now time.Time, policy FailurePolicy) bool {
	if policy != RetryFailed && policy != CountFailed {
		panic(fmt.Sprintf("scheduler: Due called with unknown FailurePolicy %d", policy))
	}
	rec, known := s.Last()
	if !known {
		return true
	}
	if policy == RetryFailed && !rec.OK {
		return true
	}
	return now.Sub(rec.Time) >= interval
}
