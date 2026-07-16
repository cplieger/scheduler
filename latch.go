package scheduler

import "os"

// Latch is a persistent, cross-process one-shot boolean backed by the presence
// of a marker file: one process Raises it, another observes it with Raised, and
// either Clears it. Unlike a Lock it holds no OS resource and does not
// auto-release — its state is purely whether the marker file exists — so it
// survives the raising process exiting, which is exactly what a one-bit signal
// between two processes that cannot signal each other needs.
//
// It is used directly as a latch between processes, e.g. a shutdown/drain
// latch: a daemon (PID 1) Raises it on SIGTERM and a separate worker — a
// docker exec child that never receives the container's SIGTERM — observes it
// with Raised and drains (typically wired into Exclusive as its gate). Raise
// is the only operation that can fail (a marker write); Raised and Clear treat
// a missing marker as the natural un-raised state and never fail.
type Latch struct {
	path string
}

// NewLatch returns a Latch backed by the marker file at path. Place path in a
// directory not writable by untrusted local users: Raise opens the marker file
// following symlinks, so it may create or open a target chosen by an attacker
// rather than the intended marker.
func NewLatch(path string) *Latch {
	return &Latch{path: path}
}

// Raise sets the latch by creating its marker file. Idempotent: raising an
// already-raised latch is a no-op. A Clear racing a concurrent Raise cannot
// lose the signal: Raise re-checks that the marker exists after writing and
// recreates it if the Clear unlinked it mid-flight (the open can attach to a
// pre-existing inode that the Clear then removes). The ambiguity resolves
// toward one spurious observation of a raised latch, never a lost raise. It
// returns an error only if the marker could not be written, leaving the
// decision of whether a missed raise is tolerable to the caller; Raise never
// panics.
func (l *Latch) Raise() error {
	for {
		f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY, 0o644) // #nosec G304 -- caller-supplied trusted marker path
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		// A concurrent Clear may have unlinked the path between the open above
		// and here; loop until a completed write is still observable. Each
		// retry creates the file anew, so the loop terminates unless Clears
		// keep arriving — and each Clear is a once-per-run-start event.
		if l.Raised() {
			return nil
		}
	}
}

// Raised reports whether the latch is currently raised (its marker file
// exists).
func (l *Latch) Raised() bool {
	_, err := os.Stat(l.path)
	return err == nil
}

// Clear lowers the latch by removing its marker file. Best-effort: a missing
// marker is already the desired state.
func (l *Latch) Clear() {
	_ = os.Remove(l.path)
}
