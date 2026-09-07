package scheduler

import (
	"bytes"
	"io"
	"os"
	"syscall"
)

// slotFileMaxBytes bounds one slot read. A slot holds a short line written
// only under the slot's own flock; the cap is hygiene against an externally
// scribbled file, not an expected size.
const slotFileMaxBytes = 1 << 16

// SlotFile is a single-slot byte payload shared across processes through one
// file, mutated by read-modify-write transactions under a short exclusive
// flock(2). It backs Exclusive's rerun counter, exported so an app can build
// its own coalescing state on the same transaction. What the bytes mean is the
// caller's policy, but a parser MUST read torn or garbage bytes as the zero
// value: a crash between Truncate and WriteAt can tear the slot. Never unlink a
// live slot — a concurrent opener would land on a different inode and lose
// mutual exclusion; "clear" writes an empty payload. See TryLock on placement.
type SlotFile struct {
	path string
}

// NewSlotFile returns a SlotFile backed by the file at path (see the type
// documentation for the trust and lifecycle rules).
func NewSlotFile(path string) *SlotFile {
	return &SlotFile{path: path}
}

// Mutate applies fn to the slot's current content under an exclusive flock and
// returns the content fn saw. fn receives the current bytes (empty on first
// use) and returns the bytes to store; returning them byte-equal leaves the
// file untouched, so a read is a Mutate whose fn returns its argument, and a
// nil return clears the slot. The lock is BLOCKING and fn runs under it, so
// keep fn small and non-blocking. before stays meaningful beside a non-nil
// error when the failure happened after the read.
func (s *SlotFile) Mutate(fn func(before []byte) []byte) (before []byte, err error) {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o644) // #nosec G304 -- caller-supplied trusted slot path
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if lockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); lockErr != nil {
		return nil, lockErr
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()

	before, err = io.ReadAll(io.LimitReader(f, slotFileMaxBytes))
	if err != nil {
		return nil, err
	}
	after := fn(before)
	if bytes.Equal(before, after) {
		return before, nil
	}
	if truncErr := f.Truncate(0); truncErr != nil {
		return before, truncErr
	}
	if _, writeErr := f.WriteAt(after, 0); writeErr != nil {
		return before, writeErr
	}
	return before, nil
}
