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
// file, mutated by atomic read-modify-write transactions under a short
// exclusive flock(2) on the file itself. It is the storage mechanism behind
// Exclusive's rerun counter, exported so an app can build its own coalescing
// state on the same transaction — for example a payload-carrying demand slot
// whose merge and claim semantics are app policy (docker-renovate-scheduler
// records WHICH repos a queued trigger wants, not just that one arrived).
//
// The transaction is content-agnostic: what the bytes mean, how concurrent
// demands merge, and when a slot counts as satisfied are the caller's parser
// and policy. Parsers must self-heal on unparseable content (treat torn or
// garbage bytes as the zero value): a crash between Truncate and WriteAt can
// leave a torn slot, and the library's own counter and every other slot user
// recover by reading garbage as empty.
//
// The slot file is created on first use and never unlinked — by the library
// or the caller — while contenders may exist: unlinking a locked file lets a
// concurrent opener land on a different inode and breaks mutual exclusion.
// "Clear" is writing an empty payload. Place the path in a directory not
// writable by untrusted local users, per the same symlink-following caveat as
// TryLock.
type SlotFile struct {
	path string
}

// NewSlotFile returns a SlotFile backed by the file at path (see the type
// documentation for the trust and lifecycle rules).
func NewSlotFile(path string) *SlotFile {
	return &SlotFile{path: path}
}

// Mutate applies fn to the slot's current content under an exclusive flock on
// the slot file and returns the content fn saw. fn receives the current bytes
// (empty on first use) and returns the bytes to store; returning content
// byte-equal to before (returning before itself is the idiom) leaves the file
// untouched, so a read is a Mutate whose fn returns its argument. A nil
// return stores an empty payload (the clear idiom).
//
// The lock is blocking: contenders serialize, and the critical section is the
// read, fn, and write of one short payload — microseconds, never a job's
// duration — so keep fn small and non-blocking (it runs under the flock).
// before stays meaningful alongside a non-nil error when the failure happened
// after the read (a truncate or write error).
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
