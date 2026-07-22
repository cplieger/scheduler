package scheduler

import (
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
)

// Lock is an advisory exclusive lock backed by flock(2). It is the overlap
// guard for a scheduled job: it serializes runs both in-process (a startup run
// racing a tick) and cross-process (an external trigger — a docker exec —
// racing the built-in loop), because flock associates the lock with the open
// file description, so two independent OpenFile calls contend even within one
// process.
type Lock struct {
	f *os.File
}

// TryLock attempts a non-blocking exclusive lock on path, creating the file if
// absent. ok is false without error when another holder currently owns the
// lock (a run is already in flight); the caller must release an acquired lock
// with Unlock. On acquisition it records the current time in the file so a
// later contender can read the holder's age via ReadHolder.
//
// Place path in a directory not writable by untrusted local users (e.g. a
// container-private /tmp or a service-owned dir, not a world-writable host
// /tmp shared with other accounts): the file is opened following symlinks and
// its holder timestamp is written with Truncate, so a pre-planted symlink at
// path would be clobbered. Callers that must harden further can place path
// under a 0700 service-owned directory.
func TryLock(path string) (l *Lock, ok bool, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644) // #nosec G304 G703 -- caller-supplied trusted lock path
	if err != nil {
		return nil, false, err
	}
	if lockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); lockErr != nil {
		_ = f.Close()
		if errors.Is(lockErr, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, lockErr
	}
	writeHolder(f)
	return &Lock{f: f}, true, nil
}

// Unlock releases the lock and closes the underlying file. The lock file is
// left on disk; its only content is the last holder's acquisition timestamp,
// reused across runs and irrelevant while the lock is free.
func (l *Lock) Unlock() {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}

// ReadHolder reads the acquisition timestamp the current holder recorded in the
// lock file. known is false when the timestamp could not be read — the holder
// had not written it yet, the line was torn mid-write, or the file is
// absent — in which case since is the zero time. The value is
// observability-only (a contender reporting how long the holder has run) and
// never affects locking correctness; it is meaningful only while the lock is
// actually held.
func ReadHolder(path string) (since time.Time, known bool) {
	f, err := os.Open(path) // #nosec G304 -- caller-supplied trusted lock path
	if err != nil {
		return time.Time{}, false
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, 64)
	n, rerr := f.ReadAt(buf, 0)
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return time.Time{}, false
	}
	t, perr := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(buf[:n])))
	if perr != nil {
		return time.Time{}, false
	}
	return t, true
}

// writeHolder records the current UTC time as the lock-acquisition timestamp.
// Best-effort: a failure only degrades the holder age a later contender can
// read, never correctness. Truncate-then-write keeps a shorter line from
// leaving a stale tail.
func writeHolder(f *os.File) {
	line := time.Now().UTC().Format(time.RFC3339Nano) + "\n"
	if err := f.Truncate(0); err != nil {
		return
	}
	_, _ = f.WriteAt([]byte(line), 0)
}
