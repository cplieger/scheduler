package scheduler

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func lockPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), ".test.lock")
}

func TestTryLockMutualExclusion(t *testing.T) {
	t.Parallel()
	path := lockPath(t)

	first, ok, err := TryLock(path)
	if err != nil || !ok {
		t.Fatalf("first TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}

	// flock is per open-file-description, so a second acquisition contends even
	// within the same process.
	second, ok, err := TryLock(path)
	if err != nil {
		t.Fatalf("second TryLock err = %v, want nil", err)
	}
	if ok {
		second.Unlock()
		t.Fatal("second TryLock acquired the lock while the first still held it")
	}

	first.Unlock()

	third, ok, err := TryLock(path)
	if err != nil || !ok {
		t.Fatalf("third TryLock after unlock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	third.Unlock()
}

func TestTryLockOpenError(t *testing.T) {
	t.Parallel()
	// A path whose parent does not exist cannot be created or opened.
	_, ok, err := TryLock(filepath.Join(t.TempDir(), "missing-dir", ".lock"))
	if err == nil {
		t.Fatal("TryLock on an uncreatable path err = nil, want an error")
	}
	if ok {
		t.Fatal("TryLock on an uncreatable path ok = true, want false")
	}
}

func TestReadHolder(t *testing.T) {
	t.Parallel()
	path := lockPath(t)

	before := time.Now()
	lock, ok, err := TryLock(path)
	if err != nil || !ok {
		t.Fatalf("TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer lock.Unlock()

	since, known := ReadHolder(path)
	if !known {
		t.Fatal("ReadHolder after acquire known = false, want true")
	}
	// The recorded time is written at acquisition, so it sits between the
	// pre-acquire timestamp and now (allowing a small clock slack).
	if since.Before(before.Add(-time.Second)) || since.After(time.Now().Add(time.Second)) {
		t.Errorf("ReadHolder since = %s, want near %s", since, before)
	}
}

func TestReadHolderMissingFile(t *testing.T) {
	t.Parallel()
	_, known := ReadHolder(filepath.Join(t.TempDir(), "never-created.lock"))
	if known {
		t.Error("ReadHolder(missing) known = true, want false")
	}
}

func TestReadHolderTornOrGarbageLine(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), ".torn.lock")
	if err := os.WriteFile(path, []byte("not-a-timestamp\n"), 0o644); err != nil {
		t.Fatalf("seeding lock file: %v", err)
	}
	since, known := ReadHolder(path)
	if known {
		t.Errorf("ReadHolder(garbage) known = true, want false")
	}
	if !since.IsZero() {
		t.Errorf("ReadHolder(garbage) since = %s, want zero time", since)
	}
}

func TestReadHolderUnreadablePath(t *testing.T) {
	t.Parallel()
	// Opening a directory succeeds but ReadAt fails with a non-EOF error, so
	// ReadHolder must report the holder as unknown rather than parse garbage.
	since, known := ReadHolder(t.TempDir())
	if known {
		t.Errorf("ReadHolder(directory) known = true, want false")
	}
	if !since.IsZero() {
		t.Errorf("ReadHolder(directory) since = %s, want zero time", since)
	}
}

func TestWriteHolderTruncatesStaleTail(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), ".holder.lock")
	// Seed a longer valid line than writeHolder produces, plus a tail. If
	// writeHolder skipped Truncate, the shorter new line would leave the
	// tail behind and ReadHolder would fail to parse the mixed content.
	seed := "2099-12-31T23:59:59.999999999Z\nleftover-stale-tail-bytes\n"
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatalf("seeding lock file: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()

	writeHolder(f)

	since, known := ReadHolder(path)
	if !known {
		t.Fatal("ReadHolder after writeHolder over a longer line: known = false, want true (stale tail not truncated)")
	}
	if d := time.Since(since); d < 0 || d > time.Minute {
		t.Errorf("ReadHolder since = %s (age %s), want a fresh near-now timestamp", since, d)
	}
}
