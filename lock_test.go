package scheduler

import (
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

func TestInFlight(t *testing.T) {
	t.Parallel()
	path := lockPath(t)

	inFlight, err := InFlight(path)
	if err != nil {
		t.Fatalf("InFlight(free) err = %v, want nil", err)
	}
	if inFlight {
		t.Error("InFlight(free) = true, want false")
	}

	lock, ok, err := TryLock(path)
	if err != nil || !ok {
		t.Fatalf("TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}

	inFlight, err = InFlight(path)
	if err != nil {
		t.Fatalf("InFlight(held) err = %v, want nil", err)
	}
	if !inFlight {
		t.Error("InFlight(held) = false, want true")
	}

	lock.Unlock()

	inFlight, err = InFlight(path)
	if err != nil {
		t.Fatalf("InFlight(released) err = %v, want nil", err)
	}
	if inFlight {
		t.Error("InFlight(released) = true, want false")
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

func TestRerunFlag(t *testing.T) {
	t.Parallel()
	flag := NewRerunFlag(filepath.Join(t.TempDir(), ".rerun"))

	if flag.Pending() {
		t.Error("Pending() on a fresh flag = true, want false")
	}

	flag.Set()
	if !flag.Pending() {
		t.Error("Pending() after Set() = false, want true")
	}

	// Set is idempotent: a second Set keeps exactly one pending rerun.
	flag.Set()
	if !flag.Pending() {
		t.Error("Pending() after a second Set() = false, want true")
	}

	flag.Clear()
	if flag.Pending() {
		t.Error("Pending() after Clear() = true, want false")
	}

	// Clear on an already-cleared flag is a no-op, not an error.
	flag.Clear()
	if flag.Pending() {
		t.Error("Pending() after a second Clear() = true, want false")
	}
}
