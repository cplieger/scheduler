package scheduler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestWaitForDrainReturnsTrueWhenNotInFlight(t *testing.T) {
	t.Parallel()
	if !WaitForDrain(context.Background(), lockPath(t), 5*time.Millisecond, time.Second) {
		t.Error("WaitForDrain with no holder = false, want true")
	}
}

func TestWaitForDrainWaitsOutInFlightRun(t *testing.T) {
	t.Parallel()
	path := lockPath(t)

	lock, ok, err := TryLock(path)
	if err != nil || !ok {
		t.Fatalf("TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	// Release the lock shortly after the drain starts polling.
	go func() {
		time.Sleep(25 * time.Millisecond)
		lock.Unlock()
	}()

	if !WaitForDrain(context.Background(), path, 5*time.Millisecond, 2*time.Second) {
		t.Error("WaitForDrain(released mid-wait) = false, want true")
	}
}

func TestWaitForDrainTimesOut(t *testing.T) {
	t.Parallel()
	path := lockPath(t)

	lock, ok, err := TryLock(path)
	if err != nil || !ok {
		t.Fatalf("TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer lock.Unlock()

	if WaitForDrain(context.Background(), path, 5*time.Millisecond, 50*time.Millisecond) {
		t.Error("WaitForDrain(held past maxWait) = true, want false")
	}
}

func TestWaitForDrainCancelled(t *testing.T) {
	t.Parallel()
	path := lockPath(t)

	lock, ok, err := TryLock(path)
	if err != nil || !ok {
		t.Fatalf("TryLock = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	defer lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if WaitForDrain(ctx, path, 5*time.Millisecond, 10*time.Second) {
		t.Error("WaitForDrain(cancelled) = true, want false")
	}
}

func TestWaitForDrainReturnsFalseOnLockError(t *testing.T) {
	t.Parallel()
	// An uncreatable lock path makes the first InFlight probe error, and
	// WaitForDrain must give up (false) rather than poll until maxWait.
	got := WaitForDrain(context.Background(),
		filepath.Join(t.TempDir(), "missing-dir", ".lock"), 5*time.Millisecond, time.Second)
	if got {
		t.Error("WaitForDrain on an uncreatable lock path = true, want false")
	}
}
