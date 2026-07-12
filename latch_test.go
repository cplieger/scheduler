package scheduler

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLatch(t *testing.T) {
	t.Parallel()
	l := NewLatch(filepath.Join(t.TempDir(), ".latch"))

	if l.Raised() {
		t.Fatal("a new latch must not be raised")
	}

	if err := l.Raise(); err != nil {
		t.Fatalf("Raise() returned error: %v", err)
	}
	if !l.Raised() {
		t.Error("latch must be raised after Raise()")
	}

	// Raise is idempotent: raising an already-raised latch stays raised.
	if err := l.Raise(); err != nil {
		t.Fatalf("second Raise() returned error: %v", err)
	}
	if !l.Raised() {
		t.Error("latch must remain raised after a second Raise()")
	}

	l.Clear()
	if l.Raised() {
		t.Error("latch must not be raised after Clear()")
	}

	// Clear on an already-cleared latch is a no-op.
	l.Clear()
	if l.Raised() {
		t.Error("latch must remain un-raised after a second Clear()")
	}
}

func TestLatchRaiseError(t *testing.T) {
	t.Parallel()
	// A marker path whose parent component is a regular file (not a directory)
	// cannot be created: OpenFile returns ENOTDIR. This is root-safe (unlike an
	// EACCES permission test, which silently passes as root).
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, []byte("x"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	l := NewLatch(filepath.Join(notADir, ".latch"))

	if err := l.Raise(); err == nil {
		t.Error("Raise() must return an error when the marker cannot be created")
	}
	if l.Raised() {
		t.Error("Raised() must be false when the marker was never created")
	}
}
