package scheduler

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestSlotFileFirstUseIsEmpty(t *testing.T) {
	t.Parallel()
	s := NewSlotFile(filepath.Join(t.TempDir(), ".slot"))

	before, err := s.Mutate(func(cur []byte) []byte {
		if len(cur) != 0 {
			t.Errorf("first-use content = %q, want empty", cur)
		}
		return []byte("payload\n")
	})
	if err != nil {
		t.Fatalf("Mutate err = %v, want nil", err)
	}
	if len(before) != 0 {
		t.Errorf("before = %q, want empty on first use", before)
	}

	// The write landed and the next transaction sees it.
	got, err := s.Mutate(func(cur []byte) []byte { return cur })
	if err != nil {
		t.Fatalf("read Mutate err = %v, want nil", err)
	}
	if string(got) != "payload\n" {
		t.Errorf("stored content = %q, want %q", got, "payload\n")
	}
}

func TestSlotFileUnchangedContentSkipsWrite(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), ".slot")
	s := NewSlotFile(path)
	if _, err := s.Mutate(func([]byte) []byte { return []byte("keep") }); err != nil {
		t.Fatalf("seed Mutate err = %v", err)
	}

	// Returning the argument (the read idiom) must leave the file untouched;
	// so must returning any byte-equal copy.
	if _, err := s.Mutate(func(cur []byte) []byte { return cur }); err != nil {
		t.Fatalf("identity Mutate err = %v", err)
	}
	if _, err := s.Mutate(func([]byte) []byte { return []byte("keep") }); err != nil {
		t.Fatalf("byte-equal Mutate err = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "keep" {
		t.Errorf("content after no-op transactions = (%q, %v), want (keep, nil)", raw, err)
	}
}

func TestSlotFileNilClears(t *testing.T) {
	t.Parallel()
	s := NewSlotFile(filepath.Join(t.TempDir(), ".slot"))
	if _, err := s.Mutate(func([]byte) []byte { return []byte("full") }); err != nil {
		t.Fatalf("seed Mutate err = %v", err)
	}
	if _, err := s.Mutate(func([]byte) []byte { return nil }); err != nil {
		t.Fatalf("clear Mutate err = %v", err)
	}
	got, err := s.Mutate(func(cur []byte) []byte { return cur })
	if err != nil || len(got) != 0 {
		t.Errorf("content after clear = (%q, %v), want empty", got, err)
	}
}

func TestSlotFileShrinkLeavesNoStaleTail(t *testing.T) {
	t.Parallel()
	s := NewSlotFile(filepath.Join(t.TempDir(), ".slot"))
	if _, err := s.Mutate(func([]byte) []byte { return []byte("a-much-longer-first-payload\n") }); err != nil {
		t.Fatalf("seed Mutate err = %v", err)
	}
	if _, err := s.Mutate(func([]byte) []byte { return []byte("x\n") }); err != nil {
		t.Fatalf("shrink Mutate err = %v", err)
	}
	got, err := s.Mutate(func(cur []byte) []byte { return cur })
	if err != nil || string(got) != "x\n" {
		t.Errorf("content after shrink = (%q, %v), want (x\\n, nil) — truncate must drop the stale tail", got, err)
	}
}

func TestSlotFileOpenError(t *testing.T) {
	t.Parallel()
	// A slot path under a regular-file parent cannot be created (ENOTDIR,
	// root-safe unlike a permissions test).
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, []byte("x"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s := NewSlotFile(filepath.Join(notADir, ".slot"))
	if _, err := s.Mutate(func(cur []byte) []byte { return cur }); err == nil {
		t.Error("Mutate on an uncreatable path err = nil, want an error")
	}
}

// TestSlotFileConcurrentCountersAreLossless is the mutual-exclusion property:
// N goroutines, each incrementing a text counter through its own SlotFile
// handle (flock is per open-file-description, so every Mutate contends for
// real), must produce exactly N increments — a lost update means two
// transactions interleaved.
func TestSlotFileConcurrentCountersAreLossless(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), ".slot")

	const workers, perWorker = 8, 50
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			s := NewSlotFile(path)
			for range perWorker {
				_, err := s.Mutate(func(cur []byte) []byte {
					n, _ := strconv.Atoi(strings.TrimSpace(string(cur)))
					return []byte(strconv.Itoa(n+1) + "\n")
				})
				if err != nil {
					t.Errorf("concurrent Mutate err = %v, want nil", err)
				}
			}
		})
	}
	wg.Wait()

	raw, err := NewSlotFile(path).Mutate(func(cur []byte) []byte { return cur })
	if err != nil {
		t.Fatalf("final read err = %v", err)
	}
	got, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	if want := workers * perWorker; got != want {
		t.Errorf("counter = %d, want %d (lost updates under concurrent Mutate)", got, want)
	}
}

func TestSlotFileGarbageReachesParserVerbatim(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), ".slot")
	garbage := []byte("torn\x00garbage")
	if err := os.WriteFile(path, garbage, 0o644); err != nil {
		t.Fatalf("seeding garbage: %v", err)
	}
	before, err := NewSlotFile(path).Mutate(func(cur []byte) []byte {
		if !bytes.Equal(cur, garbage) {
			t.Errorf("fn saw %q, want the on-disk bytes verbatim (self-healing is the parser's job)", cur)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Mutate err = %v", err)
	}
	if !bytes.Equal(before, garbage) {
		t.Errorf("before = %q, want the garbage bytes", before)
	}
}
