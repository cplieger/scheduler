package trigger

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// testPayload is the parameterized-payload stand-in: arguments that must
// replay exactly on the daemon side (the renovate shape, minus the environ).
type testPayload struct {
	Repos []string `json:"repos,omitempty"`
}

// maxSunPath is the longest usable unix-socket path: Linux sun_path is
// 108 bytes including the trailing NUL.
const maxSunPath = 107

// testSocketPath returns a unix-socket path short enough for sun_path:
// t.TempDir() embeds the full test name, which overflows the limit under a
// long TMPDIR and fails bind with EINVAL. The helper honors a configured
// TMPDIR when the short random directory it yields still fits sun_path
// (keeping test scratch inside a workspace-scoped root), and falls back to
// /tmp — where the production sockets live — only when TMPDIR is too deep.
func testSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "trig-sock-")
	if err != nil {
		t.Fatalf("mktemp for socket dir: %v", err)
	}
	path := filepath.Join(dir, "s.sock")
	if len(path) > maxSunPath {
		// TMPDIR is too deep for sun_path; fall back to /tmp.
		_ = os.RemoveAll(dir)
		if dir, err = os.MkdirTemp("/tmp", "trig-sock-"); err != nil {
			t.Fatalf("mktemp for socket dir: %v", err)
		}
		path = filepath.Join(dir, "s.sock")
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return path
}

// startExecutor drains q on a background goroutine, running fn per job (fn
// owns Start/Finish). Returns a done channel that closes when the queue
// drains.
func startExecutor[P any](q *Queue[P], fn func(*Job[P])) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for j := range q.Jobs() {
			fn(j)
		}
	}()
	return done
}

// recordHandler is a minimal capturing slog handler (the library must not
// grow a test dependency for log capture; stdlib-only, matching the parent
// package's posture).
type recordHandler struct {
	mu      *sync.Mutex
	records *[]slog.Record
}

func newRecordHandler() recordHandler {
	return recordHandler{mu: &sync.Mutex{}, records: &[]slog.Record{}}
}

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, r)
	return nil
}
func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

func (h recordHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), *h.records...)
}

// captureLogs swaps slog.Default for a capturing handler for the test's
// duration. Tests using it must not run in parallel (process-global default).
func captureLogs(t *testing.T) recordHandler {
	t.Helper()
	h := newRecordHandler()
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}
