package scheduler

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func stampPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "last-run")
}

// seedStamp writes a record directly in the stamp's file format, so tests
// control the recorded time (Record always stamps the current time).
func seedStamp(t *testing.T, path string, ts time.Time, outcome string) {
	t.Helper()
	line := ts.UTC().Format(time.RFC3339Nano) + " " + outcome + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("seeding stamp file: %v", err)
	}
}

func TestStampRecordThenLast(t *testing.T) {
	t.Parallel()
	for _, ok := range []bool{true, false} {
		t.Run(fmt.Sprintf("ok=%v", ok), func(t *testing.T) {
			t.Parallel()
			s := NewStamp(stampPath(t))

			before := time.Now()
			if err := s.Record(ok); err != nil {
				t.Fatalf("Record(%v) = %v, want nil", ok, err)
			}

			rec, known := s.Last()
			if !known {
				t.Fatal("Last() after Record known = false, want true")
			}
			if rec.OK != ok {
				t.Errorf("Last().OK = %v, want %v", rec.OK, ok)
			}
			// The recorded time is written at Record, so it sits between the
			// pre-record timestamp and now (allowing a small clock slack).
			if rec.Time.Before(before.Add(-time.Second)) || rec.Time.After(time.Now().Add(time.Second)) {
				t.Errorf("Last().Time = %s, want near %s", rec.Time, before)
			}
		})
	}
}

func TestStampLastMissingFile(t *testing.T) {
	t.Parallel()
	rec, known := NewStamp(stampPath(t)).Last()
	if known {
		t.Error("Last(missing) known = true, want false")
	}
	if !rec.Time.IsZero() || rec.OK {
		t.Errorf("Last(missing) rec = %+v, want zero value", rec)
	}
}

func TestStampLastMalformed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
	}{
		{name: "empty", content: ""},
		{name: "garbage", content: "not-a-record\n"},
		{name: "time_only", content: "2026-09-02T12:00:00Z\n"},
		{name: "torn_outcome", content: "2026-09-02T12:00:00Z o\n"},
		{name: "unknown_outcome", content: "2026-09-02T12:00:00Z done\n"},
		{name: "extra_field", content: "2026-09-02T12:00:00Z ok trailing-junk\n"},
		{name: "bad_time", content: "yesterday ok\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "last-run")
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("seeding stamp file: %v", err)
			}
			rec, known := NewStamp(path).Last()
			if known {
				t.Errorf("Last(%q) known = true, want false", tc.content)
			}
			if !rec.Time.IsZero() || rec.OK {
				t.Errorf("Last(%q) rec = %+v, want zero value", tc.content, rec)
			}
		})
	}
}

func TestStampLastUnreadablePath(t *testing.T) {
	t.Parallel()
	// Opening a directory succeeds but ReadAt fails with a non-EOF error, so
	// Last must report unknown rather than parse garbage.
	_, known := NewStamp(t.TempDir()).Last()
	if known {
		t.Error("Last(directory) known = true, want false")
	}
}

// TestStampLastPaddedFile pins that the fixed read buffer is a window, not a
// size requirement: trailing whitespace past the record still parses.
func TestStampLastPaddedFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "last-run")
	want := time.Date(2026, 9, 2, 10, 30, 0, 0, time.UTC)
	padded := want.Format(time.RFC3339Nano) + " ok\n   "
	if err := os.WriteFile(path, []byte(padded), 0o600); err != nil {
		t.Fatalf("seeding stamp file: %v", err)
	}

	rec, known := NewStamp(path).Last()
	if !known {
		t.Fatal("Last(padded) known = false, want true")
	}
	if !rec.Time.Equal(want) || !rec.OK {
		t.Errorf("Last(padded) rec = %+v, want {%s true}", rec, want)
	}
}

// TestStampRecordTruncatesStaleTail pins that a shorter record fully replaces
// a longer previous one instead of leaving a stale tail behind.
func TestStampRecordTruncatesStaleTail(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "last-run")
	seed := "2099-12-31T23:59:59.999999999Z failed leftover-stale-tail-bytes\n"
	if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
		t.Fatalf("seeding stamp file: %v", err)
	}

	s := NewStamp(path)
	if err := s.Record(true); err != nil {
		t.Fatalf("Record over a longer record = %v, want nil", err)
	}

	rec, known := s.Last()
	if !known {
		t.Fatal("Last() after Record over a longer record: known = false, want true (stale tail not truncated)")
	}
	if !rec.OK {
		t.Error("Last().OK = false, want true (the fresh record)")
	}
	if d := time.Since(rec.Time); d < 0 || d > time.Minute {
		t.Errorf("Last().Time = %s (age %s), want a fresh near-now timestamp", rec.Time, d)
	}
}

func TestStampRecordError(t *testing.T) {
	t.Parallel()
	s := NewStamp(filepath.Join(t.TempDir(), "missing-dir", "last-run"))
	if err := s.Record(true); err == nil {
		t.Error("Record into a missing directory = nil, want an error")
	}
}

func TestStampDue(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	const interval = time.Hour

	tests := []struct {
		name    string
		age     time.Duration // ignored when absent or raw is set
		outcome string
		raw     string // raw file content overriding age/outcome
		policy  FailurePolicy
		absent  bool
		want    bool
	}{
		{name: "absent_retry", absent: true, policy: RetryFailed, want: true},
		{name: "absent_count", absent: true, policy: CountFailed, want: true},
		{name: "fresh_ok_retry", age: 30 * time.Minute, outcome: "ok", policy: RetryFailed, want: false},
		{name: "fresh_ok_count", age: 30 * time.Minute, outcome: "ok", policy: CountFailed, want: false},
		{name: "stale_ok_retry", age: 2 * time.Hour, outcome: "ok", policy: RetryFailed, want: true},
		{name: "stale_ok_count", age: 2 * time.Hour, outcome: "ok", policy: CountFailed, want: true},
		{name: "boundary_age_equals_interval", age: interval, outcome: "ok", policy: RetryFailed, want: true},
		{name: "fresh_failed_retry", age: 30 * time.Minute, outcome: "failed", policy: RetryFailed, want: true},
		{name: "fresh_failed_count", age: 30 * time.Minute, outcome: "failed", policy: CountFailed, want: false},
		{name: "stale_failed_count", age: 2 * time.Hour, outcome: "failed", policy: CountFailed, want: true},
		{name: "future_ok_retry", age: -30 * time.Minute, outcome: "ok", policy: RetryFailed, want: false},
		{name: "future_failed_retry", age: -30 * time.Minute, outcome: "failed", policy: RetryFailed, want: true},
		{name: "garbage_retry", raw: "junk\n", policy: RetryFailed, want: true},
		{name: "garbage_count", raw: "junk\n", policy: CountFailed, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "last-run")
			switch {
			case tc.absent:
				// No file.
			case tc.raw != "":
				if err := os.WriteFile(path, []byte(tc.raw), 0o600); err != nil {
					t.Fatalf("seeding stamp file: %v", err)
				}
			default:
				seedStamp(t, path, now.Add(-tc.age), tc.outcome)
			}

			if got := NewStamp(path).Due(interval, now, tc.policy); got != tc.want {
				t.Errorf("Due(%s, now, %d) with age=%s outcome=%q absent=%v = %v, want %v",
					interval, tc.policy, tc.age, tc.outcome, tc.absent, got, tc.want)
			}
		})
	}
}

func TestStampDueNonPositiveInterval(t *testing.T) {
	t.Parallel()
	path := stampPath(t)
	now := time.Now()
	seedStamp(t, path, now, "ok")
	// A non-positive interval is always due, even with a brand-new success.
	if !NewStamp(path).Due(0, now, RetryFailed) {
		t.Error("Due(0, now, RetryFailed) with a fresh success = false, want true")
	}
}

func TestStampDueUnknownPolicyPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Error("Due(unknown policy) did not panic")
		}
	}()
	// Panics regardless of stamp state: the policy is validated first, so the
	// programmer error surfaces at first boot, not after the first Record.
	NewStamp(stampPath(t)).Due(time.Hour, time.Now(), FailurePolicy(42))
}
