package scheduler

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// TestStampDueAndRemainingAgree pins the invariant consumers wire on: Due is
// true exactly when Remaining is zero, and Remaining never exceeds the
// interval, across record presence, outcome, age (past and future), and both
// policies.
func TestStampDueAndRemainingAgree(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	var iteration int

	rapid.Check(t, func(t *rapid.T) {
		iteration++
		interval := time.Duration(rapid.Int64Range(1, int64(48*time.Hour)).Draw(t, "interval"))
		policy := rapid.SampledFrom([]FailurePolicy{RetryFailed, CountFailed}).Draw(t, "policy")
		path := filepath.Join(dir, fmt.Sprintf("last-run-%d", iteration))

		if rapid.Bool().Draw(t, "recorded") {
			age := time.Duration(rapid.Int64Range(-2*int64(interval), 2*int64(interval)).Draw(t, "age"))
			outcome := rapid.SampledFrom([]string{"ok", "failed"}).Draw(t, "outcome")
			line := now.Add(-age).UTC().Format(time.RFC3339Nano) + " " + outcome + "\n"
			if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
				t.Fatalf("seeding stamp file: %v", err)
			}
		}

		s := NewStamp(path)
		due := s.Due(interval, now, policy)
		rem := s.Remaining(interval, now, policy)

		if due != (rem == 0) {
			t.Fatalf("Due(%s, now, %d) = %v but Remaining = %s, want Due exactly when Remaining is 0",
				interval, policy, due, rem)
		}
		if rem < 0 || rem > interval {
			t.Fatalf("Remaining(%s, now, %d) = %s, want within [0, %s]", interval, policy, rem, interval)
		}
	})
}
