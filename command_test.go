package scheduler

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"
)

func TestNewCommandRunnerConstruction(t *testing.T) {
	t.Parallel()
	cmd := NewCommandRunner(9*time.Second)(context.Background(), "echo", "hi", "there")
	if cmd.WaitDelay != 9*time.Second {
		t.Errorf("WaitDelay = %s, want 9s", cmd.WaitDelay)
	}
	if cmd.Cancel == nil {
		t.Error("Cancel is nil, want the SIGTERM closure")
	}
	if len(cmd.Args) != 3 || cmd.Args[1] != "hi" || cmd.Args[2] != "there" {
		t.Errorf("Args = %v, want the verbatim [echo hi there]", cmd.Args)
	}
}

func TestNewCommandRunnerDefaultGrace(t *testing.T) {
	t.Parallel()
	cmd := NewCommandRunner(0)(context.Background(), "echo")
	if cmd.WaitDelay != DefaultGrace {
		t.Errorf("WaitDelay = %s, want DefaultGrace %s", cmd.WaitDelay, DefaultGrace)
	}
}

func TestNewCommandRunnerRuns(t *testing.T) {
	t.Parallel()
	if err := NewCommandRunner(DefaultGrace)(context.Background(), "true").Run(); err != nil {
		t.Errorf("running `true` = %v, want nil", err)
	}
}

func TestNewCommandRunnerCancelSendsSIGTERM(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	// The child traps SIGTERM and exits 42; a default os/exec cancel would
	// SIGKILL it (uncatchable, no exit 42), so observing 42 proves SIGTERM.
	cmd := NewCommandRunner(2*time.Second)(ctx, "sh", "-c", "trap 'exit 42' TERM; sleep 30 & wait")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the trap install before cancelling
	cancel()

	err := cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("Wait err = %v, want an *exec.ExitError", err)
	}
	if exitErr.ExitCode() != 42 {
		t.Errorf("exit code = %d, want 42 (proves the child caught SIGTERM)", exitErr.ExitCode())
	}
}
