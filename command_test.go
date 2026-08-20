package scheduler

import (
	"bufio"
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestNewCommandRunnerConstruction(t *testing.T) {
	t.Parallel()
	cmd := NewCommandRunner(9*time.Second)(t.Context(), "echo", "hi", "there")
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
	cmd := NewCommandRunner(0)(t.Context(), "echo")
	if cmd.WaitDelay != DefaultGrace {
		t.Errorf("WaitDelay = %s, want DefaultGrace %s", cmd.WaitDelay, DefaultGrace)
	}
}

func TestNewCommandRunnerRuns(t *testing.T) {
	t.Parallel()
	if err := NewCommandRunner(DefaultGrace)(t.Context(), "true").Run(); err != nil {
		t.Errorf("running `true` = %v, want nil", err)
	}
}

func TestNewCommandRunnerCancelSendsSIGTERM(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	// The child traps SIGTERM and exits 42; a default os/exec cancel would
	// SIGKILL it (uncatchable, no exit 42), so observing 42 proves SIGTERM.
	//
	// The child echoes a line AFTER installing the trap, and this test reads it
	// before cancelling. That is a handshake on the thing under test rather
	// than a sleep long enough to probably work: a fixed pause can cancel
	// before the trap is installed on a loaded runner, and the child then dies
	// to the default SIGTERM disposition with no exit 42 -- a failure that
	// looks like the production bug. synctest cannot help here, because a
	// goroutine holding a live process never becomes durably blocked, so the
	// bubble's clock could not advance.
	cmd := NewCommandRunner(2*time.Second)(ctx, "sh", "-c",
		"trap 'exit 42' TERM; echo trapped; sleep 30 & wait")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe = %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start = %v", err)
	}

	ready := bufio.NewReader(stdout)
	line, err := ready.ReadString('\n')
	if err != nil {
		t.Fatalf("reading the child's ready line = %v (want %q)", err, "trapped\n")
	}
	if got := strings.TrimSpace(line); got != "trapped" {
		t.Fatalf("child ready line = %q, want %q", got, "trapped")
	}
	cancel()

	err = cmd.Wait()
	exitErr, ok := errors.AsType[*exec.ExitError](err)
	if !ok {
		t.Fatalf("Wait err = %v, want an *exec.ExitError", err)
	}
	if exitErr.ExitCode() != 42 {
		t.Errorf("exit code = %d, want 42 (proves the child caught SIGTERM)", exitErr.ExitCode())
	}
}
