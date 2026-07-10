package scheduler

import (
	"context"
	"os/exec"
	"syscall"
	"time"
)

// DefaultGrace is the graceful-shutdown grace period the scheduling apps use:
// on context cancellation the child is sent SIGTERM and given this
// long to exit before os/exec escalates to SIGKILL.
const DefaultGrace = 5 * time.Second

// CommandRunner constructs a configured *exec.Cmd for a context and argument
// vector. It decouples job orchestration from subprocess construction so tests
// can inject a fake runner; NewCommandRunner returns the production one.
type CommandRunner func(ctx context.Context, name string, args ...string) *exec.Cmd

// NewCommandRunner returns a CommandRunner that builds a context-cancellable
// command with graceful shutdown: on cancellation the child is sent SIGTERM
// (rather than os/exec's default SIGKILL) and given grace before the SIGKILL
// escalation. The caller wires Stdout/Stderr on the returned command — capture
// them into buffers, or stream them to os.Stdout/os.Stderr — before calling
// Run. A non-positive grace uses DefaultGrace.
func NewCommandRunner(grace time.Duration) CommandRunner {
	if grace <= 0 {
		grace = DefaultGrace
	}
	return func(ctx context.Context, name string, args ...string) *exec.Cmd {
		// #nosec G204 -- name and args are consumer-supplied (operator config),
		// not an untrusted boundary; this is a generic subprocess runner.
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
		cmd.WaitDelay = grace
		return cmd
	}
}
