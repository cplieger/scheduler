package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// TestSubmit_FinalEventOverRealSocket pins the synchronous trigger contract
// end-to-end (the surface every `run`/`sync`/`scan` subcommand maps to its
// exit code): a clean run returns done ok=true, a failing run done ok=false,
// and the intermediate lifecycle reaches onEvent in wire order.
func TestSubmit_FinalEventOverRealSocket(t *testing.T) {
	tests := []struct {
		name   string
		out    Outcome
		wantOK bool
	}{
		{"clean run reports ok", Outcome{OK: true, Duration: time.Millisecond}, true},
		{"failed run reports not ok", Outcome{OK: false, Reason: "job failed"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sock, _ := startTestServer(t, func(j *Job[testPayload]) {
				j.Start()
				j.Finish(tt.out)
			})
			var kinds []string
			final, err := Submit(t.Context(), sock, testPayload{Repos: []string{"owner/repo"}}, func(ev Event) {
				kinds = append(kinds, ev.Kind)
			})
			if err != nil {
				t.Fatalf("Submit() error = %v, want nil", err)
			}
			if final.Kind != EventDone || final.OK != tt.wantOK {
				t.Errorf("final event = %+v, want done ok=%v", final, tt.wantOK)
			}
			if want := []string{EventQueued, EventStarted}; len(kinds) != 2 || kinds[0] != want[0] || kinds[1] != want[1] {
				t.Errorf("onEvent saw %v, want %v", kinds, want)
			}
		})
	}
}

// TestSubmit_DaemonUnreachable pins the no-daemon failure mode: an immediate
// ErrUnreachable (the trigger reports a failed job), never a hang.
func TestSubmit_DaemonUnreachable(t *testing.T) {
	t.Parallel()
	sock := absentSocketPath(t)
	_, err := Submit(t.Context(), sock, struct{}{}, nil)
	if !errors.Is(err, ErrUnreachable) {
		t.Errorf("Submit() error = %v, want ErrUnreachable", err)
	}
}

// TestAwaitDone_StreamHandling pins the client's event-stream tolerance: an
// unrecognized event is ignored (forward compatibility, never fatal), while
// a stream that ends before the done event — the daemon died or was stopped
// mid-run — returns ErrConnectionLost so the trigger reports a failed job
// instead of a false success.
func TestAwaitDone_StreamHandling(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		stream  string
		wantErr error
		wantOK  bool
	}{
		{
			name:   "unknown events are ignored, not fatal",
			stream: `{"event":"queued"}` + "\n" + `{"event":"future-extension"}` + "\n" + `{"event":"done","ok":true}` + "\n",
			wantOK: true,
		},
		{
			name:    "stream truncated before done errors",
			stream:  `{"event":"queued"}` + "\n" + `{"event":"started"}` + "\n",
			wantErr: ErrConnectionLost,
		},
		{
			name:    "immediate EOF errors",
			stream:  "",
			wantErr: ErrConnectionLost,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			final, err := awaitDone(t.Context(), json.NewDecoder(strings.NewReader(tt.stream)), nil)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("awaitDone(%q) error = %v, want %v", tt.stream, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("awaitDone(%q) error = %v, want nil", tt.stream, err)
			}
			if final.OK != tt.wantOK {
				t.Errorf("awaitDone(%q) ok = %v, want %v", tt.stream, final.OK, tt.wantOK)
			}
		})
	}
}

// TestSubmit_ReasonAndDurationReachTheClient pins the passthrough of the
// outcome's annotations: an OK result can carry a reason (an app-defined
// skip tolerance) and the duration lands in milliseconds.
func TestSubmit_ReasonAndDurationReachTheClient(t *testing.T) {
	sock, _ := startTestServer(t, func(j *Job[testPayload]) {
		j.Start()
		j.Finish(Outcome{OK: true, Reason: "skipped: lock held by another container", Duration: 1500 * time.Millisecond})
	})
	final, err := Submit(t.Context(), sock, testPayload{}, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !final.OK || final.Reason == "" || final.DurationMs != 1500 {
		t.Errorf("final = %+v, want ok with reason and duration_ms=1500", final)
	}
}

// TestSubmit_CancellationAbortsTheWait pins the capability the context
// parameter exists for: a triggered run is synchronous by contract and has no
// read deadline, so before ctx a caller had no way to stop waiting on a daemon
// that had accepted the job and gone quiet. The subcommand shape this serves is
// signal.NotifyContext, where an operator's Ctrl-C must unwind rather than kill
// the process with the connection half-open.
//
// The server here starts the job and never finishes it, so the only way Submit
// returns is cancellation.
func TestSubmit_CancellationAbortsTheWait(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	sock, _ := startTestServer(t, func(j *Job[testPayload]) {
		j.Start()
		close(started)
		<-release // never finish the job; the client must give up on its own
		j.Finish(Outcome{OK: false, Reason: "abandoned"})
	})
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-started
		cancel()
	}()

	_, err := Submit(ctx, sock, testPayload{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Submit() error = %v, want context.Canceled", err)
	}
	// The cancellation must not be reported as a lost connection: the caller
	// distinguishes "I gave up" from "the daemon died mid-run".
	if errors.Is(err, ErrConnectionLost) {
		t.Errorf("Submit() error = %v, want context.Canceled and NOT ErrConnectionLost", err)
	}
}

// TestSubmit_CancelledDialReportsTheCancellation pins the doc's cancellation
// clause on the DIAL arm. A cancelled DialContext error satisfies errors.Is for
// context.Canceled AND for whatever class it is wrapped in, so before the fix
// this arm returned an error matching both sentinels at once — and a caller
// testing the transport sentinel first, the order this package documents them
// in, diagnosed an operator's Ctrl-C as an unreachable daemon. The daemon here
// is up and listening, so ErrUnreachable would be false as well as unhelpful.
func TestSubmit_CancelledDialReportsTheCancellation(t *testing.T) {
	// Not parallel: Listen swaps the process-wide umask, so this test's own
	// socket directory must not be created while a sibling holds that window.
	// See TestSubmit_CancelledSendReportsTheCancellation for the measurement.
	sock, _ := startTestServer(t, runOK)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := Submit(ctx, sock, testPayload{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Submit() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrUnreachable) {
		t.Errorf("Submit() error = %v, want context.Canceled and NOT ErrUnreachable "+
			"(the daemon is listening; the caller cancelled)", err)
	}
}

// TestSubmit_CancelledSendReportsTheCancellation pins the same clause on the
// SEND arm, which is the harder half: cancellation reaches an in-flight write
// by closing the connection, so the write fails with net.ErrClosed and its
// chain carries no context error at all. No errors.Is test over the documented
// taxonomy could identify it, and unlike the dial arm no reordering in the
// caller reaches it — only consulting ctx.Err() does.
//
// The listener accepts and never reads, so a payload larger than the socket
// send buffer leaves Submit blocked inside Encode when the cancellation lands.
func TestSubmit_CancelledSendReportsTheCancellation(t *testing.T) {
	// Deliberately NOT parallel, and the reason is a process-wide side effect
	// rather than anything about this test: Listen narrows the umask to 0o177
	// around its bind, so a directory another goroutine creates inside that
	// window is born drw------- and its unprivileged owner can then neither
	// bind in it nor unlink from it. Running here, in the sequential phase, is
	// what keeps this test's own MkdirTemp out of a sibling's window.
	//
	// Measured: as a parallel test this reddened the coverage job on a real
	// runner, taking a pre-existing example down with it, while passing every
	// local run — because a root container bypasses the directory permission
	// check that an unprivileged CI user does not.
	sock := testSocketPath(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("Listen(%q) = %v", sock, err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		close(accepted)
		<-release // never read: the client's write must fill the buffer and block
		_ = conn.Close()
	}()

	// ~2 MB, well past any unix-socket send buffer.
	big := testPayload{Repos: make([]string, 200_000)}
	for i := range big.Repos {
		big.Repos[i] = "owner/repo"
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-accepted // the dial has returned, so a cancellation now lands on the write
		cancel()
	}()

	_, err = Submit(ctx, sock, big, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Submit() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrSend) {
		t.Errorf("Submit() error = %v, want context.Canceled and NOT ErrSend "+
			"(the send failed because the caller cancelled)", err)
	}
}
