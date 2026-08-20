package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
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
	sock := filepath.Join(t.TempDir(), "absent.sock")
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
