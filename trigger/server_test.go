package trigger

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// startTestServer wires a queue + fake executor + server on a temp socket.
// run receives each job and owns Start/Finish; everything tears down via
// t.Cleanup (listener close, queue close, executor drain, handler wait).
func startTestServer(t *testing.T, run func(*Job[testPayload])) (sock string, srv *Server[testPayload]) {
	t.Helper()
	sock = testSocketPath(t)
	q := NewQueue[testPayload](16)
	execDone := startExecutor(q, run)

	ln, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen() = %v", err)
	}
	srv = &Server[testPayload]{Queue: q}
	srv.Serve(ln)

	t.Cleanup(func() {
		_ = ln.Close()
		q.Close()
		<-execDone
		srv.Wait()
	})
	return sock, srv
}

// runOK is the trivial executor: every job starts and finishes clean.
func runOK(j *Job[testPayload]) {
	j.Start()
	j.Finish(Outcome{OK: true, Duration: time.Millisecond})
}

// rawRequest dials the socket, sends one raw request line, and returns the
// decoder over the event stream.
func rawRequest(t *testing.T, sock string, payload any) *json.Decoder {
	t.Helper()
	conn, err := net.DialTimeout("unix", sock, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := json.NewEncoder(conn).Encode(payload); err != nil {
		t.Fatalf("send request: %v", err)
	}
	return json.NewDecoder(conn)
}

// nextEvent decodes one event with a test deadline.
func nextEvent(t *testing.T, dec *json.Decoder) Event {
	t.Helper()
	var ev Event
	done := make(chan error, 1)
	go func() { done <- dec.Decode(&ev) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("decode event: %v", err)
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no event within 5s")
		return Event{}
	}
}

// TestListen_RemovesStaleSocketAndSetsOwnerOnly pins the boot hygiene: a
// stale socket file from a SIGKILLed predecessor is replaced, and the live
// socket is owner-only (triggering scoped to the container's user).
// Not parallel: Listen temporarily changes the process-wide umask.
func TestListen_RemovesStaleSocketAndSetsOwnerOnly(t *testing.T) {
	sock := testSocketPath(t)

	stale, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("setup stale socket: %v", err)
	}
	// Simulate a SIGKILL: the file stays, nobody listens. Closing the
	// listener would remove the file, so leak it deliberately and only
	// unlink-guard via Listen.
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = stale.Close()
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("stale socket file missing after setup: %v", err)
	}

	ln, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen() over a stale socket = %v, want nil", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	info, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat live socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket permissions = %o, want 0600 (owner-only trigger authority)", perm)
	}
}

// TestListen_UnremovableStaleSocketFailsBoot pins the stale-socket hygiene's
// failure mode: when the stale file exists but cannot be unlinked (no write
// permission on the parent), Listen must surface that permission error —
// failing boot loudly — rather than proceeding to a bind that would fail
// with a misleading address-in-use error.
func TestListen_UnremovableStaleSocketFailsBoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	parent := t.TempDir()
	sock := filepath.Join(parent, "trigger.sock")
	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatalf("setup stale file: %v", err)
	}
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })

	ln, err := Listen(sock)
	if err == nil {
		_ = ln.Close()
		t.Fatal("Listen() = nil error over an unremovable stale socket, want the unlink's permission error")
	}
	if !errors.Is(err, fs.ErrPermission) {
		t.Errorf("Listen() error = %v, want a permission error from the stale-file unlink (not a bind failure)", err)
	}
}

// TestServer_EventSequenceForCleanRun pins the wire contract: queued →
// started → done{ok:true}, in that order, one done exactly.
func TestServer_EventSequenceForCleanRun(t *testing.T) {
	sock, _ := startTestServer(t, runOK)
	dec := rawRequest(t, sock, testPayload{Repos: []string{"owner/repo"}})

	if ev := nextEvent(t, dec); ev.Kind != EventQueued {
		t.Fatalf("first event = %q, want %q", ev.Kind, EventQueued)
	}
	if ev := nextEvent(t, dec); ev.Kind != EventStarted {
		t.Fatalf("second event = %q, want %q", ev.Kind, EventStarted)
	}
	ev := nextEvent(t, dec)
	if ev.Kind != EventDone || !ev.OK {
		t.Fatalf("final event = %+v, want done ok=true", ev)
	}
}

// TestServer_PayloadReplaysExactlyToTheExecutor is the broker's core
// regression test: the request's payload reaches the executor's job
// unchanged — the arguments a queued trigger submitted are the arguments its
// run gets (the property the old cross-process designs kept losing).
func TestServer_PayloadReplaysExactlyToTheExecutor(t *testing.T) {
	var mu sync.Mutex
	var seen []testPayload
	sock, _ := startTestServer(t, func(j *Job[testPayload]) {
		mu.Lock()
		seen = append(seen, j.Payload)
		mu.Unlock()
		if j.Trigger != TriggerExternal {
			t.Errorf("socket job trigger = %q, want %q", j.Trigger, TriggerExternal)
		}
		runOK(j)
	})

	dec := rawRequest(t, sock, testPayload{Repos: []string{"cplieger/homelab", "cplieger/ci"}})
	for {
		if ev := nextEvent(t, dec); ev.Kind == EventDone {
			break
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || len(seen[0].Repos) != 2 || seen[0].Repos[0] != "cplieger/homelab" {
		t.Errorf("executor saw payloads %+v, want the request's exact repos", seen)
	}
}

// TestServer_FailedRunReportsNotOK pins the exit-code half of the trigger
// contract at the wire level, including the reason passthrough.
func TestServer_FailedRunReportsNotOK(t *testing.T) {
	sock, _ := startTestServer(t, func(j *Job[testPayload]) {
		j.Start()
		j.Finish(Outcome{OK: false, Reason: "job exploded", Duration: time.Millisecond})
	})
	dec := rawRequest(t, sock, testPayload{})
	for {
		ev := nextEvent(t, dec)
		if ev.Kind != EventDone {
			continue
		}
		if ev.OK {
			t.Error("done ok=true for a failing run, want false")
		}
		if ev.Reason != "job exploded" {
			t.Errorf("reason = %q, want the outcome's reason on the wire", ev.Reason)
		}
		return
	}
}

// TestServer_RejectsWhenQueueFull pins honest backpressure: a full queue
// answers immediately with done{ok:false, reason} instead of queueing
// unboundedly or blocking the trigger, and the rejection reaches the
// OnRejected hook with the decoded payload.
func TestServer_RejectsWhenQueueFull(t *testing.T) {
	sock := testSocketPath(t)

	// No executor: jobs sit in the queue. Capacity 1, pre-filled.
	q := NewQueue[testPayload](1)
	if err := q.Submit(NewJob(TriggerExternal, testPayload{})); err != nil {
		t.Fatalf("pre-fill submit: %v", err)
	}
	ln, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen() = %v", err)
	}
	var mu sync.Mutex
	var rejected []error
	srv := &Server[testPayload]{Queue: q, OnRejected: func(_ testPayload, err error) {
		mu.Lock()
		rejected = append(rejected, err)
		mu.Unlock()
	}}
	srv.Serve(ln)
	t.Cleanup(func() { _ = ln.Close(); srv.Wait() })

	dec := rawRequest(t, sock, testPayload{})
	ev := nextEvent(t, dec)
	if ev.Kind != EventDone || ev.OK {
		t.Fatalf("event = %+v, want immediate done ok=false", ev)
	}
	if !strings.Contains(ev.Reason, "full") {
		t.Errorf("reason = %q, want a queue-full explanation", ev.Reason)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(rejected) != 1 || !errors.Is(rejected[0], ErrFull) {
		t.Errorf("OnRejected saw %v, want exactly one ErrFull", rejected)
	}
}

// TestServer_UndecodableRequestAnswersDone pins the protocol's failure mode
// for a malformed client: an explicit done with a reason, never a hang.
func TestServer_UndecodableRequestAnswersDone(t *testing.T) {
	sock, _ := startTestServer(t, runOK)
	conn, err := net.DialTimeout("unix", sock, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write([]byte("this is not json\n")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	dec := json.NewDecoder(conn)
	ev := nextEvent(t, dec)
	if ev.Kind != EventDone || ev.OK {
		t.Fatalf("event = %+v, want done ok=false for an undecodable request", ev)
	}
}

// TestServer_HooksReceiveAcceptedPayload pins the acceptance hook: the app's
// own "triggered run queued" line gets the decoded payload.
func TestServer_HooksReceiveAcceptedPayload(t *testing.T) {
	accepted := make(chan testPayload, 1)
	sock := testSocketPath(t)
	q := NewQueue[testPayload](16)
	execDone := startExecutor(q, runOK)
	ln, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen() = %v", err)
	}
	srv := &Server[testPayload]{Queue: q, OnAccepted: func(p testPayload) { accepted <- p }}
	srv.Serve(ln)
	t.Cleanup(func() { _ = ln.Close(); q.Close(); <-execDone; srv.Wait() })

	dec := rawRequest(t, sock, testPayload{Repos: []string{"owner/hooked"}})
	for {
		if ev := nextEvent(t, dec); ev.Kind == EventDone {
			break
		}
	}
	select {
	case p := <-accepted:
		if len(p.Repos) != 1 || p.Repos[0] != "owner/hooked" {
			t.Errorf("OnAccepted payload = %+v, want the request's repos", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnAccepted never ran for an accepted request")
	}
}

// TestServer_ShutdownCancelsQueuedRequestWithExplicitResult pins the
// shutdown contract on the wire: a request queued behind an in-flight run
// receives done{ok:false, reason} when the daemon stops — the trigger
// reports a failed job instead of hanging or being silently dropped — while
// the in-flight run drains with its real outcome.
func TestServer_ShutdownCancelsQueuedRequestWithExplicitResult(t *testing.T) {
	sock := testSocketPath(t)
	q := NewQueue[testPayload](16)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	shutdown := make(chan struct{})
	execDone := startExecutor(q, func(j *Job[testPayload]) {
		select {
		case <-shutdown:
			// Admission stopped: cancel instead of run (the app executor's
			// shutdown policy, emulated).
			j.Finish(Outcome{OK: false, Reason: "cancelled: scheduler shutting down"})
		default:
			j.Start()
			close(entered)
			<-proceed
			j.Finish(Outcome{OK: true, Duration: time.Millisecond})
		}
	})

	ln, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen() = %v", err)
	}
	srv := &Server[testPayload]{Queue: q}
	srv.Serve(ln)

	// Occupy the executor, then queue a second request over the wire.
	decA := rawRequest(t, sock, testPayload{})
	<-entered
	decB := rawRequest(t, sock, testPayload{Repos: []string{"owner/queued"}})
	if ev := nextEvent(t, decB); ev.Kind != EventQueued {
		t.Fatalf("B's first event = %q, want queued", ev.Kind)
	}

	// Daemon shutdown while A runs and B waits.
	close(shutdown)
	_ = ln.Close()
	q.Close()
	close(proceed) // A completes its run

	// A: full drain — its run finished with its real (clean) outcome.
	for {
		ev := nextEvent(t, decA)
		if ev.Kind != EventDone {
			continue
		}
		if !ev.OK {
			t.Error("in-flight run reported ok=false at shutdown, want true (drained, not abandoned)")
		}
		break
	}
	// B: explicit cancellation.
	for {
		ev := nextEvent(t, decB)
		if ev.Kind != EventDone {
			continue
		}
		if ev.OK {
			t.Error("queued request reported ok=true at shutdown, want cancelled")
		}
		if !strings.Contains(ev.Reason, "shutting down") {
			t.Errorf("cancellation reason = %q, want a shutting-down explanation", ev.Reason)
		}
		break
	}

	<-execDone
	srv.Wait()
}

// TestServer_DepartedClientDoesNotBlockRunOrShutdown pins writeEvent's
// best-effort contract: a client that disconnects right after submitting
// forfeits only its own visibility — the run still executes, and the handler
// goroutine still terminates so shutdown never hangs on a dead connection.
func TestServer_DepartedClientDoesNotBlockRunOrShutdown(t *testing.T) {
	ran := make(chan struct{})
	proceed := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(proceed) }) }) // never leave the run held on an early exit
	sock, _ := startTestServer(t, func(j *Job[testPayload]) {
		close(ran)
		<-proceed // hold job completion until the client has definitely departed
		j.Start()
		j.Finish(Outcome{OK: true})
	})

	conn, err := net.DialTimeout("unix", sock, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := json.NewEncoder(conn).Encode(testPayload{Repos: []string{"owner/gone"}}); err != nil {
		t.Fatalf("send request: %v", err)
	}
	_ = conn.Close() // depart before any event arrives
	// Only now let the run complete: the done write is guaranteed to hit the
	// closed connection, exercising writeEvent's error branch.
	release.Do(func() { close(proceed) })

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not execute after the client departed")
	}
	// startTestServer's cleanup does srv.Wait(); reaching the end of the
	// test without a hang is the shutdown half of the assertion.
}

// acceptResult is one scripted Accept outcome for fakeListener.
type acceptResult struct {
	conn net.Conn
	err  error
}

// fakeListener scripts Accept outcomes; a closed channel yields net.ErrClosed
// (the shutdown signal serve exits on).
type fakeListener struct {
	results chan acceptResult
}

func (l *fakeListener) Accept() (net.Conn, error) {
	r, ok := <-l.results
	if !ok {
		return nil, net.ErrClosed
	}
	return r.conn, r.err
}
func (l *fakeListener) Close() error   { return nil }
func (l *fakeListener) Addr() net.Addr { return &net.UnixAddr{Name: "fake", Net: "unix"} }

// TestServe_ContinuesAfterTransientAcceptError pins the accept loop's
// degradation contract: a transient Accept error (fd exhaustion) is logged
// and retried — the connection accepted AFTER the error is still served — and
// net.ErrClosed still terminates the loop. A regression that treats every
// error as fatal would strand the daemon's trigger socket after one blip.
// Not parallel: captures the process-global slog default.
func TestServe_ContinuesAfterTransientAcceptError(t *testing.T) {
	rec := captureLogs(t)

	serverConn, clientConn := net.Pipe()
	ln := &fakeListener{results: make(chan acceptResult, 2)}
	ln.results <- acceptResult{err: errors.New("accept tcp: too many open files")}
	ln.results <- acceptResult{conn: serverConn}
	close(ln.results) // third Accept: net.ErrClosed -> serve returns

	// No executor and a pre-filled capacity-1 queue: the request accepted
	// after the transient error is answered immediately with done{queue full}.
	q := NewQueue[testPayload](1)
	if err := q.Submit(NewJob(TriggerExternal, testPayload{})); err != nil {
		t.Fatalf("pre-fill submit: %v", err)
	}
	srv := &Server[testPayload]{Queue: q}
	serveDone := make(chan struct{})
	go func() { defer close(serveDone); srv.serve(ln) }()

	_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(clientConn).Encode(testPayload{}); err != nil {
		t.Fatalf("send request: %v (serve stopped accepting after the transient error)", err)
	}
	var ev Event
	if err := json.NewDecoder(clientConn).Decode(&ev); err != nil {
		t.Fatalf("decode event after transient accept error: %v (serve did not keep accepting)", err)
	}
	if ev.Kind != EventDone || ev.OK {
		t.Errorf("event = %+v, want done ok=false (queue full) from the post-error connection", ev)
	}
	_ = clientConn.Close()

	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return on net.ErrClosed")
	}
	warns := 0
	for _, r := range rec.snapshot() {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, "trigger socket accept failed") {
			warns++
		}
	}
	if warns != 1 {
		t.Errorf("accept-failure warnings = %d, want 1 (the transient error must be logged)", warns)
	}
	srv.Wait()
}

// TestRelayEvents_FastRunEmitsStartedBeforeDone pins the documented wire
// order (queued -> started -> done) for a run so fast that the started signal
// and the result are BOTH ready when relayEvents wakes: the done branch must
// drain the pending started event first, never emit done alone. Iterated
// because the select choice between two ready channels is randomized.
func TestRelayEvents_FastRunEmitsStartedBeforeDone(t *testing.T) {
	t.Parallel()
	for i := range 20 {
		j := NewJob(TriggerExternal, testPayload{})
		j.Start()
		j.Finish(Outcome{OK: true, Duration: 42 * time.Millisecond})

		server, client := net.Pipe()
		relayDone := make(chan struct{})
		go func() {
			defer close(relayDone)
			defer func() { _ = server.Close() }()
			relayEvents(server, j)
		}()

		_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
		dec := json.NewDecoder(client)
		var first, second Event
		if err := dec.Decode(&first); err != nil {
			t.Fatalf("iteration %d: decode first event: %v", i, err)
		}
		if err := dec.Decode(&second); err != nil {
			t.Fatalf("iteration %d: decode second event: %v", i, err)
		}
		_ = client.Close()
		<-relayDone

		if first.Kind != EventStarted {
			t.Fatalf("iteration %d: first event = %q, want %q (a run that started must report started before done, even when both signals are ready in one select round)", i, first.Kind, EventStarted)
		}
		if second.Kind != EventDone || !second.OK {
			t.Fatalf("iteration %d: second event = %+v, want done ok=true", i, second)
		}
	}
}
