package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

// --- Trigger socket server ---
//
// The daemon listens on an in-container unix socket; each connection is one
// run request (see protocol.go). The socket replaces cross-process /tmp
// coordination (flock lattices, rerun flags, drain latches): mutual exclusion
// is the executor's single goroutine, shutdown reaches waiting clients as
// explicit cancellation results, and completion is an ordinary result
// delivery.

const (
	// requestReadTimeout bounds how long a connected client may take to send
	// its request line, so a silent connection cannot hold a handler
	// goroutine (and shutdown) hostage.
	requestReadTimeout = 30 * time.Second
	// eventWriteTimeout bounds each status write, so a dead client cannot
	// block a handler.
	eventWriteTimeout = 10 * time.Second
	// maxRequestBytes caps one request line. The largest fleet payload is a
	// forwarded environ, kernel-bounded to ~2 MiB per exec, so 8 MiB is
	// generous headroom; anything larger is a bug or abuse and is rejected as
	// undecodable.
	maxRequestBytes = 8 << 20
)

// ErrAddrInUse reports that path is bound by a LIVE listener, so this process
// must not become a second owner of it. Callers treat it as fatal: a consumer
// whose whole design rests on one owner per socket has nothing useful to do
// with a second.
var ErrAddrInUse = errors.New("socket already has a live listener")

// Listen binds the unix socket at path with owner-only permissions.
//
// It binds FIRST and treats EADDRINUSE as a question rather than an obstacle,
// because the kernel already supplies the single-instance guard this package's
// consumers depend on: binding a path a live listener still owns fails, and
// closing a listener unlinks its own path (both measured). So a leftover file
// means a SIGKILLed predecessor — the only case an unlink is for.
//
// It used to unlink unconditionally before binding, under a comment reasoning
// that an in-container /tmp is per-container, so the file could only be this
// daemon's own previous life's. Every consumer's CLI falsifies that: a second
// invocation in the same container reached this line, silently unlinked the
// live socket, bound the path and served the next request, while the first
// process — PID 1 — kept an unreferenced listener and said nothing. Measured:
// unlink-then-bind really does steal a live socket, and the three socket-shaped
// schedulers each publish an invariant that two passes can never overlap.
//
// On EADDRINUSE the path is DIALLED. Something answering proves a live owner,
// so this returns ErrAddrInUse naming it; nothing answering proves the socket
// is dead, so the file is unlinked and the bind retried exactly once. The retry
// is not a loop: a second EADDRINUSE after a successful unlink means another
// process bound it in between, which is the live-owner answer again.
func Listen(path string) (net.Listener, error) {
	ln, err := bindOwnerOnly(path)
	if err == nil || !errors.Is(err, syscall.EADDRINUSE) {
		return ln, err
	}
	if c, derr := dialProbe(path); derr == nil {
		_ = c.Close()
		return nil, fmt.Errorf("%w: %s", ErrAddrInUse, path)
	}
	if rerr := os.Remove(path); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
		return nil, rerr
	}
	ln, err = bindOwnerOnly(path)
	if err != nil && errors.Is(err, syscall.EADDRINUSE) {
		return nil, fmt.Errorf("%w: %s", ErrAddrInUse, path)
	}
	return ln, err
}

// probeTimeout bounds the liveness dial in Listen. A connect to a unix socket
// with a listening peer completes in the kernel, so this is not a latency budget
// — it is the guard against a dial that never returns taking boot with it.
const probeTimeout = 2 * time.Second

// dialProbe asks whether anything is listening on path. It exists as a named
// function because the answer decides between reclaiming a dead socket and
// refusing a live one, and because a bare net.Dial would carry no timeout.
//
// context.Background is correct here rather than a caller's context: Listen runs
// at single-threaded boot and takes no context (see its umask note), and the
// bound that matters is this probe's own, not the caller's lifetime.
func dialProbe(path string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	var d net.Dialer
	return d.DialContext(ctx, "unix", path)
}

// bindOwnerOnly binds path and leaves it readable and writable by its owner only.
func bindOwnerOnly(path string) (net.Listener, error) {
	// Narrow the umask so the socket is born owner-only; the Chmod below is
	// then belt-and-braces instead of closing a world-connectable window.
	//
	// The swap is PROCESS-WIDE, so this is safe only while nothing else in the
	// process is creating files: a directory born inside this window is
	// drw-------, and its unprivileged owner can then neither create entries in
	// it nor unlink from it. Production callers satisfy that by calling Listen
	// during single-threaded boot. A test suite does not get it for free — two
	// parallel tests, one binding and one calling os.MkdirTemp, are enough to
	// corrupt the second one's directory, and the failure is invisible under
	// root because root bypasses the directory permission check.
	oldMask := syscall.Umask(0o177)
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "unix", path)
	syscall.Umask(oldMask)
	if err != nil {
		return nil, err
	}
	// Owner-only: connecting requires write permission on the socket file,
	// which scopes triggering to the container's own user — the same
	// authority boundary `docker exec` already enforces, failing loudly at
	// connect for a mismatched exec user.
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// Server accepts run requests and bridges them onto the queue.
//
// The zero value is not usable; set Queue. The hooks are optional: they exist
// so the app can log acceptance and rejection in its own vocabulary and with
// its own payload attributes (the library never logs payload contents — a
// forwarded environment can carry secrets).
type Server[P any] struct {
	// Queue receives every decoded request as a TriggerExternal job.
	Queue *Queue[P]
	// OnAccepted, when non-nil, runs after a request is queued (the app's
	// "triggered run queued" line).
	OnAccepted func(payload P)
	// OnRejected, when non-nil, runs after a submission is rejected with
	// ErrFull or ErrClosed (the app's rejection warning). Undecodable
	// requests never reach it; the library logs those without the payload.
	OnRejected func(payload P, err error)

	// handlers tracks the accept loop plus per-connection goroutines so
	// shutdown can wait for every accepted request to receive its final
	// event before the daemon exits. The accept loop registers itself here
	// too, keeping the counter non-zero until Accept has failed with
	// net.ErrClosed — so no handler Add can race Wait at zero. Bounded:
	// every submitted job is guaranteed a result, and a not-yet-submitted
	// connection is bounded by requestReadTimeout.
	handlers sync.WaitGroup
}

// Serve starts the accept loop and returns immediately. Connections are
// served until the listener is closed (daemon shutdown); Wait blocks until
// the loop and every in-flight handler have finished.
func (s *Server[P]) Serve(ln net.Listener) {
	s.handlers.Go(func() { s.serve(ln) })
}

// Wait blocks until the accept loop has exited and every accepted request
// has its final event on the wire. Call after closing the listener and the
// queue.
func (s *Server[P]) Wait() {
	s.handlers.Wait()
}

// serve accepts connections until the listener is closed.
func (s *Server[P]) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Warn("trigger socket accept failed", "error", err)
			// A persistent accept error (e.g. fd exhaustion) must not
			// hot-spin: pace retries so the log gets one warn per second, not
			// a flood. Shutdown still exits promptly: ln.Close() makes the
			// next Accept return net.ErrClosed after at most this pause.
			time.Sleep(time.Second)
			continue
		}
		s.handlers.Go(func() {
			defer func() { _ = conn.Close() }()
			s.handle(conn)
		})
	}
}

// handle serves one connection: decode the request, submit it, stream events.
func (s *Server[P]) handle(conn net.Conn) {
	var payload P
	_ = conn.SetReadDeadline(time.Now().Add(requestReadTimeout))
	if err := json.NewDecoder(io.LimitReader(conn, maxRequestBytes)).Decode(&payload); err != nil {
		slog.Warn("trigger request rejected: undecodable", "error", err)
		writeEvent(conn, Event{Kind: EventDone, OK: false, Reason: "undecodable request"})
		return
	}
	j := NewJob(TriggerExternal, payload)
	if err := s.Queue.Submit(j); err != nil {
		if s.OnRejected != nil {
			s.OnRejected(payload, err)
		} else {
			slog.Warn("trigger request rejected", "reason", err)
		}
		writeEvent(conn, Event{Kind: EventDone, OK: false, Reason: err.Error()})
		return
	}
	if s.OnAccepted != nil {
		s.OnAccepted(payload)
	} else {
		slog.Info("triggered run queued")
	}
	writeEvent(conn, Event{Kind: EventQueued})

	relayEvents(conn, j)
}

// relayEvents streams the job's lifecycle to the client: a started event if
// the run begins (a job cancelled before starting delivers its result without
// ever starting, so it waits on both), then exactly one final done event.
func relayEvents[P any](conn net.Conn, j *Job[P]) {
	started := j.Started()
	for {
		select {
		case <-started:
			writeEvent(conn, Event{Kind: EventStarted})
			started = nil // block this case from now on; wait for the result
		case out := <-j.Result():
			if started != nil {
				// Both channels can be ready in one select round (a fast
				// run), so drain the start signal first: the wire order the
				// protocol documents (queued -> started -> done) must hold
				// for a run that actually started.
				select {
				case <-started:
					writeEvent(conn, Event{Kind: EventStarted})
				default:
				}
			}
			writeEvent(conn, Event{
				Kind:       EventDone,
				OK:         out.OK,
				DurationMs: out.Duration.Milliseconds(),
				Reason:     out.Reason,
			})
			return
		}
	}
}

// writeEvent sends one status line, best-effort: a departed client only
// forfeits its own visibility (the run itself is daemon-owned and its result
// delivery never blocks on the connection).
func writeEvent(conn net.Conn, ev Event) {
	_ = conn.SetWriteDeadline(time.Now().Add(eventWriteTimeout))
	if err := json.NewEncoder(conn).Encode(ev); err != nil {
		slog.Debug("trigger event write failed (client gone?)", "event", ev.Kind, "error", err)
	}
}
