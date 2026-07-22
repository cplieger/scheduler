package trigger

import (
	"context"
	"encoding/json"
	"errors"
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

// Listen binds the unix socket at path with owner-only permissions. A stale
// socket file from a SIGKILLed predecessor is removed first (bind fails on an
// existing path otherwise); an in-container /tmp is per-container, so the
// stale file can only be the daemon's own previous life's.
func Listen(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// Narrow the umask so the socket is born owner-only; the Chmod below is
	// then belt-and-braces instead of closing a world-connectable window.
	// Callers run Listen during single-threaded boot, before any other
	// goroutine creates files, so the process-wide umask swap is safe.
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
