package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"
)

// --- The trigger client ---
//
// A thin synchronous client for the daemon's trigger socket: it forwards one
// request and blocks until the daemon reports the run's result. The consuming
// app's subcommand maps the returned final event (and sentinel errors) to its
// own exit code and lifecycle log lines; the transport, the event ordering,
// and the failure taxonomy live here.

// DialTimeout bounds the connection attempt: the daemon is PID 1 in the same
// container, so anything slower than instant means it is not accepting.
const DialTimeout = 5 * time.Second

// Submit failure classes, distinguishable with errors.Is so the app can log
// each in its own vocabulary.
var (
	// ErrUnreachable wraps a failed dial: no daemon is accepting on the
	// socket (container down, or a mismatched exec user against the
	// owner-only socket file).
	ErrUnreachable = errors.New("cannot reach the scheduler daemon")
	// ErrSend wraps a failed request write.
	ErrSend = errors.New("cannot send trigger request")
	// ErrConnectionLost wraps an event stream that ended before the final
	// done event: the daemon died or was stopped mid-run.
	ErrConnectionLost = errors.New("connection lost before the run completed")
)

// Submit performs one triggered run via the daemon at socketPath: it sends
// payload as the request line, relays each intermediate lifecycle event to
// onEvent (EventQueued, EventStarted; nil onEvent skips relaying; unknown
// kinds are ignored for forward compatibility), and returns the final done
// event. A non-nil error wraps ErrUnreachable, ErrSend, or ErrConnectionLost,
// or is ctx's own error when the caller cancelled; the Event is only
// meaningful when the error is nil.
//
// Submit blocks for the run's full queue-wait plus execution — triggered runs
// are synchronous by contract (the trigger's exit code is the run's result),
// so there is deliberately no read deadline on the event stream. ctx is how a
// caller bounds that wait instead: cancelling it aborts the dial and closes
// the connection under an in-flight read, so Submit returns context.Canceled
// (or context.DeadlineExceeded) rather than blocking until the daemon
// answers. A subcommand should pass signal.NotifyContext so an interactive
// Ctrl-C unwinds and the daemon observes the disconnect, instead of the
// process being killed with the connection half-open.
func Submit[P any](ctx context.Context, socketPath string, payload P, onEvent func(Event)) (Event, error) {
	dialer := net.Dialer{Timeout: DialTimeout}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return Event{}, fmt.Errorf("%w: %w", ErrUnreachable, err)
	}
	defer func() { _ = conn.Close() }()

	// A context has no way to interrupt a blocking socket read, so closing the
	// connection is what unblocks awaitDone. Without this, ctx would bound the
	// dial only — and the dial is the one part of Submit that is already
	// bounded, by DialTimeout.
	stopOnCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopOnCancel()

	if err := json.NewEncoder(conn).Encode(payload); err != nil {
		return Event{}, fmt.Errorf("%w: %w", ErrSend, err)
	}
	return awaitDone(ctx, json.NewDecoder(conn), onEvent)
}

// awaitDone consumes the daemon's event stream until the final done event.
func awaitDone(ctx context.Context, dec *json.Decoder, onEvent func(Event)) (Event, error) {
	for {
		var ev Event
		if err := dec.Decode(&ev); err != nil {
			// Cancellation reaches this read by closing the connection, so the
			// decode error is the symptom and ctx.Err() is the cause. Report
			// the cause: a caller distinguishing "I gave up" from "the daemon
			// died mid-run" is the whole point of the two classes.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return Event{}, ctxErr
			}
			return Event{}, fmt.Errorf("%w: %w", ErrConnectionLost, err)
		}
		switch ev.Kind {
		case EventQueued, EventStarted:
			if onEvent != nil {
				onEvent(ev)
			}
		case EventDone:
			return ev, nil
		default:
			slog.Debug("ignoring unknown event", "event", ev.Kind)
		}
	}
}
