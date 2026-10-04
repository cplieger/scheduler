# Trigger broker

This page covers the `trigger` package, for a daemon that runs every job itself. It is for developers whose triggers must each get their own run and their own result.

## How it works

`Exclusive` coordinates runs across processes. The `trigger` package is the alternative for a daemon that owns every run. One daemon owns every job execution, and triggers only submit requests. Triggers include the built-in ticker and each subcommand started with `docker exec`.

- One bounded FIFO `trigger.Queue` feeds one executor goroutine, and that single goroutine is what keeps runs from overlapping.
- A `trigger.Server` accepts requests on an owner-only unix socket inside the container and streams `queued`, `started` and `done` events back.
- `trigger.Submit` is the synchronous client a trigger subcommand wraps.

There is no coalescing. Every accepted request gets its own run, its own arguments and its own result, in arrival order.

The request payload is a type parameter. A daemon whose runs take arguments declares a struct, such as repository names plus a forwarded environment. A daemon without arguments uses `struct{}`, which is sent as `{}` on the wire.

```go
// Daemon side: one queue, one executor goroutine, one socket server.
// trigger.Execute owns the Start/Finish lifecycle, so every job gets exactly
// one result by construction: the callback just returns the outcome.
queue := trigger.NewQueue[payload](16)
go func() {
	trigger.Execute(ctx, queue, func(ctx context.Context, trig string, p payload) trigger.Outcome {
		ok, elapsed := runPass(ctx, p) // the app's real work
		return trigger.Outcome{OK: ok, Duration: elapsed}
	})
}()
ln, err := trigger.Listen("/tmp/myapp.sock") // owner-only, stale file unlinked
srv := &trigger.Server[payload]{Queue: queue}
srv.Serve(ln)
// shutdown: ln.Close(); queue.Close(); Execute drains and returns; srv.Wait()

// Trigger subcommand: submit one run, wait for its own result.
// Pass signal.NotifyContext so Ctrl-C unwinds the wait instead of killing
// the process with the connection half-open.
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
final, err := trigger.Submit(ctx, "/tmp/myapp.sock", payload{Repos: repos}, nil)
// map final.OK / errors.Is(err, trigger.ErrUnreachable) to the exit code
```

## Guarantees

- The queue rejects a request at once when it is full or closing, with `ErrFull` or `ErrClosed`. Their messages travel the wire as the rejection reason.
- An accepted job gets exactly one result. `Execute` finishes jobs received after shutdown with `CancelledReason` instead of dropping them. It also delivers a panicking run's failure result before the panic continues, so a waiting client is never stranded.
- `Execute` is optional. A daemon whose executor policy differs writes its own loop of about 7 lines. Examples are running jobs outside the shutdown context, halting admission on an app state, or using its own cancellation wording.
- The server never logs payload contents, because a forwarded environment can carry secrets. The `OnAccepted` and `OnRejected` hooks let your app log acceptance and rejection in its own words.
- What a job does, how its result maps to health and the wording of lifecycle log lines stay in your app.

## The socket

`Listen(path)` binds the socket with owner-only permissions, so only the container's own user can trigger a run. When the path is already bound, it checks whether a live daemon answers. A live daemon makes `Listen` fail, and a socket left by a killed daemon is removed and bound again.

Call `Listen` during single-threaded boot. It narrows the process-wide umask while it binds, so a file another goroutine creates at that moment gets owner-only permissions.

A request line is capped at 8 MiB, and a client has 30 seconds after connecting to send it. A connection that closes without sending a request gets no event and is logged at DEBUG.

## Client errors

`Submit` blocks for the whole queue wait plus the run, and has no read deadline. Cancel its context to stop waiting. A non-nil error wraps one of these values, or is the context's own error:

- `ErrUnreachable` means no daemon accepted within `DialTimeout`, 5 seconds. The container may be down, or the exec user may differ from the socket's owner.
- `ErrSend` means the request could not be written.
- `ErrConnectionLost` means the event stream ended before the final `done` event, because the daemon died or was stopped mid-run.
