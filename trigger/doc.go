// Package trigger is the single-owner trigger broker shared by the fleet's
// socket-shaped scheduler daemons (docker-renovate-scheduler,
// docker-rsync-scheduler, docker-fclones-scheduler).
//
// The shape: one daemon process (PID 1) owns every job execution; triggers —
// a built-in ticker, each `run`/`sync`/`scan` client exec — only submit
// requests. A bounded FIFO Queue carries the requests to the daemon's single
// executor goroutine (mutual exclusion is that loop; nothing else may start a
// job), a Server accepts requests on an owner-only in-container unix socket
// and streams lifecycle events back, and Submit is the thin synchronous
// client that forwards one request and blocks until the run's own result.
// There is no coalescing: every accepted request gets its own run and its own
// true result, in arrival order.
//
// The request payload is a type parameter. A daemon whose runs take arguments
// declares a struct (docker-renovate-scheduler forwards repo slugs plus its
// complete environment); an argless daemon uses an empty struct, which frames
// as `{}` on the wire. Client and daemon ship in one binary inside one image,
// so the wire format (newline-delimited JSON, see Event) carries no version
// field and payload evolution needs only ordinary optional-field care.
//
// The package brokers requests and results; it deliberately owns no policy.
// What a job does, how its outcome maps to health, when shutdown cancels
// versus drains, and the exact wording of the app's lifecycle log lines all
// stay in the consuming app (the mechanism-vs-policy split the parent
// scheduler package applies to SlotFile). The library's own log lines are
// payload-free transport diagnostics only.
//
// Unix-only, like the parent package: the socket hygiene in Listen relies on
// umask(2) and unix domain sockets.
package trigger
