// Package trigger is a single-owner trigger broker for socket-shaped scheduler
// daemons. One daemon process owns every job execution, and triggers, such as a
// built-in ticker or a client exec, only submit requests.
//
// A bounded FIFO [Queue] carries requests to the daemon's single executor
// goroutine, which is the mutual exclusion. A [Server] accepts requests on an
// owner-only unix socket and streams lifecycle events back. [Submit] forwards
// one request and blocks until that run's own result. There is no coalescing:
// every accepted request gets its own run and its own result, in arrival order.
//
// The request payload is a type parameter, and an argless daemon uses an empty
// struct, framed as {}. Client and daemon ship in one binary, so the wire format
// (newline-delimited JSON, see [Event]) carries no version field. The package
// owns no policy. What a job does, when shutdown cancels or drains, and the
// app's log wording stay in the consuming app, whose payload the library never
// logs. Unix-only, like the parent package.
package trigger
