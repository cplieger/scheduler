// Package trigger is a single-owner trigger broker for socket-shaped scheduler
// daemons. One daemon process owns every job execution; triggers — a built-in
// ticker, a client exec — only submit requests. A bounded FIFO Queue carries
// them to the daemon's single executor goroutine, which IS the mutual
// exclusion; a Server accepts requests on an owner-only unix socket and streams
// lifecycle events back; Submit forwards one request and blocks until that
// run's own result. There is no coalescing: every accepted request gets its own
// run and its own true result, in arrival order.
//
// The request payload is a type parameter; an argless daemon uses an empty
// struct, framing as {}. Client and daemon ship in one binary, so the wire
// format (newline-delimited JSON, see Event) carries no version field. The
// package owns no policy: what a job does, when shutdown cancels versus drains,
// and the app's log wording stay in the consuming app, whose payload the
// library never logs. Unix-only, like the parent package.
package trigger
