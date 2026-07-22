package trigger

// --- Wire protocol (client <-> daemon, newline-delimited JSON) ---
//
// A connection carries one request and its lifecycle: the client sends a
// single JSON-encoded payload line, then reads Event lines until the final
// done. Client and daemon ship in the same binary inside the same image, so
// there is no version skew to negotiate and the wire format carries no
// version field. The request line is the payload type P encoded directly: an
// argless daemon's empty payload struct frames as `{}`, and fields added to a
// payload later must stay optional so an older client's frame keeps decoding.

// Event is one status line the daemon streams back. The client receives
// EventQueued on acceptance, EventStarted when the executor picks the request
// up (the gap between the two is queue wait behind an in-flight run), and
// exactly one EventDone as the final line.
type Event struct {
	// Kind is the event discriminator: EventQueued, EventStarted, EventDone.
	Kind string `json:"event"`
	// Reason explains a not-OK outcome that isn't a plain job failure (queue
	// full, cancelled by shutdown), or annotates an OK outcome that carries a
	// caveat (an app-defined skip tolerance).
	Reason string `json:"reason,omitempty"`
	// DurationMs is the elapsed execution time on EventDone. Zero when the
	// request was rejected or cancelled before running.
	DurationMs int64 `json:"duration_ms,omitempty"`
	// OK is meaningful only on EventDone: the run's outcome (never omitted,
	// so a failed run is explicit on the wire).
	OK bool `json:"ok"`
}

// Event kinds, in wire order.
const (
	EventQueued  = "queued"
	EventStarted = "started"
	EventDone    = "done"
)
