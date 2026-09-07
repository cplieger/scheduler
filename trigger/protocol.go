package trigger

// A connection carries one request and its lifecycle: the client sends a single
// JSON-encoded payload line, then reads Event lines until the final done. A
// field added to a payload later must stay optional, or an older client's frame
// stops decoding.

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
