package trigger

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestEvent_OKIsExplicitOnTheWire pins the protocol regression guard: a done
// event always carries "ok" (a failed run must be explicit, not an omitted
// field a lenient decoder defaults).
func TestEvent_OKIsExplicitOnTheWire(t *testing.T) {
	t.Parallel()
	for _, ok := range []bool{true, false} {
		raw, err := json.Marshal(Event{Kind: EventDone, OK: ok})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if !strings.Contains(string(raw), `"ok":`) {
			t.Errorf("wire form %s omits the ok field (ok=%v), want it explicit", raw, ok)
		}
	}
}

// TestEvent_WireNamesAreTheFleetContract pins the frame field names and event
// kinds the three shipped schedulers already speak: renaming any of them
// would break a mixed-version client/daemon pair mid-image-update.
func TestEvent_WireNamesAreTheFleetContract(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(Event{Kind: EventDone, Reason: "r", DurationMs: 7, OK: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, field := range []string{`"event":"done"`, `"reason":"r"`, `"duration_ms":7`, `"ok":true`} {
		if !strings.Contains(string(raw), field) {
			t.Errorf("wire form %s missing %s", raw, field)
		}
	}
	if EventQueued != "queued" || EventStarted != "started" || EventDone != "done" {
		t.Error("event kind constants drifted from the shipped wire values")
	}
}

// TestPayload_EmptyStructFramesAsEmptyObject pins the argless-daemon frame:
// an empty payload struct encodes as {} and decodes from it, the exact frame
// rsync- and fclones-shaped clients send today.
func TestPayload_EmptyStructFramesAsEmptyObject(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(struct{}{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(raw) != "{}" {
		t.Fatalf("empty payload frames as %s, want {}", raw)
	}
	var p struct{}
	if err := json.Unmarshal([]byte("{}"), &p); err != nil {
		t.Errorf("decode {} into empty payload: %v", err)
	}
	// Forward compatibility: an older client's bare frame must keep decoding
	// into a payload type that has since grown optional fields.
	var grown testPayload
	if err := json.Unmarshal([]byte("{}"), &grown); err != nil {
		t.Errorf("decode {} into a grown payload: %v", err)
	}
}
