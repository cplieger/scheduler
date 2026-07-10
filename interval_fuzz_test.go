package scheduler

import (
	"testing"
	"time"
)

// FuzzParseInterval asserts the two invariants every caller relies on, against
// arbitrary env values: the returned Mode is always one of the three defined
// modes, and a built-in schedule always carries a positive interval (RunLoop
// and time.NewTicker panic on a non-positive duration, so ParseInterval is the
// sole gate protecting them from a malformed env value).
func FuzzParseInterval(f *testing.F) {
	for _, seed := range []string{
		"", "   ", "6h", "45m", "1h30m", "off", "OFF", "disabled",
		"0", "0s", "0m", "-1h", "banana", "  30m  ", "100000000h",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		for _, zeroAsOnce := range []bool{false, true} {
			opts := []IntervalOption{WithIntervalLogger(silentLogger())}
			if zeroAsOnce {
				opts = append(opts, WithZeroAsOnce())
			}
			s := ParseInterval(raw, time.Hour, opts...)
			switch s.Mode {
			case ModeBuiltin, ModeExternal, ModeOnce:
			default:
				t.Fatalf("ParseInterval(%q) returned undefined Mode %d", raw, int(s.Mode))
			}
			if s.Mode == ModeBuiltin && s.Interval <= 0 {
				t.Fatalf("ParseInterval(%q) is built-in but Interval=%s (must be > 0)", raw, s.Interval)
			}
		}
	})
}
