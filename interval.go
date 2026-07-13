package scheduler

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Mode is how a container job is scheduled, derived from an interval
// environment variable by ParseInterval.
type Mode int

const (
	// ModeBuiltin runs the job once at startup, then on every interval tick. It
	// is selected by a positive interval duration and is the fallback when the
	// value is empty, unparseable, or (by default) negative.
	ModeBuiltin Mode = iota
	// ModeExternal idles: the built-in loop is disabled and runs are triggered
	// out-of-band (for example an Ofelia docker-exec of a one-shot subcommand).
	// It is selected by the "off" and "disabled" sentinels, and by a zero
	// duration unless WithZeroAsOnce is set.
	ModeExternal
	// ModeOnce runs the job exactly once, then exits. It is selected by a zero
	// duration ("0"/"0s") only when WithZeroAsOnce is passed; otherwise a zero
	// duration selects ModeExternal.
	ModeOnce
)

// Compile-time assertion: Mode implements fmt.Stringer.
var _ fmt.Stringer = ModeBuiltin

// String returns the lowercase mode name for logging.
func (m Mode) String() string {
	switch m {
	case ModeBuiltin:
		return "built-in"
	case ModeExternal:
		return "external"
	case ModeOnce:
		return "once"
	default:
		return "unknown"
	}
}

// Schedule is the parsed result of an interval environment variable: the
// built-in cadence and the selected Mode. Interval is meaningful only in
// ModeBuiltin; the other modes carry the default for reference.
type Schedule struct {
	Interval time.Duration
	Mode     Mode
}

// intervalConfig is the resolved set of ParseInterval options.
type intervalConfig struct {
	logger     *slog.Logger
	name       string
	low        time.Duration
	high       time.Duration
	zeroAsOnce bool
}

// IntervalOption configures ParseInterval.
type IntervalOption func(*intervalConfig)

// WithZeroAsOnce makes a zero duration ("0"/"0s") select ModeOnce instead of
// the default ModeExternal. Use it for a job that supports a run-once mode (a
// batch or one-shot context) in addition to a resident daemon.
func WithZeroAsOnce() IntervalOption {
	return func(c *intervalConfig) { c.zeroAsOnce = true }
}

// WithBounds clamps a positive built-in interval to [low, high], logging a
// warning when it adjusts the value. A non-positive bound is ignored, so
// WithBounds(time.Minute, 0) enforces only a floor. If both bounds are
// positive and high is lower than low, the pair is normalized so a swapped
// argument order cannot produce an interval outside the intended band. Bounds
// never apply to the external or once modes.
func WithBounds(low, high time.Duration) IntervalOption {
	return func(c *intervalConfig) {
		if low > 0 && high > 0 && high < low {
			low, high = high, low
		}
		c.low, c.high = low, high
	}
}

// WithName sets the environment-variable name used in warning logs (for
// example "SYNC_INTERVAL"), so an operator can tell which setting was
// rejected. It defaults to "interval".
func WithName(name string) IntervalOption {
	return func(c *intervalConfig) { c.name = name }
}

// WithIntervalLogger routes ParseInterval's warnings to a specific logger
// instead of slog.Default().
func WithIntervalLogger(l *slog.Logger) IntervalOption {
	return func(c *intervalConfig) { c.logger = l }
}

// ParseInterval interprets a raw interval environment value into a Schedule,
// applying the standard sentinel and fallback rules shared by every
// scheduled container job:
//
//   - empty              -> def, ModeBuiltin
//   - "off" / "disabled" -> def, ModeExternal (case-insensitive)
//   - a positive Go duration -> that duration (clamped by WithBounds), ModeBuiltin
//   - a zero duration ("0"/"0s") -> def, ModeExternal (or ModeOnce with WithZeroAsOnce)
//   - a negative duration -> def, ModeBuiltin, with a warning (a likely typo;
//     falling back to the default cadence beats silently disabling the job)
//   - anything unparseable -> def, ModeBuiltin, with a warning
//
// def is the fallback cadence used for every non-positive outcome; it is also
// carried on the returned Schedule in the external and once modes for
// reference. def must be positive: it becomes the Interval of every
// ModeBuiltin result (empty, negative, or unparseable input), and the
// library's invariant that a ModeBuiltin Schedule always carries a positive
// Interval -- which a consumer relies on when it passes the Interval straight to
// time.NewTicker (which panics on a non-positive duration) -- holds only when
// def > 0. (RunLoop itself also guards defensively, since a hand-built LoopOptions
// can carry any Interval.) Warnings are logged via slog.Default() unless
// WithIntervalLogger is set.
func ParseInterval(raw string, def time.Duration, opts ...IntervalOption) Schedule {
	c := &intervalConfig{name: "interval", logger: slog.Default()}
	for _, opt := range opts {
		opt(c)
	}

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return Schedule{Interval: def, Mode: ModeBuiltin}
	}
	switch strings.ToLower(trimmed) {
	case "off", "disabled":
		return Schedule{Interval: def, Mode: ModeExternal}
	}

	d, err := time.ParseDuration(trimmed)
	if err != nil {
		c.logger.Warn("cannot parse interval, using default",
			"name", c.name, "value", raw, "default", def.String())
		return Schedule{Interval: def, Mode: ModeBuiltin}
	}
	switch {
	case d > 0:
		return Schedule{Interval: c.clamp(d), Mode: ModeBuiltin}
	case d == 0:
		if c.zeroAsOnce {
			return Schedule{Interval: def, Mode: ModeOnce}
		}
		return Schedule{Interval: def, Mode: ModeExternal}
	default:
		c.logger.Warn("interval is negative, using default",
			"name", c.name, "value", raw, "default", def.String())
		return Schedule{Interval: def, Mode: ModeBuiltin}
	}
}

// clamp bounds a positive interval to the configured [low, high], logging when
// it adjusts. A non-positive bound is treated as unset.
func (c *intervalConfig) clamp(d time.Duration) time.Duration {
	clamped := d
	if c.low > 0 && clamped < c.low {
		clamped = c.low
	}
	if c.high > 0 && clamped > c.high {
		clamped = c.high
	}
	if clamped != d {
		c.logger.Warn("interval clamped",
			"name", c.name, "requested", d.String(), "clamped_to", clamped.String())
	}
	return clamped
}
