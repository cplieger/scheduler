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
	// duration unless WithZeroAsOnce(true) is passed.
	ModeExternal
	// ModeOnce runs the job exactly once, then exits. It is selected by a zero
	// duration ("0"/"0s") only when WithZeroAsOnce(true) is passed; otherwise a
	// zero duration selects ModeExternal.
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
	logger      *slog.Logger
	name        string
	low         time.Duration
	high        time.Duration
	zeroAsOnce  bool
	redactValue bool
}

// IntervalOption configures ParseInterval.
type IntervalOption func(*intervalConfig)

// WithZeroAsOnce selects what a zero duration ("0"/"0s") means: true makes it
// select ModeOnce — for a job that supports a run-once mode (a batch or
// one-shot context) in addition to a resident daemon — while false keeps the
// default ModeExternal, exactly as if the option were absent. Repeated
// applications resolve last-wins.
func WithZeroAsOnce(zeroAsOnce bool) IntervalOption {
	return func(c *intervalConfig) { c.zeroAsOnce = zeroAsOnce }
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

// WithRedactedValue selects whether ParseInterval's warnings echo the raw
// interval value: true keeps it out, making them field-name-only, while false
// keeps the default echo, exactly as if the option were absent. Repeated
// applications resolve last-wins. Pass true when the value passes through
// secret-capable config expansion (a YAML file with ${VAR} references): a
// config typo can place an expanded secret in the interval field, and the
// default unparseable-value warning would echo it to the startup log. Only the
// unparseable warning can ever carry such a value — a negative or clamped
// value necessarily parsed as a duration — but with true every warning
// omits the supplied value (the clamp warning keeps the resulting bound), so
// the redaction contract is uniform rather than per-branch.
func WithRedactedValue(redacted bool) IntervalOption {
	return func(c *intervalConfig) { c.redactValue = redacted }
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
//   - a zero duration ("0"/"0s") -> def, ModeExternal (or ModeOnce with WithZeroAsOnce(true))
//   - a negative duration -> def, ModeBuiltin, with a warning (a likely typo;
//     falling back to the default cadence beats silently disabling the job)
//   - anything unparseable -> def, ModeBuiltin, with a warning
//
// def is the fallback cadence used for every non-positive outcome; it is also
// carried on the returned Schedule in the external and once modes for
// reference. def must be positive and ParseInterval panics otherwise (a
// programmer error in the composition root, caught at first boot — the same
// contract as time.NewTicker): def becomes the Interval of every ModeBuiltin
// result (empty, negative, or unparseable input), and the library's invariant
// that a ModeBuiltin Schedule always carries a positive Interval -- which a
// consumer relies on when it passes the Interval straight to time.NewTicker --
// holds only when def > 0. (RunLoop itself also guards defensively, since a
// hand-built LoopOptions can carry any Interval.) Warnings are logged via
// slog.Default() unless WithIntervalLogger is set.
func ParseInterval(raw string, def time.Duration, opts ...IntervalOption) Schedule {
	if def <= 0 {
		panic("scheduler: ParseInterval def must be positive")
	}
	c := &intervalConfig{name: "interval", logger: slog.Default()}
	for _, opt := range opts {
		opt(c)
	}

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return Schedule{Interval: def, Mode: ModeBuiltin}
	}
	switch lowerASCII(trimmed) {
	case "off", "disabled":
		return Schedule{Interval: def, Mode: ModeExternal}
	}

	d, err := time.ParseDuration(trimmed)
	if err != nil {
		c.warnFallback("cannot parse interval, using default", raw, def)
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
		c.warnFallback("interval is negative, using default", raw, def)
		return Schedule{Interval: def, Mode: ModeBuiltin}
	}
}

// warnFallback logs a fallback-to-default warning, echoing the supplied raw
// value unless WithRedactedValue(true) is in effect (an expanded secret
// misplaced in the interval field must never reach the log).
func (c *intervalConfig) warnFallback(msg, raw string, def time.Duration) {
	if c.redactValue {
		c.logger.Warn(msg, "name", c.name, "default", def.String())
		return
	}
	c.logger.Warn(msg, "name", c.name, "value", raw, "default", def.String())
}

// clamp bounds a positive interval to the configured [low, high], logging when
// it adjusts. A non-positive bound is treated as unset. Under
// WithRedactedValue(true) the warning omits the requested duration and keeps
// only the applied bound (the requested value parsed as a duration so it
// cannot be a secret, but the option's contract is that no supplied value is
// echoed).
func (c *intervalConfig) clamp(d time.Duration) time.Duration {
	clamped := d
	if c.low > 0 && clamped < c.low {
		clamped = c.low
	}
	if c.high > 0 && clamped > c.high {
		clamped = c.high
	}
	if clamped != d {
		if c.redactValue {
			c.logger.Warn("interval clamped", "name", c.name, "clamped_to", clamped.String())
		} else {
			c.logger.Warn("interval clamped",
				"name", c.name, "requested", d.String(), "clamped_to", clamped.String())
		}
	}
	return clamped
}

// lowerASCII lowercases the ASCII letters in s and leaves every other byte
// unchanged. It returns s itself when there is nothing to fold, so the common
// already-lowercase value costs no allocation.
//
// strings.ToLower is deliberately NOT used to match the sentinels. Unicode
// simple case folding maps two already-assigned runes onto ASCII letters —
// U+0130 (LATIN CAPITAL LETTER I WITH DOT ABOVE) to 'i' and U+212A (KELVIN
// SIGN) to 'k' — so "dİsabled" folds onto the "disabled" sentinel and selects
// ModeExternal. That is the one wrong answer this function can give: the
// documented contract for a value that is not a sentinel and not a duration is
// the default interval plus a warning, and silently choosing ModeExternal
// instead disables the schedule with no log line at all. Folding ASCII only
// sends such a value down the ParseDuration path, where it fails and warns.
// The sentinels are ASCII, so restricting the fold cannot lose a real match.
func lowerASCII(s string) string {
	var b []byte
	for i := range len(s) {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			if b == nil {
				b = []byte(s)
			}
			b[i] = c + ('a' - 'A')
		}
	}
	if b == nil {
		return s
	}
	return string(b)
}
