package scheduler

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const testDefault = 3 * time.Hour

func TestParseInterval(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		raw          string
		zeroAsOnce   bool
		wantMode     Mode
		wantInterval time.Duration
	}{
		{name: "empty selects built-in at default", raw: "", wantMode: ModeBuiltin, wantInterval: testDefault},
		{name: "whitespace is trimmed to empty", raw: "   ", wantMode: ModeBuiltin, wantInterval: testDefault},
		{name: "positive duration selects built-in", raw: "45m", wantMode: ModeBuiltin, wantInterval: 45 * time.Minute},
		{name: "compound duration parses", raw: "1h30m", wantMode: ModeBuiltin, wantInterval: 90 * time.Minute},
		{name: "surrounding whitespace is trimmed", raw: "  6h  ", wantMode: ModeBuiltin, wantInterval: 6 * time.Hour},
		{name: "off selects external", raw: "off", wantMode: ModeExternal, wantInterval: testDefault},
		{name: "disabled selects external", raw: "disabled", wantMode: ModeExternal, wantInterval: testDefault},
		{name: "off is case-insensitive", raw: "OFF", wantMode: ModeExternal, wantInterval: testDefault},
		{name: "disabled is case-insensitive", raw: "Disabled", wantMode: ModeExternal, wantInterval: testDefault},
		{name: "zero selects external by default", raw: "0", wantMode: ModeExternal, wantInterval: testDefault},
		{name: "zero seconds selects external by default", raw: "0s", wantMode: ModeExternal, wantInterval: testDefault},
		{name: "zero selects once with option", raw: "0", zeroAsOnce: true, wantMode: ModeOnce, wantInterval: testDefault},
		{name: "zero seconds selects once with option", raw: "0s", zeroAsOnce: true, wantMode: ModeOnce, wantInterval: testDefault},
		{name: "negative falls back to built-in default", raw: "-1h", wantMode: ModeBuiltin, wantInterval: testDefault},
		{name: "unparseable falls back to built-in default", raw: "banana", wantMode: ModeBuiltin, wantInterval: testDefault},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := []IntervalOption{WithIntervalLogger(silentLogger())}
			if tc.zeroAsOnce {
				opts = append(opts, WithZeroAsOnce())
			}
			got := ParseInterval(tc.raw, testDefault, opts...)
			if got.Mode != tc.wantMode {
				t.Errorf("ParseInterval(%q).Mode = %s, want %s", tc.raw, got.Mode, tc.wantMode)
			}
			if got.Interval != tc.wantInterval {
				t.Errorf("ParseInterval(%q).Interval = %s, want %s", tc.raw, got.Interval, tc.wantInterval)
			}
		})
	}
}

func TestParseIntervalBounds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		raw          string
		low          time.Duration
		high         time.Duration
		wantInterval time.Duration
	}{
		{name: "below floor is raised to low", raw: "10s", low: time.Minute, high: time.Hour, wantInterval: time.Minute},
		{name: "above ceiling is lowered to high", raw: "9000h", low: time.Minute, high: time.Hour, wantInterval: time.Hour},
		{name: "within bounds is unchanged", raw: "30m", low: time.Minute, high: time.Hour, wantInterval: 30 * time.Minute},
		{name: "floor only leaves large values alone", raw: "9000h", low: time.Minute, high: 0, wantInterval: 9000 * time.Hour},
		{name: "ceiling only leaves small values alone", raw: "1s", low: 0, high: time.Hour, wantInterval: time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ParseInterval(tc.raw, testDefault,
				WithBounds(tc.low, tc.high), WithIntervalLogger(silentLogger()))
			if got.Mode != ModeBuiltin {
				t.Fatalf("ParseInterval(%q).Mode = %s, want built-in", tc.raw, got.Mode)
			}
			if got.Interval != tc.wantInterval {
				t.Errorf("ParseInterval(%q, bounds[%s,%s]).Interval = %s, want %s",
					tc.raw, tc.low, tc.high, got.Interval, tc.wantInterval)
			}
		})
	}
}

func TestParseIntervalWarns(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		raw         string
		wantMessage string
	}{
		{name: "unparseable warns", raw: "banana", wantMessage: "cannot parse interval"},
		{name: "negative warns", raw: "-5m", wantMessage: "interval is negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			ParseInterval(tc.raw, testDefault, WithIntervalLogger(logger), WithName("TEST_INTERVAL"))
			out := buf.String()
			if !strings.Contains(out, tc.wantMessage) {
				t.Errorf("ParseInterval(%q) log = %q, want it to contain %q", tc.raw, out, tc.wantMessage)
			}
			if !strings.Contains(out, "TEST_INTERVAL") {
				t.Errorf("ParseInterval(%q) log = %q, want it to name the env var", tc.raw, out)
			}
		})
	}
}

func TestParseIntervalClampWarns(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	ParseInterval("1s", testDefault, WithBounds(time.Minute, time.Hour), WithIntervalLogger(logger))
	if !strings.Contains(buf.String(), "interval clamped") {
		t.Errorf("clamped ParseInterval log = %q, want it to contain %q", buf.String(), "interval clamped")
	}
}

func TestParseIntervalRedactedValue(t *testing.T) {
	t.Parallel()
	// A config typo can land an expanded credential in the interval field; the
	// fixture is deliberately NOT credential-shaped so secret scanners don't
	// flag the test file itself, but the assertion contract is identical.
	misplaced := "hunter2-not-a-duration"
	cases := []struct {
		name        string
		raw         string
		opts        []IntervalOption
		wantMessage string
		wantAbsent  string
		wantMode    Mode
	}{
		{
			name: "unparseable secret stays out of the log", raw: misplaced,
			wantMessage: "cannot parse interval", wantAbsent: misplaced, wantMode: ModeBuiltin,
		},
		{
			name: "negative value is omitted", raw: "-5m",
			wantMessage: "interval is negative", wantAbsent: "-5m", wantMode: ModeBuiltin,
		},
		{
			name: "clamp omits the requested duration", raw: "1s",
			opts:        []IntervalOption{WithBounds(time.Minute, time.Hour)},
			wantMessage: "interval clamped", wantAbsent: "1s", wantMode: ModeBuiltin,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			opts := append([]IntervalOption{
				WithRedactedValue(), WithIntervalLogger(logger), WithName("TEST_INTERVAL"),
			}, tc.opts...)
			got := ParseInterval(tc.raw, testDefault, opts...)
			out := buf.String()
			if !strings.Contains(out, tc.wantMessage) {
				t.Errorf("ParseInterval(%q) log = %q, want it to contain %q", tc.raw, out, tc.wantMessage)
			}
			if strings.Contains(out, tc.wantAbsent) {
				t.Errorf("ParseInterval(%q) log = %q, must not echo the supplied value %q", tc.raw, out, tc.wantAbsent)
			}
			if !strings.Contains(out, "TEST_INTERVAL") {
				t.Errorf("ParseInterval(%q) log = %q, want the field-name-only warning to still name the field", tc.raw, out)
			}
			if got.Mode != tc.wantMode {
				t.Errorf("ParseInterval(%q) mode = %v, want %v (redaction must not change parsing)", tc.raw, got.Mode, tc.wantMode)
			}
		})
	}
}

func TestParseIntervalRedactedClampKeepsBound(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	got := ParseInterval("1s", testDefault,
		WithRedactedValue(), WithBounds(time.Minute, time.Hour), WithIntervalLogger(logger))
	if got.Interval != time.Minute {
		t.Errorf("clamped Interval = %v, want %v (redaction must not change clamping)", got.Interval, time.Minute)
	}
	if !strings.Contains(buf.String(), "clamped_to=1m0s") {
		t.Errorf("redacted clamp log = %q, want it to keep the applied bound clamped_to=1m0s", buf.String())
	}
}

func TestParseIntervalValidInputDoesNotWarn(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	ParseInterval("30m", testDefault, WithBounds(time.Minute, time.Hour), WithIntervalLogger(logger))
	if buf.Len() != 0 {
		t.Errorf("valid ParseInterval logged %q, want no output", buf.String())
	}
}

func TestModeString(t *testing.T) {
	t.Parallel()
	cases := map[Mode]string{
		ModeBuiltin:  "built-in",
		ModeExternal: "external",
		ModeOnce:     "once",
		Mode(99):     "unknown",
	}
	for mode, want := range cases {
		if got := mode.String(); got != want {
			t.Errorf("Mode(%d).String() = %q, want %q", int(mode), got, want)
		}
	}
}

func TestParseIntervalBoundsSwapped(t *testing.T) {
	t.Parallel()
	got := ParseInterval("30m", testDefault,
		WithBounds(time.Hour, time.Minute), WithIntervalLogger(silentLogger()))
	if got.Mode != ModeBuiltin {
		t.Fatalf("ParseInterval(%q, swapped bounds).Mode = %s, want built-in", "30m", got.Mode)
	}
	if got.Interval != 30*time.Minute {
		t.Errorf("ParseInterval(%q, WithBounds(1h,1m)).Interval = %s, want %s "+
			"(swapped bounds must normalize to [1m,1h])", "30m", got.Interval, 30*time.Minute)
	}
}
