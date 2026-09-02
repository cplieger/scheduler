package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"
)

func TestRunLoopFiresOnStart(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var fires atomic.Int32
	// A large interval means only the startup fire can happen; the job cancels
	// the context so the loop returns after that single run.
	RunLoop(ctx, func(context.Context) {
		fires.Add(1)
		cancel()
	}, LoopOptions{Interval: time.Hour, FireOnStart: true})

	if got := fires.Load(); got != 1 {
		t.Errorf("fires = %d, want 1 (startup fire only)", got)
	}
}

func TestRunLoopTicksRepeatedly(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var fires atomic.Int32
		done := make(chan struct{})
		go func() {
			RunLoop(ctx, func(context.Context) { fires.Add(1) },
				LoopOptions{Interval: 10 * time.Millisecond})
			close(done)
		}()

		synctest.Sleep(75 * time.Millisecond)
		cancel()
		<-done

		// Synthetic time makes the cadence exact instead of a floor.
		// FireOnStart is false, so the first run is one interval in: ticks land
		// at 10ms through 70ms and the eighth would land at 80ms, past the 75ms
		// observation point. On a real clock this could only assert ">= 2",
		// which a loop that fired twice and stalled would also satisfy.
		if got := fires.Load(); got != 7 {
			t.Errorf("fires = %d, want exactly 7 (ticks at 10ms..70ms within 75ms)", got)
		}
	})
}

func TestRunLoopReturnsOnNonPositiveInterval(t *testing.T) {
	t.Parallel()
	var fires atomic.Int32
	RunLoop(t.Context(), func(context.Context) { fires.Add(1) },
		LoopOptions{Interval: 0, FireOnStart: true})
	if got := fires.Load(); got != 0 {
		t.Errorf("fires = %d, want 0 (a non-positive interval must not loop)", got)
	}
}

func TestRunLoopDoesNotFireWhenAlreadyCancelled(t *testing.T) {
	t.Parallel()
	// Deliberately pre-cancelled, not t.Context(): the cancelled context IS
	// the fixture for this test.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var fires atomic.Int32
	RunLoop(ctx, func(context.Context) { fires.Add(1) },
		LoopOptions{Interval: time.Hour, FireOnStart: true})
	if got := fires.Load(); got != 0 {
		t.Errorf("fires = %d, want 0 (an already-cancelled context must not run the job)", got)
	}
}

func TestJitteredDelayNoJitterReturnsInterval(t *testing.T) {
	t.Parallel()
	cases := []float64{0, -0.1, -1}
	for _, fraction := range cases {
		if got := JitteredDelay(time.Hour, fraction); got != time.Hour {
			t.Errorf("JitteredDelay(1h, %v) = %s, want 1h", fraction, got)
		}
	}
	if got := JitteredDelay(0, 0.1); got != 0 {
		t.Errorf("JitteredDelay(0, 0.1) = %s, want 0", got)
	}
	// A negative interval is returned untouched as well, so a caller that
	// passed one gets its own value back rather than a jittered variant of it.
	if got := JitteredDelay(-time.Hour, 0.5); got != -time.Hour {
		t.Errorf("JitteredDelay(-1h, 0.5) = %s, want -1h", got)
	}
}

// TestJitteredDelaySpansBothSidesOfTheInterval pins that the ±band actually
// spreads. The band assertion alone cannot see this: a delay pinned to the
// interval every single time sits inside [interval−spread, interval+spread]
// and would spread no restart herd at all.
func TestJitteredDelaySpansBothSidesOfTheInterval(t *testing.T) {
	t.Parallel()
	const (
		interval = time.Hour
		draws    = 200
	)
	var below, above int
	for range draws {
		d := JitteredDelay(interval, 0.10)
		if d < interval {
			below++
		}
		if d > interval {
			above++
		}
	}
	if below == 0 || above == 0 {
		t.Errorf("JitteredDelay(%s, 0.10) over %d draws landed %d below and %d above %s, want both sides drawn",
			interval, draws, below, above, interval)
	}
}

func TestJitteredDelayWithinBand(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		interval := time.Duration(rapid.Int64Range(1, int64(24*time.Hour)).Draw(t, "interval"))
		fraction := rapid.Float64Range(0.0001, 0.9999).Draw(t, "fraction")

		got := JitteredDelay(interval, fraction)

		spread := time.Duration(fraction * float64(interval))
		if got < interval-spread || got > interval+spread {
			t.Fatalf("JitteredDelay(%s, %v) = %s, want within [%s, %s]",
				interval, fraction, got, interval-spread, interval+spread)
		}
	})
}

func TestRunLoopWithJitterTicks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var fires atomic.Int32
		done := make(chan struct{})
		go func() {
			RunLoop(ctx, func(context.Context) { fires.Add(1) },
				LoopOptions{Interval: 10 * time.Millisecond, Jitter: 0.10})
			close(done)
		}()
		synctest.Sleep(75 * time.Millisecond)
		cancel()
		<-done

		// Jitter draws each delay from [9ms, 11ms), so the count over a 75ms
		// window is bounded but not fixed: all-minimum draws tick at 9ms..72ms
		// (8 fires) and all-maximum draws at 11ms..66ms (6). Synthetic time
		// removes scheduler noise, so the band is the jitter arithmetic alone
		// rather than a floor chosen to survive a loaded runner. JitteredDelay's
		// own band is property-tested separately.
		if got := fires.Load(); got < 6 || got > 8 {
			t.Errorf("fires = %d, want 6..8 over 75ms at a jittered 10ms interval", got)
		}
	})
}

func TestJitteredDelayClampsFractionAboveOne(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		interval := time.Duration(rapid.Int64Range(1, int64(24*time.Hour)).Draw(t, "interval"))
		fraction := rapid.Float64Range(1.0, 100.0).Draw(t, "fraction")

		got := JitteredDelay(interval, fraction)

		if got < 0 || got >= 2*interval {
			t.Fatalf("JitteredDelay(%s, %v) = %s, want within [0, %s) (fraction clamped to 1)",
				interval, fraction, got, 2*interval)
		}
	})
}

func TestRunLoopFirstDelayShiftsTheFirstTick(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var fires atomic.Int32
		done := make(chan struct{})
		go func() {
			RunLoop(ctx, func(context.Context) { fires.Add(1) },
				LoopOptions{Interval: 10 * time.Millisecond, FirstDelay: 3 * time.Millisecond})
			close(done)
		}()
		synctest.Sleep(75 * time.Millisecond)
		cancel()
		<-done

		// FirstDelay phases the schedule: ticks land at 3ms, then every 10ms
		// (13ms..73ms), so exactly 8 fire within the 75ms window where the
		// default phase (10ms..70ms) would fire 7.
		if got := fires.Load(); got != 8 {
			t.Errorf("fires = %d, want exactly 8 (ticks at 3ms then 13ms..73ms within 75ms)", got)
		}
	})
}

func TestRunLoopFirstDelayNonPositiveMeansInterval(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var fires atomic.Int32
		done := make(chan struct{})
		go func() {
			RunLoop(ctx, func(context.Context) { fires.Add(1) },
				LoopOptions{Interval: 10 * time.Millisecond, FirstDelay: -time.Millisecond})
			close(done)
		}()
		synctest.Sleep(75 * time.Millisecond)
		cancel()
		<-done

		// The default phase: ticks at 10ms..70ms, exactly as with no FirstDelay.
		if got := fires.Load(); got != 7 {
			t.Errorf("fires = %d, want exactly 7 (non-positive FirstDelay keeps the interval phase)", got)
		}
	})
}

func TestRunLoopFireOnStartOverridesFirstDelay(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var fires atomic.Int32
	// FireOnStart wins: the first run is immediate despite a long FirstDelay;
	// the job cancels so the loop returns after that single run.
	RunLoop(ctx, func(context.Context) {
		fires.Add(1)
		cancel()
	}, LoopOptions{Interval: time.Hour, FirstDelay: 30 * time.Minute, FireOnStart: true})

	if got := fires.Load(); got != 1 {
		t.Errorf("fires = %d, want 1 (FireOnStart overrides FirstDelay)", got)
	}
}
