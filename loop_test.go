package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestRunLoopFiresOnStart(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fires atomic.Int32
	done := make(chan struct{})
	go func() {
		RunLoop(ctx, func(context.Context) { fires.Add(1) },
			LoopOptions{Interval: 10 * time.Millisecond})
		close(done)
	}()

	time.Sleep(75 * time.Millisecond)
	cancel()
	<-done

	// FireOnStart is false, so the first run is one interval in; ~75ms at a
	// 10ms cadence yields several ticks. Assert a conservative floor to stay
	// robust under a loaded CI scheduler.
	if got := fires.Load(); got < 2 {
		t.Errorf("fires = %d, want >= 2 over 75ms at a 10ms interval", got)
	}
}

func TestRunLoopReturnsOnNonPositiveInterval(t *testing.T) {
	t.Parallel()
	var fires atomic.Int32
	RunLoop(context.Background(), func(context.Context) { fires.Add(1) },
		LoopOptions{Interval: 0, FireOnStart: true})
	if got := fires.Load(); got != 0 {
		t.Errorf("fires = %d, want 0 (a non-positive interval must not loop)", got)
	}
}

func TestRunLoopDoesNotFireWhenAlreadyCancelled(t *testing.T) {
	t.Parallel()
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fires atomic.Int32
	done := make(chan struct{})
	go func() {
		RunLoop(ctx, func(context.Context) { fires.Add(1) },
			LoopOptions{Interval: 10 * time.Millisecond, Jitter: 0.10})
		close(done)
	}()
	time.Sleep(75 * time.Millisecond)
	cancel()
	<-done

	// Jitter keeps ticks within +/-10% of 10ms, so several fire in 75ms;
	// a conservative floor stays robust under a loaded CI scheduler.
	if got := fires.Load(); got < 2 {
		t.Errorf("fires = %d, want >= 2 over 75ms at a jittered 10ms interval", got)
	}
}
