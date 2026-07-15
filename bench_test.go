package scheduler

import (
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkParseInterval(b *testing.B) {
	logger := WithIntervalLogger(silentLogger())
	b.ReportAllocs()
	for b.Loop() {
		_ = ParseInterval("30m", time.Hour, logger)
	}
}

func BenchmarkJitteredDelay(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = JitteredDelay(time.Hour, 0.10)
	}
}

func BenchmarkTryLock(b *testing.B) {
	path := filepath.Join(b.TempDir(), ".bench.lock")
	b.ReportAllocs()
	for b.Loop() {
		lock, ok, err := TryLock(path)
		if err != nil || !ok {
			b.Fatalf("TryLock = (ok=%v, err=%v)", ok, err)
		}
		lock.Unlock()
	}
}

func BenchmarkExclusiveRun(b *testing.B) {
	e := NewExclusive(b.TempDir(), silentLogger())
	job := func() error { return nil }
	b.ReportAllocs()
	for b.Loop() {
		if out, err := e.Run(job); err != nil || out != OutcomeRan {
			b.Fatalf("Run = (%s, %v), want (ran, nil)", out, err)
		}
	}
}
