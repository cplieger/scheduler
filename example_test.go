package scheduler_test

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/cplieger/scheduler"
)

func ExampleParseInterval() {
	built := scheduler.ParseInterval("30m", time.Hour)
	fmt.Printf("%s every %s\n", built.Mode, built.Interval)

	fmt.Println(scheduler.ParseInterval("off", time.Hour).Mode)
	fmt.Println(scheduler.ParseInterval("0", time.Hour, scheduler.WithZeroAsOnce()).Mode)
	// Output:
	// built-in every 30m0s
	// external
	// once
}

func ExampleExclusive() {
	dir, err := os.MkdirTemp("", "scheduler_example_exclusive")
	if err != nil {
		return
	}
	defer func() { _ = os.RemoveAll(dir) }()

	ex := scheduler.NewExclusive(dir, slog.New(slog.DiscardHandler))
	outcome, _ := ex.Run(func() error {
		fmt.Println("cycle ran")
		return nil
	})
	fmt.Println("outcome:", outcome)
	// Output:
	// cycle ran
	// outcome: ran
}

func ExampleTryLock() {
	path := filepath.Join(os.TempDir(), "scheduler_example.lock")
	defer func() { _ = os.Remove(path) }()

	lock, ok, _ := scheduler.TryLock(path)
	fmt.Println("first acquire:", ok)

	_, contended, _ := scheduler.TryLock(path)
	fmt.Println("second acquire while held:", contended)

	lock.Unlock()
	// Output:
	// first acquire: true
	// second acquire while held: false
}
