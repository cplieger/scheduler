package scheduler_test

import (
	"fmt"
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
