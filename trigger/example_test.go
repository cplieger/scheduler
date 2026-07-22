package trigger_test

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cplieger/scheduler/v3/trigger"
)

// payload is an app's request type: the arguments one triggered run carries.
// An argless daemon uses struct{} instead.
type payload struct {
	Repos []string `json:"repos,omitempty"`
}

// Example wires the whole broker: a daemon-side queue served by one executor
// goroutine and exposed on a unix socket, and a client that submits one run
// and waits for its result — the fleet's single-owner scheduler shape.
func Example() {
	dir, err := os.MkdirTemp("/tmp", "trigger-example-")
	if err != nil {
		fmt.Println("tempdir:", err)
		return
	}
	defer func() { _ = os.RemoveAll(dir) }()
	socketPath := filepath.Join(dir, "trigger.sock")

	// Daemon side: one bounded queue, one executor goroutine (the single
	// owner of execution), one socket server bridging requests in.
	queue := trigger.NewQueue[payload](16)
	executorDone := make(chan struct{})
	go func() {
		defer close(executorDone)
		for job := range queue.Jobs() {
			job.Start()
			// The app's real work runs here, with the job's exact payload.
			job.Finish(trigger.Outcome{OK: true, Duration: 42 * time.Millisecond})
		}
	}()

	ln, err := trigger.Listen(socketPath)
	if err != nil {
		fmt.Println("listen:", err)
		return
	}
	srv := &trigger.Server[payload]{Queue: queue}
	srv.Serve(ln)

	// Client side (the `run` subcommand): submit one request, block until
	// its own result, map it to an exit code.
	final, err := trigger.Submit(socketPath, payload{Repos: []string{"owner/repo"}}, nil)
	if err != nil {
		fmt.Println("submit:", err)
		return
	}
	fmt.Println("ok:", final.OK)

	// Daemon shutdown: stop admission, drain, wait for the handlers.
	_ = ln.Close()
	queue.Close()
	<-executorDone
	srv.Wait()

	// Output:
	// ok: true
}
