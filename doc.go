// Package scheduler is the scheduling scaffold for containerized job runners:
// small, orthogonal primitives a composition root wires together, not a
// framework. It parses a *_INTERVAL value into a cadence and a mode, drives a
// startup-plus-ticker loop, records a restart-surviving last-run stamp, guards
// overlap with an advisory flock, coalesces whole cycles across processes, and
// builds subprocesses that shut down gracefully. The trigger subpackage is the
// in-process alternative for single-owner daemons, where PID 1 owns every run.
//
// Each primitive stands alone and documents its own contract. The package is
// deliberately silent about what a job does, how health is signalled, and how
// logging is configured; those belong to the consuming app. It carries no
// runtime dependencies beyond the standard library, and its flock-based
// primitives are Unix-only.
package scheduler
