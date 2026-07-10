package scheduler

import (
	"io"
	"log/slog"
)

// silentLogger returns a logger that discards output, for tests and fuzz
// targets that exercise ParseInterval's fallback paths without spamming stderr.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
