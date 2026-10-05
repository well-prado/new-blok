package node

import (
	"context"
	"log/slog"
)

type loggerKey struct{}

// Logger returns the execution logger when installed by an application runner.
// Outside an inspected execution it returns slog.Default().
func Logger(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return slog.Default()
}

// WithLogger is used by runner composition to bind structured logging to one step.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}
