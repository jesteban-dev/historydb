package helpers

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type LogFormatter struct{}

func (h *LogFormatter) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *LogFormatter) Handle(ctx context.Context, r slog.Record) error {
	fmt.Printf("[%s - %s] %s", r.Level, r.Time.Format(time.RFC3339), r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Printf(" | %s=%v", a.Key, a.Value)
		return true
	})
	fmt.Println()
	return nil
}

func (h *LogFormatter) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *LogFormatter) WithGroup(name string) slog.Handler       { return h }
