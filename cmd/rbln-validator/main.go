package main

import (
	"io"
	"log/slog"
	"os"
	"time"
)

func newLogger(w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format(time.RFC3339))
			}
			return a
		},
	}

	return slog.New(slog.NewJSONHandler(w, opts))
}

func run() error {
	slog.SetDefault(newLogger(os.Stdout))
	return NewRBLNValidatorApp().Execute()
}

func main() {
	if err := run(); err != nil {
		slog.Error("command execution failed", "err", err)
		os.Exit(1)
	}
}
