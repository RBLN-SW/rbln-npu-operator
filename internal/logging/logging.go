// Package logging configures structured slog logging for this repo's
// auxiliary binaries (rbln-validator, crd-apply).
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

// Env vars follow the project-wide RBLN_NPU_OPERATOR_* prefix so
// generic names cannot be captured by unrelated env injection.
const (
	envLogLevel  = "RBLN_NPU_OPERATOR_LOG_LEVEL"
	envLogFormat = "RBLN_NPU_OPERATOR_LOG_FORMAT"
)

// levelInfo/formatJSON/formatText are shared by parseLevel/parseFormat and
// their default/fallback paths; extracted so the repeated literals satisfy
// goconst.
const (
	levelInfo  = "info"
	formatJSON = "json"
	formatText = "text"
)

// newLogger builds the logger from already-validated settings.
func newLogger(w io.Writer, lvl slog.Level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: lvl,
		// The caller attr's cost and noise are only worth it at debug.
		AddSource:   lvl <= slog.LevelDebug,
		ReplaceAttr: replaceAttr,
	}
	var h slog.Handler
	if format == formatText {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h)
}

// SetupFromEnv reads RBLN_NPU_OPERATOR_LOG_LEVEL / _LOG_FORMAT and
// installs the process-wide default logger (stdout).
// Empty values default to info/json (the production defaults). Invalid
// values do not kill the process: only the offending variable falls back
// to its default, and a Warn carrying a "fallback" key is emitted through
// the installed logger.
func SetupFromEnv() {
	slog.SetDefault(setupFromEnv(os.Stdout))
}

// setupFromEnv builds the env-configured logger writing to w and emits the
// invalid-value warns through it. Split from SetupFromEnv so tests can
// observe the warn output.
func setupFromEnv(w io.Writer) *slog.Logger {
	lvl, levelErr := parseLevel(os.Getenv(envLogLevel))
	if levelErr != nil {
		lvl = slog.LevelInfo
	}
	format, formatErr := parseFormat(os.Getenv(envLogFormat))
	if formatErr != nil {
		format = formatJSON
	}
	logger := newLogger(w, lvl, format)
	// With level=error an invalid format's warn is suppressed by the gate —
	// accepted, since the error gate was chosen explicitly.
	if levelErr != nil {
		logger.Warn("Invalid "+envLogLevel+", using default", "error", levelErr, "fallback", levelInfo)
	}
	if formatErr != nil {
		logger.Warn("Invalid "+envLogFormat+", using default", "error", formatErr, "fallback", formatJSON)
	}
	return logger
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", levelInfo:
		return slog.LevelInfo, nil
	case "error":
		return slog.LevelError, nil
	case "warning", "warn":
		return slog.LevelWarn, nil
	case "debug":
		return slog.LevelDebug, nil
	}
	return 0, fmt.Errorf("unknown log level %q (error|warning|warn|info|debug)", s)
}

func parseFormat(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", formatJSON:
		return formatJSON, nil
	case formatText:
		return formatText, nil
	}
	return "", fmt.Errorf("unknown log format %q (json|text)", s)
}

// replaceAttr normalizes slog output: key "ts" with RFC3339Nano, lowercase
// "level", and a zap-style "caller" ("file:line") instead of the verbose
// source group.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return a
	}
	switch a.Key {
	case slog.TimeKey:
		// String-valued user "time" attrs pass through; a time-valued one is
		// indistinguishable from the record timestamp and gets rewritten too.
		if a.Value.Kind() != slog.KindTime {
			return a
		}
		a.Key = "ts"
		a.Value = slog.StringValue(a.Value.Time().Format(time.RFC3339Nano))
	case slog.LevelKey:
		lvl, ok := a.Value.Any().(slog.Level)
		if !ok {
			return a
		}
		a.Value = slog.StringValue(strings.ToLower(lvl.String()))
	case slog.SourceKey:
		src, ok := a.Value.Any().(*slog.Source)
		if !ok {
			return a
		}
		a.Key = "caller"
		a.Value = slog.StringValue(fmt.Sprintf("%s:%d", trimPath(src.File), src.Line))
	}
	return a
}

// trimPath keeps at most the last two path segments for a zap-style short caller.
func trimPath(file string) string {
	idx := strings.LastIndexByte(file, '/')
	if idx == -1 {
		return file
	}
	if idx2 := strings.LastIndexByte(file[:idx], '/'); idx2 != -1 {
		return file[idx2+1:]
	}
	return file
}
