package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/moonwalker/orbital/internal/build"
)

// https://betterstack.com/community/guides/logging/logging-in-go/

const (
	LevelTrace = slog.Level(-8)
	LevelFatal = slog.Level(12)
)

var (
	// log level variable
	Leveler = &slog.LevelVar{}

	LevelNames = map[slog.Leveler]string{
		LevelTrace: "TRACE",
		LevelFatal: "FATAL",
	}

	// logger otpions with level
	opts = &slog.HandlerOptions{
		Level: Leveler,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey {
				level := a.Value.Any().(slog.Level)
				levelLabel, exists := LevelNames[level]
				if !exists {
					levelLabel = level.String()
				}
				a.Value = slog.StringValue(levelLabel)
			}
			return a
		},
	}

	// default json logger
	logger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
)

func init() {
	// text logger in dev mode
	if build.Development() {
		logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	}

	// set default level from env
	SetLevel(os.Getenv("LOG_LEVEL"))

	// set as default logger
	slog.SetDefault(logger)
}

// set log level, default info
func SetLevel(level string) {
	switch strings.ToUpper(level) {
	case "TRACE":
		Leveler.Set(LevelTrace) // -8
	case "DEBUG":
		Leveler.Set(slog.LevelDebug) // -4
	case "INFO", "":
		Leveler.Set(slog.LevelInfo) // 0
	case "WARN":
		Leveler.Set(slog.LevelWarn) // 4
	case "ERROR":
		Leveler.Set(slog.LevelError) // 8
	case "FATAL":
		Leveler.Set(LevelFatal) // 12
	}
}

func With(args ...any) *slog.Logger {
	return slog.With(args...)
}

func Debug(msg string, args ...any) {
	slog.Debug(msg, args...)
}

func Debugf(format string, v ...any) {
	Debug(fmt.Sprintf(format, v...))
}

func Info(msg string, args ...any) {
	slog.Info(msg, args...)
}

func Warn(msg string, args ...any) {
	slog.Warn(msg, args...)
}

func Error(msg string, args ...any) {
	slog.Error(msg, args...)
}

func Fatal(msg string, args ...any) {
	logger.Log(context.Background(), LevelFatal, msg, args...)
	os.Exit(1)
}
