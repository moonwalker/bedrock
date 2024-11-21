package env

import (
	"os"

	"log/slog"
)

func Must(key string) string {
	res := os.Getenv(key)
	if len(res) == 0 {
		slog.Error("env var must be set", "key", key)
		os.Exit(1)
	}
	return res
}
