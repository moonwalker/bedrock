package build

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

const (
	defaultVersion = "0.1.0"
)

var (
	commit  = "dev"
	version = defaultVersion
)

func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, kv := range info.Settings {
		switch kv.Key {
		case "vcs.revision":
			commit = kv.Value
		}
	}
}

func Development() bool {
	return commit == "dev"
}

func Runtime() string {
	return runtime.Version()
}

func Platform() string {
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}

func Commit() string {
	return fmt.Sprintf("%.7s", commit)
}

func Version() string {
	if len(version) == 0 {
		return defaultVersion
	}
	return version
}
