package streams

import "time"

func getElapsed(start time.Time) string {
	return time.Since(start).String()
}
