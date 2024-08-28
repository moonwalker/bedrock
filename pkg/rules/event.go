package rules

import "time"

const (
	EventPrefix = "rules."
	Status      = EventPrefix + "status"
	CmdReload   = EventPrefix + "reload"
	CmdStop     = EventPrefix + "stop"
	CmdResume   = EventPrefix + "resume"
)

var (
	// using an empty struct{} here has the advantage that it doesn't require any additional space
	// and go's internal map type is optimized for that kind of values
	// therefore map[string]struct{} is a popular choice for sets in the go world
	CommandTopics = map[string]struct{}{
		CmdReload: {},
		CmdStop:   {},
		CmdResume: {},
	}
)

type Event struct {
	Received  time.Time `json:"received"`
	Processed time.Time `json:"processed"`
	DryRun    bool      `json:"dryRun"`
	Topic     string    `json:"topic"`
	Data      []byte    `json:"data"`
}
