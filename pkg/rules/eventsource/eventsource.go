package eventsource

import (
	"github.com/moonwalker/bedrock/pkg/rules"
)

type EventSource interface {
	Receive(queue string, commands chan *rules.Event, events chan *rules.Event) error
	TriggerReload() error
	Close()
}
