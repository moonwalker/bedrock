package eventsource

import (
	"encoding/json"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/moonwalker/bedrock/pkg/rules"
)

var wg sync.WaitGroup

// $ go test -count=1 -run TestNatsEventsource -v pkg/rules/eventsource/*.go
func TestNatsEventsource(t *testing.T) {
	commands := make(chan *rules.Event)
	events := make(chan *rules.Event)

	evs := NewNatsEventSource(nats.DefaultURL)
	err := evs.Receive("test", commands, events)
	if err != nil {
		t.Fatal(err)
	}

	reschan := make(chan *rules.EvalResult)
	go eventsLoop(events, reschan, time.Second*1)
	go handleExecResults(reschan)
	publishEvents(t, 10)

	wg.Wait()
}

func fakeExecRule(e *rules.Event, delay time.Duration) *rules.EvalResult {
	time.Sleep(delay)
	return &rules.EvalResult{Event: e}
}

func eventsLoop(events chan *rules.Event, reschan chan *rules.EvalResult, delay time.Duration) {
	for e := range events {
		reschan <- fakeExecRule(e, delay)
	}
}

func handleExecResults(execres chan *rules.EvalResult) {
	for res := range execres {
		log.Println("event processed on topic: " + res.Event.Topic)
		wg.Done()
	}
}

func publishEvents(t *testing.T, count int) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatal(err)
	}

	b, err := json.Marshal(map[string]interface{}{
		"foo": "bar",
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < count; i++ {
		err := nc.Publish("TEST", b)
		if err == nil {
			wg.Add(1)
		}
	}
}
