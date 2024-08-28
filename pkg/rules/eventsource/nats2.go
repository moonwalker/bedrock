package eventsource

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"

	"github.com/moonwalker/bedrock/pkg/rules"
)

type natsEventSourceV2 struct {
	url      string
	con      *nats.Conn
	cmdSub   *nats.Subscription
	workSubs []*nats.Subscription
}

func NewNatsEventSourceV2(url string) EventSource {
	return &natsEventSourceV2{url: url}
}

func (s *natsEventSourceV2) Receive(queue string, commands chan *rules.Event, events chan *rules.Event, topics []string) error {
	var err error
	s.con, err = nats.Connect(s.url,
		nats.ErrorHandler(errorHandler),
		nats.DisconnectHandler(disconnectHandler),
		nats.ReconnectHandler(reconnectHandler),
		nats.ClosedHandler(closedHandler),
	)
	if err != nil {
		return err
	}

	// subscription for commands
	s.cmdSub, err = s.con.Subscribe(rules.EventPrefix+"*", func(msg *nats.Msg) {
		_, cmd := rules.CommandTopics[msg.Subject]
		if cmd {
			commands <- &rules.Event{
				Topic: msg.Subject,
			}
		}
	})
	if err != nil {
		return err
	}

	// subscription for workloads
	for _, topic := range topics {
		err = s.subscribe(queue, events, topic)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *natsEventSourceV2) TriggerReload() error {
	nc, err := nats.Connect(s.url)
	if err != nil {
		return err
	}
	defer nc.Close()

	nc.Publish(rules.CmdReload, nil)
	nc.Flush()

	return nc.LastError()
}

func (s *natsEventSourceV2) Close() {
	if s.cmdSub != nil {
		s.cmdSub.Unsubscribe()
	}
	if s.workSubs != nil {
		for _, sub := range s.workSubs {
			sub.Unsubscribe()
		}
	}
	s.con.Close()
}

func (s *natsEventSourceV2) subscribe(queue string, events chan *rules.Event, topic string) error {
	sub, err := s.con.QueueSubscribe(topic, queue, func(msg *nats.Msg) {
		go func() {
			dryRun := gjson.GetBytes(msg.Data, rules.DRY_RUN_KEY).Bool()
			events <- &rules.Event{
				Received: time.Now().UTC(),
				DryRun:   dryRun,
				Topic:    msg.Subject,
				Data:     msg.Data,
			}
		}()
	})
	if err != nil {
		return err
	}

	s.workSubs = append(s.workSubs, sub)

	// set no limits for workload subscription, just in case
	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		return err
	}

	return nil
}
