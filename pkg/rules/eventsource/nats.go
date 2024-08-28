package eventsource

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"

	"github.com/moonwalker/bedrock/pkg/rules"
)

type natsEventSource struct {
	url     string
	con     *nats.Conn
	cmdSub  *nats.Subscription
	workSub *nats.Subscription
}

func NewNatsEventSource(url string) EventSource {
	return &natsEventSource{url: url}
}

func (s *natsEventSource) Receive(queue string, commands chan *rules.Event, events chan *rules.Event, topics []string) error {
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
	s.workSub, err = s.con.QueueSubscribe("*", queue, func(msg *nats.Msg) {
		_, cmd := rules.CommandTopics[msg.Subject]
		if !cmd {
			go func() {
				received := time.Now().UTC()
				dryRun := gjson.GetBytes(msg.Data, rules.DRY_RUN_KEY).Bool()
				topic := msg.Subject
				data := msg.Data

				// custom event
				if msg.Subject == "CUSTOM_EVENT" {
					topic = gjson.GetBytes(msg.Data, "topic").String()
					data = []byte(gjson.GetBytes(msg.Data, "payload").Raw)
				}

				events <- &rules.Event{
					Received: received,
					DryRun:   dryRun,
					Topic:    topic,
					Data:     data,
				}
			}()
		}
	})
	if err != nil {
		return err
	}

	// set no limits for workload subscription, just in case
	err = s.workSub.SetPendingLimits(-1, -1)
	if err != nil {
		return err
	}

	return nil
}

func (s *natsEventSource) TriggerReload() error {
	nc, err := nats.Connect(s.url)
	if err != nil {
		return err
	}
	defer nc.Close()

	nc.Publish(rules.CmdReload, nil)
	nc.Flush()

	return nc.LastError()
}

func (s *natsEventSource) Close() {
	if s.cmdSub != nil {
		s.cmdSub.Unsubscribe()
	}
	if s.workSub != nil {
		s.workSub.Unsubscribe()
	}
	s.con.Close()
}

// error handler helper functions

func errorHandler(nc *nats.Conn, sub *nats.Subscription, err error) {
	slog.Error("nats error", "err", err.Error())

	if err == nats.ErrSlowConsumer {
		pendingMsgs, pendingBytes, err := sub.Pending()
		if err != nil {
			slog.Error("failed to get pending messages", "err", err.Error())
			return
		}
		droppedMsgs, err := sub.Dropped()
		if err != nil {
			slog.Error("failed to get dropped messages", "err", err.Error())
			return
		}
		slog.Error("falling behind with pending messages",
			"droppedMsgs", droppedMsgs,
			"pendingMsgs", pendingMsgs,
			"pendingBytes", pendingBytes,
			"subject", sub.Subject,
		)
	}
}

func disconnectHandler(nc *nats.Conn) {
	slog.Debug("nats disconnected", "lastError", nc.LastError())
}

func reconnectHandler(nc *nats.Conn) {
	slog.Debug("nats reconnected", "url", nc.ConnectedUrl())
}

func closedHandler(nc *nats.Conn) {
	slog.Debug("nats connection closed", "reason", nc.LastError())
}
