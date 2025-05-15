package streams

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type HandlerFunc func(context.Context, string, map[string][]string, []byte) error

func ConsumeMessages(nc *nats.Conn, streamName string, subject string, durable string, handler HandlerFunc) (jetstream.ConsumeContext, error) {
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject,
		Durable:       durable,
		DeliverPolicy: jetstream.DeliverLastPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, err
	}

	return cons.Consume(func(msg jetstream.Msg) {
		if handler != nil {
			err := handler(ctx, msg.Subject(), msg.Headers(), msg.Data())
			if err != nil {
				msg.Nak()
			} else {
				msg.Ack()
			}
		}
	})
}
