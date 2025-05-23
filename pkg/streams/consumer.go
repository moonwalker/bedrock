package streams

import (
	"context"
	"sync"

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

func ConsumeAllMessagesSync(nc *nats.Conn, streamName string, subject string, handler HandlerFunc) error {
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckAllPolicy,
	})
	if err != nil {
		return err
	}

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		return err
	}
	msgCount := 0
	for _, v := range si.State.Subjects {
		msgCount += int(v)
	}

	wg := sync.WaitGroup{}
	wg.Add(msgCount)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		if handler != nil {
			err := handler(ctx, msg.Subject(), msg.Headers(), msg.Data())
			if err != nil {
				return
			}
			msg.Ack()
			wg.Done()
		}
	})
	if err != nil {
		return err
	}

	wg.Wait()
	cc.Stop()

	return nil
}
