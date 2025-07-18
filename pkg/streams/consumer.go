package streams

import (
	"context"
	"errors"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type HandlerFunc func(context.Context, string, map[string][]string, []byte, uint64) error

type HandlerFuncNoAck func(context.Context, *jetstream.Msg) error

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
			meta, err := msg.Metadata()
			if err != nil {
				return
			}
			err = handler(ctx, msg.Subject(), msg.Headers(), msg.Data(), meta.Sequence.Stream)
			if err != nil {
				msg.Nak()
			} else {
				msg.Ack()
			}
		}
	})
}

func ConsumeMessagesNoAck(nc *nats.Conn, streamName string, subject string, durable string, handler HandlerFuncNoAck) (jetstream.ConsumeContext, error) {
	ctx := context.Background()

	if handler == nil {
		return nil, errors.New("handler cannot be nil")
	}

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
		handler(ctx, &msg)
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
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:     jetstream.AckAllPolicy,
	})
	if err != nil {
		return err
	}

	si, err := stream.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		return err
	}
	/*msgCount := 0
	for _, v := range si.State.Subjects {
		msgCount += int(v)
	}*/
	msgCount := len(si.State.Subjects)

	wg := sync.WaitGroup{}
	wg.Add(msgCount)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		if handler != nil {
			meta, err := msg.Metadata()
			if err != nil {
				return
			}
			err = handler(ctx, msg.Subject(), msg.Headers(), msg.Data(), meta.Sequence.Stream)
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
