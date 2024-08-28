package streams

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type jetstreamConsumer struct {
	StreamName string
	jStream    *Stream
	stream     jetstream.Stream
	conn       *nats.Conn
	cc         jetstream.ConsumeContext
	handlers   map[string]handlerFunc
}

type handlerFunc func(context.Context, *Stream, string, []byte, string) error

func NewJetstreamConsumer(jStream *Stream, streamName string) *jetstreamConsumer {
	handlers := make(map[string]handlerFunc)
	return &jetstreamConsumer{handlers: handlers, StreamName: streamName, jStream: jStream}
}

func (c *jetstreamConsumer) init() error {
	var err error
	c.conn, err = c.jStream.natsConnect()
	if err != nil {
		return err
	}

	js, err := jetstream.New(c.conn)
	if err != nil {
		return err
	}

	c.stream, err = js.Stream(context.Background(), c.StreamName)
	if err != nil {
		return err
	}

	return nil
}

func (c *jetstreamConsumer) Handle(subject string, handler handlerFunc) {
	c.handlers[subject] = handler
}

func (c *jetstreamConsumer) ReceiveMessages(durable string) error {
	// initialise connection, jetstream
	err := c.init()
	if err != nil {
		return err
	}

	ctx := context.Background()
	cons, err := c.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		DeliverPolicy: jetstream.DeliverLastPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	c.cc, err = cons.Consume(func(msg jetstream.Msg) {
		s := strings.Split(msg.Subject(), ".")
		handler := c.handlers[s[0]]
		if handler != nil {
			// fmt.Println("ReceiveMessages", msg.Subject(), string(msg.Data()), msg.Headers())
			publishedBy := getHeader(msg.Headers(), "publishedBy")
			err = handler(ctx, c.jStream, msg.Subject(), msg.Data(), publishedBy)
			if err != nil {
				//msg.Nak()
				msg.Ack()
				fmt.Println(err.Error())
			} else {
				msg.Ack()
			}
		} else {
			fmt.Println("no consumer for subject", s)
		}
	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
		fmt.Println("consumer error", err.Error())
	}))
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	return nil
}

func (c *jetstreamConsumer) Close() {
	defer c.conn.Close()
	c.cc.Stop()
}

func getHeader(headers map[string][]string, name string) string {
	for hn, hv := range headers {
		if hn == name {
			if len(hv) > 0 {
				return hv[0]
			}
		}
	}
	return ""
}
