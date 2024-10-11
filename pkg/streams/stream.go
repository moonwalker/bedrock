package streams

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	FETCH_NO_WAIT   = 100000
	MAX_ACK_PENDING = -1 //unlimited
	MAX_DELIVERY    = -1 // unlimited
	MAX_BYTES       = -1 // unlimited
)

type KeyHistory struct {
	Created  time.Time `json:"created"`
	Revision uint64    `json:"revision"`
	Value    string    `json:"value"`
}

type Stream struct {
	nc         *nats.Conn
	streamName string
	connPool   *NatsConnPool
}

func NewStream(url string, streamName string) (*Stream, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	return &Stream{nc: nc, streamName: streamName}, nil
}

func NewStreamWithConn(nc *nats.Conn, streamName string) *Stream {
	return &Stream{
		nc:         nc,
		streamName: streamName,
	}
}

func NewStreamWithConnPool(url string, streamName string, options ...nats.Option) *Stream {
	return &Stream{
		streamName: streamName,
		connPool:   NewNatsConnPool(url, options...),
	}
}

func (s *Stream) natsConnect() (*nats.Conn, error) {
	if s.nc != nil {
		return s.nc, nil
	}

	return s.connPool.GetConnection()
}

func (s *Stream) CreateStream(subjects []string) (jetstream.Stream, error) {
	return s.CreateStreamWithConfig(subjects, jetstream.StreamConfig{
		Name:     s.streamName,
		Subjects: subjects,
		MaxBytes: MAX_BYTES,
	})
}

func (s *Stream) CreateStreamWithConfig(subjects []string, config jetstream.StreamConfig) (jetstream.Stream, error) {
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	if config.Name == "" {
		config.Name = s.streamName
	}
	if config.Subjects == nil {
		config.Subjects = subjects
	}

	j, err := js.Stream(context.Background(), s.streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			j, err = js.CreateStream(context.Background(), config)
		}
		if err != nil {
			return nil, err
		}
	}

	return j, nil
}

func (s *Stream) GetStream(name string) (jetstream.Stream, error) {
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(context.Background(), name)
	return j, err
}

func (s *Stream) CreateConsumer(stream string, durable string) (jetstream.Consumer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(ctx, stream)
	if err != nil {
		return nil, err
	}

	return j.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:   durable,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
}

func (s *Stream) GetMessageBySequence(stream string, sequence uint64) ([]byte, error) {
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(context.Background(), stream)
	if err != nil {
		return nil, err
	}

	start0 := time.Now()
	m, err := j.GetMsg(context.Background(), sequence)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	slog.Debug("get message by sequence", "stream", stream, "sequence", sequence, "elapsed", elapsed0)

	return m.Data, nil
}

func (s *Stream) GetMessageByID(j jetstream.Stream, sequence uint64) ([]byte, error) {
	start0 := time.Now()
	m, err := j.GetMsg(context.Background(), sequence)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	slog.Debug("get message by sequence", "sequence", sequence, "elapsed", elapsed0)

	return m.Data, nil
}

func (s *Stream) FetchAll(filters []string, startTime *time.Time) ([][]byte, error) {
	start0 := time.Now()
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(context.Background(), s.streamName)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	cc := jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckNonePolicy,
		MaxAckPending:  MAX_ACK_PENDING,
		MaxDeliver:     MAX_DELIVERY,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		FilterSubjects: filters,
	}

	if startTime != nil {
		cc.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		cc.OptStartTime = startTime
	}

	start1 := time.Now()
	consumer, err := j.CreateOrUpdateConsumer(context.Background(), cc)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	res := make([][]byte, 0)
	start2 := time.Now()
	mb, err := consumer.FetchNoWait(FETCH_NO_WAIT)
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	i := 0
	for m := range mb.Messages() {
		i++
		res = append(res, m.Data())
	}

	slog.Debug("fetch all messages",
		"filters", filters,
		"create stream", elapsed0,
		"create consumer", elapsed1,
		"fetch messages", elapsed2,
	)

	return res, nil
}

func (s *Stream) LastPerSubject(filters []string) (map[string][][]byte, error) {
	start0 := time.Now()
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(context.Background(), s.streamName)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	res := make(map[string][][]byte, 0)
	cc := jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckNonePolicy,
		MaxAckPending:  MAX_ACK_PENDING,
		MaxDeliver:     MAX_DELIVERY,
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		FilterSubjects: filters,
	}

	start1 := time.Now()
	consumer, err := j.CreateOrUpdateConsumer(context.Background(), cc)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	mb, err := consumer.FetchNoWait(FETCH_NO_WAIT)
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	for m := range mb.Messages() {
		s := m.Subject()
		if res[s] == nil {
			res[s] = make([][]byte, 0)
		}
		res[s] = append(res[s], m.Data())
	}

	slog.Debug("fetch last message per subject",
		"filters", filters,
		"create stream", elapsed0,
		"create consumer", elapsed1,
		"fetch messages", elapsed2,
		"count", len(res),
	)

	return res, nil
}

func (s *Stream) LastBySubject(filters []string) ([]byte, error) {
	start0 := time.Now()
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	start1 := time.Now()
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	j, err := js.Stream(context.Background(), s.streamName)
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	start3 := time.Now()
	var res []byte
	rm, err := j.GetLastMsgForSubject(context.Background(), filters[0])
	if err != nil {
		if err == jetstream.ErrMsgNotFound {
			return res, nil
		}
		return nil, err
	}
	res = rm.Data
	elapsed3 := getElapsed(start3)

	slog.Debug("get last message for subject",
		"filters", filters,
		"s.natsConnect", elapsed0,
		"jetstream.New", elapsed1,
		"js.Stream", elapsed2,
		"s.GetLastMsgForSubject", elapsed3,
	)

	return res, err
}

func (s *Stream) Publish(subject string, payload []byte) (*jetstream.PubAck, error) {
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	pa, err := js.Publish(context.Background(), subject, payload)
	if err != nil {
		return nil, err
	}
	elapsed := getElapsed(start)

	slog.Debug("publish message",
		"elapsed", elapsed,
		"subject", subject,
	)

	return pa, nil
}

func (s *Stream) PublishMsg(subject string, payload []byte, publisher string) (*jetstream.PubAck, error) {
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	pa, err := js.PublishMsg(context.Background(), &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header: nats.Header{
			"publishedBy": []string{publisher},
		},
	})
	if err != nil {
		return nil, err
	}
	elapsed := getElapsed(start)

	slog.Debug("publish message", "elapsed", elapsed)

	return pa, nil
}
