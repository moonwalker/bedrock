package streams

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

const (
	CONN_POOL_SIZE  = 100
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
	nc                  *nats.Conn
	mutex               *sync.RWMutex
	poolSize            int
	pool                chan *nats.Conn
	streamName          string
	natsURL             string
	natsNkeyUser        string
	natsNkeySeed        string
	natsCredentialsPath string
}

func NewStream(url, streamName string) *Stream {
	return &Stream{
		mutex:      new(sync.RWMutex),
		poolSize:   CONN_POOL_SIZE,
		pool:       make(chan *nats.Conn, CONN_POOL_SIZE),
		natsURL:    url,
		streamName: streamName,
	}
}

func (this *Stream) SetNKeys(user, seed string) {
	this.natsNkeyUser = user
	this.natsNkeySeed = seed
}

func (this *Stream) SetCredentialsPath(path string) {
	this.natsCredentialsPath = path
}

func (this *Stream) natsConnect() (*nats.Conn, error) {
	// singleton
	once := sync.OnceValues(func() (*nats.Conn, error) {

		// connect with nkeys if specified
		if len(this.natsNkeyUser) > 0 && len(this.natsNkeySeed) > 0 {
			return nats.Connect(this.natsURL, nats.Nkey(this.natsNkeyUser, this.sigHandler))
		}

		// connect with credentials if exists
		if _, err := os.Stat(this.natsCredentialsPath); err == nil {
			return nats.Connect(this.natsURL, nats.UserCredentials(this.natsCredentialsPath))
		}

		// regular connection
		return nats.Connect(this.natsURL)
	})

	return once()
}

func (this *Stream) natsConnectPool() (*nats.Conn, error) {
	connect := func() (*nats.Conn, error) {
		// connect with nkeys if specified
		if len(this.natsNkeyUser) > 0 && len(this.natsNkeySeed) > 0 {
			return nats.Connect(this.natsURL, nats.Nkey(this.natsNkeyUser, this.sigHandler))
		}

		// connect with credentials if exists
		if _, err := os.Stat(this.natsCredentialsPath); err == nil {
			return nats.Connect(this.natsURL, nats.UserCredentials(this.natsCredentialsPath))
		}

		// regular connection
		return nats.Connect(this.natsURL)
	}

	this.mutex.RLock()
	defer this.mutex.RUnlock()

	var nc *nats.Conn
	var err error
	select {
	case nc = <-this.pool:
		// reuse exists pool
		if nc.IsConnected() != true {
			// close to be sure
			nc.Close()
			// disconnected conn, create new *nats.Conn
			nc, err = connect()
		}
	default:
		// create *nats.Conn
		nc, err = connect()
	}

	return nc, err
}

func (this *Stream) sigHandler(b []byte) ([]byte, error) {
	sk, err := nkeys.FromSeed([]byte(this.natsNkeySeed))
	if err != nil {
		return nil, err
	}
	return sk.Sign(b)
}

func (this *Stream) Close() {
	// this.nc.Close()
}

func (this *Stream) ClosePool() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	close(this.pool)
	for nc := range this.pool {
		nc.Close()
	}

	this.pool = make(chan *nats.Conn, CONN_POOL_SIZE)
}

func (this *Stream) CreateStream(subjects []string) (jetstream.Stream, error) {
	return this.CreateStreamWithConfig(subjects, jetstream.StreamConfig{
		Name:     this.streamName,
		Subjects: subjects,
		MaxBytes: MAX_BYTES,
	})
}

func (this *Stream) CreateStreamWithConfig(subjects []string, config jetstream.StreamConfig) (jetstream.Stream, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	if config.Name == "" {
		config.Name = this.streamName
	}
	if config.Subjects == nil {
		config.Subjects = subjects
	}

	s, err := js.Stream(context.Background(), this.streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			s, err = js.CreateStream(context.Background(), config)
		}
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (this *Stream) GetStream(name string) (jetstream.Stream, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(context.Background(), name)
	return s, err
}

func (this *Stream) CreateConsumer(stream string, durable string) (jetstream.Consumer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(ctx, stream)
	if err != nil {
		return nil, err
	}

	return s.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:   durable,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
}

func (this *Stream) GetMessageBySequence(stream string, sequence uint64) ([]byte, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(context.Background(), stream)
	if err != nil {
		return nil, err
	}

	start0 := time.Now()
	m, err := s.GetMsg(context.Background(), sequence)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	slog.Debug("get message by sequence", "stream", stream, "sequence", sequence, "elapsed", elapsed0)

	return m.Data, nil
}

func (this *Stream) GetMessageByID(s jetstream.Stream, sequence uint64) ([]byte, error) {
	start0 := time.Now()
	m, err := s.GetMsg(context.Background(), sequence)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	slog.Debug("get message by sequence", "sequence", sequence, "elapsed", elapsed0)

	return m.Data, nil
}

func (this *Stream) FetchAllMessages(filters []string, startTime *time.Time) ([][]byte, error) {
	start0 := time.Now()
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(context.Background(), this.streamName)
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
	consumer, err := s.CreateOrUpdateConsumer(context.Background(), cc)
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

func (this *Stream) FetchLastMessagePerSubject(filters []string) (map[string][][]byte, error) {
	start0 := time.Now()
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(context.Background(), this.streamName)
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
	consumer, err := s.CreateOrUpdateConsumer(context.Background(), cc)
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

func (this *Stream) FetchLastMessageBySubject(filters []string) ([]byte, error) {
	start0 := time.Now()
	nc, err := this.natsConnect()
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
	s, err := js.Stream(context.Background(), this.streamName)
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	start3 := time.Now()
	var res []byte
	rm, err := s.GetLastMsgForSubject(context.Background(), filters[0])
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
		"this.natsConnect", elapsed0,
		"jetstream.New", elapsed1,
		"js.Stream", elapsed2,
		"s.GetLastMsgForSubject", elapsed3,
	)

	return res, err

	// var res []byte
	// cc := jetstream.ConsumerConfig{
	// 	AckPolicy:      jetstream.AckNonePolicy,
	// 	MaxAckPending:  MAX_ACK_PENDING,
	// 	MaxDeliver:     MAX_DELIVERY,
	// 	DeliverPolicy:  jetstream.DeliverLastPolicy,
	// 	FilterSubjects: filters,
	// }

	// start1 := time.Now()
	// consumer, err := s.CreateOrUpdateConsumer(context.Background(), cc)
	// if err != nil {
	// 	return nil, err
	// }
	// elapsed1 := getElapsed(start1)

	// start2 := time.Now()
	// mb, err := consumer.FetchNoWait(1)
	// if err != nil {
	// 	return nil, err
	// }
	// elapsed2 := getElapsed(start2)

	// for m := range mb.Messages() {
	// 	res = m.Data()
	// 	break
	// }

	// slog.Debug("fetch last message per subject",
	// 	"filters", filters,
	// 	"create stream", elapsed0,
	// 	"create consumer", elapsed1,
	// 	"fetch messages", elapsed2,
	// )

	// return res, nil
}

func (this *Stream) Publish(subject string, payload []byte) (*jetstream.PubAck, error) {
	nc, err := this.natsConnect()
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

func (this *Stream) PublishMsg(subject string, payload []byte, publisher string) (*jetstream.PubAck, error) {
	nc, err := this.natsConnect()
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
