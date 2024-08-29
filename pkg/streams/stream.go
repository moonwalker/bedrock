package streams

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

const (
	FETCH_NO_WAIT   = 100000
	MAX_ACK_PENDING = -1
	MAX_DELIVERY    = -1
	MAX_BYTES       = 1000000000 // 1 GiB
)

type KeyHistory struct {
	Created  time.Time `json:"created"`
	Revision uint64    `json:"revision"`
	Value    string    `json:"value"`
}

type Stream struct {
	streamName          string
	natsURL             string
	natsNkeyUser        string
	natsNkeySeed        string
	natsCredentialsPath string
	EntriesPrefix       string
	SchemasPrefix       string
}

func NewStream(url, streamName, entriesPrefix, schemasPrefix string) *Stream {
	return &Stream{
		natsURL:       url,
		streamName:    streamName,
		EntriesPrefix: entriesPrefix,
		SchemasPrefix: schemasPrefix,
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

func (this *Stream) sigHandler(b []byte) ([]byte, error) {
	sk, err := nkeys.FromSeed([]byte(this.natsNkeySeed))
	if err != nil {
		return nil, err
	}
	return sk.Sign(b)
}

func (this *Stream) CreateStream(subjects []string) (jetstream.Stream, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	return js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     this.streamName,
		Subjects: subjects,
		MaxBytes: MAX_BYTES,
	})
}

func (this *Stream) GetStream(name string) (*nats.Conn, jetstream.Stream, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, nil, err
	}
	//defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}

	s, err := js.Stream(context.Background(), name)
	return nc, s, err
}

func (this *Stream) CreateConsumer(stream string, durable string) (jetstream.Consumer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}
	defer nc.Close()

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
	defer nc.Close()

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
	defer nc.Close()

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
	defer nc.Close()

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
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(context.Background(), this.streamName)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	var res []byte
	cc := jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckNonePolicy,
		MaxAckPending:  MAX_ACK_PENDING,
		MaxDeliver:     MAX_DELIVERY,
		DeliverPolicy:  jetstream.DeliverLastPolicy,
		FilterSubjects: filters,
	}

	start1 := time.Now()
	consumer, err := s.CreateOrUpdateConsumer(context.Background(), cc)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	mb, err := consumer.FetchNoWait(1)
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	for m := range mb.Messages() {
		res = m.Data()
		break
	}

	slog.Debug("fetch last message per subject",
		"filters", filters,
		"create stream", elapsed0,
		"create consumer", elapsed1,
		"fetch messages", elapsed2,
	)

	return res, nil
}

func (this *Stream) Publish(subject string, payload []byte) (*jetstream.PubAck, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}
	defer nc.Close()

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
		"streamName", this.streamName,
	)

	return pa, nil
}

func (this *Stream) PublishMsg(subject string, payload []byte, publisher string) (*jetstream.PubAck, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}
	defer nc.Close()

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

func (this *Stream) CreateBucket(bucket string, history int) error {
	nc, err := this.natsConnect()
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	_, err = js.KeyValue(context.Background(), bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			_, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
				Bucket:   bucket,
				History:  uint8(history),
				MaxBytes: MAX_BYTES,
			})
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *Stream) AddKeyValue(bucket string, key string, value []byte) error {
	nc, err := this.natsConnect()
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	start1 := time.Now()
	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	_, err = kv.Create(context.Background(), key, value)
	elapsed2 := getElapsed(start2)

	slog.Debug("add key-value",
		"Get KV store", elapsed1,
		"Add keyValue", elapsed2,
	)

	return err
}

func (this *Stream) PutKeyValue(bucket string, key string, value []byte) error {
	nc, err := this.natsConnect()
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	start1 := time.Now()
	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	_, err = kv.Put(context.Background(), key, value)
	elapsed2 := getElapsed(start2)

	slog.Debug("put key-value",
		"Get KV store", elapsed1,
		"Put keyValue", elapsed2,
	)

	return err
}

func (this *Stream) DeleteKeyValue(bucket string, key string) error {
	nc, err := this.natsConnect()
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	start1 := time.Now()
	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	err = kv.Delete(context.Background(), key)
	elapsed2 := getElapsed(start2)

	slog.Debug("delete key-value",
		"Get KV store", elapsed1,
		"Put keyValue", elapsed2,
	)

	return err
}

func (this *Stream) UpdateKeyValue(bucket string, key string, value []byte, revision uint64) error {
	nc, err := this.natsConnect()
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	start1 := time.Now()
	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	_, err = kv.Update(context.Background(), key, value, revision)
	elapsed2 := getElapsed(start2)

	slog.Debug("update key-value",
		"Get KV store", elapsed1,
		"Add keyValue", elapsed2,
	)

	return err
}

func (this *Stream) GetValueByKey(bucket string, key string) ([]byte, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	start1 := time.Now()
	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	kve, err := kv.Get(context.Background(), key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	slog.Debug("get value by key",
		"Get KV store", elapsed1,
		"Get Value", elapsed2,
	)

	return kve.Value(), nil
}

func (this *Stream) GetKeys(bucket string) ([]string, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return nil, err
	}

	keys, err := kv.ListKeys(context.Background())
	if err != nil {
		return nil, err
	}

	resp := make([]string, 0)
	for key := range keys.Keys() {
		resp = append(resp, key)
	}

	return resp, nil
}

func (this *Stream) GetKVs(nc *nats.Conn, bucket string) (map[string][]byte, error) {
	if nc == nil {
		nc, err := this.natsConnect()
		if err != nil {
			return nil, err
		}
		defer nc.Close()
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	keys, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}

	resp := make(map[string][]byte)
	for key := range keys.Keys() {
		kve, err := kv.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		resp[key] = kve.Value()
	}

	return resp, nil
}

func (this *Stream) GetKeyHistory(bucket string, key string) error {
	nc, err := this.natsConnect()
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	start1 := time.Now()
	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	kveH, err := kv.History(context.Background(), key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil
		}
		return err
	}
	elapsed2 := getElapsed(start2)

	hist := make([]KeyHistory, 0)
	for _, h := range kveH {
		hist = append(hist, KeyHistory{
			Created:  h.Created(),
			Revision: h.Revision(),
			Value:    string(h.Value()),
		})
	}

	slog.Debug("get key history",
		"Get KV store", elapsed1,
		"Get history", elapsed2,
		"History", hist,
	)

	return nil
}

func getElapsed(start time.Time) int64 {
	return time.Since(start).Microseconds()
}
