package streams

// Temporary benchmark file to compare different LastPerSubject implementations
// This file should be DELETED once we've confirmed the optimal approach
// Run with: go test -bench=BenchmarkLastPerSubject -benchmem -benchtime=5x pkg/streams/*.go

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// v0.2.33 implementation - FetchNoWait, no timeout
func (s *Stream) lastPerSubject_v0233(filters []string) (map[string][][]byte, error) {
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
		subject := m.Subject()
		if res[subject] == nil {
			res[subject] = make([][]byte, 0, 1)
		}
		res[subject] = append(res[subject], m.Data())
	}

	slog.Debug("v0.2.33: fetch last message per subject",
		"filters", filters,
		"create stream", elapsed0,
		"create consumer", elapsed1,
		"fetch messages", elapsed2,
		"count", len(res),
	)

	return res, nil
}

// v0.3.0 implementation - Fetch with 1s timeout
func (s *Stream) lastPerSubject_v030(filters []string) (map[string][][]byte, error) {
	ctx := context.Background()
	start0 := time.Now()
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(ctx, s.streamName)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	res := make(map[string][][]byte)

	cc := jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckNonePolicy,
		DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
		FilterSubjects:    filters,
		InactiveThreshold: 5 * time.Second,
	}

	start1 := time.Now()
	consumer, err := j.CreateConsumer(ctx, cc)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	mb, err := consumer.Fetch(FETCH_NO_WAIT, jetstream.FetchMaxWait(1*time.Second))
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	for m := range mb.Messages() {
		subject := m.Subject()
		if res[subject] == nil {
			res[subject] = make([][]byte, 0, 1)
		}
		res[subject] = append(res[subject], m.Data())
	}

	slog.Debug("v0.3.0: fetch last message per subject",
		"filters", filters,
		"create stream", elapsed0,
		"create consumer", elapsed1,
		"fetch messages", elapsed2,
		"count", len(res),
	)

	return res, nil
}

// v0.3.1 implementation - NumPending + FetchNoWait
func (s *Stream) lastPerSubject_v031(filters []string) (map[string][][]byte, error) {
	ctx := context.Background()
	start0 := time.Now()
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(ctx, s.streamName)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	res := make(map[string][][]byte)

	cc := jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckNonePolicy,
		DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
		FilterSubjects:    filters,
		InactiveThreshold: 1 * time.Second,
	}

	start1 := time.Now()
	consumer, err := j.CreateConsumer(ctx, cc)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	consumerInfo, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	numPending := consumerInfo.NumPending

	start2 := time.Now()
	mb, err := consumer.FetchNoWait(int(numPending))
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	for m := range mb.Messages() {
		subject := m.Subject()
		if res[subject] == nil {
			res[subject] = make([][]byte, 0, 1)
		}
		res[subject] = append(res[subject], m.Data())
	}

	slog.Debug("v0.3.1: fetch last message per subject",
		"filters", filters,
		"create stream", elapsed0,
		"create consumer", elapsed1,
		"fetch messages", elapsed2,
		"pending", numPending,
		"count", len(res),
	)

	return res, nil
}

// Current implementation - Fetch with 2s timeout
func (s *Stream) lastPerSubject_current(filters []string) (map[string][][]byte, error) {
	ctx := context.Background()
	start0 := time.Now()
	nc, err := s.natsConnect()
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	j, err := js.Stream(ctx, s.streamName)
	if err != nil {
		return nil, err
	}
	elapsed0 := getElapsed(start0)

	res := make(map[string][][]byte)

	cc := jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckNonePolicy,
		DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
		FilterSubjects:    filters,
		InactiveThreshold: 1 * time.Second,
	}

	start1 := time.Now()
	consumer, err := j.CreateConsumer(ctx, cc)
	if err != nil {
		return nil, err
	}
	elapsed1 := getElapsed(start1)

	start2 := time.Now()
	mb, err := consumer.Fetch(FETCH_NO_WAIT, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		return nil, err
	}
	elapsed2 := getElapsed(start2)

	for m := range mb.Messages() {
		subject := m.Subject()
		if res[subject] == nil {
			res[subject] = make([][]byte, 0, 1)
		}
		res[subject] = append(res[subject], m.Data())
	}

	slog.Debug("current: fetch last message per subject",
		"filters", filters,
		"create stream", elapsed0,
		"create consumer", elapsed1,
		"fetch messages", elapsed2,
		"count", len(res),
	)

	return res, nil
}

// Benchmarks

func BenchmarkLastPerSubject_v0233(b *testing.B) {
	streamName := "dreamz"
	filter := []string{"core.dreamz.*.entries.*.>"}

	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := jStream.lastPerSubject_v0233(filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLastPerSubject_v030(b *testing.B) {
	streamName := "dreamz"
	filter := []string{"core.dreamz.*.entries.*.>"}

	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := jStream.lastPerSubject_v030(filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLastPerSubject_v031(b *testing.B) {
	streamName := "dreamz"
	filter := []string{"core.dreamz.*.entries.*.>"}

	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := jStream.lastPerSubject_v031(filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLastPerSubject_Current(b *testing.B) {
	streamName := "dreamz"
	filter := []string{"core.dreamz.*.entries.*.>"}

	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := jStream.lastPerSubject_current(filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}
