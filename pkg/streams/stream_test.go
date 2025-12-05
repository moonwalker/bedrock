package streams

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	natsURL    = nats.DefaultURL
	streamName = "TEST_STREAM"
)

// $ go test -run TestNatsConnect -count=1 -v pkg/streams/*.go
func TestNatsConnect(t *testing.T) {
	js, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatal(err)
	}

	js.CreateStream([]string{"foo.*"})
	js.CreateStream([]string{"foo.*"})
	js.CreateStream([]string{"foo.*"})
}

func TestCreateKV(t *testing.T) {
	bucket := "testbucket"
	key := "testkey"
	val := "testval"

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			kv, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
				Bucket: bucket,
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	err = kv.Delete(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = kv.Create(context.Background(), key, []byte(val))
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsume(t *testing.T) {
	svc := "servicename"

	consumer_subjects := []string{"FOO.*"}
	publish_subject := "FOO.TEST1"
	publish_count := 10

	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatal(err)
	}

	_, err = jStream.CreateStream(consumer_subjects)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}

	// consumer
	cons, err := jStream.CreateConsumer(streamName, svc)
	if err != nil {
		t.Fatal(err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		wg.Done()
	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
		t.Error(err)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()

	// publish
	wg.Add(publish_count)
	for i := 0; i < publish_count; i++ {
		_, err = jStream.Publish(publish_subject, []byte{})
		if err != nil {
			t.Error(err)
		}
	}

	wg.Wait()
}

func TestFetchMessages(t *testing.T) {
	streamName := "transaction"
	filters := []string{"transaction.*.balance_transaction"}

	tt := time.Now().Local().Add(-1 * time.Minute * time.Duration(35))
	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatal(err)
	}

	jStream.CreateStream(filters)
	msgs, err := jStream.FetchAll(filters, &tt)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("messages:", msgs)
}

// $ go test -run TestLastPerSubject -count=1 -v pkg/streams/*.go
func TestLastPerSubject(t *testing.T) {
	streamName := "content_dreamz"
	filter := []string{"core.content_dreamz.*.entries.*.>"}

	jStream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatal(err)
	}
	lmps, err := jStream.LastPerSubject(filter)
	if err != nil {
		t.Fatal(err)
	}

	total := 0
	contentTypes := make([]string, 0)
	entryCount := make(map[string]int)
	for subject := range lmps {
		ct := strings.Split(subject, ".")[4]
		if entryCount[ct] == 0 {
			contentTypes = append(contentTypes, ct)
		}
		entryCount[ct]++
		total++
	}

	sort.Slice(contentTypes, func(i, j int) bool {
		return contentTypes[i] < contentTypes[j]
	})

	for _, ct := range contentTypes {
		fmt.Printf("%s: %d\n", ct, entryCount[ct])
	}
	fmt.Printf("total entries: %d\n", total)
}
