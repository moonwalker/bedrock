package streams

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func (this *Stream) CreateBucket(bucket string, history int, ttl time.Duration) error {
	nc, err := this.natsConnect()
	if err != nil {
		return err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	_, err = js.KeyValue(context.Background(), bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			cfg := jetstream.KeyValueConfig{
				Bucket:   bucket,
				MaxBytes: MAX_BYTES,
			}
			if history > 0 {
				cfg.History = uint8(history)
			}
			if ttl != 0 {
				cfg.TTL = ttl
			}
			_, err = js.CreateKeyValue(context.Background(), cfg)
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

func (this *Stream) GetKVs(bucket string) (map[string][]byte, error) {
	nc, err := this.natsConnect()
	if err != nil {
		return nil, err
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
