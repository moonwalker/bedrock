package redistore

import (
	"log/slog"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/moonwalker/bedrock/pkg/store"
)

var (
	DEL_SCRIPT = redis.NewScript(0, `for i, k in ipairs(redis.call('KEYS', ARGV[1])) do redis.call('DEL', k) end`)
)

type redistore struct {
	pool *redis.Pool
}

func New(redisURL string) store.Store {
	return &redistore{
		pool: &redis.Pool{
			MaxActive: 5,
			MaxIdle:   5,
			Wait:      true,
			Dial: func() (redis.Conn, error) {
				return redis.DialURL(redisURL)
			},
		},
	}
}

func (s *redistore) GetInternalStore() interface{} {
	return s.pool
}

func (s *redistore) Get(key string) (value []byte, err error) {
	// defer debugDuration(time.Now(), "GET", key)

	c := s.pool.Get()
	defer c.Close()

	res, err := c.Do("GET", key)
	if res != nil {
		return redis.Bytes(res, err)
	}
	return nil, err
}

func (s *redistore) Set(key string, value []byte, options *store.WriteOptions) error {
	cmd := "SET"
	useTTL := options != nil && options.TTL > 0
	if useTTL {
		cmd = "SETEX"
	}

	// defer debugDuration(time.Now(), cmd, key)

	c := s.pool.Get()
	defer c.Close()

	if useTTL {
		return c.Send(cmd, key, options.TTL, value)
	}

	return c.Send(cmd, key, value)
}

func (s *redistore) Delete(key string) error {
	// defer debugDuration(time.Now(), "DEL", key)

	c := s.pool.Get()
	defer c.Close()

	return c.Send("DEL", key)
}

func (s *redistore) DeleteAll(pattern string) error {
	// defer debugDuration(time.Now(), "DEL_SCRIPT", pattern)

	c := s.pool.Get()
	defer c.Close()

	_, err := DEL_SCRIPT.Do(c, pattern)
	return err
}

func (s *redistore) Exists(key string) (bool, error) {
	// defer debugDuration(time.Now(), "EXISTS", key)

	c := s.pool.Get()
	defer c.Close()

	return redis.Bool(c.Do("EXISTS", key))
}

func (s *redistore) Expire(key string, ttl int64) error {
	// defer debugDuration(time.Now(), "EXPIRE", key, ttl)

	c := s.pool.Get()
	defer c.Close()

	return c.Send("EXPIRE", key, ttl)
}

func (s *redistore) Scan(prefix string, skip int, limit int, fn func(key string, val []byte)) error {
	defer debugDuration(time.Now(), "SCAN")

	c := s.pool.Get()
	defer c.Close()

	var (
		count  int
		cursor int = skip
		keys   []string
	)

	for {
		args := []interface{}{cursor, "MATCH", prefix}
		if limit > 0 {
			args = append(args, "COUNT", limit)
		}

		values, err := redis.Values(c.Do("SCAN", args...))
		if err != nil {
			return err
		}

		_, err = redis.Scan(values, &cursor, &keys)
		if err != nil {
			return err
		}

		for _, key := range keys {
			val, err := redis.Bytes(c.Do("GET", key))
			if err != nil {
				return err
			}
			fn(key, val)
			count++
		}

		if cursor == 0 || (limit > 0 && count >= limit) {
			break
		}
	}

	return nil
}

func (s *redistore) Count(prefix string) int {
	c := s.pool.Get()
	defer c.Close()

	var (
		count  int
		cursor int
		keys   []string
	)

	for {
		values, _ := redis.Values(c.Do("SCAN", cursor, "MATCH", prefix))
		redis.Scan(values, &cursor, &keys)

		for _ = range keys {
			count++
		}

		if cursor == 0 {
			break
		}
	}

	return count
}

func (s *redistore) Close() error {
	c := s.pool.Get()
	return c.Close()
}

func debugDuration(start time.Time, cmd string, args ...interface{}) {
	elapsed := time.Since(start)
	slog.Debug("redis command", "cmd", cmd, "args", args, "took", elapsed.String())
}
