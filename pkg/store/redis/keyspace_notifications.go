package redistore

import (
	"fmt"
	"strings"

	"github.com/gomodule/redigo/redis"

	"github.com/moonwalker/bedrock/pkg/store"
)

const (
	// E: keyspace events
	// g: generic commands
	// $: string commands
	// s: set commands
	// x: expired events
	knconfig   = "Eg$sx"
	psubchan   = "__key*__:*"
	locksuffix = "__lock"
)

type changedCallback func(string, string)
type expiredCallback func(string, string)
type deletedCallback func(string, string)

type KeyspaceNotifications struct {
	withLock         bool
	redisPool        *redis.Pool
	changedCallbacks map[string]changedCallback
	expiredCallbacks map[string]expiredCallback
	deletedCallbacks map[string]deletedCallback
}

type KeyspaceNotificationsOption func(*KeyspaceNotifications)

func NotifyWithLock() KeyspaceNotificationsOption {
	return func(kn *KeyspaceNotifications) {
		kn.withLock = true
	}
}

func NewKeyspaceNotifications(s store.Store, opts ...KeyspaceNotificationsOption) (*KeyspaceNotifications, error) {
	redisPool, ok := s.GetInternalStore().(*redis.Pool)
	if !ok {
		return nil, fmt.Errorf("failed to get redis pool")
	}

	kn := &KeyspaceNotifications{
		redisPool:        redisPool,
		changedCallbacks: make(map[string]changedCallback),
		expiredCallbacks: make(map[string]expiredCallback),
		deletedCallbacks: make(map[string]deletedCallback),
	}

	for _, opt := range opts {
		opt(kn)
	}

	return kn, nil
}

func (k *KeyspaceNotifications) Listen() error {
	// connection for pubsub
	pubSubConn := k.redisPool.Get()
	defer pubSubConn.Close()

	// set keyspace notifications config
	_, err := pubSubConn.Do("CONFIG", "SET", "notify-keyspace-events", knconfig)
	if err != nil {
		return err
	}

	// subscribe to all key events and subscribes
	psc := redis.PubSubConn{Conn: pubSubConn}
	err = psc.PSubscribe(psubchan)
	if err != nil {
		return err
	}

	// lock
	var lockConn redis.Conn
	if k.withLock {
		// connection for locks
		lockConn = k.redisPool.Get()
		defer lockConn.Close()
	}

	// event handler
	eventHandler := func(op, key string) {
		switch op {
		case "set":
			k.changedHandler(key)
		case "del":
			k.deletedHandler(key)
		case "expired":
			k.expiredHandler(key)
		}
	}

	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			// parse key
			key := string(v.Data)

			// skip lock events
			if strings.HasSuffix(key, locksuffix) {
				continue
			}

			// parse event
			event := parseRedisEvent(v.Channel)

			// with locking
			if k.withLock {
				if k.lock(lockConn, key) {
					eventHandler(event, key)
					k.release(lockConn, key)
				}
			} else {
				eventHandler(event, key)
			}

		case error:
			return v
		}
	}
}

func (k *KeyspaceNotifications) KeyChanged(pattern string, cb changedCallback) {
	k.changedCallbacks[pattern] = cb
}

func (k *KeyspaceNotifications) KeyDeleted(pattern string, cb deletedCallback) {
	k.deletedCallbacks[pattern] = cb
}

func (k *KeyspaceNotifications) KeyExpired(pattern string, cb expiredCallback) {
	k.expiredCallbacks[pattern] = cb
}

// private

func (k *KeyspaceNotifications) lock(conn redis.Conn, key string) bool {
	i, _ := redis.Int(conn.Do("SETNX", fmt.Sprintf("%s:%s", key, locksuffix), 1))
	return i != 0
}

func (k *KeyspaceNotifications) release(conn redis.Conn, key string) {
	conn.Do("DEL", fmt.Sprintf("%s:%s", key, locksuffix))
}

func (k *KeyspaceNotifications) changedHandler(key string) {
	for pattern, cb := range k.changedCallbacks {
		if matched, match := match(pattern, key); matched {
			cb(key, match)
		}
	}
}

func (k *KeyspaceNotifications) deletedHandler(key string) {
	for pattern, cb := range k.deletedCallbacks {
		if matched, match := match(pattern, key); matched {
			cb(key, match)
		}
	}
}

func (k *KeyspaceNotifications) expiredHandler(key string) {
	for pattern, cb := range k.expiredCallbacks {
		if matched, match := match(pattern, key); matched {
			cb(key, match)
		}
	}
}

func match(pattern string, value string) (matched bool, match string) {
	i := strings.Index(pattern, "*")
	if i > -1 {
		pattern = strings.Replace(pattern, "*", "", 1)
		if strings.HasPrefix(value, pattern) {
			matched = true
			match = value[i:]
		}
		return
	}
	return pattern == value, value
}

func parseRedisEvent(channel string) string {
	switch channel {
	case "__keyevent@0__:set":
		return "set"
	case "__keyevent@0__:del":
		return "del"
	case "__keyevent@0__:expired":
		return "expired"
	}
	return ""
}
