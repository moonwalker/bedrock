package repo

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/moonwalker/bedrock/pkg/rules"
	"github.com/moonwalker/bedrock/pkg/store"
	redistore "github.com/moonwalker/bedrock/pkg/store/redis"
)

const (
	RULE_PREFIX = "rules" // => rules:id {..}
)

type redisRuleRepo struct {
	store  store.Store
	prefix string
}

func NewRedisRuleRepo(redisStoreURL string) (RuleRepo, error) {
	store := redistore.New(redisStoreURL)
	return &redisRuleRepo{store, RULE_PREFIX}, nil
}

func (s *redisRuleRepo) Name() string {
	return "redis"
}

func (s *redisRuleRepo) Get(id string) (*rules.Rule, error) {
	key, err := RedisFmtKey(id, s.prefix)
	if err != nil {
		return nil, err
	}

	val, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}

	var r rules.Rule
	err = json.Unmarshal(val, &r)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (s *redisRuleRepo) Save(rule *rules.Rule) error {
	key, err := RedisFmtKey(rule.ID, s.prefix)
	if err != nil {
		return err
	}

	val, err := json.Marshal(rule)
	if err != nil {
		return err
	}

	return s.store.Set(key, val, nil)
}

func (s *redisRuleRepo) Expire(id string, ttl int64) error {
	key, err := RedisFmtKey(id, s.prefix)
	if err != nil {
		return err
	}

	return s.store.Expire(key, ttl)
}

func (s *redisRuleRepo) Remove(id string) error {
	key, err := RedisFmtKey(id, s.prefix)
	if err != nil {
		return err
	}

	return s.store.Delete(key)
}

func (s *redisRuleRepo) RemoveAll() error {
	return s.store.DeleteAll(s.prefix + ":*")
}

func (s *redisRuleRepo) Each(skip int, limit int, fn func(rule *rules.Rule)) error {
	return s.store.Scan(s.prefix+":*", skip, limit, func(key string, val []byte) {
		// val, err := s.store.Get(key)
		// if err != nil {
		// 	return
		// }

		var r rules.Rule
		err := json.Unmarshal(val, &r)
		if err != nil {
			return
		}

		fn(&r)
	})
}

func (s *redisRuleRepo) Count() int {
	return s.store.Count(s.prefix + ":*")
}

func (s *redisRuleRepo) Active() int {
	count := 0
	s.Each(0, 0, func(r *rules.Rule) {
		if r.Active {
			count++
		}
	})
	return count
}

func (s *redisRuleRepo) Close() {
	// no op
}

func RedisFmtKey(id string, prefix string) (string, error) {
	if len(id) == 0 {
		return "", errors.New("id not specified")
	}
	return fmt.Sprintf("%s:%s", prefix, id), nil
}
