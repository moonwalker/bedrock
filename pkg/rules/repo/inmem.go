package repo

import (
	"errors"
	"fmt"

	"github.com/moonwalker/bedrock/pkg/rules"
)

type inMemoryRuleRepo map[string]interface{}

func NewInMemoryRuleRepo() RuleRepo {
	return &inMemoryRuleRepo{}
}

func (s *inMemoryRuleRepo) Name() string {
	return "in-memory"
}

func (s *inMemoryRuleRepo) Get(id string) (*rules.Rule, error) {
	rule, ok := (*s)[id].(*rules.Rule)
	if !ok {
		return nil, fmt.Errorf("rule not found with id: %s", id)
	}
	return rule, nil
}

func (s *inMemoryRuleRepo) Save(rule *rules.Rule) error {
	(*s)[rule.ID] = rule
	return nil
}

func (s *inMemoryRuleRepo) Expire(id string, ttl int64) error {
	return errors.New("not implemented")
}

func (s *inMemoryRuleRepo) Remove(id string) error {
	delete(*s, id)
	return nil
}

func (s *inMemoryRuleRepo) RemoveAll() error {
	return errors.New("not implemented")
}

func (s *inMemoryRuleRepo) Count() int {
	return len(*s)
}

func (s *inMemoryRuleRepo) Active() int {
	c := 0
	s.Each(0, 0, func(r *rules.Rule) {
		if r.Active {
			c++
		}
	})
	return c
}

func (s *inMemoryRuleRepo) Each(skip int, limit int, fn func(rule *rules.Rule)) error {
	for _, r := range *s {
		rule, ok := r.(*rules.Rule)
		if !ok {
			continue
		}
		fn(rule)
	}
	return nil
}

func (s *inMemoryRuleRepo) Close() {
	// no op
}
