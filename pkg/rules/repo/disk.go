package repo

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/moonwalker/bedrock/pkg/rules"
)

type diskRuleRepo struct {
	root string
}

func NewDiskRuleRepo(root string) RuleRepo {
	return &diskRuleRepo{root}
}

func (s *diskRuleRepo) Name() string {
	return "disk"
}

func (s *diskRuleRepo) Get(id string) (*rules.Rule, error) {
	return nil, errors.New("not implemented")
}

func (s *diskRuleRepo) Save(rule *rules.Rule) error {
	return errors.New("not implemented")
}

func (s *diskRuleRepo) Expire(id string, ttl int64) error {
	return errors.New("not implemented")
}

func (s *diskRuleRepo) Remove(id string) error {
	return errors.New("not implemented")
}

func (s *diskRuleRepo) RemoveAll() error {
	return errors.New("not implemented")
}

func (s *diskRuleRepo) Count() int {
	count := 0
	s.Each(0, 0, func(r *rules.Rule) {
		count++
	})
	return count
}

func (s *diskRuleRepo) Active() int {
	count := 0
	s.Each(0, 0, func(r *rules.Rule) {
		if r.Active {
			count++
		}
	})
	return count
}

func (s *diskRuleRepo) Each(skip int, limit int, fn func(rule *rules.Rule)) error {
	return filepath.Walk(s.root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		data, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		e, r := decode(data)
		if e != nil {
			return e
		}
		fn(r)
		return nil
	})
}
