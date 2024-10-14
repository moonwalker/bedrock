package repo

import (
	"encoding/json"

	yaml "gopkg.in/yaml.v2"

	"github.com/moonwalker/bedrock/pkg/rules"
)

type RuleRepo interface {
	Name() string
	Get(id string) (*rules.Rule, error)
	Save(rule *rules.Rule) error
	Expire(id string, ttl int64) error
	Remove(id string) error
	RemoveAll() error
	Each(skip int, limit int, fn func(rule *rules.Rule)) error
	Count() int
	Active() int
	Close()
}

func isJson(data []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(data, &js) == nil
}

func decode(data []byte) (error, *rules.Rule) {
	r := &rules.Rule{}
	if isJson(data) {
		err := json.Unmarshal(data, r)
		if err != nil {
			return err, nil
		}
		return nil, r
	} else {
		err := yaml.Unmarshal(data, r)
		if err != nil {
			return err, nil
		}
		return nil, r
	}
}
