package repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/moonwalker/bedrock/pkg/rules"
	"github.com/moonwalker/bedrock/pkg/streams"
)

const (
	STREAM_NAME  = "rules"
	RULE_SUBJECT = "rule.>"

	fmtRuleSubject = "rule.%s"
	ruleFilter     = "rule.*"
)

var STREAM_SUBJECTS = []string{
	RULE_SUBJECT,
}

type jetstreamRuleRepo struct {
	streamName string
	subject    string
	jStream    *streams.Stream
}

func NewJetstreamRuleRepo(natsURL string) (RuleRepo, error) {
	jsInst := &jetstreamRuleRepo{STREAM_NAME, RULE_SUBJECT, streams.NewStream(natsURL, STREAM_NAME, "", "")}
	err := jsInst.init()
	return jsInst, err
}

func (s *jetstreamRuleRepo) Name() string {
	return "jetstream"
}

func (s *jetstreamRuleRepo) init() error {
	_, err := s.jStream.CreateStream(STREAM_SUBJECTS)
	return err
}

func (s *jetstreamRuleRepo) Get(id string) (*rules.Rule, error) {
	key, err := FmtSubjectKey(id, fmtRuleSubject)
	if err != nil {
		return nil, err
	}
	val, err := s.jStream.FetchLastMessageBySubject([]string{key})
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

func (s *jetstreamRuleRepo) Save(rule *rules.Rule) error {
	old, err := s.Get(rule.ID)
	if err == nil {
		diffs := CompareRules(old, rule)
		if len(diffs) > 0 {
			rule.Changes = strings.Join(diffs, "\n")
		}
	}

	key, err := FmtSubjectKey(rule.ID, fmtRuleSubject)
	if err != nil {
		return err
	}

	rval, err := json.Marshal(rule)
	if err != nil {
		return err
	}

	_, err = s.jStream.Publish(key, rval)
	return err
}

func (s *jetstreamRuleRepo) Expire(id string, ttl int64) error {
	return nil
}

func (s *jetstreamRuleRepo) Remove(id string) error {
	r, err := s.Get(id)
	if err != nil {
		return err
	}

	r.Archive = true
	return s.Save(r)
}

func (s *jetstreamRuleRepo) RemoveAll() error {
	err := s.Each(0, 0, func(rule *rules.Rule) {
		err := s.Remove(rule.ID)
		if err != nil {
			return
		}
	})
	return err
}

func (s *jetstreamRuleRepo) Each(skip int, limit int, fn func(rule *rules.Rule)) error {
	rs, err := s.jStream.FetchLastMessagePerSubject([]string{ruleFilter})
	if err != nil {
		return nil
	}

	for _, vals := range rs {
		for _, val := range vals {
			var r rules.Rule
			err = json.Unmarshal(val, &r)
			if err != nil {
				return err
			}
			fn(&r)
		}
	}

	return nil
}

func (s *jetstreamRuleRepo) Count() int {
	rs, err := s.jStream.FetchLastMessagePerSubject([]string{ruleFilter})
	if err != nil {
		return 0
	}
	return len(rs)
}

func (s *jetstreamRuleRepo) Active() int {
	count := 0
	s.Each(0, 0, func(r *rules.Rule) {
		if r.Active {
			count++
		}
	})
	return count
}

// helpers

func FmtSubjectKey(id string, fmtSubject string) (string, error) {
	if len(id) == 0 {
		return "", errors.New("id not specified")
	}
	return fmt.Sprintf(fmtSubject, id), nil
}

func CompareRules(old *rules.Rule, new *rules.Rule) []string {
	res := make([]string, 0)

	// bool fields
	if old.Scheduled != new.Scheduled {
		res = append(res, fmt.Sprintf("Scheduled updated to %v", new.Scheduled))
	}
	if old.Chainable != new.Chainable {
		res = append(res, fmt.Sprintf("Chainable updated to %v", new.Chainable))
	}
	if old.NoCriterias != new.NoCriterias {
		res = append(res, fmt.Sprintf("NoCriterias updated to %v", new.NoCriterias))
	}

	// string fields
	if old.Cron != new.Cron {
		res = append(res, fmt.Sprintf("Cron updated to %s", new.Cron))
	}
	if old.Desc != new.Desc {
		res = append(res, fmt.Sprintf("Desc updated to %s", new.Desc))
	}
	if old.Event != new.Event {
		res = append(res, fmt.Sprintf("Event updated to %s", new.Event))
	}
	if old.Tags != new.Tags {
		res = append(res, fmt.Sprintf("Tags updated to %s", new.Tags))
	}
	if old.Title != new.Title {
		res = append(res, fmt.Sprintf("Title updated to %s", new.Title))
	}
	if old.RunAt != new.RunAt {
		res = append(res, fmt.Sprintf("RunAt updated to %s", new.RunAt))
	}

	// int fields
	if old.OnFail != new.OnFail {
		res = append(res, fmt.Sprintf("OnFail updated to %d", new.OnFail))
	}
	if old.OnMet != new.OnMet {
		res = append(res, fmt.Sprintf("OnMet updated to %d", new.OnUnmet))
	}
	if old.OnUnmet != new.OnUnmet {
		res = append(res, fmt.Sprintf("OnUnmet updated to %d", new.OnUnmet))
	}

	// time.Time fields
	if old.ValidFrom != new.ValidFrom {
		res = append(res, fmt.Sprintf("ValidFrom updated to %s", new.ValidFrom))
	}
	if old.ValidTo != new.ValidTo {
		res = append(res, fmt.Sprintf("ValidTo updated to %s", new.ValidTo))
	}

	// spec fields
	if old.Active != new.Active {
		if new.Active {
			res = append(res, "Rule enabled")
		} else {
			res = append(res, "Rule disabled")
		}
	}

	if old.Archive != new.Archive {
		if new.Archive {
			res = append(res, "Rule archived")
		} else {
			res = append(res, "Rule unarchived")
		}
	}

	res = append(res, compareActions(old.Actions, new.Actions)...)
	res = append(res, compareCriterias(old.Criterias, new.Criterias)...)
	res = append(res, compareActions(old.UnmetActions, new.UnmetActions)...)

	return res
}

func compareActions(old []*rules.Action, new []*rules.Action) []string {
	res := make([]string, 0)
	foundMethod := false
	foundAction := false

	for _, oa := range old {
		foundMethod = false
		foundAction = false
		for _, na := range new {
			if oa.Method == na.Method {
				foundMethod = true
				if oa.Alias == na.Alias {
					foundAction = true
					foundParameter := false
					for _, oap := range oa.Parameters {
						foundParameter = false
						for _, nap := range na.Parameters {
							if oap.IsDynamic == nap.IsDynamic && oap.Type == nap.Type && oap.Value == nap.Value {
								foundParameter = true
							}
						}
						if !foundParameter {
							res = append(res, fmt.Sprintf("Parameter of method %s was changed or removed", oa.Method))
						}
					}
					for _, nap := range na.Parameters {
						foundParameter = false
						for _, oap := range oa.Parameters {
							if oap.IsDynamic == nap.IsDynamic && oap.Type == nap.Type && oap.Value == nap.Value {
								foundParameter = true
							}
						}
						if !foundParameter {
							res = append(res, fmt.Sprintf("Parameter was added to method %s (Value: %v, Type: %v, IsDynamic: %v)", na.Method, nap.Value, nap.Type, nap.IsDynamic))
						}
					}
				}
			}
		}
		if !foundMethod {
			res = append(res, fmt.Sprintf("Action with method %s was removed", oa.Method))
		}
		if !foundAction {
			res = append(res, fmt.Sprintf("%s action parameters changed to %s", oa.Alias, oa.Alias))
		}
	}

	for _, na := range new {
		foundMethod = false
		for _, oa := range old {
			if oa.Method == na.Method {
				foundMethod = true
			}
		}
		if !foundMethod {
			res = append(res, fmt.Sprintf("Action with method %s was added", na.Method))
		}
	}

	return res
}

func compareCriterias(old []*rules.Criteria, new []*rules.Criteria) []string {
	res := make([]string, 0)
	foundDatasource := false
	foundCriteria := false

	for _, oc := range old {
		foundDatasource = false
		foundCriteria = false
		for _, nc := range new {
			if oc.DataSource.ID == nc.DataSource.ID {
				foundDatasource = true
				foundCondition := false
				for _, oco := range oc.Conditions {
					for _, nco := range nc.Conditions {
						if oco.Type == nco.Type && oco.Value == nco.Value && oco.Comparer == nco.Comparer {
							foundCondition = true
						}
					}
				}
				if !foundCondition {
					res = append(res, fmt.Sprintf("Conditions of criteria with datasource %s was changed", oc.DataSource.ID))
				}
			}
		}
		if !foundDatasource {
			res = append(res, fmt.Sprintf("Criteria with datasource %s was removed", oc.DataSource.ID))
		}
	}

	for _, nc := range new {
		foundCriteria = false
		for _, oc := range old {
			if oc.DataSource.ID == nc.DataSource.ID {
				foundCriteria = true
			}
		}
		if !foundCriteria {
			res = append(res, fmt.Sprintf("Criteria with datasource %s was added", nc.DataSource.ID))
		}
	}

	return res
}
