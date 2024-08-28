// $ go test -v pkg/rules/*.go

package rules

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRuleOnFail(t *testing.T) {
	r := &Rule{}
	assert.Equal(t, 0, ON_FAIL_DEACTIVATE)
	assert.Equal(t, ON_FAIL_DEACTIVATE, r.OnFail)
}

func GenerateDepositSuccessRule() Rule {
	conds := make([]*Condition, 0)
	conds = append(conds, &Condition{
		Field:     "marketCode",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "SE",
		Type:      PARAMTYPE_STRING,
	})
	conds = append(conds, &Condition{
		Field:     "locale",
		Comparer:  COMPARER_NOT_EQUAL,
		Connector: CONNECTOR_OR,
		Value:     "en-se",
		Type:      PARAMTYPE_STRING,
	})
	crits := make([]*Criteria, 0)
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"getUser", "user", "user"},
		Conditions: conds,
		Connector:  CONNECTOR_AND,
	})
	conds2 := make([]*Condition, 0)
	conds2 = append(conds2, &Condition{
		Field:     "currency",
		Comparer:  COMPARER_NOT_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "SEK",
		Type:      PARAMTYPE_STRING,
	})
	conds21 := make([]*Condition, 0)
	conds21 = append(conds21, &Condition{
		Field:     "marketCode",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "SE",
		Type:      PARAMTYPE_STRING,
	})
	conds21 = append(conds21, &Condition{
		Field:     "locale",
		Comparer:  COMPARER_NOT_EQUAL,
		Connector: CONNECTOR_OR,
		Value:     "en-se",
		Type:      PARAMTYPE_STRING,
	})
	conds2 = append(conds2, &Condition{
		Items: conds21,
	})
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"getUser", "user", "user"},
		Conditions: conds2,
		Connector:  CONNECTOR_OR,
	})
	params3 := make([]*Parameter, 0)
	params3 = append(params3, &Parameter{
		Type:  PARAMTYPE_STRING,
		Value: "FIRST_DEPOSIT",
	})
	filters3 := make([]*Condition, 0)
	filters3 = append(filters3, &Condition{
		Field:     "baseAmount",
		Comparer:  COMPARER_GREATER_OR_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "100",
		Type:      PARAMTYPE_FLOAT,
	})
	conds3 := make([]*Condition, 0)
	conds3 = append(conds3, &Condition{
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "SEK",
		Type:      PARAMTYPE_STRING,
	})
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"find", "find", "find"},
		Connector:  CONNECTOR_AND,
		Parameters: params3,
		Conditions: conds3,
		Filters:    filters3,
	})
	params4 := make([]*Parameter, 0)
	params4 = append(params4, &Parameter{
		Type:  PARAMTYPE_STRING,
		Value: "DEPOSIT",
	})
	filters4 := make([]*Condition, 0)
	filters4 = append(filters4, &Condition{
		Field:     "since",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "30",
		Type:      PARAMTYPE_NUMBER,
	})
	filters4 = append(filters4, &Condition{
		Field:     "date",
		Comparer:  COMPARER_LESS,
		Connector: CONNECTOR_AND,
		Value:     time.Now().UTC().Format(time.RFC3339),
		Type:      PARAMTYPE_DATE,
	})
	conds4 := make([]*Condition, 0)
	conds4 = append(conds4, &Condition{
		Field:     "length",
		Comparer:  COMPARER_GREATER,
		Connector: CONNECTOR_AND,
		Value:     "10",
		Type:      PARAMTYPE_NUMBER,
	})
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"events", "events", "events"},
		Connector:  CONNECTOR_AND,
		Returns:    RETURNTYPE_ARRAY,
		Parameters: params4,
		Filters:    filters4,
		Conditions: conds4,
	})
	conds5 := make([]*Condition, 0)
	conds5 = append(conds5, &Condition{
		Field:     "",
		Comparer:  COMPARER_GREATER,
		Connector: CONNECTOR_AND,
		Value:     "10",
		Type:      PARAMTYPE_NUMBER,
	})
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"countEvents", "count", "count"},
		Returns:    PARAMTYPE_NUMBER,
		Connector:  CONNECTOR_OR,
		Parameters: params4,
		Conditions: conds5,
	})

	conds6 := make([]*Condition, 0)
	conds6 = append(conds6, &Condition{
		Field:     "marketCode",
		Comparer:  COMPARER_NOT_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "SE",
		Type:      PARAMTYPE_STRING,
	})
	conds6 = append(conds6, &Condition{
		Field:     "locale",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_OR,
		Value:     "en-se",
		Type:      PARAMTYPE_STRING,
	})
	crits2 := make([]*Criteria, 0)
	crits2 = append(crits2, &Criteria{
		DataSource: &DataSource{"getUser", "user", "user"},
		Conditions: conds6,
		Connector:  CONNECTOR_AND,
	})
	conds7 := make([]*Condition, 0)
	conds7 = append(conds7, &Condition{
		Field:     "currency",
		Comparer:  COMPARER_NOT_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "SEK",
		Type:      PARAMTYPE_STRING,
	})
	crits2 = append(crits2, &Criteria{
		DataSource: &DataSource{"getUser", "user", "user"},
		Conditions: conds7,
		Connector:  CONNECTOR_OR,
	})
	crits = append(crits, &Criteria{
		Items: crits2,
	})
	acts := make([]*Action, 0)
	param := &Parameter{
		Type:  PARAMTYPE_STRING,
		Value: "testrule DEPOSIT",
	}
	acts = append(acts, &Action{
		Method:     "printf",
		Parameters: []*Parameter{param},
	})
	return Rule{
		ID:        "fake_rule",
		Title:     "test DEPOSIT on SE user in english",
		Active:    false,
		Event:     "DEPOSIT",
		Criterias: crits,
		Actions:   acts,
	}
}

func GenerateGameRoundRule() Rule {
	conds := make([]*Condition, 0)
	conds = append(conds, &Condition{
		Field:     "balanceAfter",
		Comparer:  COMPARER_LESS,
		Connector: CONNECTOR_AND,
		Value:     "0.1",
		Type:      PARAMTYPE_FLOAT,
	})
	crits := make([]*Criteria, 0)
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"getEvent", "event", "event"},
		Conditions: conds,
		Connector:  CONNECTOR_AND,
	})
	conds2 := make([]*Condition, 0)
	conds2 = append(conds2, &Condition{
		Field:     "pendingWithdrawals",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "0",
		Type:      PARAMTYPE_NUMBER,
	})
	conds21 := make([]*Condition, 0)
	conds21 = append(conds21, &Condition{
		Field:     "marketCode",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "FI",
		Type:      PARAMTYPE_STRING,
	})
	conds21 = append(conds21, &Condition{
		Field:     "marketCode",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_OR,
		Value:     "SE",
		Type:      PARAMTYPE_STRING,
	})
	conds2 = append(conds2, &Condition{
		Items: conds21,
	})
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"getUser", "user", "user"},
		Conditions: conds2,
		Connector:  CONNECTOR_AND,
	})
	params3 := make([]*Parameter, 0)
	params3 = append(params3, &Parameter{
		Type:  PARAMTYPE_STRING,
		Value: "WITHDRAWAL",
	})
	conds3 := make([]*Condition, 0)
	conds3 = append(conds3, &Condition{
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "0",
		Type:      PARAMTYPE_NUMBER,
	})
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"countEvents", "count", "count"},
		Returns:    PARAMTYPE_NUMBER,
		Connector:  CONNECTOR_AND,
		Parameters: params3,
		Conditions: conds3,
	})
	params4 := make([]*Parameter, 0)
	params4 = append(params4, &Parameter{
		Type:  PARAMTYPE_STRING,
		Value: "DEPOSIT",
	})
	filters4 := make([]*Condition, 0)
	filters4 = append(filters4, &Condition{
		Field:     "depositCount",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "1",
		Type:      PARAMTYPE_NUMBER,
	})
	conds4 := make([]*Condition, 0)
	conds4 = append(conds4, &Condition{
		Field:     "baseAmount",
		Comparer:  COMPARER_GREATER_OR_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "200",
		Type:      PARAMTYPE_FLOAT,
	})
	conds4 = append(conds4, &Condition{
		Field:     "baseAmount",
		Comparer:  COMPARER_LESS_OR_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "400",
		Type:      PARAMTYPE_FLOAT,
	})
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"find", "find", "find"},
		Connector:  CONNECTOR_AND,
		Parameters: params4,
		Filters:    filters4,
		Conditions: conds4,
	})
	acts := make([]*Action, 0)
	param := &Parameter{
		Type:  PARAMTYPE_STRING,
		Value: "testrule GAME_ROUND",
	}
	acts = append(acts, &Action{
		Method:     "printf",
		Parameters: []*Parameter{param},
	})
	return Rule{
		ID:        "test_leon",
		Title:     "test leon GAME_ROUND",
		Active:    false,
		Event:     "GAME_ROUND",
		Criterias: crits,
		Actions:   acts,
	}
}
