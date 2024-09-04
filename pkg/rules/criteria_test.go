package rules

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRuleOnMetReason(t *testing.T) {
	conds := make([]*Condition, 0)
	conds = append(conds, &Condition{
		Field:     "userId",
		Comparer:  COMPARER_EQUAL,
		Connector: CONNECTOR_AND,
		Value:     "1",
		Type:      PARAMTYPE_NUMBER,
	})
	conds = append(conds, &Condition{
		Field:     "marketCode",
		Comparer:  COMPARER_NOT_EQUAL,
		Connector: CONNECTOR_OR,
		Value:     "SE",
		Type:      PARAMTYPE_STRING,
	})
	crits := make([]*Criteria, 0)
	crits = append(crits, &Criteria{
		DataSource: &DataSource{"getUser", "user", "user"},
		Conditions: conds,
		Connector:  CONNECTOR_AND,
	})

	e := &UserForTest{
		UserID:     1,
		MarketCode: "SE",
	}

	dsMethods := FuncMap{
		"getUser": func(ctx *Context) (*UserForTest, error) {
			return &UserForTest{UserID: 1, MarketCode: "SE"}, nil
		},
	}

	ctx := NewContext(e)
	res, err := evalCriterias(MakeValueFuncs(dsMethods), ctx, crits)
	assert.Nil(t, err)
	assert.Nil(t, res.unmetCondition)
	assert.True(t, res.ok)

	fmt.Println(res.met())
	// assert.Equal(t, ON_FAIL_DEACTIVATE, r.OnFail)
}
