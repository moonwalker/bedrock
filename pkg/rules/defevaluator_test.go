// $ go test -v pkg/rules/*.go
// $ go test -bench=. pkg/rules/*.go

package rules

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type UserForTest struct {
	UserID     int64  `json:"userId"`
	MarketCode string `json:"marketCode"`
}

type WalletForTest struct {
	DepositCount int64 `json:"depositCount"`
}

type AccountBase struct {
	UserID         int64 `json:"userId"`
	ProviderUserID int64 `json:"providerUserId"`
}

type Login struct {
	AccountBase
}

var (
	testEvalRule1 = &Rule{
		Criterias: []*Criteria{
			&Criteria{
				DataSource: &DataSource{
					ID:    "event",
					Alias: "event",
				},
				Connector: "&&",
				Conditions: []*Condition{
					&Condition{
						Field:     "baseBalanceAfter",
						Comparer:  "<",
						Connector: "&&",
						Value:     "0.5",
						Type:      PARAMTYPE_FLOAT,
					},
					&Condition{
						Field:     "activityType",
						Comparer:  "==",
						Connector: "&&",
						Value:     "1",
						Type:      PARAMTYPE_NUMBER,
					},
				},
			},
			&Criteria{
				DataSource: &DataSource{
					ID:    "getUser",
					Alias: "user",
				},
				Connector: "&&",
				Conditions: []*Condition{
					&Condition{
						Field:     "marketCode",
						Comparer:  "==",
						Connector: "||",
						Value:     "SE",
						Type:      PARAMTYPE_STRING,
					},
					&Condition{
						Field:     "marketCode",
						Comparer:  "==",
						Connector: "||",
						Value:     "FI",
						Type:      PARAMTYPE_STRING,
					},
				},
			},
		},
		Actions: []*Action{
			&Action{
				Method: "addTag",
				Parameters: []*Parameter{
					&Parameter{
						Value: "NDC1",
					},
				},
			},
		},
	}
)

func TestEvaluate(t *testing.T) {
	dsMethods := FuncMap{
		"getUser": getUser(t),
	}

	actMethods := FuncMap{
		"addTag": addTag(t),
	}

	e := &GameTransaction{
		BaseBalanceAfter: 0.4,
		ActivityType:     1,
	}

	ctx := NewContext(e)

	res, err := evalCriterias(MakeValueFuncs(dsMethods), ctx, testEvalRule1.Criterias)
	assert.Nil(t, err)
	assert.True(t, res.ok)

	if res.ok {
		err = runActions(MakeValueFuncs(actMethods), ctx, testEvalRule1.Actions, false)
		assert.Nil(t, err)
	}
}

func TestEvalJsonFile1(t *testing.T) {
	rule := &Rule{}

	data, _ := os.ReadFile("./test_fixtures/test_rule_01.json")
	err := json.Unmarshal(data, &rule)
	assert.Nil(t, err)

	assert.Equal(t, "GAME_TRANSACTION", rule.Event)

	dsMethods := FuncMap{
		"getUser": getUser(t),
	}

	e := &GameTransaction{
		BaseBalanceAfter: 5.4,
		ActivityType:     1,
	}

	ctx := NewContext(e)

	res, err := evalCriterias(MakeValueFuncs(dsMethods), ctx, rule.Criterias)
	assert.Nil(t, err)
	assert.False(t, res.ok)
}

func TestEvalJsonFile2(t *testing.T) {
	rule := &Rule{}

	data, _ := os.ReadFile("./test_fixtures/test_rule_02.json")
	err := json.Unmarshal(data, &rule)
	assert.Nil(t, err)

	assert.Equal(t, "LOGIN", rule.Event)

	dsMethods := FuncMap{
		"getUser": func(ctx *Context) (*UserForTest, error) {
			return &UserForTest{UserID: 85}, nil
		},
	}

	e := &Login{
		AccountBase: AccountBase{
			ProviderUserID: 62,
			UserID:         85,
		},
	}

	ctx := NewContext(e)
	res, err := evalCriterias(MakeValueFuncs(dsMethods), ctx, rule.Criterias)

	assert.Nil(t, err)
	assert.False(t, res.ok)
}

func TestEvalJsonFile3(t *testing.T) {
	rule := &Rule{}

	data, _ := os.ReadFile("./test_fixtures/test_rule_03.json")
	err := json.Unmarshal(data, &rule)
	assert.Nil(t, err)

	assert.Equal(t, "LOGIN", rule.Event)

	dsMethods := FuncMap{
		"getWallet": func(ctx *Context) (*WalletForTest, error) {
			return &WalletForTest{DepositCount: 3}, nil
		},
	}

	e := &Login{
		AccountBase: AccountBase{
			ProviderUserID: 62,
			UserID:         85,
		},
	}

	ctx := NewContext(e)

	res, err := evalCriterias(MakeValueFuncs(dsMethods), ctx, rule.Criterias)

	assert.Nil(t, err)
	assert.False(t, res.ok)
}

// $ go test -run TestBasicDataSourceType -count=1 -v pkg/rules/*.go
func TestBasicDataSourceType(t *testing.T) {
	crit := []*Criteria{
		&Criteria{
			DataSource: &DataSource{
				ID:    "countEvents",
				Key:   "count",
				Alias: "count_test",
			},
			Returns: "number",
			Conditions: []*Condition{
				&Condition{
					Field:    "count",
					Comparer: "==",
					Value:    "42",
					Type:     "number",
				},
			},
		},
	}

	dsMethods := FuncMap{
		"countEvents": countEvents(t, 42),
	}

	ctx := NewContext(nil)
	res, err := evalCriterias(MakeValueFuncs(dsMethods), ctx, crit)

	assert.Nil(t, err)
	assert.True(t, res.ok)
}

// $ go test -run TestDynamicParams -count=1 -v pkg/rules/*.go
func TestDynamicParams(t *testing.T) {
	ctx := NewContext(nil)

	type TestValue struct {
		Name     string
		Value    interface{}
		Testable bool
	}

	values := []TestValue{
		TestValue{"testValue1", "str", true},
		TestValue{"testValue2", int64(100), true},
		TestValue{"testValue3", 100.1, true},
		TestValue{"testValue4", 100.3, true},
		TestValue{"testValue5", true, true},
		TestValue{"testValue6", time.Now(), false},
		TestValue{"testValue7", time.Now().String(), false},
		TestValue{"testValue8", "2D", false},
		TestValue{"testValue9", map[string]interface{}{"one": float64(1), "two": float64(2)}, true},
	}

	for _, v := range values {
		ctx.SetFact(v.Name, v.Value)
	}

	testDynamicParams := []*Parameter{
		&Parameter{IsDynamic: true, Type: "string", Value: "testValue1"},
		&Parameter{IsDynamic: true, Type: "number", Value: "testValue2"},
		&Parameter{IsDynamic: true, Type: "float", Value: "testValue3"},
		&Parameter{IsDynamic: true, Type: "fraction", Value: "testValue4"},
		&Parameter{IsDynamic: true, Type: "bool", Value: "testValue5"},
		&Parameter{IsDynamic: true, Type: "date", Value: "testValue6"},
		&Parameter{IsDynamic: true, Type: "datetime", Value: "testValue7"},
		&Parameter{IsDynamic: true, Type: "duration", Value: "testValue8"},
		&Parameter{IsDynamic: true, Type: "eventFields", Value: "testValue9"},
	}

	res := makeArgs(ctx, testDynamicParams)
	assert.Equal(t, len(res), 10)
	for i, v := range res {
		if i > 0 {
			if values[i-1].Testable {
				assert.Equal(t, values[i-1].Value, v)
			}
		}
	}
}

func getUser(t *testing.T) func(ctx *Context) (*UserForTest, error) {
	return func(ctx *Context) (*UserForTest, error) {
		return &UserForTest{MarketCode: "SE"}, nil
	}
}

func addTag(t *testing.T) func(ctx *Context, tag string) error {
	return func(ctx *Context, tag string) error {
		assert.Equal(t, "NDC1", tag)
		return nil
	}
}

func countEvents(t *testing.T, mock int) func(ctx *Context) (int, error) {
	return func(ctx *Context) (int, error) {
		return mock, nil
	}
}

// benchmark

type benchParams struct {
	Region  string `json:"region"`
	Country string `json:"country"`
	Segment int    `json:"segment"`
	Value   int    `json:"value"`
}

func BenchmarkEvaluate(b *testing.B) {
	ctx := NewContext(&benchParams{
		Region:  "EU",
		Country: "SE",
		Segment: 1,
		Value:   100,
	})

	// (region == "EU" || country == "SE") && (value >= 100 || segment == 1)

	crit := []*Criteria{
		&Criteria{
			DataSource: &DataSource{
				ID:    "event",
				Alias: "event",
			},
			Conditions: []*Condition{
				&Condition{
					Field:    "region",
					Comparer: "==",
					Value:    "EU",
					Type:     PARAMTYPE_STRING,
				},
				&Condition{
					Field:     "country",
					Comparer:  "==",
					Connector: "||",
					Value:     "SE",
					Type:      PARAMTYPE_STRING,
				},
			},
		},
		&Criteria{
			DataSource: &DataSource{
				ID:    "event",
				Alias: "event",
			},
			Connector: "&&",
			Conditions: []*Condition{
				&Condition{
					Field:    "value",
					Comparer: ">=",
					Value:    "100",
					Type:     PARAMTYPE_FLOAT,
				},
				&Condition{
					Field:     "segment",
					Comparer:  "==",
					Connector: "||",
					Value:     "1",
					Type:      PARAMTYPE_NUMBER,
				},
			},
		},
	}

	var err error
	var res *evalCriteriasResult

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		res, err = evalCriterias(nil, ctx, crit)
	}
	b.StopTimer()

	if err != nil {
		b.Fatal(err)
	}

	if !res.ok {
		b.Fail()
	}
}
