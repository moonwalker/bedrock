// $ go test -v pkg/rules/*.go

package rules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	assert.True(t, ConnectCondition(true, true, CONNECTOR_AND))
	assert.False(t, ConnectCondition(true, false, CONNECTOR_AND))
	assert.True(t, ConnectCondition(true, false, CONNECTOR_OR))
	assert.False(t, ConnectCondition(false, false, CONNECTOR_OR))
}

func TestConditionEvaluate(t *testing.T) {
	event := make(map[string]interface{})
	event["currency"] = "SEK"
	event["baseValue"] = 2.0
	event["depositCount"] = float64(4)
	event["duration"] = "3d"
	event["date"] = "2006-01-02T15:04:05.999999999Z"
	event["expiresAt"] = "2020-08-28T21:55:00Z"

	ctx := &Context{}

	condition := &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_NOT_EQUAL,
		Value:     "NOK",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_AND,
	}
	e := evaluate(ctx, event, condition)
	assert.True(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "EUR",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.False(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "SEK",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_OR,
	}
	e = evaluate(ctx, event, condition)
	assert.True(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "CAD",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_OR,
	}
	e = evaluate(ctx, event, condition)
	assert.False(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "baseValue",
		Comparer:  COMPARER_BETWEEN,
		Value:     "1<>3",
		Type:      PARAMTYPE_FLOAT,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.True(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "baseValue",
		Comparer:  COMPARER_BETWEEN,
		Value:     "3<>4",
		Type:      PARAMTYPE_FLOAT,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.False(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "depositCount",
		Comparer:  COMPARER_BETWEEN,
		Value:     "1<>5",
		Type:      PARAMTYPE_NUMBER,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.True(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "duration",
		Comparer:  COMPARER_BETWEEN,
		Value:     "1d<>1w",
		Type:      PARAMTYPE_DURATION,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.False(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "date",
		Comparer:  COMPARER_BETWEEN,
		Value:     "2006-01-02T15:00:00Z<>2006-01-02T15:05:00Z",
		Type:      PARAMTYPE_DATE,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.True(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "date",
		Comparer:  COMPARER_BETWEEN,
		Value:     "2006-01-02T16:00:00Z<>2006-01-02T17:00:00Z",
		Type:      PARAMTYPE_DATE,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.False(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "expiresAt",
		Comparer:  COMPARER_GREATER_OR_EQUAL,
		Value:     "2020-08-28T21:56:00.000000000Z",
		Type:      PARAMTYPE_DATETIME,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.False(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "expiresAt",
		Comparer:  COMPARER_LESS_OR_EQUAL,
		Value:     "2020-08-28T21:56:00.000000000Z",
		Type:      PARAMTYPE_DATETIME,
		Connector: CONNECTOR_AND,
	}
	e = evaluate(ctx, event, condition)
	assert.True(t, e)
}

func TestEvaluateConditions(t *testing.T) {
	conditions := make([]*Condition, 0)
	event := make(map[string]interface{})
	event["currency"] = "SEK"

	condition := &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_NOT_EQUAL,
		Value:     "NOK",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_AND,
	}
	conditions = append(conditions, condition)
	e, _ := EvaluateConditions(nil, event, conditions)
	assert.True(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "EUR",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_AND,
	}
	conditions = append(conditions, condition)
	e, _ = EvaluateConditions(nil, event, conditions)
	assert.False(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "SEK",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_OR,
	}
	conditions = append(conditions, condition)
	e, _ = EvaluateConditions(nil, event, conditions)
	assert.True(t, e)

	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "CAD",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_AND,
	}
	conditions = append(conditions, condition)
	e, _ = EvaluateConditions(nil, event, conditions)
	assert.False(t, e)

	ctx := &Context{
		Facts: []byte("{\"dynamicValue\":\"SEK\"}"),
	}
	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "dynamicValue",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_OR,
		IsDynamic: true,
	}
	conditions = append(conditions, condition)
	e, _ = EvaluateConditions(ctx, event, conditions)
	assert.True(t, e)

	ctx = &Context{
		Facts: []byte("{\"dynamicValue\":\"CAD\"}"),
	}
	condition = &Condition{
		Alias:     "event",
		Field:     "currency",
		Comparer:  COMPARER_EQUAL,
		Value:     "dynamicValue",
		Type:      PARAMTYPE_STRING,
		Connector: CONNECTOR_AND,
		IsDynamic: true,
	}
	conditions = append(conditions, condition)
	e, _ = EvaluateConditions(ctx, event, conditions)
	assert.False(t, e)
}
