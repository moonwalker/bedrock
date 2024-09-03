package rules

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/moonwalker/bedrock/pkg/parse"
)

const (
	CONNECTOR_AND string = "&&"
	CONNECTOR_OR  string = "||"

	COMPARER_EQUAL            string = "=="
	COMPARER_NOT_EQUAL        string = "!="
	COMPARER_GREATER          string = ">"
	COMPARER_GREATER_OR_EQUAL string = ">="
	COMPARER_LESS             string = "<"
	COMPARER_LESS_OR_EQUAL    string = "<="
	COMPARER_BETWEEN          string = "<>"
	COMPARER_EXISTS           string = "!!"
	COMPARER_NOT_SET          string = "null"
	COMPARER_IN               string = "in"
	COMPARER_NOT_IN           string = "nin"
	COMPARER_MATCH            string = "match"
	COMPARER_MATCH_NOT        string = "dontmatch"
	COMPARER_ANY              string = "any"
	COMPARER_ALL              string = "all"
	COMPARER_NONE             string = "none"

	PARAMTYPE_STRING   string = "string"
	PARAMTYPE_NUMBER   string = "number"
	PARAMTYPE_FLOAT    string = "float"
	PARAMTYPE_FRACTION string = "fraction"
	PARAMTYPE_BOOL     string = "bool"
	PARAMTYPE_DATE     string = "date"
	PARAMTYPE_DATETIME string = "datetime"
	PARAMTYPE_EVENT    string = "eventType"
	PARAMTYPE_DURATION string = "duration"
	PARAMTYPE_FIELDS   string = "eventFields"

	RETURNTYPE_ARRAY string = "array"

	AGGREGATE_AVARAGE string = "avg"
	AGGREGATE_TOTAL   string = "sum"
	AGGREGATE_MINIMUM string = "min"
	AGGREGATE_MAXIMUM string = "max"
)

type Condition struct {
	Field     string       `json:"field"`
	Comparer  string       `json:"comparer"`
	Connector string       `json:"connector"`
	Value     string       `json:"value"`
	Type      string       `json:"type"`
	Items     []*Condition `json:"items"`
	Method    string       `json:"method"`
	Alias     string       `json:"alias"`
	IsDynamic bool         `json:"isDynamic"`
}

func ComparerTypeName(condition *Condition) string {
	if condition.Type == PARAMTYPE_NUMBER || condition.Type == PARAMTYPE_FLOAT || condition.Type == PARAMTYPE_FRACTION {
		switch condition.Comparer {
		case COMPARER_EQUAL:
			return "equal_to"
		case COMPARER_NOT_EQUAL:
			return "not_equal_to"
		case COMPARER_GREATER:
			return "greater_than"
		case COMPARER_GREATER_OR_EQUAL:
			return "greater_or_equal_than"
		case COMPARER_LESS:
			return "less_than"
		case COMPARER_LESS_OR_EQUAL:
			return "less_or_equal_than"
		case COMPARER_BETWEEN:
			return "between"
		case COMPARER_EXISTS:
			return "set"
		case COMPARER_NOT_SET:
			return "not_set"
		}
	} else if condition.Type == PARAMTYPE_DATE || condition.Type == PARAMTYPE_DATETIME {
		switch condition.Comparer {
		case COMPARER_EQUAL:
			return "on"
		case COMPARER_GREATER:
			return "after"
		case COMPARER_GREATER_OR_EQUAL:
			return "not_earlier_than"
		case COMPARER_LESS:
			return "before"
		case COMPARER_LESS_OR_EQUAL:
			return "not_later_than"
		case COMPARER_BETWEEN:
			return "between"
		case COMPARER_EXISTS:
			return "set"
		case COMPARER_NOT_SET:
			return "not_set"
		}
	}
	return ""
}

func EvaluateConditions(ctx *Context, ds interface{}, conditions []*Condition) (bool, *Condition) {
	var unmet *Condition
	ok := false
	i := 0
	l := len(conditions)
	if l > 0 {
		conditions[0].Connector = CONNECTOR_OR
	} else {
		ok = true
	}
	for i < l {
		if i == 0 || ok || conditions[i].Connector == CONNECTOR_OR {
			if len(conditions[i].Items) > 0 {
				itemsOk, itemsUnmet := EvaluateConditions(ctx, ds, conditions[i].Items)
				ok = ConnectCondition(ok, itemsOk, conditions[i].Connector)
				if !ok && unmet == nil {
					unmet = itemsUnmet
				}
			} else {
				// when not the first and already 'not ok' ignoring AND criterias
				ok = connectCondition(ctx, ds, conditions[i], ok)
				if !ok && unmet == nil {
					unmet = conditions[i]
				}
			}
			if ok && conditions[i].Connector == CONNECTOR_OR {
				// an OR criteria is met ignoring previous unmet
				unmet = nil
			}
		}
		i++
	}
	return ok, unmet
}

func ConnectCondition(res bool, eval bool, connector string) bool {
	if connector == CONNECTOR_AND {
		return res && eval
	}
	return res || eval
}

func connectCondition(ctx *Context, ds interface{}, c *Condition, ok bool) bool {
	e := evaluate(ctx, ds, c)
	return ConnectCondition(ok, e, c.Connector)
}

func evaluate(ctx *Context, ds interface{}, condition *Condition) bool {
	if ds == nil {
		return false
	}

	// field value
	var fv interface{}

	compositeDS, compositeType := ds.(map[string]interface{})
	if compositeType {
		// datasource is composite type (struct)
		fv = compositeDS[condition.Field]
		if condition.Field == "date" || condition.Field == "happened" {
			fv = compositeDS["date"]
		}
	} else {
		// datasource is basic type (int, string, etc.)
		fv = ds
	}

	// fmt.Println("evaluate", condition, ds, condition.Field, fv)

	if fv == nil {
		// datasource is undefined
		return condition.Comparer == COMPARER_NOT_SET || condition.Comparer == COMPARER_NONE
	}

	value := condition.Value
	if condition.IsDynamic {
		value = GetDynamicParamValue(ctx, value)
		if value == "" {
			return false
		}
	}

	switch condition.Comparer {
	case COMPARER_ANY:
		f := fv.([]interface{})
		v := strings.Split(value, ",")
		return anyOfString(f, v)
	case COMPARER_ALL:
		f := fv.([]interface{})
		v := strings.Split(value, ",")
		return allOfString(f, v)
	case COMPARER_NONE:
		f := fv.([]interface{})
		v := strings.Split(value, ",")
		return noneOfString(f, v)
	case COMPARER_BETWEEN:
		return compareBetweenValue(fv, value, condition.Type)
	}

	switch condition.Type {
	case PARAMTYPE_STRING:
		return compareString(fv.(string), value, condition.Comparer)
	case PARAMTYPE_NUMBER:
		n := parse.ParseFloat(value)
		return CompareFloat(fv.(float64), n, condition.Comparer)
	case PARAMTYPE_FLOAT:
		n := parse.ParseFloat(value)
		return CompareFloat(fv.(float64), n, condition.Comparer)
	case PARAMTYPE_FRACTION:
		n := parse.ParseFloat(value)
		return CompareFloat(fv.(float64), n, condition.Comparer)
	case PARAMTYPE_DATE, PARAMTYPE_DATETIME:
		a := parse.ParseDate(fv)
		b := parse.ParseDate(value)
		return compareDate(a, b, condition.Comparer)
	case PARAMTYPE_DURATION:
		a := parse.ParseDate(fv)
		b := parse.ParseHappened(value)
		return compareInt(a.Unix(), b, condition.Comparer)
	case PARAMTYPE_BOOL:
		a := parse.ParseBool(fv)
		b := parse.ParseBool(value)
		return compareBool(a, b, condition.Comparer)
	}

	return false
}

func compareBetweenValue(fv interface{}, value string, paramType string) bool {
	values := strings.Split(value, COMPARER_BETWEEN)
	if len(values) == 2 {
		switch paramType {
		case PARAMTYPE_NUMBER:
			a := parse.ParseFloat(values[0])
			b := parse.ParseFloat(values[1])
			return CompareFloat(fv.(float64), a, COMPARER_GREATER_OR_EQUAL) && CompareFloat(fv.(float64), b, COMPARER_LESS_OR_EQUAL)
		case PARAMTYPE_FLOAT:
			a := parse.ParseFloat(values[0])
			b := parse.ParseFloat(values[1])
			return CompareFloat(fv.(float64), a, COMPARER_GREATER_OR_EQUAL) && CompareFloat(fv.(float64), b, COMPARER_LESS_OR_EQUAL)
		case PARAMTYPE_FRACTION:
			a := parse.ParseFloat(values[0])
			b := parse.ParseFloat(values[1])
			return CompareFloat(fv.(float64), a, COMPARER_GREATER_OR_EQUAL) && CompareFloat(fv.(float64), b, COMPARER_LESS_OR_EQUAL)
		case PARAMTYPE_DATE, PARAMTYPE_DATETIME:
			d := parse.ParseDate(fv)
			a := parse.ParseDate(values[0])
			b := parse.ParseDate(values[1])
			return compareDate(d, a, COMPARER_GREATER_OR_EQUAL) && compareDate(d, b, COMPARER_LESS_OR_EQUAL)
		case PARAMTYPE_DURATION:
			d := parse.ParseDate(fv)
			a := parse.ParseHappened(values[0])
			b := parse.ParseHappened(values[1])
			return compareInt(d.Unix(), a, COMPARER_GREATER_OR_EQUAL) && compareInt(d.Unix(), b, COMPARER_LESS_OR_EQUAL)
		}
	}
	return false
}

func GetDynamicParamValue(ctx *Context, value string) string {
	if ctx != nil && ctx.GetFact(value).Exists() {
		val := ctx.GetFact(value).Value()
		if val != nil {
			return fmt.Sprintf("%v", val)
		}
	}
	return ""
}

func compareString(a, b string, c string) bool {

	if c == COMPARER_EQUAL {
		return a == b
	}

	if c == COMPARER_NOT_EQUAL {
		return a != b
	}

	if c == COMPARER_IN {
		return csvContains(a, b)
	}

	if c == COMPARER_NOT_IN {
		return !csvContains(a, b)
	}

	if c == COMPARER_MATCH {
		r, _ := regexp.Compile(b)
		return r.Match([]byte(a))
	}

	if c == COMPARER_MATCH_NOT {
		r, _ := regexp.Compile(b)
		return !r.Match([]byte(a))
	}

	if c == COMPARER_EXISTS {
		return a != ""
	}
	if c == COMPARER_NOT_SET {
		return a == ""
	}

	return false
}

func CompareFloat(a, b float64, c string) bool {
	if c == COMPARER_EQUAL {
		return a == b
	}

	if c == COMPARER_NOT_EQUAL {
		return a != b
	}

	if c == COMPARER_GREATER {
		return a > b
	}

	if c == COMPARER_GREATER_OR_EQUAL {
		return a >= b
	}

	if c == COMPARER_LESS {
		return a < b
	}

	if c == COMPARER_LESS_OR_EQUAL {
		return a <= b
	}

	if c == COMPARER_EXISTS {
		return true
	}

	if c == COMPARER_NOT_SET {
		return false
	}

	return false
}

func compareInt(a, b int64, c string) bool {
	if c == COMPARER_EQUAL {
		return a == b
	}

	if c == COMPARER_NOT_EQUAL {
		return a != b
	}

	if c == COMPARER_GREATER {
		return a > b
	}

	if c == COMPARER_GREATER_OR_EQUAL {
		return a >= b
	}

	if c == COMPARER_LESS {
		return a < b
	}

	if c == COMPARER_LESS_OR_EQUAL {
		return a <= b
	}

	if c == COMPARER_EXISTS {
		return true
	}

	if c == COMPARER_NOT_SET {
		return false
	}

	return false
}

func compareDate(a, b time.Time, c string) bool {
	if c == COMPARER_EQUAL {
		return a.Unix() == b.Unix()
	}

	if c == COMPARER_NOT_EQUAL {
		return a.Unix() != b.Unix()
	}

	if c == COMPARER_GREATER {
		return a.Unix() > b.Unix()
	}

	if c == COMPARER_GREATER_OR_EQUAL {
		return a.Unix() >= b.Unix()
	}

	if c == COMPARER_LESS {
		return a.Unix() < b.Unix()
	}

	if c == COMPARER_LESS_OR_EQUAL {
		return a.Unix() <= b.Unix()
	}

	if c == COMPARER_EXISTS {
		return !a.IsZero()
	}

	if c == COMPARER_NOT_SET {
		return a.IsZero()
	}

	return false
}

func compareBool(a, b bool, c string) bool {
	if c == COMPARER_EQUAL {
		return a == b
	}

	if c == COMPARER_NOT_EQUAL {
		return a != b
	}

	if c == COMPARER_EXISTS {
		return true
	}

	if c == COMPARER_NOT_SET {
		return false
	}

	return false
}

func csvContains(a, b string) bool {
	for _, s := range strings.Split(b, ",") {
		if a == strings.TrimSpace(s) {
			return true
		}
	}
	return false
}

func anyOfString(fields []interface{}, values []string) bool {
	for _, v := range values {
		for _, f := range fields {
			s, ok := f.(string)
			if ok && v == s {
				return true
			}
		}
	}
	return false
}

func allOfString(fields []interface{}, values []string) bool {
	for _, v := range values {
		found := false
		for _, f := range fields {
			s, ok := f.(string)
			if ok && v == s {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func noneOfString(fields []interface{}, values []string) bool {
	for _, v := range values {
		for _, f := range fields {
			s, ok := f.(string)
			if ok && v == s {
				return false
			}
		}
	}
	return true
}
