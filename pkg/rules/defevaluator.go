package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"strings"
	"time"

	"github.com/moonwalker/bedrock/pkg/parse"
)

// wrapper to fulfill evaluator interface
type DefaultEvaluator struct{}

func (e *DefaultEvaluator) EvaluateRule(funcs ValueFuncs, ctx *Context, rule *Rule, event *Event) *EvalResult {
	return evaluateRule(funcs, ctx, rule, event)
}

// actual implementation

func evaluateRule(funcs ValueFuncs, ctx *Context, rule *Rule, event *Event) *EvalResult {
	start := time.Now()
	crits, err := evalCriterias(funcs, ctx, rule.Criterias)
	if err == nil {
		if crits.ok {
			err = runActions(funcs, ctx, rule.Actions, event.DryRun)
		} else {
			err = runActions(funcs, ctx, rule.UnmetActions, event.DryRun)
		}
	}
	event.Processed = time.Now().UTC()
	return &EvalResult{
		Rule:        rule,
		Event:       event,
		Context:     ctx,
		Error:       err,
		Met:         crits.ok,
		Unmet:       !crits.ok,
		MetReason:   crits.met(),
		UnmetReason: crits.unmet(),
		Duration:    time.Since(start),
	}
}

func (e *EvalResult) MarshalJSON() ([]byte, error) {
	type Alias EvalResult
	var errorMessage string
	if e.Error != nil {
		errorMessage = e.Error.Error()
	}
	return json.Marshal(&struct {
		*Alias
		Unmet    string `json:"unmet,omitempty"`
		Error    string `json:"error,omitempty"`
		Duration string `json:"duration"`
	}{
		Alias:    (*Alias)(e),
		Unmet:    e.UnmetReason,
		Error:    errorMessage,
		Duration: e.Duration.String(),
	})
}

// criterias

type evalCriteriasResult struct {
	ok                   bool
	unmetDataSource      interface{}
	unmetDataSourceAlias string
	unmetCondition       *Condition
	metCriterias         []*Criteria
}

func fmtMetCriteria(criteria *Criteria) string {
	var sb strings.Builder

	for i, condition := range criteria.Conditions {
		if i > 0 {
			if condition.Connector == CONNECTOR_AND {
				sb.WriteString(" and ")
			} else {
				sb.WriteString(" or ")
			}
		}
		sb.WriteString(criteria.DataSource.Alias)
		if condition.Field != "" {
			sb.WriteString(".")
			sb.WriteString(condition.Field)
		}

		if condition.Comparer == COMPARER_EXISTS {
			sb.WriteString("is set")
		} else if condition.Comparer == COMPARER_NOT_SET {
			sb.WriteString("is not set")
		} else {
			sb.WriteString(" ")
			sb.WriteString(condition.Comparer)
			sb.WriteString(" ")
		}
		sb.WriteString(condition.Value)
	}

	return sb.String()
}

func (res *evalCriteriasResult) met() string {
	if !res.ok {
		return ""
	}

	if len(res.metCriterias) == 0 {
		return "n/a"
	}

	var sb strings.Builder

	for i, c := range res.metCriterias {
		if i > 0 {
			if c.Connector == CONNECTOR_AND {
				sb.WriteString(" and ")
			} else {
				sb.WriteString(" or ")
			}
		}
		if len(c.Items) > 0 {
			sb.WriteString(" ( ")
			sb.WriteString(fmtMetCriteria(c))
			sb.WriteString(" ) ")
		} else {
			sb.WriteString(fmtMetCriteria(c))
		}
	}

	return sb.String()
}

func (res *evalCriteriasResult) unmet() string {
	if res.ok {
		return ""
	}

	if res.unmetCondition == nil {
		return "n/a"
	}

	// condition
	condField := res.unmetDataSourceAlias
	_, compositeType := res.unmetDataSource.(map[string]interface{})
	if compositeType {
		if len(res.unmetCondition.Field) > 0 {
			condField += "." + res.unmetCondition.Field
		}
	}

	// comparer
	compField := res.unmetCondition.Comparer
	if compField == COMPARER_EXISTS {
		compField = "is set"
	} else if compField == COMPARER_NOT_SET {
		compField = "is not set"
	}

	return fmt.Sprintf("%s %s %s", condField, compField, res.unmetCondition.Value)
}

func evalCriterias(funcs ValueFuncs, ctx *Context, criterias []*Criteria) (*evalCriteriasResult, error) {
	res := &evalCriteriasResult{ok: false, metCriterias: make([]*Criteria, 0)}

	i := 0
	l := len(criterias)
	if l == 0 {
		res.ok = true
	} else {
		criterias[0].Connector = CONNECTOR_OR
	}
	for i < l {
		// when not the first and already 'not ok' ignoring AND criterias
		if i == 0 || res.ok || criterias[i].Connector == CONNECTOR_OR {
			if len(criterias[i].Items) > 0 {
				itemsRes, itemsErr := evalCriterias(funcs, ctx, criterias[i].Items)
				if itemsErr != nil {
					return itemsRes, itemsErr
				}
				if itemsRes.ok {
					res.metCriterias = append(res.metCriterias, criterias[i])
				}
				res.ok = ConnectCondition(res.ok, itemsRes.ok, criterias[i].Connector)
				if itemsRes.unmetCondition != nil {
					res.unmetCondition = itemsRes.unmetCondition
				}
				if !itemsRes.ok {
					res.unmetDataSource = itemsRes.unmetDataSource
					res.unmetDataSourceAlias = itemsRes.unmetDataSourceAlias
				}
				if !res.ok {
					res.metCriterias = make([]*Criteria, 0)
				}
			} else {
				err := evaluateCriteria(res, funcs, ctx, criterias[i])
				if err != nil {
					return res, err
				}
			}
		}
		i++
	}

	return res, nil
}

func evaluateCriteria(res *evalCriteriasResult, funcs ValueFuncs, ctx *Context, criteria *Criteria) error {
	ds, err := ensureDataSource(funcs, ctx, criteria.DataSource, criteria.Parameters, criteria.Filters, criteria.Aggregations)
	if err != nil {
		return err
	}

	conditionsOk, unmetCondition := EvaluateConditions(ctx, ds, criteria.Conditions)
	if conditionsOk {
		res.metCriterias = append(res.metCriterias, criteria)
	}
	res.ok = ConnectCondition(res.ok, conditionsOk, criteria.Connector)
	if unmetCondition != nil {
		res.unmetCondition = unmetCondition
	}
	if !res.ok {
		res.unmetDataSource = ds
		res.unmetDataSourceAlias = criteria.DataSource.Alias
		res.metCriterias = make([]*Criteria, 0)
	}

	return nil
}

// actions

func RunAction(funcs ValueFuncs, ctx *Context, action *Action, dryRun bool) error {
	defer ctx.NewSpan(action.Method, action.Parameters).Finish()
	method := action.Method

	if dryRun {
		slog.Debug("run action in dry mode", "method", action.Method, "params", action.Parameters)
		// in dry run we'll try to call the "fake" func, if exists
		method = DRY_RUN_KEY + method
	}

	args := makeArgs(ctx, action.Parameters)
	res, err := callFunc(funcs, method, args...)
	if err != nil && !dryRun {
		return err
	}

	if len(action.Alias) > 0 {
		return ctx.SetFact(action.Alias, res)
	}

	return nil
}

func runActions(funcs ValueFuncs, ctx *Context, actions []*Action, dryRun bool) error {
	for _, action := range actions {
		err := RunAction(funcs, ctx, action, dryRun)
		if err != nil {
			return err
		}
	}
	return nil
}

// datasource

func ensureDataSource(funcs ValueFuncs, ctx *Context, ds *DataSource, params []*Parameter, filters []*Condition, aggregations []*Condition) (interface{}, error) {
	if !ctx.GetFact(ds.Alias).Exists() {
		defer ctx.NewSpan(ds.ID, params).Finish()
		args := makeArgs(ctx, params)
		args = append(args, filters)
		args = append(args, aggregations)
		res, err := callFunc(funcs, ds.ID, args...)
		if err != nil {
			return nil, err
		}
		err = ctx.SetFact(ds.Alias, res)
		if err != nil {
			return nil, err
		}
	}
	return getDataSource(ctx, ds)
}

func getDataSource(ctx *Context, ds *DataSource) (interface{}, error) {
	if !ctx.GetFact(ds.Alias).Exists() {
		return nil, fmt.Errorf("datasource: '%s' not found on context", ds.Alias)
	}
	val := ctx.GetFact(ds.Alias).Value()
	if val == nil {
		return nil, nil
	}
	return val, nil
}

// common

func makeArgs(ctx *Context, params []*Parameter) []interface{} {
	args := []interface{}{
		ctx,
	}
	for _, p := range params {
		if p.IsDynamic {
			v := getDynamicArgsValue(ctx, p.Value, p.Type)
			args = append(args, v)
		} else {
			switch p.Type {
			case PARAMTYPE_NUMBER:
				n := parse.ParseInt64(p.Value)
				args = append(args, n)
			case PARAMTYPE_FLOAT:
				f := parse.ParseFloat(p.Value)
				args = append(args, f)
			case PARAMTYPE_FRACTION:
				f := parse.ParseFloat(p.Value)
				args = append(args, f)
			case PARAMTYPE_BOOL:
				b := parse.ParseBool(p.Value)
				args = append(args, b)
			case PARAMTYPE_DATE:
				d := parse.ParseDate(p.Value)
				args = append(args, d)
			case PARAMTYPE_DATETIME:
				dt := parse.ParseDate(p.Value)
				args = append(args, dt)
			case PARAMTYPE_DURATION:
				h := parse.ParseHappened(p.Value)
				args = append(args, h)
			case PARAMTYPE_FIELDS:
				var v map[string]interface{}
				json.Unmarshal([]byte(p.Value), &v)
				args = append(args, v)
			case PARAMTYPE_STRING:
				text := interpolateStringArgs(ctx, p.Value)
				args = append(args, text)
			default:
				args = append(args, p.Value)
			}
		}
	}
	return args
}

func getDynamicArgsValue(ctx *Context, value string, paramType string) interface{} {
	if ctx != nil && ctx.GetFact(value).Exists() {
		v := ctx.GetFact(value).Value()
		switch paramType {
		case PARAMTYPE_NUMBER:
			return int64(v.(float64))
		case PARAMTYPE_FLOAT:
			return v.(float64)
		case PARAMTYPE_FRACTION:
			return v.(float64)
		case PARAMTYPE_BOOL:
			return v.(bool)
		case PARAMTYPE_DATE:
			d := v.(string)
			return parse.ParseDate(d)
		case PARAMTYPE_DATETIME:
			d := v.(string)
			return parse.ParseDate(d)
		case PARAMTYPE_DURATION:
			d := v.(string)
			return parse.ParseHappened(d)
		case PARAMTYPE_FIELDS:
			return v.(map[string]interface{})
		case PARAMTYPE_STRING:
			return interpolateStringArgs(ctx, fmt.Sprintf("%v", v))
		default:
			return v.(string)
		}
	}
	return nil
}

func interpolateStringArgs(ctx *Context, value string) string {
	text := value
	t, err := template.New("").Funcs(template.FuncMap{
		"format": func(format string, v interface{}) string {
			switch t := v.(type) {
			case string:
				pt, _ := time.Parse(time.RFC3339, t)
				return pt.Format(format)
			case time.Time:
				return t.Format(format)
			case int64:
				ut := time.Unix(t, 0)
				return ut.Format(format)
			default:
				return fmt.Sprintf("%v", v)
			}
		},
	}).Parse(value)
	if err == nil {
		var tpl bytes.Buffer
		err = t.Execute(&tpl, ctx.Facts.Map())
		if err == nil {
			text = tpl.String()
		}
	}

	// here it's possible that "\n" is actually the escaped version of a line break character
	// replace these with real line breaks by searching for the escaped version and replacing with the non escaped version
	return strings.Replace(text, `\n`, "\n", -1)
}
