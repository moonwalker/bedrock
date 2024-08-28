package rules

import (
	"time"
)

type EvalResult struct {
	Rule        *Rule         `json:"rule"`
	Event       *Event        `json:"event"`
	Context     *Context      `json:"context"`
	Unmet       bool          `json:"unmet"`
	UnmetReason string        `json:"-"`
	Error       error         `json:"-"`
	Duration    time.Duration `json:"-"`
}

// evaluator interface to support swappable implementations
type Evaluator interface {
	EvaluateRule(funcs ValueFuncs, ctx *Context, rule *Rule, event *Event) *EvalResult
}
