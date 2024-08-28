package rules

import (
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// context

type Context struct {
	Facts     Facts  `json:"facts"`
	Spans     Spans  `json:"spans,omitempty"`
	RuleID    string `json:"ruleId"`
	RuleTitle string `json:"ruleTitle"`
}

const (
	factsEventKey = "event"
)

func NewContext(eventData interface{}) *Context {
	ctx := &Context{}

	switch v := eventData.(type) {
	case []byte: // nats
		ctx.Facts, _ = sjson.SetRawBytes(ctx.Facts, factsEventKey, v)
	case string: // rpc
		ctx.Facts, _ = sjson.SetRawBytes(ctx.Facts, factsEventKey, []byte(v))
	default: // tests, etc.
		ctx.SetFact(factsEventKey, v)
	}

	return ctx
}

func (ctx *Context) SetFact(path string, value interface{}) (err error) {
	return ctx.Facts.Set(path, value)
}

func (ctx *Context) GetFact(path string) gjson.Result {
	return ctx.Facts.Get(path)
}

func (ctx *Context) NewSpan(name string, args []*Parameter) *Span {
	vals := []string{"ctx"}
	if args != nil {
		for _, a := range args {
			vals = append(vals, a.Value)
		}
	}
	called := fmt.Sprintf("%s(%s)", name, strings.Join(vals, ", "))
	span := &Span{Name: name, Args: args, Called: called, start: time.Now()}
	ctx.Spans = append(ctx.Spans, span)
	return span
}

// facts

type Facts []byte

func (f Facts) Get(path string) gjson.Result {
	return gjson.GetBytes(f, path)
}

func (f *Facts) Set(path string, value interface{}) (err error) {
	*f, err = sjson.SetBytes(*f, path, value)
	return
}

func (f Facts) MarshalJSON() ([]byte, error) {
	return f, nil
}

func (f *Facts) UnmarshalJSON(data []byte) error {
	*f = data
	return nil
}

func (f Facts) String() string {
	return string(f)
}

func (f Facts) Map() map[string]interface{} {
	res, ok := gjson.ParseBytes(f).Value().(map[string]interface{})
	if !ok {
		return nil
	}
	return res
}

// spans

type Spans []*Span

type Span struct {
	start    time.Time
	Name     string       `json:"-"`
	Args     []*Parameter `json:"-"`
	Called   string       `json:"called"`
	Duration string       `json:"duration"`
}

func (s *Span) Finish() {
	s.Duration = time.Since(s.start).String()
}
