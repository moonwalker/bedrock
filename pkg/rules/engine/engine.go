package engine

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/moonwalker/bedrock/pkg/parse"
	"github.com/moonwalker/bedrock/pkg/rules"
	"github.com/moonwalker/bedrock/pkg/rules/repo"
	"github.com/moonwalker/bedrock/pkg/worker"
)

const (
	RULES_EXEC_ERROR = "RULES_EXEC_ERROR"
)

type Engine struct {
	sync.Mutex
	debug            *bool
	enabled          bool
	onStats          func(*EngineStats)
	rules            []*rules.Rule
	funcs            rules.ValueFuncs
	evaluator        rules.Evaluator
	Repo             repo.RuleRepo
	Commands         chan *rules.Event
	Events           chan *rules.Event
	schedulerPoolURL string
}

type EngineStats struct {
	EngineEnabled     bool   `json:"engineEnabled"`
	RulesRepo         string `json:"rulesRepo"`
	ActiveRulesLoaded int    `json:"activeRulesLoaded"`
}

func NewRuleEngine(debug *bool, repo repo.RuleRepo, funcs rules.FuncMap, evaluator rules.Evaluator) *Engine {
	e := &Engine{
		debug:     debug,
		enabled:   true,
		funcs:     rules.MakeValueFuncs(funcs),
		evaluator: evaluator,
		Repo:      repo,
		Commands:  make(chan *rules.Event),
		Events:    make(chan *rules.Event),
	}
	e.loadRules()
	go e.commandsLoop()
	return e
}

func (e *Engine) ProcessEvents(schedulerPoolURL string) chan *rules.EvalResult {
	e.schedulerPoolURL = schedulerPoolURL
	reschan := make(chan *rules.EvalResult)
	go e.eventsLoop(reschan)
	return reschan
}

func (e *Engine) OnStats(interval time.Duration, fn func(stats *EngineStats)) {
	e.onStats = fn
	e.emitStats()                      // emit first immediately
	ticker := time.NewTicker(interval) // then emit every interval
	go func() {
		for range ticker.C {
			e.emitStats()
		}
	}()
}

func (e *Engine) ExecRule(rule *rules.Rule, event *rules.Event) *rules.EvalResult {
	ctx := rules.NewContext(event.Data)

	// engine disabled
	if !e.enabled {
		return &rules.EvalResult{
			Rule:        rule,
			Event:       event,
			Context:     ctx,
			Unmet:       true,
			UnmetReason: "rules processing disabled",
		}
	}

	// check period
	if !validPeriod(rule) {
		return &rules.EvalResult{
			Rule:        rule,
			Event:       event,
			Context:     ctx,
			Unmet:       true,
			UnmetReason: fmt.Sprintf("rule valid from %s to %s", rule.ValidFrom, rule.ValidTo),
		}
	}

	// check criterias
	if !validCriterias(rule, event.Topic) {
		return &rules.EvalResult{
			Rule:        rule,
			Event:       event,
			Context:     ctx,
			Unmet:       true,
			UnmetReason: "no valid criterias",
		}
	}

	// rule scheduled but deactivated
	if rule.Scheduled && !rule.Active {
		return &rules.EvalResult{
			Rule:        rule,
			Event:       event,
			Context:     ctx,
			Unmet:       true,
			UnmetReason: "scheduled rule not active",
		}
	}

	// append some basic rule info to the context
	// this can be handy in query functions and actions
	ctx.RuleID = rule.ID
	ctx.RuleTitle = rule.Title

	// set basic facts
	// this can be useful for templating functions for example
	ctx.SetFact("topic", event.Topic)

	slog.Debug("executing rule", "event", event, "rule", rule)

	return e.evaluator.EvaluateRule(e.funcs, ctx, rule, event)
}

func (e *Engine) emitStats() {
	if e.onStats != nil {
		e.onStats(&EngineStats{
			EngineEnabled:     e.enabled,
			ActiveRulesLoaded: len(e.rules),
			RulesRepo:         e.Repo.Name(),
		})
	}
}

func (e *Engine) commandsLoop() {
	// commands
	for cmd := range e.Commands {
		// reload rules from repo
		if cmd.Topic == rules.CmdReload {
			e.Lock()
			e.loadRules()
			e.emitStats()
			e.Unlock()
		}
		// disable rules processing
		if cmd.Topic == rules.CmdStop {
			e.Lock()
			e.enabled = false
			e.emitStats()
			e.Unlock()
		}
		// enable rules processing
		if cmd.Topic == rules.CmdResume {
			e.Lock()
			e.enabled = true
			e.emitStats()
			e.Unlock()
		}
	}
}

func (e *Engine) eventsLoop(reschan chan *rules.EvalResult) {
	// events
	for event := range e.Events {
		// skip events when rules processing disabled
		if !e.enabled {
			continue
		}
		// find rule candidates to execute
		for _, rule := range e.findRules(event.Topic) {
			if rule.Scheduled {
				err := e.scheduleRule(rule, event)
				if err != nil {
					slog.Error("failed to schedule rule", "err", err.Error())
				}
			} else {
				reschan <- e.ExecRule(rule, event)
			}
		}
	}
}

func (e *Engine) loadRules() {
	e.rules = make([]*rules.Rule, 0)
	e.Repo.Each(0, 0, func(rule *rules.Rule) {
		if rule.Active {
			e.rules = append(e.rules, rule)
		}
	})
	if *e.debug {
		slog.Info("rules loaded", "count", len(e.rules))
	}
}

func (e *Engine) findRules(topic string) []*rules.Rule {
	match := make([]*rules.Rule, 0)
	for _, r := range e.rules {
		if r.Event == topic {
			match = append(match, r)
		}
	}
	if *e.debug {
		slog.Info("rules match", "count", len(match))
	}
	return match
}

func (e *Engine) scheduleRule(rule *rules.Rule, event *rules.Event) error {
	d := worker.NewDispatcher("worker", e.schedulerPoolURL, 100)
	defer d.Close()

	runAt, err := time.Parse(time.RFC3339Nano, rule.RunAt)
	if err != nil {
		p := parse.ParseScheduled(rule.RunAt)
		if p == 0 {
			return fmt.Errorf("invalid time: %s", rule.RunAt)
		}
		runAt = time.Unix(p, 0).UTC()
	}

	// in dry-run "fake" scheduling for a short random interval between 3 and 30 seconds
	if event.DryRun {
		min := 3
		max := 30
		n := rand.Intn(max-min) + min
		runAt = time.Now().Add(time.Duration(n) * time.Second)
	}

	now := time.Now().UTC()
	job := &worker.Job{
		Queue: "rules:delayed",
		Args: worker.Args{
			"ruleID": rule.ID,
			"event":  event,
		},
		CreatedAt: &now,
		RunAt:     &runAt,
		Type:      worker.TypeScheduled,
	}

	return d.EnqueueJob(job)
}

func validCriterias(rule *rules.Rule, topic string) bool {
	if topic == RULES_EXEC_ERROR || rule.NoCriterias {
		return true
	}
	return len(rule.Criterias) > 0
}

func validPeriod(rule *rules.Rule) bool {
	now := time.Now()
	return (rule.ValidFrom.IsZero() || rule.ValidFrom.Before(now)) &&
		(rule.ValidTo.IsZero() || rule.ValidTo.After(now))
}
