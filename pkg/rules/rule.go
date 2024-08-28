package rules

import (
	"time"
)

const (
	DRY_RUN_KEY        = "dryRun"
	ON_FAIL_DEACTIVATE = 0
	ON_FAIL_KEEPACTIVE = 1
	ON_UNMET_NOOP      = 0
	ON_UNMET_LOG       = 1
	ON_MET_NOOP        = 0
	ON_MET_LOG         = 1
)

type Rule struct {
	ID           string      `json:"id"`
	Archive      bool        `json:"archive"`
	Active       bool        `json:"active"`
	OnUnmet      int         `json:"onUnmet"`
	OnMet        int         `json:"onMet"`
	OnFail       int         `json:"onFail"`
	Title        string      `json:"title"`
	Desc         string      `json:"desc"`
	Tags         string      `json:"tags"`
	Created      time.Time   `json:"created"`
	Updated      time.Time   `json:"updated"`
	Event        string      `json:"event"`
	Criterias    []*Criteria `json:"criterias"`
	NoCriterias  bool        `json:"noCriterias"`
	Chainable    bool        `json:"chainable"`
	Actions      []*Action   `json:"actions"`
	UnmetActions []*Action   `json:"unmetActions"`
	Scheduled    bool        `json:"scheduled"`
	RunAt        string      `json:"runat"`
	Cron         string      `json:"cron"`
	CreatedBy    string      `json:"createdBy"`
	UpdatedBy    string      `json:"updatedBy"`
	ValidFrom    time.Time   `json:"validFrom"`
	ValidTo      time.Time   `json:"validTo"`
	Changes      string      `json:"changes"`
}

type Criteria struct {
	DataSource   *DataSource  `json:"datasource"`
	Parameters   []*Parameter `json:"parameters"`
	Returns      string       `json:"returns"`
	Connector    string       `json:"connector"`
	Items        []*Criteria  `json:"items"`
	Conditions   []*Condition `json:"conditions"`
	Filters      []*Condition `json:"filters"`
	Aggregations []*Condition `json:"aggregations"`
}

type DataSource struct {
	ID    string `json:"id"`
	Key   string `json:"key"`
	Alias string `json:"alias"`
}

type Parameter struct {
	Type      string `json:"type"`
	Value     string `json:"value"`
	IsDynamic bool   `json:"isDynamic"`
}

type Action struct {
	Method     string       `json:"method"`
	Parameters []*Parameter `json:"parameters"`
	Alias      string       `json:"alias"`
}

type ActionTrigger struct {
	Action Action                 `json:"action"`
	DryRun bool                   `json:"dryRun"`
	Event  map[string]interface{} `json:"event"`
}
