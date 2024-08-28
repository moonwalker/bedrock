package rules

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type GameTransaction struct {
	ActivityType     int     `json:"activityType"`
	BaseBalanceAfter float32 `json:"baseBalanceAfter"`
}

// $ go test -run TestContextNoFacts -count=1 -v pkg/rules/*.go
func TestContextNoFacts(t *testing.T) {
	ctx := NewContext(nil)
	assert.Nil(t, ctx.GetFact("event").Value())
}

// $ go test -run TestContextWithFacts -count=1 -v pkg/rules/*.go
func TestContextWithFacts(t *testing.T) {
	e := &GameTransaction{
		BaseBalanceAfter: 0.4,
		ActivityType:     1,
	}

	ctx := NewContext(e)
	assert.Equal(t, 0.4, ctx.GetFact("event.baseBalanceAfter").Float())
	assert.Equal(t, int64(1), ctx.GetFact("event.activityType").Int())

	swn := struct {
		Context interface{} `json:"ctx"`
	}{
		Context: ctx.Facts,
	}
	assert.NotNil(t, swn.Context)
}

// $ go test -run TestContextFactsMarshalArgs -count=1 -v pkg/rules/*.go
func TestContextFactsMarshalArgs(t *testing.T) {
	e := &GameTransaction{
		BaseBalanceAfter: 0.4,
		ActivityType:     1,
	}

	ctx := NewContext(e)
	var job jobWithArgs
	j, _ := json.Marshal(newJobWithArgs(ctx.Facts))
	json.Unmarshal(j, &job)

	fj, err := json.Marshal(job.Args[0])
	assert.Nil(t, err)
	f := Facts(fj)
	assert.Equal(t, int64(1), f.Get("event.activityType").Int())
}

type jobWithArgs struct {
	Args []interface{} ` json:"args"`
}

func newJobWithArgs(args ...interface{}) *jobWithArgs {
	return &jobWithArgs{
		Args: args,
	}
}
