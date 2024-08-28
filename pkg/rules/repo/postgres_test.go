// $ go test -v pkg/rules/repo/*.go

package repo

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/moonwalker/bedrock/pkg/rules"
)

func TestPostgresRuleRepo(t *testing.T) {
	repo, err := NewPostgresRuleRepo("postgres://postgres@localhost?sslmode=disable")
	assert.Nil(t, err)
	assert.NotNil(t, repo)

	repo.Save(&rules.Rule{ID: "1", Active: true})
	repo.Save(&rules.Rule{ID: "2", Active: true, Desc: "foo"})
	repo.Save(&rules.Rule{ID: "3", Active: true})
	repo.Save(&rules.Rule{ID: "4", Active: true})
	repo.Save(&rules.Rule{ID: "5", Active: false})

	assert.Equal(t, repo.Count(), 5)
	assert.Equal(t, repo.Active(), 4)

	var ids string
	repo.Each(2, 3, func(rule *rules.Rule) {
		ids += rule.ID
	})
	assert.Equal(t, ids, "345")

	repo.Remove("1")
	assert.Equal(t, repo.Count(), 4)
	assert.Equal(t, repo.Active(), 3)

	r2, err := repo.Get("2")
	assert.Nil(t, err)
	assert.NotNil(t, r2)
	assert.Equal(t, r2.Desc, "foo")

	repo.RemoveAll()
	assert.Equal(t, repo.Count(), 0)
}
