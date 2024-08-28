// $ go test -v pkg/rules/*.go

package rules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventCmdTopics(t *testing.T) {
	_, cmd := CommandTopics[CmdReload]
	assert.True(t, cmd)
	_, cmd = CommandTopics[CmdStop]
	assert.True(t, cmd)
	_, cmd = CommandTopics[CmdResume]
	assert.True(t, cmd)
}
