// $ go test -v pkg/parse/*.go

package parse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	now := time.Now().Unix()
	m := ParseHappened("2m")
	assert.True(t, m != 0 && m < now, "2 minutes ago: %v", time.Unix(m, 0))
	h := ParseHappened("2h")
	assert.True(t, h != 0 && h < now, "2 hours ago: %v", time.Unix(h, 0))
	D := ParseHappened("2D")
	assert.True(t, D != 0 && D < now, "2 days ago: %v", time.Unix(D, 0))
	W := ParseHappened("2W")
	assert.True(t, W != 0 && W < now, "2 weeks ago: %v", time.Unix(W, 0))
	M := ParseHappened("2M")
	assert.True(t, M != 0 && M < now, "2 months ago: %v", time.Unix(M, 0))
	Y := ParseHappened("2Y")
	assert.True(t, Y != 0 && Y < now, "2 years ago: %v", time.Unix(Y, 0))

	m = ParseScheduled("2m")
	assert.True(t, m != 0 && m > now, "2 minutes later: %v", time.Unix(m, 0))
	h = ParseScheduled("2h")
	assert.True(t, h != 0 && h > now, "2 hours later: %v", time.Unix(h, 0))
	D = ParseScheduled("2D")
	assert.True(t, D != 0 && D > now, "2 days later: %v", time.Unix(D, 0))
	W = ParseScheduled("2W")
	assert.True(t, W != 0 && W > now, "2 weeks later: %v", time.Unix(W, 0))
	M = ParseScheduled("2M")
	assert.True(t, M != 0 && M > now, "2 months later: %v", time.Unix(M, 0))
	Y = ParseScheduled("2Y")
	assert.True(t, Y != 0 && Y > now, "2 years later: %v", time.Unix(Y, 0))

	p := ParseHappened("2x")
	assert.True(t, p == 0, "invalid time ago: 2x")
	p = ParseHappened("foo")
	assert.True(t, p == 0, "invalid time ago: foo")
	p = ParseScheduled("2x")
	assert.True(t, p == 0, "invalid time later: 2x")
	p = ParseScheduled("foo")
	assert.True(t, p == 0, "invalid time later: foo")
}
