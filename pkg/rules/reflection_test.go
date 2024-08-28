// $ go test -v pkg/rules/*.go

package rules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafeCall(t *testing.T) {
	funcs := MakeValueFuncs(FuncMap{
		"boom": func(a string) error {
			panic("oops")
		},
	})

	_, err := callFunc(funcs, "boom", 123)
	assert.Error(t, err)
}

// $ go test -run TestActionsCall -count=1 -v pkg/rules/*.go
func TestActionsCall(t *testing.T) {
	funcs := MakeValueFuncs(FuncMap{
		"actNoRes": func() error {
			return nil
		},
		"actWithRes": func(a, b int) (int, error) {
			return a + b, nil
		},
	})

	_, err := callFunc(funcs, "actNoRes")
	assert.NoError(t, err)

	res, err := callFunc(funcs, "actWithRes", 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, res, 4)
}
