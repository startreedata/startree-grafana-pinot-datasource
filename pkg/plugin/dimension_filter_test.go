package plugin

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDimensionFilter(t *testing.T) {
	testArgs := []struct {
		operator string
		expected string
	}{
		{FilterOpEquals, `("dim" = 'val1' AND "dim" = 'val2')`},
		{FilterOpNotEquals, `("dim" != 'val1' AND "dim" != 'val2')`},
		{FilterOpContains, `("dim" contains 'val1' AND "dim" contains 'val2')`},
		{FilterOpNotContains, `(not "dim" contains 'val1' AND not "dim" contains 'val2')`},
		{FilterOpLike, `("dim" like 'val1' AND "dim" like 'val2')`},
		{FilterOpNotLike, `(not "dim" like 'val1' AND not "dim" like 'val2')`},
		{FilterOpGreaterThan, `("dim" > 'val1' AND "dim" > 'val2')`},
		{FilterOpLessThan, `("dim" < 'val1' AND "dim" < 'val2')`},
		{FilterOpGreaterThanOrEqual, `("dim" >= 'val1' AND "dim" >= 'val2')`},
		{FilterOpLessThanOrEqual, `("dim" <= 'val1' AND "dim" <= 'val2')`},
	}
	for _, args := range testArgs {
		t.Run(args.operator, func(t *testing.T) {
			assert.Equal(t, args.expected, dimensionFilterExpr(DimensionFilter{
				ColumnName: "dim",
				Operator:   args.operator,
				ValueExprs: []string{`'val1'`, `'val2'`},
			}))
		})
	}
}
