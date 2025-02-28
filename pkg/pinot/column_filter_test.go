package pinot

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnFilterExpr(t *testing.T) {
	testArgs := []struct {
		operator FilterOperator
		expected SqlExpr
	}{
		{FilterOpEquals, `("dim"['key'] = 'val1' OR "dim"['key'] = 'val2')`},
		{FilterOpNotEquals, `("dim"['key'] != 'val1' OR "dim"['key'] != 'val2')`},
		{FilterOpContains, `("dim"['key'] contains 'val1' OR "dim"['key'] contains 'val2')`},
		{FilterOpNotContains, `(not "dim"['key'] contains 'val1' OR not "dim"['key'] contains 'val2')`},
		{FilterOpLike, `("dim"['key'] like 'val1' OR "dim"['key'] like 'val2')`},
		{FilterOpNotLike, `(not "dim"['key'] like 'val1' OR not "dim"['key'] like 'val2')`},
		{FilterOpGreaterThan, `("dim"['key'] > 'val1' OR "dim"['key'] > 'val2')`},
		{FilterOpLessThan, `("dim"['key'] < 'val1' OR "dim"['key'] < 'val2')`},
		{FilterOpGreaterThanOrEqual, `("dim"['key'] >= 'val1' OR "dim"['key'] >= 'val2')`},
		{FilterOpLessThanOrEqual, `("dim"['key'] <= 'val1' OR "dim"['key'] <= 'val2')`},
		{FilterOpIn, `("dim"['key'] in 'val1' OR "dim"['key'] in 'val2')`},
		{FilterOpNotIn, `("dim"['key'] not in 'val1' OR "dim"['key'] not in 'val2')`},
	}
	for _, args := range testArgs {
		t.Run(string(args.operator), func(t *testing.T) {
			assert.Equal(t, args.expected, ColumnFilterExpr(ColumnFilter{
				ColumnName: "dim",
				ColumnKey:  "key",
				Operator:   args.operator,
				ValueExprs: []string{`'val1'`, `'val2'`},
			}))
		})
	}
}
