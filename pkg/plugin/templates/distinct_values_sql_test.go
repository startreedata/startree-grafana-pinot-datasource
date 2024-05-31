package templates

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRenderDistinctValues(t *testing.T) {
	want := `
SELECT DISTINCT "dim"
FROM "my_table"
WHERE ts >= 10 AND ts <= 20
    AND ("dim1" = 'val1')
    AND ("dim2" = 'val2')
ORDER BY "dim" ASC
`

	got, err := RenderDistinctValuesSql(DistinctValuesSqlParams{
		ColumnName:           "dim",
		TableName:            "my_table",
		TimeFilterExpr:       "ts >= 10 AND ts <= 20",
		DimensionFilterExprs: []string{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
	})
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}
