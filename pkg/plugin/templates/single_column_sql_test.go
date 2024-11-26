package templates

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRenderSingleColumnSql(t *testing.T) {
	t.Run("with filters", func(t *testing.T) {
		want := `SELECT "dim"
FROM "my_table"
WHERE ts >= 10 AND ts < 20
    AND ("dim1" = 'val1')
    AND ("dim2" = 'val2')
ORDER BY "dim" ASC
LIMIT 100;`
		got, err := RenderSingleColumnSql(SingleColumnSqlParams{
			ColumnName:           "dim",
			TableName:            "my_table",
			TimeFilterExpr:       "ts >= 10 AND ts < 20",
			DimensionFilterExprs: []string{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("without filters", func(t *testing.T) {
		want := `SELECT "dim"
FROM "my_table"
ORDER BY "dim" ASC
LIMIT 100;`
		got, err := RenderSingleColumnSql(SingleColumnSqlParams{
			ColumnName: "dim",
			TableName:  "my_table",
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})
}
