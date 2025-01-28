package templates

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRenderLogSql(t *testing.T) {
	want := `SELECT
    "message" AS 'message_alias',
    "col1" AS 'alias1',
    "col2" AS 'alias2',
    "ts"
FROM "my_table"
WHERE "message" IS NOT NULL
    AND "ts" >= 10 AND "ts" <= 20
    AND ("dim1" = 'val1')
    AND ("dim2" = 'val2')
ORDER BY
    "ts" ASC,
    "message_alias" ASC
LIMIT 1000;`

	got, err := RenderLogSql(LogSqlParams{
		TableNameExpr:  `"my_table"`,
		TimeColumn:     "ts",
		LogColumnExpr:  `"message"`,
		LogColumnAlias: "message_alias",
		MetadataColumns: []ExprWithAlias{
			{Expr: `"col1"`, Alias: "alias1"},
			{Expr: `"col2"`, Alias: "alias2"},
		},
		TimeFilterExpr:       `"ts" >= 10 AND "ts" <= 20`,
		DimensionFilterExprs: []string{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		Limit:                1000,
	})
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}
