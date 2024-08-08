package templates

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRenderSingleMetricSql(t *testing.T) {
	want := `SELECT
    "met" AS "metric",
    "ts" AS "time"
FROM
    "my_table"
WHERE
    "met" IS NOT NULL
    AND "ts" >= 10 AND "ts" <= 20
    AND ("dim1" = 'val1')
    AND ("dim2" = 'val2')
ORDER BY "time" DESC
LIMIT 1000;`

	got, err := RenderSingleMetricSql(SingleMetricSqlParams{
		TableName:            "my_table",
		TimeColumn:           "ts",
		TimeColumnAlias:      "time",
		MetricColumn:         "met",
		MetricColumnAlias:    "metric",
		TimeFilterExpr:       `"ts" >= 10 AND "ts" <= 20`,
		DimensionFilterExprs: []string{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		Limit:                1000,
	})
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}
