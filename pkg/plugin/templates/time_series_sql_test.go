package templates

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRenderTimeSeriesSql(t *testing.T) {
	t.Run("no group by", func(t *testing.T) {
		want := `
SELECT
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS') AS "time",
    sum("met") AS "metric"
FROM
    "my_table"
WHERE
    "ts" >= 10 AND "ts" <= 20
GROUP BY
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS')
ORDER BY "time" ASC
LIMIT 1000000
`

		got, err := RenderTimeSeriesSql(TimeSeriesSqlParams{
			TableName:           "my_table",
			TimeColumn:          "ts",
			MetricColumn:        "met",
			AggregationFunction: "sum",
			TimeFilterExpr:      `"ts" >= 10 AND "ts" <= 20`,
			TimeGroupExpr:       `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS')`,
			TimeColumnAlias:     "time",
			MetricColumnAlias:   "metric",
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("with group by", func(t *testing.T) {
		want := `
SELECT
    "dim1",
    "dim2",
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS') AS "time",
    sum("met") AS "metric"
FROM
    "my_table"
WHERE
    "ts" >= 10 AND "ts" <= 20
    AND ("dim1" = 'val1')
    AND ("dim2" = 'val2')
GROUP BY
    "dim1",
    "dim2",
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS')
ORDER BY "time" ASC
LIMIT 1000000
`

		got, err := RenderTimeSeriesSql(TimeSeriesSqlParams{
			TableName:            "my_table",
			DimensionColumns:     []string{"dim1", "dim2"},
			TimeColumn:           "ts",
			MetricColumn:         "met",
			AggregationFunction:  "sum",
			TimeFilterExpr:       `"ts" >= 10 AND "ts" <= 20`,
			TimeGroupExpr:        `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS')`,
			TimeColumnAlias:      "time",
			MetricColumnAlias:    "metric",
			DimensionFilterExprs: []string{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

}
