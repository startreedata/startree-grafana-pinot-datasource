package pinot

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRenderDistinctValues(t *testing.T) {
	t.Run("with filters", func(t *testing.T) {
		want := `SELECT DISTINCT "dim"
FROM "my_table"
WHERE "dim" IS NOT NULL
    AND ts >= 10 AND ts < 20
    AND ("dim1" = 'val1')
    AND ("dim2" = 'val2')
ORDER BY "dim" ASC
LIMIT 100;`
		got, err := RenderDistinctValuesSql(DistinctValuesSqlParams{
			ColumnExpr:           `"dim"`,
			TableName:            "my_table",
			TimeFilterExpr:       "ts >= 10 AND ts < 20",
			DimensionFilterExprs: []SqlExpr{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("without filters", func(t *testing.T) {
		want := `SELECT DISTINCT "dim"
FROM "my_table"
WHERE "dim" IS NOT NULL
ORDER BY "dim" ASC
LIMIT 100;`
		got, err := RenderDistinctValuesSql(DistinctValuesSqlParams{
			ColumnExpr: `"dim"`,
			TableName:  "my_table",
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})
}

func TestRenderTimeSeriesSql(t *testing.T) {
	want := `SELECT
    "dim1",
    "dim2" AS 'dim2Alias',
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
    "time"
ORDER BY
    "time" DESC,
    "metric" ASC
LIMIT 10000;`

	got, err := RenderTimeSeriesSql(TimeSeriesSqlParams{
		TableNameExpr:         `"my_table"`,
		GroupByColumnExprs:    []ExprWithAlias{{Expr: `"dim1"`}, {Expr: `"dim2"`, Alias: `dim2Alias`}},
		MetricColumnExpr:      `"met"`,
		AggregationFunction:   "sum",
		TimeFilterExpr:        `"ts" >= 10 AND "ts" <= 20`,
		TimeGroupExpr:         `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS')`,
		TimeColumnAliasExpr:   `"time"`,
		MetricColumnAliasExpr: `"metric"`,
		DimensionFilterExprs:  []SqlExpr{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		Limit:                 10000,
		OrderByExprs:          []SqlExpr{`"time" DESC`, `"metric" ASC`},
	})
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

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
		TableNameExpr:         `"my_table"`,
		TimeColumn:            "ts",
		TimeColumnAliasExpr:   `"time"`,
		MetricColumnExpr:      `"met"`,
		MetricColumnAliasExpr: `"metric"`,
		TimeFilterExpr:        `"ts" >= 10 AND "ts" <= 20`,
		DimensionFilterExprs:  []SqlExpr{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		Limit:                 1000,
	})
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

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
		DimensionFilterExprs: []SqlExpr{`("dim1" = 'val1')`, `("dim2" = 'val2')`},
		Limit:                1000,
	})
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}
