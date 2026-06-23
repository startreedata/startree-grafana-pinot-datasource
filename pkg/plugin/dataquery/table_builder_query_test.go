package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTableBuilderQuery_Validate(t *testing.T) {
	newQuery := func() TableBuilderQuery {
		return TableBuilderQuery{
			TimeRange:    TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
			TableName:    "my_table",
			TimeColumn:   "ts",
			Dimensions:   []ComplexField{{Name: "fabric"}},
			Aggregations: []Aggregation{{Function: "SUM", Column: ComplexField{Name: "value"}}},
			Limit:        10,
		}
	}

	t.Run("success", func(t *testing.T) {
		assert.NoError(t, newQuery().Validate())
	})
	t.Run("dimensions only is valid", func(t *testing.T) {
		query := newQuery()
		query.Aggregations = nil
		assert.NoError(t, query.Validate())
	})
	t.Run("aggregations only is valid", func(t *testing.T) {
		query := newQuery()
		query.Dimensions = nil
		assert.NoError(t, query.Validate())
	})
	t.Run("no table name", func(t *testing.T) {
		query := newQuery()
		query.TableName = ""
		assert.ErrorContains(t, query.Validate(), "TableName is required")
	})
	t.Run("no time column", func(t *testing.T) {
		query := newQuery()
		query.TimeColumn = ""
		assert.ErrorContains(t, query.Validate(), "TimeColumn is required")
	})
	t.Run("no dimensions or aggregations", func(t *testing.T) {
		query := newQuery()
		query.Dimensions = nil
		query.Aggregations = nil
		assert.ErrorContains(t, query.Validate(), "at least one dimension or aggregation is required")
	})
	t.Run("blank aggregation does not count", func(t *testing.T) {
		query := newQuery()
		query.Dimensions = nil
		query.Aggregations = []Aggregation{{Function: "SUM"}} // missing column for non-count
		assert.ErrorContains(t, query.Validate(), "at least one dimension or aggregation is required")
	})
}

func TestTableBuilderQuery_RenderSqlQuery(t *testing.T) {
	ctx := context.Background()
	client := test_helpers.SetupPinotAndCreateClient(t)

	t.Run("multiple aggregations, group by, order by, limit", func(t *testing.T) {
		query := TableBuilderQuery{
			TimeRange:  TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
			TableName:  "benchmark",
			TimeColumn: "ts",
			Dimensions: []ComplexField{{Name: "fabric"}, {Name: "pattern"}},
			Aggregations: []Aggregation{
				{Function: "SUM", Column: ComplexField{Name: "value"}},
				{Function: "AVG", Column: ComplexField{Name: "value"}},
				{Function: "COUNT"},
			},
			DimensionFilters: []DimensionFilter{{
				ColumnName: "fabric",
				ValueExprs: []string{"'fabric_001'"},
				Operator:   "=",
			}},
			OrderByClauses: []OrderByClause{{ColumnName: "SUM(value)", Direction: "DESC"}},
			Limit:          10,
		}

		want := pinot.NewSqlQuery(`SELECT
    "fabric",
    "pattern",
    SUM("value") AS "SUM(value)",
    AVG("value") AS "AVG(value)",
    COUNT(*) AS "COUNT(*)"
FROM
    "benchmark"
WHERE
    "ts" >= 0 AND "ts" < 1000
    AND ("fabric" = 'fabric_001')
GROUP BY
    "fabric",
    "pattern"
ORDER BY
    "SUM(value)" DESC
LIMIT 10;`)

		got, err := query.RenderSqlQuery(ctx, client)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("dimensions only renders no group by", func(t *testing.T) {
		query := TableBuilderQuery{
			TimeRange:  TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
			TableName:  "benchmark",
			TimeColumn: "ts",
			Dimensions: []ComplexField{{Name: "fabric"}},
		}

		got, err := query.RenderSqlQuery(ctx, client)
		assert.NoError(t, err)
		assert.Equal(t, `SELECT
    "fabric"
FROM
    "benchmark"
WHERE
    "ts" >= 0 AND "ts" < 1000
LIMIT 100000;`, got.Sql)
	})

	t.Run("aggregation only renders no group by", func(t *testing.T) {
		query := TableBuilderQuery{
			TimeRange:    TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
			TableName:    "benchmark",
			TimeColumn:   "ts",
			Aggregations: []Aggregation{{Function: "SUM", Column: ComplexField{Name: "value"}}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		}

		got, err := query.RenderSqlQuery(ctx, client)
		assert.NoError(t, err)
		assert.Equal(t, `SELECT
    SUM("value") AS "SUM(value)"
FROM
    "benchmark"
WHERE
    "ts" >= 0 AND "ts" < 1000
LIMIT 100000;`, got.Sql)
		assert.Equal(t, []pinot.QueryOption{{Name: "timeoutMs", Value: "1"}}, got.QueryOptions)
	})

	t.Run("complex field dimension and aggregation", func(t *testing.T) {
		query := TableBuilderQuery{
			TimeRange:  TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
			TableName:  "benchmark",
			TimeColumn: "ts",
			Dimensions: []ComplexField{{Name: "attrs", Key: "city"}},
			Aggregations: []Aggregation{
				{Function: "MAX", Column: ComplexField{Name: "attrs", Key: "latency"}},
			},
		}

		got, err := query.RenderSqlQuery(ctx, client)
		assert.NoError(t, err)
		assert.Equal(t, `SELECT
    "attrs"['city'] AS 'attrs[city]',
    MAX("attrs"['latency']) AS "MAX(attrs['latency'])"
FROM
    "benchmark"
WHERE
    "ts" >= 0 AND "ts" < 1000
GROUP BY
    "attrs"['city']
LIMIT 100000;`, got.Sql)
	})
}

func TestTableBuilderQuery_RenderSqlWithMacros(t *testing.T) {
	query := TableBuilderQuery{
		TimeRange:  TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
		TableName:  "my_table",
		TimeColumn: "ts",
		Dimensions: []ComplexField{{Name: "fabric"}},
		Aggregations: []Aggregation{
			{Function: "SUM", Column: ComplexField{Name: "value"}},
			{Function: "COUNT"},
		},
		OrderByClauses: []OrderByClause{{ColumnName: "SUM(value)", Direction: "DESC"}},
		Limit:          10,
		QueryOptions:   []QueryOption{{Name: "timeoutMs", Value: "1"}},
	}

	want := `SELECT
    "fabric",
    SUM("value") AS "SUM(value)",
    COUNT(*) AS "COUNT(*)"
FROM
    $__table()
WHERE
    $__timeFilter("ts")
GROUP BY
    "fabric"
ORDER BY
    "SUM(value)" DESC
LIMIT 10;

SET timeoutMs=1;`

	got, err := query.RenderSqlWithMacros()
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestTableBuilderQuery_Execute(t *testing.T) {
	ctx := context.Background()
	client := test_helpers.SetupPinotAndCreateClient(t)

	got := TableBuilderQuery{
		TimeRange: TimeRange{
			From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
		},
		TableName:  "benchmark",
		TimeColumn: "ts",
		Dimensions: []ComplexField{{Name: "fabric"}},
		Aggregations: []Aggregation{
			{Function: "SUM", Column: ComplexField{Name: "value"}},
			{Function: "COUNT"},
		},
		OrderByClauses: []OrderByClause{{ColumnName: "SUM(value)", Direction: "DESC"}},
		Limit:          10,
	}.Execute(client, ctx)

	assert.Equal(t, backend.StatusOK, got.Status)
	assert.NotEmpty(t, got.Frames)

	var fieldNames []string
	for _, field := range got.Frames[0].Fields {
		fieldNames = append(fieldNames, field.Name)
	}
	assert.Equal(t, []string{"fabric", "SUM(value)", "COUNT(*)"}, fieldNames)
}
