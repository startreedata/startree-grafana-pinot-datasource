package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimeSeriesBuilderQuery_Validate(t *testing.T) {
	newQuery := func() TimeSeriesBuilderQuery {
		return TimeSeriesBuilderQuery{
			TimeRange: TimeRange{
				To:   time.Unix(0, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "my_table",
			TimeColumn:          "my_time_column",
			MetricColumn:        ComplexField{Name: "my_metric"},
			GroupByColumns:      []ComplexField{{Name: "dim"}},
			AggregationFunction: "SUM",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   "=",
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		}
	}

	t.Run("success", func(t *testing.T) {
		assert.NoError(t, newQuery().Validate())
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
	t.Run("no metric column", func(t *testing.T) {
		query := newQuery()
		query.MetricColumn.Name = ""
		assert.ErrorContains(t, query.Validate(), "MetricColumn is required")
	})
	t.Run("no aggregation function", func(t *testing.T) {
		query := newQuery()
		query.AggregationFunction = ""
		assert.ErrorContains(t, query.Validate(), "AggregationFunction is required")
	})
}

func TestTimeSeriesBuilderQuery_RenderSql(t *testing.T) {
	ctx := context.Background()
	client := test_helpers.SetupPinotAndCreateClient(t)

	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		query := TimeSeriesBuilderQuery{
			TimeRange:           TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
			Granularity:         "1:MINUTES",
			TableName:           "derivedTimeBuckets",
			TimeColumn:          "ts",
			MetricColumn:        ComplexField{Name: "value"},
			AggregationFunction: "SUM",
			GroupByColumns:      []ComplexField{{Name: "fabric"}},
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   "=",
			}, {
				ColumnName: "fabric",
				ValueExprs: []string{"'fabric_001'"},
				Operator:   "=",
			}},
			Limit:  1_000,
			Legend: "test-legend",
		}

		want := pinotlib.NewSqlQuery(`SELECT
    "fabric",
    "ts_1m" AS "__time",
    SUM("value") AS "__metric"
FROM
    "derivedTimeBuckets"
WHERE
    "ts" >= 0 AND "ts" < 60000
    AND ("fabric" = 'fabric_001')
GROUP BY
    "fabric",
    "__time"
ORDER BY
    "__time" DESC
LIMIT 1000;`)

		got, _, err := query.RenderSqlQuery(ctx, client)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=COUNT", func(t *testing.T) {
		query := TimeSeriesBuilderQuery{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        ComplexField{Name: "my_metric"},
			GroupByColumns:      []ComplexField{{Name: "dim"}},
			AggregationFunction: "COUNT",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   "=",
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		}

		gotQuery, gotTimeFormat, err := query.RenderSqlQuery(ctx, client)
		assert.NoError(t, err)
		assert.Equal(t, `SELECT
    "dim",
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS') AS "__time",
    COUNT("*") AS "__metric"
FROM
    "benchmark"
WHERE
    "ts" >= 0 AND "ts" < 1000
GROUP BY
    "dim",
    "__time"
ORDER BY
    "__time" DESC
LIMIT 100000;`, gotQuery.Sql)
		assert.Equal(t, []pinotlib.QueryOption{{Name: "timeoutMs", Value: "1"}}, gotQuery.QueryOptions)
		assert.Equal(t, pinotlib.DateTimeFormatMillisecondsEpoch(), gotTimeFormat)
	})

	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		query := TimeSeriesBuilderQuery{
			TimeRange: TimeRange{
				To:   time.Unix(3600, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "hourlyEvents",
			TimeColumn:          "ts",
			MetricColumn:        ComplexField{Name: "my_metric"},
			AggregationFunction: "NONE",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   "=",
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		}

		gotQuery, gotTimeFormat, err := query.RenderSqlQuery(ctx, client)
		assert.NoError(t, err)
		assert.Equal(t, `SELECT
    "my_metric" AS "__metric",
    "ts" AS "__time"
FROM
    "hourlyEvents"
WHERE
    "my_metric" IS NOT NULL
    AND "ts" >= 0 AND "ts" < 1
ORDER BY "__time" DESC
LIMIT 1000;`, gotQuery.Sql)
		assert.Equal(t, []pinotlib.QueryOption{{Name: "timeoutMs", Value: "1"}}, gotQuery.QueryOptions)
		assert.Equal(t, "1:HOURS:EPOCH", gotTimeFormat.LegacyString())
	})
}

func TestTimeSeriesBuilderQuery_RenderSqlWithMacros(t *testing.T) {
	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		query := TimeSeriesBuilderQuery{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        ComplexField{Name: "my_metric"},
			GroupByColumns:      []ComplexField{{Name: "dim"}},
			AggregationFunction: "SUM",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   "=",
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		}

		want := `SELECT
    "dim",
    $__timeGroup("ts", '1:SECONDS') AS $__timeAlias(),
    SUM("my_metric") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("ts", '1:SECONDS')
GROUP BY
    "dim",
    $__timeAlias()
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;

SET timeoutMs=1;`

		got, err := query.RenderSqlWithMacros()
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=COUNT", func(t *testing.T) {
		query := TimeSeriesBuilderQuery{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "my_table",
			TimeColumn:          "my_time_column",
			MetricColumn:        ComplexField{Name: "my_metric"},
			GroupByColumns:      []ComplexField{{Name: "dim"}},
			AggregationFunction: "COUNT",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   "=",
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		}

		want := `SELECT
    "dim",
    $__timeGroup("my_time_column", '1:SECONDS') AS $__timeAlias(),
    COUNT("*") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("my_time_column", '1:SECONDS')
GROUP BY
    "dim",
    $__timeAlias()
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;

SET timeoutMs=1;`

		got, err := query.RenderSqlWithMacros()
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		query := TimeSeriesBuilderQuery{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "my_table",
			TimeColumn:          "my_time_column",
			MetricColumn:        ComplexField{Name: "my_metric"},
			AggregationFunction: "NONE",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   "=",
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		}

		want := `SELECT
    "my_metric" AS $__metricAlias(),
    "my_time_column" AS $__timeAlias()
FROM
    $__table()
WHERE
    "my_metric" IS NOT NULL
    AND $__timeFilter("my_time_column")
ORDER BY $__timeAlias() DESC
LIMIT 1000;

SET timeoutMs=1;`

		got, err := query.RenderSqlWithMacros()
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

}

func TestTimeSeriesBuilderQuery_Execute(t *testing.T) {
	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) ExecutableQuery {
			return TimeSeriesBuilderQuery{
				TimeRange:           testCase.TimeRange,
				IntervalSize:        testCase.IntervalSize,
				TableName:           testCase.TableName,
				TimeColumn:          testCase.TimeColumn,
				MetricColumn:        ComplexField{Name: testCase.TargetColumn},
				AggregationFunction: AggregationFunctionNone,
				Limit:               1_000,
				Legend:              "test-legend",
			}
		}

		// TODO: Add happy path & partial data test

		t.Run("no rows", func(t *testing.T) {
			runSqlQueryNoRows(t, newDriver)
		})
		t.Run("column dne", func(t *testing.T) {
			runSqlQueryColumnDne(t, newDriver)
		})
		t.Run("pinot unreachable", func(t *testing.T) {
			runSqlQueryPinotUnreachable(t, newDriver)
		})
	})

	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) ExecutableQuery {
			return TimeSeriesBuilderQuery{
				TimeRange:           testCase.TimeRange,
				IntervalSize:        testCase.IntervalSize,
				TableName:           testCase.TableName,
				TimeColumn:          testCase.TimeColumn,
				MetricColumn:        ComplexField{Name: testCase.TargetColumn},
				AggregationFunction: "SUM",
				Limit:               1_000,
				Legend:              "test-legend",
			}
		}

		wantFrames := func(times []time.Time, values []float64) data.Frames {
			return data.Frames{data.NewFrame("response",
				data.NewField("value", data.Labels{}, sliceToPointers(values)).SetConfig(&data.FieldConfig{DisplayNameFromDS: "test-legend"}),
				data.NewField("time", nil, times),
			)}
		}

		t.Run("happy path", func(t *testing.T) {
			runSqlQuerySumHappyPath(t, newDriver, wantFrames)
		})
		t.Run("partial data", func(t *testing.T) {
			runSqlQuerySumPartialResults(t, newDriver, wantFrames)
		})
		t.Run("no rows", func(t *testing.T) {
			runSqlQueryNoRows(t, newDriver)
		})
		t.Run("column dne", func(t *testing.T) {
			runSqlQueryColumnDne(t, newDriver)
		})
		t.Run("pinot unreachable", func(t *testing.T) {
			runSqlQueryPinotUnreachable(t, newDriver)
		})
	})
}
