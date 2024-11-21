package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRenderPinotQlBuilderSqlWithMacros(t *testing.T) {
	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		want := `SET timeoutMs=1;

SELECT
    "dim",
    $__timeGroup("my_time_column", '1:SECONDS') AS $__timeAlias(),
    SUM("my_metric") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("my_time_column", '1:SECONDS')
GROUP BY
    "dim",
    $__timeGroup("my_time_column", '1:SECONDS')
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`

		got, err := RenderPinotQlBuilderSqlWithMacros(context.Background(), PinotQlBuilderParams{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "my_table",
			TimeColumn:          "my_time_column",
			MetricColumn:        "my_metric",
			GroupByColumns:      []string{"dim"},
			AggregationFunction: "SUM",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   FilterOpEquals,
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=COUNT", func(t *testing.T) {
		want := `SET timeoutMs=1;

SELECT
    "dim",
    $__timeGroup("my_time_column", '1:SECONDS') AS $__timeAlias(),
    COUNT("*") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("my_time_column", '1:SECONDS')
GROUP BY
    "dim",
    $__timeGroup("my_time_column", '1:SECONDS')
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`

		got, err := RenderPinotQlBuilderSqlWithMacros(context.Background(), PinotQlBuilderParams{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "my_table",
			TimeColumn:          "my_time_column",
			MetricColumn:        "my_metric",
			GroupByColumns:      []string{"dim"},
			AggregationFunction: "COUNT",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   FilterOpEquals,
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		want := `SET timeoutMs=1;

SELECT
    "my_metric" AS $__metricAlias(),
    "my_time_column" AS $__timeAlias()
FROM
    $__table()
WHERE
    "my_metric" IS NOT NULL
    AND $__timeFilter("my_time_column", '1:SECONDS')
ORDER BY $__timeAlias() DESC
LIMIT 1000;`

		got, err := RenderPinotQlBuilderSqlWithMacros(context.Background(), PinotQlBuilderParams{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "my_table",
			TimeColumn:          "my_time_column",
			MetricColumn:        "my_metric",
			AggregationFunction: "NONE",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   FilterOpEquals,
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})
}

func TestRenderPinotQlBuilderSql(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		want := `SET timeoutMs=1;

SELECT
    "fabric",
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS') AS "time",
    SUM("value") AS "metric"
FROM
    "benchmark"
WHERE
    "ts" >= 0 AND "ts" < 1000
GROUP BY
    "fabric",
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS')
ORDER BY
    "time" DESC
LIMIT 100000;`

		got, err := RenderPinotQlBuilderSql(context.Background(), client, PinotQlBuilderParams{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			GroupByColumns:      []string{"fabric"},
			AggregationFunction: "SUM",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   FilterOpEquals,
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=COUNT", func(t *testing.T) {
		want := `SET timeoutMs=1;

SELECT
    "fabric",
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS') AS "time",
    COUNT("*") AS "metric"
FROM
    "benchmark"
WHERE
    "ts" >= 0 AND "ts" < 1000
GROUP BY
    "fabric",
    DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS')
ORDER BY
    "time" DESC
LIMIT 100000;`

		got, err := RenderPinotQlBuilderSql(context.Background(), client, PinotQlBuilderParams{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			GroupByColumns:      []string{"fabric"},
			AggregationFunction: "COUNT",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   FilterOpEquals,
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		want := `SET timeoutMs=1;

SELECT
    "value" AS "metric",
    "ts" AS "time"
FROM
    "benchmark"
WHERE
    "value" IS NOT NULL
    AND "ts" >= 0 AND "ts" < 1000
ORDER BY "time" DESC
LIMIT 1000;`

		got, err := RenderPinotQlBuilderSql(context.Background(), client, PinotQlBuilderParams{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			AggregationFunction: "NONE",
			Limit:               -1,
			Granularity:         "1:SECONDS",
			MaxDataPoints:       1000,
			DimensionFilters: []DimensionFilter{{
				ColumnName: "",
				ValueExprs: []string{},
				Operator:   FilterOpEquals,
			}},
			QueryOptions: []QueryOption{{Name: "timeoutMs", Value: "1"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("DerivedTimeCols", func(t *testing.T) {
		want := `SELECT
    "ts_1m" AS "time",
    SUM("value") AS "metric"
FROM
    "derivedTimeBuckets"
WHERE
    "ts" >= 0 AND "ts" < 60000
GROUP BY
    "ts_1m"
ORDER BY
    "time" DESC
LIMIT 1000;`

		got, err := RenderPinotQlBuilderSql(context.Background(), client, PinotQlBuilderParams{
			TimeRange:           TimeRange{To: time.Unix(1, 0), From: time.Unix(0, 0)},
			Granularity:         "1:MINUTES",
			TableName:           "derivedTimeBuckets",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			AggregationFunction: "SUM",
			Limit:               1_000,
			Legend:              "test-legend",
		})
		assert.NoError(t, err)
		assert.Equal(t, want, got)

	})

}

func TestPinotQlBuilderDriver_Execute(t *testing.T) {
	t.Run("AggregationFunction="+AggregationFunctionNone, func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			return NewPinotQlBuilderDriver(testCase.Client, PinotQlBuilderParams{
				TimeRange:           testCase.TimeRange,
				IntervalSize:        testCase.IntervalSize,
				TableName:           testCase.TableName,
				TimeColumn:          testCase.TimeColumn,
				MetricColumn:        testCase.TargetColumn,
				AggregationFunction: AggregationFunctionNone,
				Limit:               1_000,
				Legend:              "test-legend",
			})
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
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			return NewPinotQlBuilderDriver(testCase.Client, PinotQlBuilderParams{
				TimeRange:           testCase.TimeRange,
				IntervalSize:        testCase.IntervalSize,
				TableName:           testCase.TableName,
				TimeColumn:          testCase.TimeColumn,
				MetricColumn:        testCase.TargetColumn,
				AggregationFunction: "SUM",
				Limit:               1_000,
				Legend:              "test-legend",
			})
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
