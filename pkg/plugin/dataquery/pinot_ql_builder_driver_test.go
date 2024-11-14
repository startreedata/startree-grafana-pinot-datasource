package dataquery

import (
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewPinotQlBuilderDriver(t *testing.T) {
	newParams := func() PinotQlBuilderParams {
		return PinotQlBuilderParams{
			TableSchema: pinotlib.TableSchema{
				DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
					Name:     "my_time_column",
					DataType: "LONG",
					Format:   pinotlib.FormatMillisecondsEpoch,
				}},
			},
			TimeRange: TimeRange{
				To:   time.Unix(0, 0),
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
		}
	}

	t.Run("success", func(t *testing.T) {
		params := newParams()
		got, gotErr := NewPinotQlBuilderDriver(newParams())
		assert.NoError(t, gotErr)
		assert.Equal(t, params, got.params)
		assert.NotNil(t, got.timeExprBuilder)
		assert.Equal(t, DefaultMetricColumnAlias, got.MetricColumnAlias)
		assert.Equal(t, DefaultTimeColumnAlias, got.TimeColumnAlias)
	})

	t.Run("no table name", func(t *testing.T) {
		params := newParams()
		params.TableName = ""
		got, gotErr := NewPinotQlBuilderDriver(params)
		assert.Nil(t, got)
		assert.Error(t, gotErr)
	})
	t.Run("no time column", func(t *testing.T) {
		params := newParams()
		params.TimeColumn = ""
		got, gotErr := NewPinotQlBuilderDriver(params)
		assert.Nil(t, got)
		assert.Error(t, gotErr)
	})
	t.Run("no metric column", func(t *testing.T) {
		params := newParams()
		params.MetricColumn = ""
		got, gotErr := NewPinotQlBuilderDriver(params)
		assert.Nil(t, got)
		assert.Error(t, gotErr)
	})
	t.Run("no aggregation function", func(t *testing.T) {
		params := newParams()
		params.AggregationFunction = ""
		got, gotErr := NewPinotQlBuilderDriver(params)
		assert.Nil(t, got)
		assert.Error(t, gotErr)
	})
}

func TestPinotQlBuilderDriver_RenderPinotSql(t *testing.T) {
	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		params := PinotQlBuilderParams{
			TableSchema: pinotlib.TableSchema{
				DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
					Name:     "my_time_column",
					DataType: "LONG",
					Format:   pinotlib.FormatMillisecondsEpoch,
				}},
			},
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
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		t.Run("expandMacros=false", func(t *testing.T) {
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

			got, err := driver.RenderPinotSql(false)
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})

		t.Run("expandMacros=true", func(t *testing.T) {
			want := `SET timeoutMs=1;

SELECT
    "dim",
    DATETIMECONVERT("my_time_column", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS') AS "time",
    SUM("my_metric") AS "metric"
FROM
    "my_table"
WHERE
    "my_time_column" >= 0 AND "my_time_column" < 1000
GROUP BY
    "dim",
    DATETIMECONVERT("my_time_column", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS')
ORDER BY
    "time" DESC
LIMIT 100000;`

			got, err := driver.RenderPinotSql(true)
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})
	})

	t.Run("AggregationFunction="+AggregationFunctionCount, func(t *testing.T) {
		params := PinotQlBuilderParams{
			TableSchema: pinotlib.TableSchema{
				DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
					Name:     "my_time_column",
					DataType: "LONG",
					Format:   pinotlib.FormatMillisecondsEpoch,
				}},
			},
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
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		t.Run("expandMacros=false", func(t *testing.T) {
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

			got, err := driver.RenderPinotSql(false)
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})

		t.Run("expandMacros=true", func(t *testing.T) {
			want := `SET timeoutMs=1;

SELECT
    "dim",
    DATETIMECONVERT("my_time_column", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS') AS "time",
    COUNT("*") AS "metric"
FROM
    "my_table"
WHERE
    "my_time_column" >= 0 AND "my_time_column" < 1000
GROUP BY
    "dim",
    DATETIMECONVERT("my_time_column", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS')
ORDER BY
    "time" DESC
LIMIT 100000;`

			got, err := driver.RenderPinotSql(true)
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})
	})

	t.Run("AggregationFunction="+AggregationFunctionNone, func(t *testing.T) {
		params := PinotQlBuilderParams{
			TableSchema: pinotlib.TableSchema{
				DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
					Name:     "my_time_column",
					DataType: "LONG",
					Format:   pinotlib.FormatMillisecondsEpoch,
				}},
			},
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
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		t.Run("expandMacros=false", func(t *testing.T) {
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

			got, err := driver.RenderPinotSql(false)
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})

		t.Run("expandMacros=true", func(t *testing.T) {
			want := `SET timeoutMs=1;

SELECT
    "my_metric" AS "metric",
    "my_time_column" AS "time"
FROM
    "my_table"
WHERE
    "my_metric" IS NOT NULL
    AND "my_time_column" >= 0 AND "my_time_column" < 1000
ORDER BY "time" DESC
LIMIT 1000;`

			got, err := driver.RenderPinotSql(true)
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})
	})

}

func TestPinotQlBuilderDriver_Execute(t *testing.T) {
	t.Run("AggregationFunction="+AggregationFunctionNone, func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			return NewPinotQlBuilderDriver(PinotQlBuilderParams{
				PinotClient:         testCase.Client,
				TableSchema:         testCase.TableSchema,
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
			return NewPinotQlBuilderDriver(PinotQlBuilderParams{
				PinotClient:         testCase.Client,
				TableSchema:         testCase.TableSchema,
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
