package dataquery

import (
	"context"
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestTimeSeriesBuilderParams_Validate(t *testing.T) {
	newParams := func() TimeSeriesBuilderParams {
		return TimeSeriesBuilderParams{
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
		params := newParams()
		assert.NoError(t, params.Validate())
	})
	t.Run("no table name", func(t *testing.T) {
		params := newParams()
		params.TableName = ""
		assert.ErrorContains(t, params.Validate(), "TableName is required")
	})
	t.Run("no time column", func(t *testing.T) {
		params := newParams()
		params.TimeColumn = ""
		assert.ErrorContains(t, params.Validate(), "TimeColumn is required")
	})
	t.Run("no metric column", func(t *testing.T) {
		params := newParams()
		params.MetricColumn.Name = ""
		assert.ErrorContains(t, params.Validate(), "MetricColumn is required")
	})
	t.Run("no aggregation function", func(t *testing.T) {
		params := newParams()
		params.AggregationFunction = ""
		assert.ErrorContains(t, params.Validate(), "AggregationFunction is required")
	})
}

func TestRenderTimeSeriesSql(t *testing.T) {
	ctx := context.Background()

	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		client := test_helpers.SetupPinotAndCreateClient(t)
		schema, err := client.GetTableSchema(ctx, "derivedTimeBuckets")
		require.NoError(t, err)
		tableConfigs, err := client.ListTableConfigs(ctx, "derivedTimeBuckets")
		require.NoError(t, err)

		params := TimeSeriesBuilderParams{
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

		want := `SELECT
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
LIMIT 1000;`

		got, err := RenderTimeSeriesSql(ctx, params, schema, tableConfigs)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=COUNT", func(t *testing.T) {
		schema := pinotlib.TableSchema{
			DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
				Name:     "my_time_column",
				DataType: "LONG",
				Format:   "1:MILLISECONDS:EPOCH",
			}},
		}
		params := TimeSeriesBuilderParams{
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

		want := `SET timeoutMs=1;

SELECT
    "dim",
    DATETIMECONVERT("my_time_column", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:SECONDS') AS "__time",
    COUNT("*") AS "__metric"
FROM
    "my_table"
WHERE
    "my_time_column" >= 0 AND "my_time_column" < 1000
GROUP BY
    "dim",
    "__time"
ORDER BY
    "__time" DESC
LIMIT 100000;`

		got, err := RenderTimeSeriesSql(ctx, params, schema, nil)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		schema := pinotlib.TableSchema{
			DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
				Name:     "my_time_column",
				DataType: "LONG",
				Format:   "1:MILLISECONDS:EPOCH",
			}},
		}
		params := TimeSeriesBuilderParams{
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

		want := `SET timeoutMs=1;

SELECT
    "my_metric" AS "__metric",
    "my_time_column" AS "__time"
FROM
    "my_table"
WHERE
    "my_metric" IS NOT NULL
    AND "my_time_column" >= 0 AND "my_time_column" < 1000
ORDER BY "__time" DESC
LIMIT 1000;`

		got, err := RenderTimeSeriesSql(ctx, params, schema, nil)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})
}

func TestRenderTimeSeriesSqlWithMacros(t *testing.T) {
	ctx := context.Background()
	schema := pinotlib.TableSchema{
		DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
			Name:     "my_time_column",
			DataType: "LONG",
			Format:   "1:MILLISECONDS:EPOCH",
		}},
	}

	t.Run("AggregationFunction=SUM", func(t *testing.T) {
		params := TimeSeriesBuilderParams{
			TimeRange: TimeRange{
				To:   time.Unix(1, 0),
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
    $__timeAlias()
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`

		got, err := RenderTimeSeriesSqlWithMacros(ctx, params, schema, nil)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=COUNT", func(t *testing.T) {
		params := TimeSeriesBuilderParams{
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
    $__timeAlias()
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`

		got, err := RenderTimeSeriesSqlWithMacros(ctx, params, schema, nil)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		params := TimeSeriesBuilderParams{
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

		want := `SET timeoutMs=1;

SELECT
    "my_metric" AS $__metricAlias(),
    "my_time_column" AS $__timeAlias()
FROM
    $__table()
WHERE
    "my_metric" IS NOT NULL
    AND $__timeFilter("my_time_column")
ORDER BY $__timeAlias() DESC
LIMIT 1000;`

		got, err := RenderTimeSeriesSqlWithMacros(ctx, params, schema, nil)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
	})

}

func TestExecuteTimeSeriesBuilderQuery(t *testing.T) {
	t.Run("AggregationFunction=NONE", func(t *testing.T) {
		newDriver := func(testCase DriverTestCase) (Driver, error) {
			return DriverFunc(func(ctx context.Context) backend.DataResponse {
				return ExecuteTimeSeriesBuilderQuery(ctx, testCase.Client, TimeSeriesBuilderParams{
					TimeRange:           testCase.TimeRange,
					IntervalSize:        testCase.IntervalSize,
					TableName:           testCase.TableName,
					TimeColumn:          testCase.TimeColumn,
					MetricColumn:        ComplexField{Name: testCase.TargetColumn},
					AggregationFunction: AggregationFunctionNone,
					Limit:               1_000,
					Legend:              "test-legend",
				})
			}), nil
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
			return DriverFunc(func(ctx context.Context) backend.DataResponse {
				return ExecuteTimeSeriesBuilderQuery(ctx, testCase.Client, TimeSeriesBuilderParams{
					TimeRange:           testCase.TimeRange,
					IntervalSize:        testCase.IntervalSize,
					TableName:           testCase.TableName,
					TimeColumn:          testCase.TimeColumn,
					MetricColumn:        ComplexField{Name: testCase.TargetColumn},
					AggregationFunction: "SUM",
					Limit:               1_000,
					Legend:              "test-legend",
				})
			}), nil
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

func TestFilterExprsFrom(t *testing.T) {
	var filters []DimensionFilter
	assert.NoError(t, json.NewDecoder(strings.NewReader(`[
	  {
		"columnName": "AirlineID",
		"operator": "=",
		"valueExprs": [
		  "19393",
		  "19790"
		]
	  },
	  {
		"columnName": "ArrTime",
		"operator": ">",
		"valueExprs": [
		  "-2147483648"
		]
	  },
	  {
		"columnName": "Cancelled",
		"operator": "=",
		"valueExprs": [
		  "0"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "like",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"operator": "like",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "in",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "not in",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {
		"columnName": "Carrier",
		"operator": "invalid",
		"valueExprs": [
		  "'DL'"
		]
	  },
	  {}
	]`)).Decode(&filters))

	got := FilterExprsFrom(filters)
	assert.EqualValues(t, []string{
		`("AirlineID" = 19393 OR "AirlineID" = 19790)`,
		`("ArrTime" > -2147483648)`,
		`("Cancelled" = 0)`,
		`("Carrier" like 'DL')`,
		`("Carrier" in 'DL')`,
		`("Carrier" not in 'DL')`,
	}, got)
}
