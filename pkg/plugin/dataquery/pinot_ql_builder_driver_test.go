package dataquery

import (
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
					Format:   FormatMillisecondsEpoch,
				}},
			},
			TimeRange: TimeRange{
				To:   time.Unix(0, 0),
				From: time.Unix(0, 0),
			},
			IntervalSize:        100,
			DatabaseName:        "default",
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
	params := PinotQlBuilderParams{
		TableSchema: pinotlib.TableSchema{
			DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
				Name:     "my_time_column",
				DataType: "LONG",
				Format:   FormatMillisecondsEpoch,
			}},
		},
		TimeRange: TimeRange{
			To:   time.Unix(0, 0),
			From: time.Unix(0, 0),
		},
		IntervalSize:        100,
		DatabaseName:        "default",
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
    $__timeGroup("my_time_column") AS $__metricAlias(),
    SUM("my_metric") AS $__timeAlias()
FROM
    $__table()
WHERE
    $__timeFilter("my_time_column")
GROUP BY
    "dim",
    $__timeGroup("my_time_column")
ORDER BY
    $__metricAlias() DESC
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
    "my_time_column" >= 0 AND "my_time_column" <= 1000
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

}
