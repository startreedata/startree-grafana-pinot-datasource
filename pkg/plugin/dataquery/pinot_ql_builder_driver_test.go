package dataquery

import (
	"github.com/startree/pinot/pkg/plugin/pinotlib"
	"github.com/stretchr/testify/assert"
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
		assert.Equal(t, params, got.PinotQlBuilderParams)
		assert.NotNil(t, got.TimeExpressionBuilder)
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
