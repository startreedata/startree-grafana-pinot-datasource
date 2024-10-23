package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
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
}

func TestPinotQlBuilderDriver_Execute(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	benchmarkTableSchema, err := client.GetTableSchema(context.Background(), "benchmark")
	require.NoError(t, err)

	partialTableSchema, err := client.GetTableSchema(context.Background(), "partial")
	require.NoError(t, err)

	unreachableClient, err := pinotlib.NewPinotClient(pinotlib.PinotClientProperties{
		ControllerUrl: "not a url",
		BrokerUrl:     "not a url",
	})
	require.NoError(t, err)

	t.Run("happy path", func(t *testing.T) {
		params := PinotQlBuilderParams{
			PinotClient: client,
			TableSchema: benchmarkTableSchema,
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize:        1 * time.Minute,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			AggregationFunction: "SUM",
			Limit:               1_000,
			Legend:              "test-legend",
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
		assert.Equal(t, data.Frames{data.NewFrame("response",
			data.NewField("value", data.Labels{}, sliceToPointers([]float64{
				4.995000894259197e+07,
				4.9950041761314005e+07,
				4.9949916961369045e+07,
				4.994997804782016e+07,
				4.995001567005852e+07,
			})).SetConfig(&data.FieldConfig{DisplayNameFromDS: "test-legend"}),
			data.NewField("time", nil, []time.Time{
				time.Date(2024, 10, 1, 0, 4, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 3, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 2, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 1, 0, 0, time.UTC),
				time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			}),
		)}, got.Frames, "DataResponse.Frames")
		assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
		assert.NoError(t, got.Error, "DataResponse.Error")
	})

	t.Run("partial data", func(t *testing.T) {
		params := PinotQlBuilderParams{
			PinotClient: client,
			TableSchema: partialTableSchema,
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 2, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize:        1 * time.Minute,
			TableName:           "partial",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			AggregationFunction: "SUM",
			Limit:               1_000,
			Legend:              "test-legend",
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
		assert.Equal(t, data.Frames{data.NewFrame("response",
			data.NewField("value", data.Labels{}, sliceToPointers([]float64{
				603.623178859666,
				598.1350673119193,
				600.9085597026183,
				598.2744783346354,
				601.2399258074636,
			})).SetConfig(&data.FieldConfig{DisplayNameFromDS: "test-legend"}),
			data.NewField("time", nil, []time.Time{
				time.Date(2024, 10, 2, 0, 4, 0, 0, time.UTC),
				time.Date(2024, 10, 2, 0, 3, 0, 0, time.UTC),
				time.Date(2024, 10, 2, 0, 2, 0, 0, time.UTC),
				time.Date(2024, 10, 2, 0, 1, 0, 0, time.UTC),
				time.Date(2024, 10, 2, 0, 0, 0, 0, time.UTC),
			}),
		)}, got.Frames, "DataResponse.Frames")
		assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource, "DataResponse.ErrorSource")
		assertBrokerExceptionErrorWithCodes(t, got.Error, 305)
	})

	t.Run("empty table", func(t *testing.T) {
		params := PinotQlBuilderParams{
			PinotClient: client,
			TableSchema: benchmarkTableSchema,
			TimeRange: TimeRange{
				From: time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 11, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize:        1 * time.Minute,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			AggregationFunction: "SUM",
			Limit:               1_000,
			Legend:              "test-legend",
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
		assert.NoError(t, got.Error, "DataResponse.Error")

	})

	t.Run("bad query", func(t *testing.T) {
		params := PinotQlBuilderParams{
			PinotClient: client,
			TableSchema: benchmarkTableSchema,
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize:        1 * time.Minute,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        "not_a_column",
			AggregationFunction: "SUM",
			Limit:               1_000,
			Legend:              "test-legend",
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource, "DataResponse.ErrorSource")
		assertBrokerExceptionErrorWithCodes(t, got.Error, 710)
	})

	t.Run("unreachable", func(t *testing.T) {
		params := PinotQlBuilderParams{
			PinotClient: unreachableClient,
			TableSchema: benchmarkTableSchema,
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize:        1 * time.Minute,
			TableName:           "benchmark",
			TimeColumn:          "ts",
			MetricColumn:        "value",
			AggregationFunction: "SUM",
			Limit:               1_000,
			Legend:              "test-legend",
		}

		driver, err := NewPinotQlBuilderDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Equal(t, backend.ErrorSourcePlugin, got.ErrorSource, "DataResponse.ErrorSource")
		assert.Error(t, got.Error, "DataResponse.Error")
	})
}

func sliceToPointers[V any](arr []V) []*V {
	res := make([]*V, len(arr))
	for i := range arr {
		res[i] = &arr[i]
	}
	return res
}

func assertBrokerExceptionErrorWithCodes(t *testing.T, err error, codes ...int) {
	var brokerError *pinotlib.BrokerExceptionError
	if assert.ErrorAs(t, err, &brokerError) {
		assert.NotEmpty(t, brokerError.Exceptions)
		var exceptionCodes []int
		for _, exception := range brokerError.Exceptions {
			exceptionCodes = append(exceptionCodes, exception.ErrorCode)
		}
		sort.Ints(exceptionCodes)
		sort.Ints(codes)
		assert.Equal(t, codes, exceptionCodes, "exception codes")
	}
}
