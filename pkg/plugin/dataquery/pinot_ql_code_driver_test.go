package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type DriverTestParams struct {
	Client    pinotlib.PinotClient
	TimeRange TimeRange
	TableName string
	Schema    pinotlib.TableSchema
}

func TestPinotQlCodeDriver_Execute(t *testing.T) {
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
		params := PinotQlCodeDriverParams{
			PinotClient:       client,
			TableName:         "benchmark",
			TimeColumnAlias:   "time",
			MetricColumnAlias: "value",
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize: 1 * time.Minute,
			TableSchema:  benchmarkTableSchema,
			DisplayType:  DisplayTypeTimeSeries,
			Legend:       "test-legend",
			Code: `SELECT
    $__timeGroup("ts") AS $__timeAlias(),
    SUM("value") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("ts")
GROUP BY
    $__timeGroup("ts")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`,
		}

		driver, err := NewPinotQlCodeDriver(params)
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
		params := PinotQlCodeDriverParams{
			PinotClient:       client,
			TableName:         "partial",
			TimeColumnAlias:   "time",
			MetricColumnAlias: "value",
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 2, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize: 1 * time.Minute,
			TableSchema:  partialTableSchema,
			DisplayType:  DisplayTypeTimeSeries,
			Legend:       "test-legend",
			Code: `SELECT
    $__timeGroup("ts") AS $__timeAlias(),
    SUM("value") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("ts")
GROUP BY
    $__timeGroup("ts")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`,
		}

		driver, err := NewPinotQlCodeDriver(params)
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
		params := PinotQlCodeDriverParams{
			PinotClient:       client,
			TableName:         "benchmark",
			TimeColumnAlias:   "time",
			MetricColumnAlias: "value",
			TimeRange: TimeRange{
				From: time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 11, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize: 1 * time.Minute,
			TableSchema:  benchmarkTableSchema,
			DisplayType:  DisplayTypeTimeSeries,
			Legend:       "test-legend",
			Code: `SELECT
    $__timeGroup("ts") AS $__timeAlias(),
    SUM("value") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("ts")
GROUP BY
    $__timeGroup("ts")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`,
		}

		driver, err := NewPinotQlCodeDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
		assert.NoError(t, got.Error, "DataResponse.Error")
	})

	t.Run("bad query", func(t *testing.T) {
		params := PinotQlCodeDriverParams{
			PinotClient:       client,
			TableName:         "benchmark",
			TimeColumnAlias:   "time",
			MetricColumnAlias: "value",
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize: 1 * time.Minute,
			TableSchema:  benchmarkTableSchema,
			DisplayType:  DisplayTypeTimeSeries,
			Legend:       "test-legend",
			Code: `SELECT
    $__timeGroup("ts") AS $__timeAlias(),
    SUM("not_a_column") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("ts")
GROUP BY
    $__timeGroup("ts")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`,
		}

		driver, err := NewPinotQlCodeDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource, "DataResponse.ErrorSource")
		assertBrokerExceptionErrorWithCodes(t, got.Error, 710)
	})

	t.Run("unreachable", func(t *testing.T) {
		params := PinotQlCodeDriverParams{
			PinotClient:       unreachableClient,
			TableName:         "benchmark",
			TimeColumnAlias:   "time",
			MetricColumnAlias: "value",
			TimeRange: TimeRange{
				From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
			},
			IntervalSize: 1 * time.Minute,
			TableSchema:  benchmarkTableSchema,
			DisplayType:  DisplayTypeTimeSeries,
			Legend:       "test-legend",
			Code: `SELECT
    $__timeGroup("ts") AS $__timeAlias(),
    SUM("value") AS $__metricAlias()
FROM
    $__table()
WHERE
    $__timeFilter("ts")
GROUP BY
    $__timeGroup("ts")
ORDER BY
    $__timeAlias() DESC
LIMIT 100000;`,
		}

		driver, err := NewPinotQlCodeDriver(params)
		require.NoError(t, err)

		got := driver.Execute(context.Background())

		assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
		assert.Empty(t, got.Frames, "DataResponse.Frames")
		assert.Equal(t, backend.ErrorSourcePlugin, got.ErrorSource, "DataResponse.ErrorSource")
		assert.Error(t, got.Error, "DataResponse.Error")
	})
}
